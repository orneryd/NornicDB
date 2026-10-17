package search

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// liveMutation is one public node or edge mutation. A nil node or edge with
// its ID set is a removal.
type liveMutation struct {
	nodeID string
	node   *storage.Node
	edgeID string
	edge   *storage.Edge
}

// applyWrite is the owner's live-mutation boundary (F-I86 B1, decision B).
// A write is applied to the live indexes on return and never runs or waits
// for a population:
//   - While a population is pending, the write is also recorded (latest op
//     per node and edge; a removal is an op). The population re-applies the
//     record in the same locked step as each source replacement and again
//     before it admits NODE, so the write stays visible and no older batch
//     or restored image hides it (completeNodePopulation, population start).
//   - On a persisted service whose sources are not yet admitted, the write is
//     provisional: it stays in memory and performs no restore step (file-store
//     creation, saved-metadata restore); the population withdraws and
//     re-applies it around the restore. If no population is running, the write
//     starts one as a registered background worker.
//
// load, when set, supplies the mutation under the boundary instead of m: the
// node's current stored state (nil: removed). Publishers that may run out of
// order use it, so a later update or removal stays authoritative.
//
// On success the caller owns indexMu and must call release.
func (s *Service) applyWrite(dimensions int, m liveMutation, load func() (liveMutation, int, error)) (mutation liveMutation, release func(), err error) {
	resume := func() {}
	s.indexMu.Lock()
	unlock := func() { s.indexMu.Unlock(); resume() }
	if err := s.lifecycleErr(); err != nil {
		unlock()
		return m, nil, err
	}
	if err := s.awaitBudgetAdmissionLocked(); err != nil {
		unlock()
		return m, nil, err
	}
	for loaded := false; ; {
		if load != nil && !loaded {
			if m, dimensions, err = load(); err != nil {
				unlock()
				return m, nil, err
			}
			loaded = true
		}
		if !s.needsVectorDimensionsLocked(dimensions) {
			break
		}
		// Joining construction must precede indexMu. Retirement cancels the
		// build, so this join is prompt; a loader reads storage again after.
		s.indexMu.Unlock()
		resume = s.pauseHNSWBuild()
		s.indexMu.Lock()
		if err := s.lifecycleErr(); err != nil {
			unlock()
			return m, nil, err
		}
		if load != nil {
			if m, dimensions, err = load(); err != nil {
				unlock()
				return m, nil, err
			}
		}
		s.replaceEmptyVectorDimensionsLocked(dimensions)
		break
	}
	restorable := s.nodePopulationRestorable()
	var start *nodePopulationAdmission
	if job := s.pendingNodeAdmission(); job != nil {
		s.recordPopulationWriteLocked(m)
		if restorable {
			start = job
		}
	} else if restorable && !s.nodeSourcesCompleteLocked() {
		start = s.claimNodeAdmission()
		s.recordPopulationWriteLocked(m)
	}
	if err := s.materializeNodeFulltextLocked(); err != nil {
		unlock()
		return m, nil, err
	}
	s.provisionalWrite = restorable && !s.nodeSourcesCompleteLocked()
	return m, func() {
		s.provisionalWrite = false
		unlock()
		if start != nil {
			s.ensureAdmissionWorker(start)
		}
	}, nil
}

// ensureAdmissionWorker runs job in the background unless a worker already
// owns it. The worker is registered with the owner, so Clear and Close
// cancel and join it. A worker refused during teardown leaves the job for
// the next write, build or persist.
func (s *Service) ensureAdmissionWorker(job *nodePopulationAdmission) {
	if !job.worker.CompareAndSwap(false, true) {
		return
	}
	ctx, done, ok := s.runWorker(context.Background(), workerAdmission)
	if !ok {
		job.worker.Store(false)
		return
	}
	go func() {
		defer done()
		defer job.worker.Store(false)
		_ = s.waitOrRunNodeAdmission(ctx, job)
	}()
}

// pendingNodeAdmission returns the NODE population that has not finished, or
// nil.
func (s *Service) pendingNodeAdmission() *nodePopulationAdmission {
	s.baseBuildControlMu.Lock()
	job := s.nodeAdmission
	s.baseBuildControlMu.Unlock()
	if job == nil {
		return nil
	}
	select {
	case <-job.done:
		return nil
	default:
		return job
	}
}

// nodePopulationRestorable reports whether a population may install saved
// node sources over the live ones (the same paths runBasePopulationOperation
// uses).
func (s *Service) nodePopulationRestorable() bool {
	if !s.persistEnabled.Load() {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.fulltextIndexPath != "" || s.vectorIndexPath != ""
}

// recordPopulationWriteLocked keeps the latest state of each mutated node and
// edge for the population to fold in. Caller holds indexMu.
func (s *Service) recordPopulationWriteLocked(m liveMutation) {
	if m.nodeID != "" {
		if s.nodePopulationWrites == nil {
			s.nodePopulationWrites = make(map[string]*storage.Node)
		}
		s.nodePopulationWrites[m.nodeID] = storage.CopyNode(m.node)
	}
	if m.edgeID != "" {
		if s.edgePopulationWrites == nil {
			s.edgePopulationWrites = make(map[string]*storage.Edge)
		}
		s.edgePopulationWrites[m.edgeID] = storage.CopyEdge(m.edge)
	}
}

// populationWriteDimensions returns the vector dimensions of the first
// recorded node, which the population may have to adopt before replaying it.
func (s *Service) populationWriteDimensions() int {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	for _, node := range s.nodePopulationWrites {
		if d := firstVectorDimensions(node); d > 0 {
			return d
		}
	}
	return 0
}

// replayPopulationWritesLocked applies every recorded mutation over the
// population's batches, in the same order as the public writers. A write the
// restored budget refuses is dropped, as the live write would have been.
// Caller holds indexMu; the records stay until the job finishes successfully.
func (s *Service) replayPopulationWritesLocked(ctx context.Context) error {
	skipFulltext := !s.bm25Enabled.Load()
	for id, node := range s.nodePopulationWrites {
		if err := ctx.Err(); err != nil {
			return err
		}
		if node == nil {
			s.removeNodeLocked(id)
			continue
		}
		if err := s.indexNodeLocked(node, skipFulltext); err != nil {
			if !errors.Is(err, ErrIndexMemoryBudgetExceeded) {
				return err
			}
			s.logPrintf("📇 BuildIndexes: live write for node %s exceeds the index memory budget; not indexed", id)
		}
	}
	for id, edge := range s.edgePopulationWrites {
		if err := ctx.Err(); err != nil {
			return err
		}
		if edge == nil {
			s.RemoveRelationship(storage.EdgeID(id))
			s.removeEdgeLocked(id)
			continue
		}
		s.IndexRelationship(edge)
		if err := s.indexEdgeLocked(edge); err != nil {
			if !errors.Is(err, ErrIndexMemoryBudgetExceeded) {
				return err
			}
			s.logPrintf("📇 BuildIndexes: live write for relationship %s exceeds the index memory budget; not indexed", id)
		}
	}
	return nil
}

func (s *Service) hasPopulationWritesLocked() bool {
	return len(s.nodePopulationWrites) > 0 || len(s.edgePopulationWrites) > 0
}

// admitNodePopulationForPersist completes NODE population before a persist
// when live writes are waiting on it. Persisting runs off the write path (a
// debounced timer or an owner's shutdown flush), so it may join or run the
// population; saving the incomplete live state would replace a complete image.
func (s *Service) admitNodePopulationForPersist() {
	s.indexMu.RLock()
	needed := s.hasPopulationWritesLocked() && !s.nodeSourcesCompleteLocked()
	s.indexMu.RUnlock()
	if !needed {
		s.joinWriteStartedPopulation()
		return
	}
	ctx := s.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.waitOrRunNodeAdmission(ctx, s.claimNodeAdmission()); err != nil {
		s.logPrintf("📇 Persist: node population for live writes did not complete: %v", err)
	}
	s.joinWriteStartedPopulation()
}

// joinWriteStartedPopulation waits for a population a live write started to
// return completely, not only to admit NODE: until it returns it still marks
// the build in progress, which defers the BM25 save. Explicit builds persist
// their own result and are not joined, so a shutdown flush never waits out a
// full build.
func (s *Service) joinWriteStartedPopulation() {
	if s.writeStartedPopulation.Load() {
		s.buildMu.Lock()
		s.buildMu.Unlock()
	}
}

// withdrawPopulationWritesLocked removes the recorded writes from the live
// indexes before a population replaces the sources, so provisional live
// entries cannot make a saved image or a fresh store look out of date. The
// caller re-applies the record (replayPopulationWritesLocked) before it
// releases indexMu, so the writes never leave search.
func (s *Service) withdrawPopulationWritesLocked() {
	for id := range s.nodePopulationWrites {
		s.removeNodeLocked(id)
	}
	for id := range s.edgePopulationWrites {
		s.RemoveRelationship(storage.EdgeID(id))
		s.removeEdgeLocked(id)
	}
}

// isRecordedWriteLocked reports whether the record holds a newer state of
// node id than a population batch read from storage. Caller holds indexMu.
func (s *Service) isRecordedWriteLocked(id string) bool {
	_, ok := s.nodePopulationWrites[id]
	return ok
}

// clearPopulationWritesLocked forgets the record once a population has
// admitted NODE with every write applied. Caller holds indexMu.
func (s *Service) clearPopulationWritesLocked() {
	clear(s.nodePopulationWrites)
	clear(s.edgePopulationWrites)
}

// indexBudgeted reports whether the service enforces any index memory budget.
func (s *Service) indexBudgeted() bool {
	return s.bm25MemoryMaxBytes > 0 || s.vectorMemoryMaxBytes > 0 || s.metadataMemoryMaxBytes > 0
}

// awaitBudgetAdmissionLocked is the one exception to "a write never waits for
// a population" (F-I86, orchestrator decision on budgets). On a service with
// an index memory budget, whether a write fits is only known once its
// population has admitted NODE with the restored usage, and callers act on
// ErrIndexMemoryBudgetExceeded, so such a write waits for NODE admission (not
// for ANN) and returns that population's error if it failed, as before.
// Unbudgeted services never wait. Caller holds indexMu; it is released while
// waiting and held again on a nil return.
func (s *Service) awaitBudgetAdmissionLocked() error {
	for s.indexBudgeted() {
		job := s.pendingNodeAdmission()
		if job == nil {
			if !s.nodePopulationRestorable() || s.nodeSourcesCompleteLocked() {
				return nil
			}
			job = s.claimNodeAdmission()
		}
		s.indexMu.Unlock()
		s.ensureAdmissionWorker(job)
		lifetime := s.lifecycleCtx
		if lifetime == nil {
			lifetime = context.Background()
		}
		select {
		case <-job.done:
		case <-lifetime.Done():
		}
		s.indexMu.Lock()
		if err := s.lifecycleErr(); err != nil {
			return err
		}
		select {
		case <-job.done:
			// A population refused or cancelled before it replaced anything
			// leaves complete sources; only one that left them incomplete fails
			// the write.
			if job.err != nil && !s.nodeSourcesCompleteLocked() {
				return job.err
			}
		default:
		}
	}
	return nil
}

// Test seams for the population's restore step: populationStageHook runs
// where the saved sources are read from disk, populationRestoreHook right
// after the recorded writes are withdrawn. Nil outside tests.
var populationStageHook, populationRestoreHook atomic.Pointer[func(*Service)]

func runPopulationHook(hook *atomic.Pointer[func(*Service)], s *Service) {
	if fn := hook.Load(); fn != nil {
		(*fn)(s)
	}
}
