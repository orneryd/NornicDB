package search

import (
	"context"
	"errors"

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

// applyWrite is the owner's live-mutation boundary (F-I86 B1). A write never
// runs or waits for a NODE population:
//   - While one is pending on a restorable (persisted) service, the mutation
//     is recorded and not applied (apply=false): the population may still
//     replace the sources with the saved image, so it folds the record in
//     before admitting NODE (completeNodePopulation).
//   - While one is pending on an in-memory service, nothing can replace the
//     live source; the node is applied at once and also recorded, so a batch
//     the population read earlier cannot hide it.
//   - With none pending, a restorable service whose initial population was
//     never admitted records the mutation and starts admission as a
//     registered background worker.
//
// With apply=true the caller owns indexMu and must call release; with
// apply=false there is nothing to release. Private node/edge batch writers
// never enter this public boundary.
func (s *Service) applyWrite(dimensions int, m liveMutation) (release func(), apply bool, err error) {
	s.indexMu.Lock()
	if err := s.lifecycleErr(); err != nil {
		s.indexMu.Unlock()
		return nil, false, err
	}
	restorable := s.nodePopulationRestorable()
	if job := s.pendingNodeAdmission(); job != nil {
		if restorable {
			s.recordPopulationWriteLocked(m)
			s.indexMu.Unlock()
			s.ensureAdmissionWorker(job)
			return nil, false, nil
		}
		if m.nodeID != "" {
			s.recordPopulationWriteLocked(m)
		}
	} else if restorable && !s.nodeSourcesCompleteLocked() {
		s.baseBuildControlMu.Lock()
		paused := s.baseBuildPaused > 0
		s.baseBuildControlMu.Unlock()
		if paused {
			s.indexMu.Unlock()
			return nil, false, ErrSearchIndexBuilding
		}
		job := s.claimNodeAdmission()
		s.recordPopulationWriteLocked(m)
		s.indexMu.Unlock()
		s.ensureAdmissionWorker(job)
		return nil, false, nil
	}
	resume := func() {}
	if s.needsVectorDimensionsLocked(dimensions) {
		// Joining construction must precede indexMu; the replacement rechecks
		// the dimensions after mutation ownership is reacquired. Retirement
		// cancels the build, so this join is prompt.
		s.indexMu.Unlock()
		resume = s.pauseHNSWBuild()
		s.indexMu.Lock()
		if err := s.lifecycleErr(); err != nil {
			s.indexMu.Unlock()
			resume()
			return nil, false, err
		}
		s.replaceEmptyVectorDimensionsLocked(dimensions)
	}
	if err := s.materializeNodeFulltextLocked(); err != nil {
		s.indexMu.Unlock()
		resume()
		return nil, false, err
	}
	return func() { s.indexMu.Unlock(); resume() }, true, nil
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
		return
	}
	ctx := s.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.waitOrRunNodeAdmission(ctx, s.claimNodeAdmission()); err != nil {
		s.logPrintf("📇 Persist: node population for live writes did not complete: %v", err)
	}
}
