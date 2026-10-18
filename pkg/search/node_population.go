package search

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// baseOperationMutex keeps the existing base-operation exclusion, with a
// selectable acquisition for mutations waiting only for its NODE phase.
type baseOperationMutex struct {
	once sync.Once
	free chan struct{}
}

func (m *baseOperationMutex) token() chan struct{} {
	m.once.Do(func() { m.free = make(chan struct{}, 1); m.free <- struct{}{} })
	return m.free
}

func (m *baseOperationMutex) Lock()   { <-m.token() }
func (m *baseOperationMutex) Unlock() { m.token() <- struct{}{} }

// nodeSourceRecord is the one initial-population and source-authority record.
// indexMu protects it; identity, dirty state and positive counts do not establish
// completeness. A parked fulltext checkpoint remains the logical text source.
type nodeSourceRecord struct {
	textComplete            bool
	vectorsComplete         bool
	memoryAuthoritative     bool
	fulltextCheckpoint      string
	fulltextCheckpointCount int
	// unpopulatedLiveWrites: node writes applied while the sources were
	// incomplete and no population ran (nodeWriteUnpopulated). Persistence then
	// populates first, and the population never loads the BM25 checkpoint.
	unpopulatedLiveWrites bool
}

type nodeSourceChange uint8

const (
	nodeTextRestored nodeSourceChange = iota
	nodeVectorsRestored
	nodePopulationAdmitted
	nodePopulationFailed
	nodeVectorMemoryMoved
	nodeVectorCleared
	nodeVectorReset
	nodeVectorFileMoved
	nodeFulltextReset
	// nodeWriteUnpopulated: a node write applied while the sources are
	// incomplete. Unless a saved vector image is already open (the write then
	// applied to it), the saved node images are retired now: the resident
	// indexes become the authority, so no later admission, live or population,
	// restores an image over this write, and the population completes them
	// from storage.
	nodeWriteUnpopulated
)

// changeNodeSourcesLocked is the sole completion/authority writer. The
// replacing operation supplies its actual meaning; no caller infers it from
// counts, a dirty backend, a whole-service ready flag or pointer identity.
func (s *Service) changeNodeSourcesLocked(change nodeSourceChange) {
	switch change {
	case nodeTextRestored:
		s.nodeSources.textComplete = true
	case nodeVectorsRestored:
		s.nodeSources.vectorsComplete = true
	case nodePopulationAdmitted:
		s.nodeSources.textComplete, s.nodeSources.vectorsComplete = true, true
		s.nodeSources.unpopulatedLiveWrites = false
		if s.vectorFileStore == nil {
			s.nodeSources.memoryAuthoritative = true
		}
	case nodePopulationFailed:
		s.nodeSources.textComplete, s.nodeSources.vectorsComplete = false, false
	case nodeVectorMemoryMoved:
		s.nodeSources.memoryAuthoritative = true
	case nodeVectorCleared:
		s.nodeSources.vectorsComplete, s.nodeSources.memoryAuthoritative = true, true
	case nodeVectorReset:
		s.nodeSources.vectorsComplete, s.nodeSources.memoryAuthoritative = false, true
	case nodeVectorFileMoved:
		s.nodeSources.memoryAuthoritative = false
	case nodeFulltextReset:
		s.nodeSources.textComplete = false
	case nodeWriteUnpopulated:
		s.nodeSources.unpopulatedLiveWrites = true
		if s.vectorFileStore == nil {
			s.nodeSources.memoryAuthoritative = true
		}
	}
}

func (s *Service) setNodeFulltextCheckpointLocked(path string, count int) {
	s.nodeSources.fulltextCheckpoint = path
	s.nodeSources.fulltextCheckpointCount = count
}

// nodePopulationAdmission is the NODE phase of the existing base operation,
// not another worker or ANN job. baseBuildControlMu protects publication; err
// is immutable after done closes. A caller waiting for NODE never waits for ANN.
// baseBuildControlMu protects the queued live writes and indexMu the IDs
// they wrote (node_population_writes.go); queued is set while any write is
// queued.
type nodePopulationAdmission struct {
	done         chan struct{}
	err          error
	deferred     []deferredIndexWrite
	queued       atomic.Bool
	writtenNodes map[string]struct{}
	writtenEdges map[string]struct{}
	// pendingOps counts queued operations that change a setting an entry
	// decision reads (the dimension mode, the embedding space, the index
	// flags) and are not applied yet; populated is set once its batches and
	// stream are complete; bytes is the payload the queued operations retain.
	pendingOps int
	populated  bool
	bytes      int
	// flags, when flagsQueued, are the index flags the queued SetIndexFlags
	// calls leave (bm25, vector).
	flags       [2]bool
	flagsQueued bool
}

func (s *Service) nodeSourcesCompleteLocked() bool {
	return (!s.bm25Enabled.Load() || s.nodeSources.textComplete) &&
		(!s.vectorEnabled.Load() || s.nodeSources.vectorsComplete)
}

func (s *Service) claimNodeAdmission() *nodePopulationAdmission {
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	if job := s.nodeAdmission; job != nil {
		select {
		case <-job.done:
		default:
			return job
		}
	}
	job := &nodePopulationAdmission{done: make(chan struct{})}
	s.nodeAdmission = job
	s.populationRunning.Store(true)
	return job
}

func (s *Service) finishNodeAdmission(job *nodePopulationAdmission, err error) {
	s.publishNodeAdmission(job, err, "")
}

// publishNodeAdmission publishes NODE. The queued live writes are applied
// first, outside the write lock; the last of them, and closing the job, share
// one indexMu section, so every write is applied either here or directly
// after it. A failed population marks its sources incomplete before any
// queued write is applied, as a write after the failure would find them.
// parkPath, set by an explicit build that has just saved BM25 there (caller
// holds persistRunMu), parks BM25 through the warmup only when no queued
// write changed it since the save.
func (s *Service) publishNodeAdmission(job *nodePopulationAdmission, err error, parkPath string) {
	if err != nil {
		s.indexMu.Lock()
		if !nodeAdmissionClosed(job) {
			s.changeNodeSourcesLocked(nodePopulationFailed)
		}
		s.indexMu.Unlock()
	}
	// Under a steady write stream each pass applies what arrived during the
	// previous one; after a few the rest is applied under the lock.
	for pass := 0; pass < 3 && job.queued.Load(); pass++ {
		s.applyDeferredIndexWrites(job)
	}
	parked := false
	s.indexMu.Lock()
	s.baseBuildControlMu.Lock()
	if !nodeAdmissionClosed(job) {
		job.pendingOps -= s.applyDeferredIndexWritesLocked(job, job.takeDeferredLocked(true))
		if err == nil && parkPath != "" {
			parked = s.parkFulltextLocked(parkPath)
		}
		job.err = err
		close(job.done)
		if s.nodeAdmission == job {
			s.populationRunning.Store(false)
		}
	}
	s.baseBuildControlMu.Unlock()
	s.indexMu.Unlock()
	if parked {
		s.logPrintf("📇 BuildIndexes: cleared BM25 in-memory state (will reload after warmup)")
	}
}

func nodeAdmissionClosed(job *nodePopulationAdmission) bool {
	select {
	case <-job.done:
		return true
	default:
		return false
	}
}

// awaitNodeAdmission waits for the running population job and returns its
// outcome. A live write never starts or runs a population.
func (s *Service) awaitNodeAdmission(job *nodePopulationAdmission) error {
	ctx := s.lifecycleCtx
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-job.done:
		return job.err
	}
}

// lockNodeMutation admits a live write. While a build populates the node
// sources the write is queued on it (queueOnPopulation) and queued is true;
// with an index memory budget it waits for the population instead. Otherwise
// the write applies at once, also before any population (the build that
// follows rebuilds or restores around it). A write never starts or runs a
// population. Unless queued, on success the caller owns indexMu and must call
// the returned release.
// Private node/edge batch writers never enter this public boundary. nodeWrite
// marks a node write: only those change the node sources, so only they make
// the saved node images stale before a population.
func (s *Service) lockNodeMutation(dimensions int, nodeWrite bool, write deferredIndexWrite) (release func(), queued bool, err error) {
	for {
		queued, mustWait, err := s.queueOnPopulation(write)
		if queued || err != nil {
			return nil, queued, err
		}
		s.indexMu.Lock()
		if s.lifecycleCtx != nil {
			if err := s.lifecycleCtx.Err(); err != nil {
				s.indexMu.Unlock()
				return nil, false, err
			}
		}
		s.baseBuildControlMu.Lock()
		job, paused := s.nodeAdmission, s.baseBuildPaused > 0
		pending := false
		if job != nil {
			select {
			case <-job.done:
			default:
				pending = true
			}
		}
		s.baseBuildControlMu.Unlock()
		if !pending {
			if err := s.admitNodeWriteLocked(nodeWrite); err != nil {
				s.indexMu.Unlock()
				return nil, false, err
			}
			if !s.needsVectorDimensionsLocked(dimensions) {
				return s.indexMu.Unlock, false, nil
			}
			// Joining construction must precede indexMu. Recheck the SAME source
			// record and dimensions after reacquiring mutation ownership.
			s.indexMu.Unlock()
			resume := s.pauseHNSWBuild()
			s.indexMu.Lock()
			if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
				err := s.lifecycleCtx.Err()
				s.indexMu.Unlock()
				resume()
				return nil, false, err
			}
			s.baseBuildControlMu.Lock()
			current := s.nodeAdmission
			pending = false
			if current != nil {
				select {
				case <-current.done:
				default:
					pending = true
				}
			}
			s.baseBuildControlMu.Unlock()
			if pending {
				s.indexMu.Unlock()
				resume()
				continue
			}
			if err := s.admitNodeWriteLocked(nodeWrite); err != nil {
				s.indexMu.Unlock()
				resume()
				return nil, false, err
			}
			s.replaceEmptyVectorDimensionsLocked(dimensions)
			return func() { s.indexMu.Unlock(); resume() }, false, nil
		}
		if paused {
			s.indexMu.Unlock()
			return nil, false, ErrSearchIndexBuilding
		}
		if s.populationDefersWrites() && !mustWait {
			// The population began after queueOnPopulation looked: queue on it.
			s.indexMu.Unlock()
			continue
		}
		s.indexMu.Unlock()
		if err := s.awaitNodeAdmission(job); err != nil {
			return nil, false, err
		}
	}
}

// admitNodeWriteLocked restores parked BM25 before a live write, then records
// a node write applied to incomplete sources. A retired checkpoint leaves the
// text source incomplete, so the write applies as an unpopulated write and the
// next population rebuilds BM25 from storage. Caller holds indexMu.
func (s *Service) admitNodeWriteLocked(nodeWrite bool) error {
	if err := s.materializeNodeFulltextLocked(); err != nil && !errors.Is(err, errFulltextCheckpointRetired) {
		return err
	}
	if nodeWrite && !s.nodeSourcesCompleteLocked() {
		s.changeNodeSourcesLocked(nodeWriteUnpopulated)
	}
	return nil
}

func (s *Service) needsVectorDimensionsLocked(dimensions int) bool {
	if dimensions <= 0 || s.vectorDimensionsFixed.Load() {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.embeddingCountLocked() == 0 && s.vectorIndexDimensionsLocked() != dimensions
}

// materializeNodeFulltextLocked is the single checkpoint reader: population
// entry and exit, the post-ANN tail, and live mutations. Once a writer
// restores it, the tail cannot overwrite that writer's newer documents with
// the older checkpoint. A checkpoint that cannot be reloaded is retired:
// BM25 is cleared, the text source becomes incomplete and the snapshot is
// removed, so the next population rebuilds BM25 from storage; the error then
// wraps errFulltextCheckpointRetired. After Close the checkpoint stays parked:
// the service is gone, and the restart checks the snapshot against storage.
func (s *Service) materializeNodeFulltextLocked() error {
	path := s.nodeSources.fulltextCheckpoint
	if path == "" {
		return nil
	}
	if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
		return s.lifecycleCtx.Err()
	}
	err := s.reloadFulltextAfterWarmup(path, s.nodeSources.fulltextCheckpointCount)
	s.setNodeFulltextCheckpointLocked("", 0)
	if err == nil {
		return nil
	}
	s.fulltext().Clear()
	s.changeNodeSourcesLocked(nodeFulltextReset)
	if removeErr := removeFulltextCheckpoint(path); removeErr != nil {
		err = errors.Join(err, removeErr)
	}
	return fmt.Errorf("%w: %w", errFulltextCheckpointRetired, err)
}

func (s *Service) nodePopulationCanPersist() bool {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
		return false
	}
	return s.nodeSourcesCompleteLocked()
}

// removeVectorArtifactsLocked retires the saved coordinates and their global
// graph together. It is shared by authoritative replacement and base reset.
// Caller owns indexMu; no readers/builders may still own a closing source.
func (s *Service) removeVectorArtifactsLocked() error {
	paths := []string{}
	if s.vectorIndexPath != "" {
		paths = append(paths, s.vectorIndexPath+".vec", s.vectorIndexPath+".meta")
	}
	for _, path := range paths {
		if err := security.RemoveRootedPath(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// The compressed bundle encodes the retired coordinates. Its store-version
	// check compares only count and slots, which a replacement can repeat.
	if dir := ivfpqBundleDir(s.ivfpqPersistenceBasePath(s.vectorIndexPath, s.hnswIndexPath)); dir != "" {
		if err := security.RemoveAllRootedPath(dir); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return s.removeSavedGlobalGraphLocked()
}

func (s *Service) removeSavedGlobalGraphLocked() error {
	if s.hnswIndexPath == "" {
		return nil
	}
	if err := security.RemoveRootedPath(s.hnswIndexPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (s *Service) clearNodeVectorMetadataLocked() {
	clear(s.nodeLabels)
	clear(s.nodeTypes)
	clear(s.nodeNamedVector)
	clear(s.nodePropVector)
	clear(s.nodeChunkVectors)
	clear(s.nodeChunkSpans)
}

// resetBaseVectorSource replaces only the node source. Relationship payload
// and ledger ownership remain in the canonical relationship completion owner.
func (s *Service) resetBaseVectorSource() error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.Lock()
	if s.vectorFileStore != nil {
		_ = s.vectorFileStore.Close()
		s.vectorFileStore = nil
	}
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	s.clearNodeVectorMetadataLocked()
	s.changeNodeSourcesLocked(nodeVectorReset)
	s.mu.Unlock()
	capacityErr := s.restoreNodeIndexCapacityLocked()
	s.retireVectorSourceLocked(false)
	if capacityErr != nil {
		return capacityErr
	}
	return s.removeVectorArtifactsLocked()
}

type nodePopulationPlan struct {
	fulltextPath     string
	hnswPath         string
	nodesRebuilt     bool
	skipIteration    bool
	forceHNSWRebuild bool
	vectorCount      int
	// relationships are the stored relationships an explicit build read where
	// it reads its nodes (readStoredRelationships).
	relationships []*storage.Edge
}

// completeNodePopulation publishes NODE before ANN. Initial live mutations use
// the same source owner but never execute the later phase. Live writes stay
// queued on job until publishNodeAdmission, so the relationship stream and the
// base persist below run with the population as the only source writer.
func (s *Service) completeNodePopulation(ctx context.Context, job *nodePopulationAdmission, plan nodePopulationPlan, explicit bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.applyDeferredIndexWrites(job)
	s.indexMu.Lock()
	if err := ctx.Err(); err != nil {
		s.indexMu.Unlock()
		return err
	}
	// An unadopted saved graph cannot follow the first live mutation. Likewise
	// a rebuilt/current live population cannot leave its older graph reusable
	// if the following ANN warmup is cancelled or the process is restarted.
	if !explicit || plan.nodesRebuilt || plan.forceHNSWRebuild {
		if err := s.removeSavedGlobalGraphLocked(); err != nil {
			s.indexMu.Unlock()
			return err
		}
	}
	s.changeNodeSourcesLocked(nodePopulationAdmitted)
	// An admitted empty population needs the same durable empty file source as
	// populated ingestion; otherwise a last removal could leave no restart proof.
	if s.vectorEnabled.Load() {
		s.ensureVectorFileStore()
	}
	s.indexMu.Unlock()
	if !explicit {
		s.completePopulationSequence(job)
		s.finishNodeAdmission(job, nil)
		return nil
	}
	// Admit a reusable saved graph before NODE publishes. After it every live
	// delta must reach a graph already installed or the canonical construction
	// job's snapshot/log.
	s.indexMu.Lock()
	s.restoreNodePopulationANN(ctx, plan)
	s.indexMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	// The explicit build applies stored edges from a snapshot stream. Live
	// edge writes meanwhile are queued, and an edge one of them wrote is never
	// replaced by the stream's older copy.
	s.setBuildPhase("iterating_relationship_vectors")
	if err := s.buildRelationshipVectorIndexes(ctx, job, plan.relationships); err != nil {
		// The node population is complete; a relationship failure does
		// not undo it, so a retry keeps skipping the node iteration.
		s.finishNodeAdmission(job, nil)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The population is complete: every queued operation, administrative ones
	// included, is applied before the base save.
	s.completePopulationSequence(job)
	if !plan.nodesRebuilt {
		s.finishNodeAdmission(job, nil)
		return s.completeBaseIndexBuild(ctx)
	}
	s.setBuildPhase("persisting_base_indexes")
	if err := s.persistBaseIndexesAndPublish(ctx, job, plan.fulltextPath != ""); err != nil {
		return err
	}
	return s.completeBaseIndexBuild(ctx)
}

// persistBaseIndexesAndPublish saves the rebuilt base indexes and publishes
// NODE. No live write is applied while they are saved, so BM25's no-copy
// snapshot reads sources nobody else changes. With park set, BM25 is parked
// through the warmup at the checkpoint just saved, only when no queued write
// changed it since. Every exit of the population restores parked BM25
// (runBasePopulationAttempt).
func (s *Service) persistBaseIndexesAndPublish(ctx context.Context, job *nodePopulationAdmission, park bool) error {
	if !s.nodePopulationForPersist(ctx) || !s.persistEnabled.Load() {
		s.finishNodeAdmission(job, nil)
		return nil
	}
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	saved, err := s.persistBaseIndexesLocked()
	if err != nil {
		return err
	}
	if !park {
		saved = ""
	}
	s.publishNodeAdmission(job, nil, saved)
	return nil
}

// nodePopulationForPersist reports whether a persist outside a population may
// write the indexes; that needs complete node sources. A service no build
// populated keeps its saved artifacts untouched, unless live writes were
// applied to it: persisting those needs the node population first, which this
// persist (not a live write) then runs. When a build or a pause holds the base
// operation it skips, as before.
func (s *Service) nodePopulationForPersist(ctx context.Context) bool {
	if s.nodePopulationCanPersist() {
		return true
	}
	s.indexMu.RLock()
	wrote := s.nodeSources.unpopulatedLiveWrites
	s.indexMu.RUnlock()
	if !wrote || (s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil) {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.buildMu.token():
		err := s.runBasePopulationOperation(ctx, s.claimNodeAdmission(), false)
		s.buildMu.Unlock()
		if err != nil {
			s.logPrintf("📇 Persist: preserving unopened indexes; node population failed: %v", err)
			return false
		}
		return s.nodePopulationCanPersist()
	default:
		return false
	}
}
