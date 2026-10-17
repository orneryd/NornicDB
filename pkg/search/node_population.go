package search

import (
	"context"
	"os"
	"sync"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/security"
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
)

// nodePopulationAdmission is the NODE phase of the existing base operation,
// not another worker or ANN job. baseBuildControlMu protects publication; err
// is immutable after done closes. A caller waiting for NODE never waits for ANN.
type nodePopulationAdmission struct {
	done chan struct{}
	err  error
	// worker is set while a registered admission worker owns the job.
	worker atomic.Bool
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
	return job
}

// finishNodeAdmission publishes the job's outcome. A failure retires the
// completeness record only when the operation had started replacing sources;
// one refused or cancelled before that left the existing sources intact.
func (s *Service) finishNodeAdmission(job *nodePopulationAdmission, err error, sourcesTouched bool) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	select {
	case <-job.done:
		return
	default:
	}
	if err != nil && sourcesTouched {
		s.changeNodeSourcesLocked(nodePopulationFailed)
		s.setLifecycleState(lifecycleUnloaded)
	}
	if err == nil {
		// Every recorded write was replayed at admission or applied to the
		// complete source afterwards.
		s.clearPopulationWritesLocked()
	}
	job.err = err
	close(job.done)
}

// waitOrRunNodeAdmission joins job or, once it owns buildMu, runs it. Only
// background callers use it: the admission worker and persistence.
func (s *Service) waitOrRunNodeAdmission(ctx context.Context, job *nodePopulationAdmission) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-job.done:
		return job.err
	case <-s.buildMu.token():
		defer s.buildMu.Unlock()
		select {
		case <-job.done:
			return job.err
		default:
		}
		return s.runBasePopulationOperation(ctx, job, false)
	}
}

func (s *Service) needsVectorDimensionsLocked(dimensions int) bool {
	if dimensions <= 0 || s.vectorDimensionsFixed.Load() {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.embeddingCountLocked() == 0 && s.vectorIndexDimensionsLocked() != dimensions
}

// materializeNodeFulltextLocked is the single checkpoint reader for a live
// mutation or the post-ANN tail. Once a writer restores it, the tail cannot
// overwrite that writer's newer documents with the older checkpoint.
func (s *Service) materializeNodeFulltextLocked() error {
	if s.nodeSources.fulltextCheckpoint == "" {
		return nil
	}
	if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
		return s.lifecycleCtx.Err()
	}
	if err := s.reloadFulltextAfterWarmup(s.nodeSources.fulltextCheckpoint, s.nodeSources.fulltextCheckpointCount); err != nil {
		return err
	}
	s.setNodeFulltextCheckpointLocked("", 0)
	return nil
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
		s.setVectorFileStoreLocked(nil)
	}
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	s.clearNodeVectorMetadataLocked()
	s.changeNodeSourcesLocked(nodeVectorReset)
	s.mu.Unlock()
	resetErr := s.restoreNodeIndexCapacityLocked()
	s.retireVectorSourceLocked(false)
	if resetErr == nil {
		resetErr = s.removeVectorArtifactsLocked()
	}
	// The replacement is a source transition: re-apply the live writes in the
	// same locked step, so they never leave search (F-I86, decision B), also
	// when the reset itself failed after clearing the source (sol-r1 finding 2).
	replayErr := s.replayPopulationWritesLocked(context.Background())
	if resetErr != nil {
		return resetErr
	}
	return replayErr
}

type nodePopulationPlan struct {
	fulltextPath     string
	hnswPath         string
	nodesRebuilt     bool
	skipIteration    bool
	forceHNSWRebuild bool
	vectorCount      int
}

// completeNodePopulation publishes NODE before relationships/ANN. Initial live
// mutations use the same source owner but never execute either later phase.
func (s *Service) completeNodePopulation(ctx context.Context, job *nodePopulationAdmission, plan nodePopulationPlan, explicit bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Recorded live writes may carry the first vectors; adopt their dimensions
	// the same way the population's own first batch does.
	s.maybeAutoSetVectorDimensions(s.populationWriteDimensions())
	s.indexMu.Lock()
	if err := ctx.Err(); err != nil {
		s.indexMu.Unlock()
		return err
	}
	// An unadopted saved graph cannot follow the first live mutation. Likewise
	// a rebuilt/current live population cannot leave its older graph reusable
	// if the following ANN warmup is cancelled or the process is restarted.
	if !explicit || plan.nodesRebuilt || plan.forceHNSWRebuild || s.hasPopulationWritesLocked() {
		if err := s.removeSavedGlobalGraphLocked(); err != nil {
			s.indexMu.Unlock()
			return err
		}
	}
	if err := s.replayPopulationWritesLocked(ctx); err != nil {
		s.indexMu.Unlock()
		return err
	}
	s.changeNodeSourcesLocked(nodePopulationAdmitted)
	// An admitted empty population needs the same durable empty file source as
	// populated ingestion; otherwise a last removal could leave no restart proof.
	if s.vectorEnabled.Load() {
		s.ensureVectorFileStore()
	}
	s.indexMu.Unlock()
	if explicit && plan.nodesRebuilt {
		s.setBuildPhase("persisting_base_indexes")
		if err := s.persistBaseIndexes(); err != nil {
			return err
		}
		if plan.fulltextPath != "" {
			s.indexMu.Lock()
			s.setNodeFulltextCheckpointLocked(plan.fulltextPath, s.fulltext().Count())
			s.fulltext().Clear()
			s.indexMu.Unlock()
			s.logPrintf("📇 BuildIndexes: cleared BM25 in-memory state (will reload after warmup)")
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if explicit {
		// Admit a reusable saved graph while initial mutation admission is still
		// excluded. After NODE publication every live delta must reach a graph
		// already installed or the canonical construction job's snapshot/log.
		s.indexMu.Lock()
		if s.hasPopulationWritesLocked() {
			// Writes since admission are in the source but not in a saved graph.
			if err := s.removeSavedGlobalGraphLocked(); err != nil {
				s.indexMu.Unlock()
				return err
			}
		}
		s.restoreNodePopulationANN(ctx, plan)
		s.indexMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.finishNodeAdmission(job, nil, true)
	if !explicit {
		return nil
	}
	return s.completeBaseIndexBuild(ctx)
}
