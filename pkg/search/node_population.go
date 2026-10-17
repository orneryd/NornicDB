package search

import (
	"context"
	"os"

	"github.com/orneryd/nornicdb/pkg/security"
)

// nodeSourceRecord is the one initial-population and source-authority record.
// indexMu protects it; identity, dirty state and positive counts do not establish
// completeness. A parked fulltext checkpoint remains the logical text source.
type nodeSourceRecord struct {
	textComplete            bool
	vectorsComplete         bool
	memoryAuthoritative     bool
	fulltextCheckpoint      string
	fulltextCheckpointCount int
	// unpopulatedLiveWrites: live writes applied while the sources were
	// incomplete and no population ran. Persistence then populates first.
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
	nodeSourcesCleared
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
	case nodeSourcesCleared:
		// A dropped database: both sources are empty, complete and in memory.
		s.nodeSources.textComplete, s.nodeSources.vectorsComplete = true, true
		s.nodeSources.memoryAuthoritative = true
	}
}

func (s *Service) setNodeFulltextCheckpointLocked(path string, count int) {
	s.nodeSources.fulltextCheckpoint = path
	s.nodeSources.fulltextCheckpointCount = count
}

// nodePopulationAdmission is the NODE phase of the existing base operation,
// not another worker or ANN job. baseBuildControlMu protects publication; err
// is immutable after done closes. A caller waiting for NODE never waits for ANN.
type nodePopulationAdmission struct {
	done chan struct{}
	err  error
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

func (s *Service) finishNodeAdmission(job *nodePopulationAdmission, err error) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.baseBuildControlMu.Lock()
	defer s.baseBuildControlMu.Unlock()
	select {
	case <-job.done:
		return
	default:
	}
	if err != nil {
		s.changeNodeSourcesLocked(nodePopulationFailed)
	}
	job.err = err
	close(job.done)
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
// sources the write waits for it; otherwise it applies at once, also before
// any population (the build that follows rebuilds or restores around it). A
// write never starts or runs a population. On success the caller owns indexMu
// and must call the returned release.
// Private node/edge batch writers never enter this public boundary.
func (s *Service) lockNodeMutation(dimensions int) (func(), error) {
	for {
		s.indexMu.Lock()
		if s.lifecycleCtx != nil {
			if err := s.lifecycleCtx.Err(); err != nil {
				s.indexMu.Unlock()
				return nil, err
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
			if !s.nodeSourcesCompleteLocked() {
				s.nodeSources.unpopulatedLiveWrites = true
			}
			if err := s.materializeNodeFulltextLocked(); err != nil {
				s.indexMu.Unlock()
				return nil, err
			}
			if !s.needsVectorDimensionsLocked(dimensions) {
				return s.indexMu.Unlock, nil
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
				return nil, err
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
			if err := s.materializeNodeFulltextLocked(); err != nil {
				s.indexMu.Unlock()
				resume()
				return nil, err
			}
			s.replaceEmptyVectorDimensionsLocked(dimensions)
			return func() { s.indexMu.Unlock(); resume() }, nil
		}
		if paused {
			s.indexMu.Unlock()
			return nil, ErrSearchIndexBuilding
		}
		s.indexMu.Unlock()
		if err := s.awaitNodeAdmission(job); err != nil {
			return nil, err
		}
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
}

// completeNodePopulation publishes NODE before relationships/ANN. Initial live
// mutations use the same source owner but never execute either later phase.
func (s *Service) completeNodePopulation(ctx context.Context, job *nodePopulationAdmission, plan nodePopulationPlan, explicit bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if explicit && plan.nodesRebuilt {
		s.setBuildPhase("persisting_base_indexes")
		// BM25 is parked through the warmup only when this persist wrote its
		// checkpoint: the index was dirty and the save marked it persisted. An
		// empty index never written has no checkpoint to reload, and a failed
		// save leaves none current, so it stays resident.
		fulltextUnsaved := s.fulltext().IsDirty()
		if err := s.persistBaseIndexes(); err != nil {
			return err
		}
		if plan.fulltextPath != "" && fulltextUnsaved && !s.fulltext().IsDirty() {
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
		s.restoreNodePopulationANN(ctx, plan)
		s.indexMu.Unlock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.finishNodeAdmission(job, nil)
	if !explicit {
		return nil
	}
	return s.completeBaseIndexBuild(ctx)
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
	switch {
	case s.buildMu.TryLock():
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
