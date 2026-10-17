package search

import (
	"github.com/orneryd/nornicdb/pkg/security"
	"os"
	"path/filepath"
)

// ClearVectorIndex removes all embeddings from the vector index.
// This is used when regenerating all embeddings to reset the index count.
// It also removes persisted vector/ANN artifacts and returns any removal error.
// Live mutations and derived builds finish before the source is cleared.
//
// Example:
//
//	if err := svc.ClearVectorIndex(); err != nil { return err }
func (s *Service) ClearVectorIndex() error {
	return s.clearIndexes(false)
}

// ClearIndexes retires all collection-owned search data and capacity accounting.
// Use after dropping a database when its provider may reuse this service.
// Example: after dropping storage, call svc.ClearIndexes() before recreation.
func (s *Service) ClearIndexes() error {
	return s.clearIndexes(true)
}

func (s *Service) clearIndexes(all bool) error {
	// Clustering copies the population and publishes into it later; join it
	// whatever context its caller supplied. Then cancel and join base loading
	// (which owns buildMu) and ANN construction before retiring their sources.
	resumeClusters := s.pauseClusterRuns()
	defer resumeClusters()
	resumeBase := s.pauseBaseBuild(true)
	defer resumeBase()
	resume := s.pauseHNSWBuild()
	defer resume()
	// Persistence and clearing own the same file resources. Retire on-disk
	// coordinates too, so a reused service cannot reopen the dropped population.
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	defer s.indexChanged()
	// Serialize source retirement and final transition publication. Release mu
	// before pipelineMu, matching pipeline snapshot ordering.
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	s.mu.Lock()
	s.changeNodeSourcesLocked(nodeVectorCleared)
	if s.vectorFileStore != nil {
		_ = s.vectorFileStore.Close()
		s.vectorFileStore = nil
	}
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	if s.gpuEmbeddingIndex != nil {
		s.gpuEmbeddingIndex.Clear()
	}
	if s.clusterIndex != nil {
		s.clusterIndex.Clear()
	}
	s.clearNodeVectorMetadataLocked()
	clear(s.edgeTypes)
	clear(s.edgePropVector)
	s.clearVectorIndexCapacityLocked()
	if all {
		if ft := s.fulltext(); ft != nil {
			ft.Clear()
		}
		// The dropped population's parked text checkpoint is removed below;
		// the next writer repopulates text from the replacement storage.
		s.setNodeFulltextCheckpointLocked("", 0)
		s.changeNodeSourcesLocked(nodeFulltextReset)
		clear(s.indexCapacityByNode)
		s.bm25ResidentBytes = 0
		s.bm25MetadataBytes = 0
	}
	if s.resultCache != nil {
		s.resultCache.Invalidate()
	}
	s.mu.Unlock()

	// Retire cluster-derived publication before clearing it.
	s.clusterGeneration.Add(1)
	s.clusterHNSWMu.Lock()
	s.clusterHNSW = nil
	s.clusterHNSWMu.Unlock()
	s.ivfpqMu.Lock()
	s.ivfpqIndex = nil
	s.ivfpqOverlay.Reset()
	s.ivfpqMu.Unlock()
	s.clearClusterLexicalProfiles()

	// Also clear HNSW index if it exists (frees memory from tombstones).
	// HNSW uses tombstones for deletes which never free memory, so explicit Clear() is required.
	s.hnswMu.Lock()
	if s.hnswIndex != nil {
		s.hnswIndex.Clear()
	}
	s.hnswMu.Unlock()
	s.retireVectorSourceLocked(false) // Recreate lazily from replacement vectors.

	if all && s.fulltextIndexPath != "" {
		if err := security.RemoveRootedPath(s.fulltextIndexPath); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if s.hnswIndexPath != "" {
		if err := security.RemoveAllRootedPath(filepath.Join(filepath.Dir(s.hnswIndexPath), "hnsw_ivf")); err != nil {
			return err
		}
	}
	// Saved coordinates, the global graph and the compressed bundle.
	return s.removeVectorArtifactsLocked()
}
