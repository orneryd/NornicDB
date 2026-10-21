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
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	resume := s.pauseHNSWBuild()
	defer resume()
	// Persistence and clearing own the same file resources. Retire on-disk
	// coordinates too, so a reused service cannot reopen the dropped population.
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	defer s.indexChanged()
	// Lock order: pipelineMu -> indexMu -> clusterStateMu -> mu -> leaf locks.
	s.pipelineMu.Lock()
	defer s.pipelineMu.Unlock()
	s.cacheDerivedVectorPipelineLocked(nil)

	s.indexMu.Lock()
	// Clearing the cluster generation is one cluster-state change.
	s.clusterStateMu.Lock()
	s.mu.Lock()
	s.closeVectorFileStoreLocked()
	if s.vectorIndex != nil {
		s.vectorIndex.Clear()
	}
	if s.servingGPUIndex() != nil {
		s.servingGPUIndex().Clear()
	}
	s.retireClusterGeneration(emptyClusterIndex)
	clear(s.nodeLabels)
	clear(s.nodeTypes)
	clear(s.nodeNamedVector)
	clear(s.nodePropVector)
	clear(s.nodeChunkVectors)
	s.vectorMembers.clear()
	clear(s.nodeChunkSpans)
	clear(s.edgeTypes)
	clear(s.edgePropVector)
	for id, usage := range s.indexCapacityByNode {
		usage.vectorResident = 0
		usage.vectorMetadata = 0
		s.indexCapacityByNode[id] = usage
	}
	clear(s.indexCapacityByEdge)
	s.vectorResidentBytes = 0
	s.vectorMetadataBytes = 0
	if all {
		s.hnswQuantizationOverride = nil
		if ft := s.fulltext(); ft != nil {
			ft.Clear()
		}
		clear(s.indexCapacityByNode)
		s.bm25ResidentBytes = 0
		s.bm25MetadataBytes = 0
	}
	if s.resultCache != nil {
		s.resultCache.Invalidate()
	}
	s.mu.Unlock()
	s.clusterStateMu.Unlock()
	s.indexMu.Unlock()

	// pipelineMu is held: the pipeline was dropped above, and no query can
	// cache another before this publication.
	s.publishVectorServing(func() {
		s.ivfpqMu.Lock()
		s.ivfpqIndex = nil
		s.ivfpqOverlay.Reset()
		s.ivfpqMu.Unlock()

		// Also clear HNSW index if it exists (frees memory from tombstones).
		// HNSW uses tombstones for deletes which never free memory, so explicit Clear() is required.
		s.hnswMu.Lock()
		if s.servingHNSW() != nil {
			s.servingHNSW().Clear()
			s.hnswIndex = nil // Reset to nil so it can be recreated lazily
		}
		s.hnswMu.Unlock()
	}, false)
	paths := []string{}
	if all && s.fulltextIndexPath != "" {
		paths = append(paths, s.fulltextIndexPath)
	}
	if s.vectorIndexPath != "" {
		paths = append(paths, s.vectorIndexPath+".vec", s.vectorIndexPath+".meta")
	}
	if s.hnswIndexPath != "" {
		paths = append(paths, s.hnswIndexPath)
		if err := security.RemoveAllRootedPath(filepath.Join(filepath.Dir(s.hnswIndexPath), "hnsw_ivf")); err != nil {
			return err
		}
	}
	for _, path := range paths {
		if err := security.RemoveRootedPath(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	if dir := ivfpqBundleDir(s.ivfpqPersistenceBasePath(s.vectorIndexPath, s.hnswIndexPath)); dir != "" {
		if err := security.RemoveAllRootedPath(dir); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
