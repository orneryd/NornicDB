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
	s.buildMu.Lock()
	defer s.buildMu.Unlock()
	// Persistence and clearing own the same file resources. Retire on-disk
	// coordinates too, so a reused service cannot reopen the dropped population.
	s.persistRunMu.Lock()
	defer s.persistRunMu.Unlock()
	defer s.indexChanged()
	// Lock order: pipelineMu -> mu -> hnswMu, matching pipeline construction paths.
	s.pipelineMu.Lock()
	defer s.pipelineMu.Unlock()
	s.vectorPipeline = nil

	s.indexMu.Lock()
	s.mu.Lock()
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
	clear(s.nodeLabels)
	clear(s.nodeTypes)
	clear(s.nodeNamedVector)
	clear(s.nodePropVector)
	clear(s.nodeChunkVectors)
	clear(s.nodeChunkSpans)
	clear(s.edgeTypes)
	clear(s.edgePropVector)
	s.mu.Unlock()
	s.indexMu.Unlock()

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
		s.hnswIndex = nil // Reset to nil so it can be recreated lazily
	}
	s.hnswMu.Unlock()
	paths := []string{}
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
