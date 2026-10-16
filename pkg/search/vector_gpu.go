package search

import (
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// SetGPUManager enables GPU acceleration for exact brute-force vector search.
// This is independent from k-means clustering. Replacing the manager preserves
// the native graph and retires descriptors captured from the previous backend.
func (s *Service) SetGPUManager(manager *gpu.Manager) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if s.lifecycleCtx != nil && s.lifecycleCtx.Err() != nil {
		return
	}

	s.mu.Lock()
	retired := s.gpuEmbeddingIndex
	s.gpuManager = manager
	s.gpuEmbeddingIndex = nil
	vi, vfs := s.vectorIndex, s.vectorFileStore
	s.mu.Unlock()

	// The same barrier orders live mutations, completed transitions and a
	// factory already holding pipelineMu. Never take pipelineMu under mu.
	defer func() {
		s.retireVectorSourceLocked(true)
		if retired != nil {
			// Release waits for index readers and preserves CPU coordinates,
			// so a reader already holding the retired descriptor can finish.
			retired.Release()
		}
	}()
	if manager != nil && manager.IsEnabled() {
		if err := s.ensureGPUIndexSynced(vi, vfs); err != nil {
			s.logPrintf("GPU vector index initialization failed: %v", err)
		}
	}
}

// gpuVectorState reads the configuration and its derived index together.
// Factories do not own indexMu, so their reads must share the setter's mu.
func (s *Service) gpuVectorState() (*gpu.Manager, *gpu.EmbeddingIndex) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.gpuManager, s.gpuEmbeddingIndex
}

// ensureGPUIndexSynced is the single builder for manager replacement and runtime
// strategy changes. The caller owns indexMu, excluding source/backend writers.
func (s *Service) ensureGPUIndexSynced(vi *VectorIndex, vfs *VectorFileStore) error {
	manager, current := s.gpuVectorState()
	if manager == nil || !manager.IsEnabled() {
		return localizedError(localization.SearchGPUNotEnabled(), nil)
	}
	if current != nil {
		return nil
	}
	dim := s.VectorIndexDimensions()
	if dim <= 0 {
		return localizedError(localization.SearchVectorDimensionsInvalid(), nil)
	}
	cfg := gpu.DefaultEmbeddingIndexConfig(dim)
	cfg.GPUEnabled = true
	cfg.AutoSync = true
	cfg.BatchThreshold = 1000
	index := gpu.NewEmbeddingIndex(manager, cfg)
	published := false
	defer func() {
		if !published {
			index.Release()
		}
	}()

	if vfs != nil && vfs.Count() > 0 {
		if err := vfs.IterateChunked(10000, func(ids []string, vectors [][]float32) error {
			return index.AddBatch(ids, vectors)
		}); err != nil {
			return err
		}
	} else if vi != nil {
		vi.mu.RLock()
		ids := make([]string, 0, len(vi.vectors))
		vectors := make([][]float32, 0, len(vi.vectors))
		for id, vector := range vi.vectors {
			if len(vector) > 0 {
				ids = append(ids, id)
				vectors = append(vectors, vector)
			}
		}
		vi.mu.RUnlock()
		if err := index.AddBatch(ids, vectors); err != nil {
			return err
		}
	}
	if err := index.SyncToGPU(); err != nil {
		return err
	}
	s.mu.Lock()
	s.gpuEmbeddingIndex = index
	s.mu.Unlock()
	published = true
	return nil
}
