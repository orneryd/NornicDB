package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

func gpuIndexOf(svc *Service) *gpu.EmbeddingIndex {
	svc.mu.RLock()
	defer svc.mu.RUnlock()
	return svc.gpuEmbeddingIndex
}

// Attaching a GPU manager builds no GPU copy of the vectors: a search outside
// the GPU brute-force range never builds one, and the first search that
// chooses GPU brute-force builds it with every vector.
func TestGPUBruteForceIndexBuiltOnlyWhenChosen(t *testing.T) {
	cfg := gpu.DefaultConfig()
	cfg.Enabled = true
	cfg.FallbackOnError = true
	manager, err := gpu.NewManager(cfg)
	require.NoError(t, err)
	if !manager.IsEnabled() {
		t.Skip("GPU not available in this environment")
	}
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")

	svc := NewServiceWithDimensions(newNamespacedEngine(t), 4)
	for i := 0; i < 50; i++ {
		indexTestNode(t, svc, fmt.Sprintf("n-%d", i), testVec4(i))
	}
	svc.SetGPUManager(manager)
	require.Nil(t, gpuIndexOf(svc), "attaching the manager builds nothing")

	// 50 vectors, below the GPU brute-force range: HNSW, no GPU copy.
	t.Setenv("NORNICDB_VECTOR_GPU_BRUTE_MIN_N", "100")
	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())
	require.Nil(t, gpuIndexOf(svc))

	// In range: the strategy builds the GPU copy and searches it.
	t.Setenv("NORNICDB_VECTOR_GPU_BRUTE_MIN_N", "10")
	svc.pipelineMu.Lock()
	svc.vectorPipeline = nil
	svc.pipelineMu.Unlock()
	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteGPU, svc.currentPipelineStrategy())
	gi := gpuIndexOf(svc)
	require.NotNil(t, gi)
	require.Equal(t, 50, gi.Count())

	// A new manager drops the old manager's copy.
	svc.SetGPUManager(manager)
	require.Nil(t, gpuIndexOf(svc))
}
