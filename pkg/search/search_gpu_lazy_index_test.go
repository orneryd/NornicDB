package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
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

// Hold the canonical snapshot boundary and verify that the public pipeline's
// first upload also owns mutation admission. No native timing assumption is
// needed: either upload owns indexMu or the former pipeline-only lock is held.
func TestGPUFirstPipelineUploadOwnsMutationAdmission(t *testing.T) {
	cfg := gpu.DefaultConfig()
	cfg.Enabled = true
	cfg.FallbackOnError = true
	manager, err := gpu.NewManager(cfg)
	require.NoError(t, err)
	if !manager.IsEnabled() {
		t.Skip("GPU not available")
	}
	t.Setenv("NORNICDB_VECTOR_GPU_BRUTE_MIN_N", "1")
	t.Setenv("NORNICDB_VECTOR_GPU_BRUTE_MAX_N", "1000")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 4)
	defer svc.Close()
	indexTestNode(t, svc, "a", []float32{1, 0, 0, 0})
	svc.SetGPUManager(manager)
	svc.vectorIndex.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := svc.getOrCreateVectorPipeline(context.Background()); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	admitted := false
	for time.Now().Before(deadline) {
		if !svc.indexMu.TryLock() {
			admitted = true
			break
		}
		svc.indexMu.Unlock()
		if !svc.pipelineMu.TryLock() {
			break
		}
		svc.pipelineMu.Unlock()
		time.Sleep(time.Millisecond)
	}
	mutation := make(chan error, 1)
	go func() { mutation <- svc.IndexNode(&storage.Node{ID: "b", ChunkEmbeddings: [][]float32{{0, 1, 0, 0}}}) }()
	svc.vectorIndex.mu.Unlock()
	require.NoError(t, <-done)
	require.NoError(t, <-mutation)
	require.True(t, admitted, "initial snapshot and publication must own mutation admission")
	require.Equal(t, 2, gpuIndexOf(svc).Count())
	hits, err := svc.VectorQueryNodes(context.Background(), []float32{0, 1, 0, 0}, VectorQuerySpec{Limit: 1})
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, "b", hits[0].ID)
	require.NoError(t, svc.RemoveNode("b"))
	require.Equal(t, 1, gpuIndexOf(svc).Count())
}
