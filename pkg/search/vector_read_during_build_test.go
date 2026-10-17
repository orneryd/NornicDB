package search

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestApproximateReadDuringBackgroundBuildAnswersExactly: while the graph
// builds in the background, a read that asks for approximate (unrescored)
// scoring answers at once with the exact score from the exact fallback
// (#1230). Once the graph is published, the same read gets the graph's
// approximate score (#1372).
func TestApproximateReadDuringBackgroundBuildAnswersExactly(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_VECTOR_HNSW_QUANTIZATION", "binary")
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { _ = svc.Close() })
	for _, item := range []struct {
		id string
		v  []float32
	}{{"near", []float32{1, 0.1}}, {"far", []float32{-1, -0.1}}} {
		node := &storage.Node{ID: storage.NodeID(item.id), ChunkEmbeddings: [][]float32{item.v}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	previous := hnswBeforeInstallHook
	hnswBeforeInstallHook = func(context.Context) {
		startOnce.Do(func() { close(started) })
		<-release
	}
	t.Cleanup(func() { unblock(); hnswBeforeInstallHook = previous })

	no, minimum := false, -1.0
	exactScore := float64(1.1) / math.Sqrt(2*1.01)
	read := func() *SearchResponse {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := svc.Search(ctx, "", []float32{1, 1}, &SearchOptions{Limit: 1, MinSimilarity: &minimum, QuantizationRescore: &no})
		require.NoError(t, err, "a read during the build must not wait for it")
		require.Len(t, resp.Results, 1)
		require.Equal(t, "near", resp.Results[0].ID)
		return resp
	}
	first := read()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("background construction did not start")
	}
	for _, resp := range []*SearchResponse{first, read()} {
		require.Equal(t, "vector_brute", resp.Metrics.VectorSearchMethod)
		require.InDelta(t, exactScore, resp.Results[0].Score, 1e-6)
	}

	unblock()
	_, err := svc.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	resp := read()
	require.NotEqual(t, "vector_brute", resp.Metrics.VectorSearchMethod)
	require.InDelta(t, 1, resp.Results[0].Score, 1e-6, "the graph's approximate binary score")
}
