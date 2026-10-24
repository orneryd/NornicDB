package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Every chunk of a node indexed after the HNSW graph exists reaches it, not
// only the main embedding.
func TestHNSWLiveIndexTakesEveryChunk(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	svc := NewServiceWithDimensions(engine, 3)
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "first", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))
	index, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !svc.hnswRebuildInFlight.Load() }, 5e9, 1e6)

	require.NoError(t, svc.IndexNode(&storage.Node{ID: "multi", ChunkEmbeddings: [][]float32{{1, 0, 0}, {0, 0, 1}, {1, 1, 0}}}))
	require.Equal(t, 4, index.Size())
	results, err := index.Search(context.Background(), []float32{0, 0, 1}, 1, 0)
	require.NoError(t, err)
	require.Equal(t, nodeChunkVectorKey("multi", 1), results[0].ID)
}
