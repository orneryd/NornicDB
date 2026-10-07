package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestIndexRevisionAdvancesOnEveryIndexChange (#945).
func TestIndexRevisionAdvancesOnEveryIndexChange(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 2)
	var nilService *Service
	require.Zero(t, nilService.IndexRevision())
	node := &storage.Node{ID: "nornic:n", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}}
	edge := &storage.Edge{ID: "nornic:e", StartNode: "nornic:n", EndNode: "nornic:n", Type: "R", Properties: map[string]any{"v": []float32{1, 0}}}
	for name, change := range map[string]func(){
		"index node":              func() { require.NoError(t, svc.IndexNode(node)) },
		"remove node":             func() { require.NoError(t, svc.RemoveNode(node.ID)) },
		"index edge":              func() { require.NoError(t, svc.IndexEdge(edge)) },
		"remove edge":             func() { require.NoError(t, svc.RemoveEdge(edge.ID)) },
		"clear vectors":           func() { svc.ClearVectorIndex() },
		"remove property vectors": func() { svc.RemovePropertyVectorIndex("v") },
		"embedding space":         func() { svc.SetEmbeddingSpace("another-space") },
		"index flags":             func() { require.True(t, svc.SetIndexFlags(!svc.BM25Enabled(), true)) },
		"build":                   func() { require.NoError(t, svc.BuildIndexes(context.Background())) },
	} {
		before := svc.IndexRevision()
		change()
		require.Greater(t, svc.IndexRevision(), before, name)
	}
	before := svc.IndexRevision()
	require.False(t, svc.SetIndexFlags(svc.BM25Enabled(), svc.VectorEnabled()))
	require.Equal(t, before, svc.IndexRevision(), "unchanged flags change nothing")
}

// TestSearchResultCacheKeyedByIndexRevision: a change that reaches the index
// after a result was cached (IndexNode used to invalidate the cache before
// indexing, so a search in between could cache the old result) is seen by
// the next search (#945).
func TestSearchResultCacheKeyedByIndexRevision(t *testing.T) {
	store := storage.NewMemoryEngine()
	svc := NewService(store)
	ctx := context.Background()
	for _, id := range []string{"nornic:a", "nornic:b"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha"}})
		require.NoError(t, err)
	}
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]any{"content": "alpha"}}))
	opts := DefaultSearchOptions()
	first, err := svc.Search(ctx, "alpha", nil, opts)
	require.NoError(t, err)
	require.Len(t, first.Results, 1)

	// Index b without the cache invalidation IndexNode does, as a search
	// that cached its result while b was being indexed would leave it.
	svc.fulltext().Index("nornic:b", "alpha")
	svc.indexChanged()
	second, err := svc.Search(ctx, "alpha", nil, opts)
	require.NoError(t, err)
	require.Len(t, second.Results, 2)
}
