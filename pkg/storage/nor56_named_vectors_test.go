package storage

import (
	"context"
	"strings"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// nor56NamedVectorNode is a node whose named vector lives inline in its body
// while its chunk vectors are large enough to be stored under their own keys.
func nor56NamedVectorNode(id NodeID) *Node {
	node := &Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "keep"},
		NamedEmbeddings: map[string][]float32{"title": {1, 2, 3}}, ChunkEmbeddings: make([][]float32, 16),
		EmbedMeta: map[string]any{"chunk_count": int64(16)}}
	for i := range node.ChunkEmbeddings {
		node.ChunkEmbeddings[i] = make([]float32, 1536)
		node.ChunkEmbeddings[i][0] = float32(i + 1)
	}
	return node
}

func nor56RequireVectors(t *testing.T, round int, path string, got, want *Node) {
	t.Helper()
	require.Equal(t, want.NamedEmbeddings, got.NamedEmbeddings, "round %d, %s: named vectors of %s", round, path, got.ID)
	require.Equal(t, want.ChunkEmbeddings, got.ChunkEmbeddings, "round %d, %s: chunk vectors of %s", round, path, got.ID)
}

// TestNOR56FullStreamsKeepNamedVectorsWhenBodyCacheWarm: a full read returns
// the same named vectors and chunks on a cold and on a warm body cache,
// through direct streaming, chunk streaming, the stored-node collection, a
// namespace over tracing, and a point read that misses the node cache.
func TestNOR56FullStreamsKeepNamedVectorsWhenBodyCacheWarm(t *testing.T) {
	engine := fixture.Own(t, fixture.Track(t, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	separate := nor56NamedVectorNode("test:separate")
	inline := &Node{ID: "test:inline", NamedEmbeddings: map[string][]float32{"title": {4, 5}}, ChunkEmbeddings: [][]float32{{6, 7}}}
	expected := map[NodeID]*Node{separate.ID: separate, inline.ID: inline}
	for _, node := range expected {
		_, err := engine.CreateNode(ctx, CopyNode(node))
		require.NoError(t, err)
	}
	check := func(round int, path string, nodes []*Node) {
		t.Helper()
		require.Len(t, nodes, len(expected), "round %d, %s", round, path)
		for _, got := range nodes {
			id := got.ID
			if !strings.HasPrefix(string(id), "test:") {
				id = "test:" + id // a namespace view publishes user-visible IDs
			}
			nor56RequireVectors(t, round, path, got, expected[id])
		}
	}
	for round := 0; round < 3; round++ {
		var streamed []*Node
		require.NoError(t, engine.StreamNodes(ctx, func(node *Node) error { streamed = append(streamed, node); return nil }))
		check(round, "StreamNodes", streamed)
		var chunked []*Node
		require.NoError(t, engine.StreamNodeChunks(ctx, 10, func(batch []*Node) error { chunked = append(chunked, batch...); return nil }))
		check(round, "StreamNodeChunks", chunked)
		collected, err := CollectStoredNodes(ctx, engine)
		require.NoError(t, err)
		check(round, "CollectStoredNodes", collected)
		var scoped []*Node
		require.NoError(t, NewNamespacedEngine(NewTracedEngine(engine), "test").StreamNodes(ctx, func(node *Node) error { scoped = append(scoped, node); return nil }))
		check(round, "namespace over traced StreamNodes", scoped)
	}
	// A point read that misses the node cache reads through the same body
	// cache the streams warmed.
	engine.nodeCacheMu.Lock()
	engine.nodeCache = make(map[NodeID]*Node)
	engine.queryNodeCache = nil
	engine.nodeCacheMu.Unlock()
	got, err := engine.GetNode(ctx, separate.ID)
	require.NoError(t, err)
	nor56RequireVectors(t, 3, "GetNode", got, separate)
}
