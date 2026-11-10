package storage

// A batch read without stored vectors reads a node's body from either image
// the node read cache holds of it, and keeps the query image of a node it had
// to decode (NOR-407).

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"testing"

	"github.com/stretchr/testify/require"
)

// dropNodeReadCaches empties the engine's two node read caches.
func dropNodeReadCaches(engine *BadgerEngine) {
	engine.nodeCacheMu.Lock()
	engine.resetNodeReadCachesLocked()
	engine.nodeCacheMu.Unlock()
}

func TestBatchReadWithoutVectorsReadsThroughTheNodeCache(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	ids := []NodeID{"a:1", "a:2"}
	for _, id := range ids {
		_, err := engine.CreateNode(t.Context(), &Node{
			ID:              id,
			Labels:          []string{"Doc"},
			Properties:      map[string]interface{}{"title": string(id)},
			NamedEmbeddings: map[string][]float32{"default": {0.5, 0.25}},
		})
		require.NoError(t, err)
	}
	read := func(title func(NodeID) string) {
		t.Helper()
		nodes, err := engine.BatchGetNodesWithoutEmbeddings(t.Context(), ids)
		require.NoError(t, err)
		require.Len(t, nodes, len(ids))
		for _, id := range ids {
			require.Equal(t, []string{"Doc"}, nodes[id].Labels)
			require.Equal(t, map[string]interface{}{"title": title(id)}, nodes[id].Properties)
			require.Empty(t, nodes[id].NamedEmbeddings)
			require.Empty(t, nodes[id].ChunkEmbeddings)
		}
	}
	// The cache holds nothing of the nodes: the read decodes them, and keeps
	// their query images.
	dropNodeReadCaches(engine)
	read(func(id NodeID) string { return string(id) })
	engine.nodeCacheMu.Lock()
	kept, full := len(engine.queryNodeCache), len(engine.nodeCache)
	// Mark the images, to tell a read of them from a read of the store.
	for _, id := range ids {
		if image := engine.queryNodeCache[id]; image != nil {
			marked := copyNode(image)
			marked.Properties["title"] = "image of " + string(id)
			engine.queryNodeCache[id] = marked
		}
	}
	engine.nodeCacheMu.Unlock()
	require.Equal(t, len(ids), kept)
	require.Zero(t, full, "a read without vectors keeps no full image")

	// The next read is served by the query images alone.
	read(func(id NodeID) string { return "image of " + string(id) })
}
