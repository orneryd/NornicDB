package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNodeCacheUnrelatedWritesKeepWarmNodes(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	b.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
	target := &Node{ID: "test:warm-target", Properties: map[string]any{"v": int64(7)}, ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err := b.CreateNode(target)
	require.NoError(t, err)
	check := func() {
		t.Helper()
		b.nodeCacheMu.RLock()
		n, warm := b.cachedNodeLocked(target.ID, false, 0)
		b.nodeCacheMu.RUnlock()
		require.True(t, warm, "unrelated mutation must not force a fresh decode")
		require.Equal(t, int64(7), n.Properties["v"])
		got, err := b.GetNode(target.ID)
		require.NoError(t, err)
		require.Equal(t, n.Properties, got.Properties)
	}
	other := &Node{ID: "test:warm-other", ChunkEmbeddings: [][]float32{{1, 0}}}
	_, err = b.CreateNode(other)
	require.NoError(t, err)
	check()
	other.Properties = map[string]any{"v": int64(1)}
	require.NoError(t, b.UpdateNode(other))
	check()
	edge := &Edge{ID: "test:warm-edge", StartNode: target.ID, EndNode: other.ID, Type: "WARM"}
	require.NoError(t, b.CreateEdge(edge))
	check()
	edge.Properties = map[string]any{"v": int64(1)}
	require.NoError(t, b.UpdateEdge(edge))
	check()
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	other.Properties["v"] = int64(2)
	require.NoError(t, tx.UpdateNode(other))
	require.NoError(t, tx.Commit())
	check()
	require.NoError(t, b.DeleteNode(other.ID))
	check()
}
