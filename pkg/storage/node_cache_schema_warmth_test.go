package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Namespaced writes register new labels/types before the entity commit. Schema
// token persistence must revoke old callbacks without expiring unrelated bodies.
func TestNodeCacheSchemaTokensKeepWarmNodes(t *testing.T) {
	b := NewMemoryEngine().BadgerEngine
	t.Cleanup(func() { _ = b.Close() })
	store := NewNamespacedEngine(b, "warm")
	for _, id := range []NodeID{"a", "b"} {
		_, err := store.CreateNode(&Node{ID: id, Labels: []string{"Person"}, Properties: map[string]any{"id": string(id)}})
		require.NoError(t, err)
	}
	check := func() {
		t.Helper()
		b.nodeCacheMu.RLock()
		_, a := b.cachedNodeLocked("warm:a", false, 0)
		_, z := b.cachedNodeLocked("warm:b", false, 0)
		b.nodeCacheMu.RUnlock()
		require.True(t, a, "schema-only persistence must preserve the first node")
		require.True(t, z, "schema-only persistence must preserve the second node")
	}
	check()
	require.NoError(t, store.CreateEdge(&Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "FIRST_TYPE"}))
	check()
	require.NoError(t, store.GetSchema().RegisterTokens([]string{"NewLabel"}, []string{"NEW_TYPE"}))
	check()
	got, err := store.GetNode("a")
	require.NoError(t, err)
	require.Equal(t, "a", got.Properties["id"])
}
