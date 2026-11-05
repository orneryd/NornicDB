package storage

import (
	"github.com/stretchr/testify/require"
	"testing"
)

// Snapshot-bound decoding may consult this transaction's token claims, but must
// neither stage durable work nor hand out a mutable stored image.
func TestStoredEdgeDecodePreservesCommitWork(t *testing.T) {
	engine := createTestBadgerEngine(t)
	for _, id := range []NodeID{"decode:a", "decode:b"} {
		_, err := engine.CreateNode(t.Context(), &Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(t.Context(), &Edge{ID: "decode:e", StartNode: "decode:a", EndNode: "decode:b", Type: "LINK", Properties: map[string]any{"weight": int64(3)}}))
	tx, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	require.NoError(t, tx.SetNamespace("decode"))
	before := tx.PendingChanges(true)
	require.Empty(t, tx.pendingWrites)
	require.Empty(t, tx.pendingDeletes)
	require.Empty(t, tx.GetMetadata())
	require.False(t, tx.HasKnowledgePolicyChanges())
	first, err := tx.GetEdge(t.Context(), "decode:e")
	require.NoError(t, err)
	require.Equal(t, int64(3), first.Properties["weight"])
	first.Properties["weight"] = int64(99)
	again, err := tx.GetEdge(t.Context(), "decode:e")
	require.NoError(t, err)
	require.Equal(t, int64(3), again.Properties["weight"])
	require.Equal(t, NodeID("decode:a"), again.StartNode)
	require.Equal(t, NodeID("decode:b"), again.EndNode)
	require.Equal(t, before, tx.PendingChanges(true), "reading token bindings must not stage entity work")
	require.Empty(t, tx.pendingWrites, "token reads stage no native writes")
	require.Empty(t, tx.pendingDeletes, "token reads stage no native deletes")
	require.Empty(t, tx.GetMetadata())
	require.False(t, tx.HasKnowledgePolicyChanges())
}
