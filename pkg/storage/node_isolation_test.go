package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTransactionLabelReadsAreIsolated: a node a transaction hands out from
// its label scan is the caller's own, so changing it before UpdateNode leaves
// the old version the update starts from (and the transaction's snapshot)
// unchanged (#963).
func TestTransactionLabelReadsAreIsolated(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&Node{ID: "test:scan", Labels: []string{"Doc"}, Properties: map[string]any{"text": "stored"}})
	require.NoError(t, err)

	tx, err := engine.BadgerEngine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for round := 0; round < 2; round++ { // a fresh scan, then the cached one
		var nodes []*Node
		require.NoError(t, tx.StreamNodesByLabelProjected("Doc", nil, func(node *Node) error {
			nodes = append(nodes, node)
			return nil
		}))
		require.Len(t, nodes, 1)
		require.Equal(t, "stored", nodes[0].Properties["text"])
		nodes[0].Properties["text"] = "changed by the caller"
	}
	snapshot, err := tx.GetNode("test:scan")
	require.NoError(t, err)
	require.Equal(t, "stored", snapshot.Properties["text"])
	require.NoError(t, tx.Rollback())
}
