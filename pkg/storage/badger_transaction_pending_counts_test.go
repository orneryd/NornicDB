package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBadgerTransactionPendingCountDeltas: a transaction's staged label and
// relationship-type count changes, for its own namespace only; a transaction
// not pinned to the namespace keeps none (ok false).
func TestBadgerTransactionPendingCountDeltas(t *testing.T) {
	base, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })

	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, tracked := tx.PendingNodeLabelCountDelta("tenant", "L")
	require.False(t, tracked, "not pinned yet")
	_, tracked = tx.PendingNodeLabelCountDelta("", "L")
	require.False(t, tracked)

	for _, id := range []NodeID{"tenant:a", "tenant:b"} {
		_, err := tx.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, tx.CreateEdge(&Edge{ID: "tenant:e", StartNode: "tenant:a", EndNode: "tenant:b", Type: "T"}))

	delta, tracked := tx.PendingNodeLabelCountDelta("tenant", "L")
	require.True(t, tracked)
	require.EqualValues(t, 2, delta)
	delta, tracked = tx.PendingEdgeTypeCountDelta("tenant", "T")
	require.True(t, tracked)
	require.EqualValues(t, 1, delta)
	_, tracked = tx.PendingEdgeTypeCountDelta("other", "T")
	require.False(t, tracked)
	_, tracked = tx.PendingEdgeTypeCountDelta("", "T")
	require.False(t, tracked)

	require.NoError(t, tx.DeleteEdge("tenant:e"))
	require.NoError(t, tx.DeleteNode("tenant:b"))
	delta, _ = tx.PendingNodeLabelCountDelta("tenant", "L")
	require.EqualValues(t, 1, delta)
	delta, _ = tx.PendingEdgeTypeCountDelta("tenant", "T")
	require.EqualValues(t, 0, delta)
}
