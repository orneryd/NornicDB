package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBulkCreatedRelationshipPublishesVersionedAdjacency(t *testing.T) {
	engine := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "snapshot_bulk:owner", Labels: []string{"Owner"}},
		{ID: "snapshot_bulk:item", Labels: []string{"Item"}},
	}))

	beforeCreate, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, beforeCreate.SetNamespace("snapshot_bulk"))
	t.Cleanup(func() {
		if beforeCreate.Status == TxStatusActive {
			_ = beforeCreate.Rollback()
		}
	})

	require.NoError(t, engine.BulkCreateEdges([]*Edge{{
		ID:        "snapshot_bulk:relationship",
		StartNode: "snapshot_bulk:owner",
		EndNode:   "snapshot_bulk:item",
		Type:      "HAS",
	}}))

	beforeOutgoing, err := beforeCreate.GetOutgoingEdges("snapshot_bulk:owner")
	require.NoError(t, err)
	require.Empty(t, beforeOutgoing, "a snapshot opened before the create must not observe the relationship")
	require.NoError(t, beforeCreate.Rollback())

	afterCreate, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, afterCreate.SetNamespace("snapshot_bulk"))
	t.Cleanup(func() {
		if afterCreate.Status == TxStatusActive {
			_ = afterCreate.Rollback()
		}
	})

	outgoing, err := afterCreate.GetOutgoingEdges("snapshot_bulk:owner")
	require.NoError(t, err)
	require.Len(t, outgoing, 1)
	require.Equal(t, EdgeID("snapshot_bulk:relationship"), outgoing[0].ID)

	incoming, err := afterCreate.GetIncomingEdges("snapshot_bulk:item")
	require.NoError(t, err)
	require.Len(t, incoming, 1)
	require.Equal(t, EdgeID("snapshot_bulk:relationship"), incoming[0].ID)
}
