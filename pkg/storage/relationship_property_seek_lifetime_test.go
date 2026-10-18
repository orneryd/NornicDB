package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Retain the transaction handle after each terminal outcome. Its immutable
// equality set must be released at the common snapshot close boundary, including
// failed commits; an admission rejection that leaves it active keeps its view.
func TestRelationshipEqualitySeekSnapshotOwnerLifetime(t *testing.T) {
	for _, end := range []string{"commit", "rollback", "conflict", "cancel_then_rollback"} {
		t.Run(end, func(t *testing.T) {
			engine, err := NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			ns := NewNamespacedEngine(engine, "test")
			for _, id := range []NodeID{"a", "b"} {
				_, err := ns.CreateNode(&Node{ID: id})
				require.NoError(t, err)
			}
			require.NoError(t, ns.CreateEdge(&Edge{ID: "e", StartNode: "a", EndNode: "b", Type: "T", Properties: map[string]interface{}{"p": "old"}}))
			tx, err := engine.BeginTransaction()
			require.NoError(t, err)
			defer tx.Rollback()
			require.NoError(t, tx.SetNamespace("test"))
			edges, answered, err := tx.SeekRelationships(context.Background(), "T", []string{"p"}, []interface{}{"old"})
			require.NoError(t, err)
			require.True(t, answered)
			require.Len(t, edges, 1)
			require.NotNil(t, tx.relationshipSeekIndexes)
			require.Len(t, tx.relationshipSeekIndexes.entries, 1)
			require.Empty(t, engine.relationshipUniqueIndexes.entries, "snapshot indexes must never join the engine set")
			switch end {
			case "commit":
				require.NoError(t, tx.Commit())
			case "rollback":
				require.NoError(t, tx.Rollback())
			case "conflict":
				edges[0].Properties["p"] = "mine"
				require.NoError(t, tx.UpdateEdge(edges[0]))
				peer, err := ns.GetEdge("e")
				require.NoError(t, err)
				peer.Properties["p"] = "peer"
				require.NoError(t, ns.UpdateEdge(peer))
				require.ErrorIs(t, tx.Commit(), ErrConflict)
			case "cancel_then_rollback":
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, tx.CommitContext(ctx), context.Canceled)
				require.True(t, tx.IsActive())
				require.NotNil(t, tx.relationshipSeekIndexes)
				require.NoError(t, tx.Rollback())
			}
			require.False(t, tx.IsActive())
			require.Nil(t, tx.relationshipSeekIndexes, "ended transaction retained a graph-sized equality set")
		})
	}
}
// Package storage - BadgerDB transaction wrapper with ACID guarantees.
//
// This file implements atomic transactions for BadgerDB with full constraint
// validation and rollback support.
