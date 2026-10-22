package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// A shared equality index must retain stored holders for integrity checks,
// while every query view applies the visibility owner, including a later
// suppression change and a transaction's pending update of a hidden record.
func TestRelationshipEqualitySeekPreservesStoredHoldersAndVisibleReads(t *testing.T) {
	engine, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	ns := NewNamespacedEngine(engine, "test")
	for _, id := range []NodeID{"a", "b"} {
		_, err := ns.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, ns.GetSchema().AddRangeIndexForEntity("query_key", "KEYED", []string{"key"}, ConstraintEntityRelationship))
	require.NoError(t, ns.CreateEdge(&Edge{ID: "hidden", StartNode: "a", EndNode: "b", Type: "KEYED", Properties: map[string]interface{}{"key": "held"}}))
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", true))
	query := func(value string) []*Edge {
		t.Helper()
		edges, answered, err := ns.SeekRelationships(context.Background(), "KEYED", []string{"key"}, []interface{}{value})
		require.NoError(t, err)
		require.True(t, answered)
		return edges
	}
	// Populate through a query first: uniqueness must reuse a complete stored
	// population, even though that first query cannot see the holder.
	require.Empty(t, query("held"))
	require.NoError(t, ns.GetSchema().AddConstraint(Constraint{Name: "unique_key", Type: ConstraintUnique, EntityType: ConstraintEntityRelationship, Label: "KEYED", Properties: []string{"key"}}))
	require.Error(t, ns.CreateEdge(&Edge{ID: "duplicate", StartNode: "a", EndNode: "b", Type: "KEYED", Properties: map[string]interface{}{"key": "held"}}))
	_, err = engine.GetStoredEdge("test:hidden")
	require.NoError(t, err)
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", false))
	require.Len(t, query("held"), 1)
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", true))
	require.Empty(t, query("held"))

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tx.Rollback()) })
	require.NoError(t, tx.SetNamespace("test"))
	seek := func(value string) []*Edge {
		t.Helper()
		edges, answered, err := tx.SeekRelationships(context.Background(), "KEYED", []string{"key"}, []interface{}{value})
		require.NoError(t, err)
		require.True(t, answered)
		return edges
	}
	require.Empty(t, seek("held"))
	// The snapshot index is immutable, but visibility remains the selected
	// view's decision; clearing a manual suppression cannot leave an index miss.
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", false))
	require.Len(t, seek("held"), 1)
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", true))
	stored, err := tx.GetStoredEdge("test:hidden")
	require.NoError(t, err)
	stored.Properties["key"] = "pending"
	require.NoError(t, tx.UpdateEdge(stored))
	require.Empty(t, seek("held"))
	require.Empty(t, seek("pending"))
	stored, err = tx.GetStoredEdge("test:hidden")
	require.NoError(t, err)
	require.Equal(t, "pending", stored.Properties["key"])
	require.NoError(t, engine.SetEdgeManuallySuppressed("test:hidden", false))
	require.Len(t, seek("pending"), 1)
}
