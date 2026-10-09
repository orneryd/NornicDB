package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The transaction wrapper streams one relationship type of its database —
// committed edges overlaid with the transaction's pending writes — with
// user-facing IDs, so constraint validation inside a transaction neither
// loads AllEdges nor sees other databases.
func TestTransactionStorageWrapper_StreamEdgesByType(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	for _, db := range []string{"a", "b"} {
		ns := storage.NewNamespacedEngine(base, db)
		for _, id := range []storage.NodeID{"x", "y"} {
			_, err := ns.CreateNode(&storage.Node{ID: id, Labels: []string{"N"}, Properties: map[string]any{}})
			require.NoError(t, err)
		}
		require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "e1", StartNode: "x", EndNode: "y", Type: "REL", Properties: map[string]any{"k": "dup"}}))
		require.NoError(t, ns.CreateEdge(&storage.Edge{ID: "o1", StartNode: "x", EndNode: "y", Type: "OTHER", Properties: map[string]any{"k": "dup"}}))
	}

	require.NoError(t, base.EnsureNamespaceMVCC("a"))
	tx, err := base.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, tx.SetNamespace("a"))
	require.NoError(t, tx.CreateEdge(&storage.Edge{ID: "a:e2", StartNode: "a:y", EndNode: "a:x", Type: "REL", Properties: map[string]any{"k": "dup"}}))
	wrapper := &transactionStorageWrapper{tx: tx, underlying: storage.NewNamespacedEngine(base, "a"), namespace: "a", separator: ":", mutatedNodeIDs: map[string]struct{}{}}

	var got []string
	require.NoError(t, storage.StreamEdgesByType(context.Background(), wrapper, "REL", func(edge *storage.Edge) error {
		got = append(got, string(edge.ID)+":"+string(edge.StartNode)+"->"+string(edge.EndNode))
		return nil
	}))
	sort.Strings(got)
	require.Equal(t, []string{"e1:x->y", "e2:y->x"}, got)

	err = storage.ValidateConstraintOnCreationForEngine(wrapper, storage.Constraint{
		Name: "u", Type: storage.ConstraintUnique, EntityType: storage.ConstraintEntityRelationship, Label: "REL", Properties: []string{"k"},
	})
	require.Error(t, err, "the pending edge duplicates the committed one")
	require.NotContains(t, err.Error(), "a:")
}
