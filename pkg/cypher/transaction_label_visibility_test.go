package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

// A transaction pins data versions, not the live manual visibility policy.
// Replaying its label population must agree with point reads after suppression.
func TestExplicitTransactionLabelRowsFollowLiveSuppression(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {
		for _, suffix := range []string{"RETURN n", "RETURN n.value", "RETURN count(n)"} {
			t.Run(suffix, func(t *testing.T) {
				base := newTestMemoryEngine(t)
				store := storage.NewNamespacedEngine(base, "labelvisibility")
				ctx := t.Context()
				id, err := store.CreateNode(ctx, &storage.Node{ID: "one", Labels: []string{"Visible"}, Properties: map[string]any{"value": int64(7)}})
				require.NoError(t, err)
				exec := NewStorageExecutor(store)
				exec.cache = nil
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
				query := "MATCH (n:Visible) " + suffix
				first, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Len(t, first.Rows, 1)
				require.NoError(t, store.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(id), true))
				tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
				require.True(t, ok)
				_, err = tx.GetQueryNode(ctx, "labelvisibility:one")
				require.ErrorIs(t, err, storage.ErrNotFound)
				hidden, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				if suffix == "RETURN count(n)" {
					require.Equal(t, [][]any{{int64(0)}}, hidden.Rows)
				} else {
					require.Empty(t, hidden.Rows)
				}
				require.NoError(t, store.SetManualSuppression(ctx, storage.StoredEntityNodeKind, string(id), false))
				shown, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, first.Rows, shown.Rows)
			})
		}
	})
}
