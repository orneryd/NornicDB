package cypher

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The existing fill-entry observer cancels a real public DDL request after its
// stored-node collection, before the storage backfill prepares posting keys.
func TestDDLIndexNodeCancellationPublishesNothingAndHealthyRetry(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNICDB_PARSER", parser)
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ddl_cancel")
			exec := NewStorageExecutor(store)
			_, err := exec.Execute(t.Context(), "CREATE (:Item {p:'kept',q:1})", nil)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var observed atomic.Bool
			t.Cleanup(storage.SetSchemaIndexFillHookForTesting(func(name, label string) error {
				if name == "cancel_pair" && label == "Item" && observed.CompareAndSwap(false, true) {
					cancel()
				}
				return nil
			}))
			_, err = exec.Execute(ctx, "CREATE INDEX cancel_pair FOR (n:Item) ON (n.p,n.q)", nil)
			require.True(t, observed.Load(), "request must reach the actual storage fill owner")
			require.ErrorIs(t, err, context.Canceled)
			_, found := store.GetSchema().GetCompositeIndex("cancel_pair")
			require.False(t, found, "canceled schema transaction publishes no index")
			_, err = exec.Execute(t.Context(), "CREATE INDEX cancel_pair FOR (n:Item) ON (n.p,n.q)", nil)
			require.NoError(t, err)
			idx, found := store.GetSchema().GetCompositeIndex("cancel_pair")
			require.True(t, found)
			ids, err := idx.LookupFull(t.Context(), "kept", int64(1))
			require.NoError(t, err)
			require.Len(t, ids, 1)
			result, err := exec.Execute(t.Context(), "MATCH (n:Item {p:'kept',q:1}) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}
