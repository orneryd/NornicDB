package cypher_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Public handlers receive the transaction-scoped executor. Querying it through
// Execute must see each pending write, rather than a shared result-cache entry.
func TestPublicProcedureCallbackReadsPendingImplicitWrites(t *testing.T) {
	for _, carrier := range []string{"callback context and storage", "storage only", "context only"} {
		t.Run(carrier, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			t.Cleanup(cypher.ClearUserProcedures)
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
			const read = "MATCH (n:CallbackTxCache) RETURN count(n) AS c"
			// Warm the committed cache before entering the implicit transaction.
			before, err := exec.Execute(context.Background(), read, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, before.Rows)
			require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{
				Name: "cacheprobe.readcount", Signature: "cacheprobe.readcount() :: (c :: INTEGER)",
				Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0, Returns: []cypher.ProcedureColumn{{Name: "c", Type: "INTEGER"}},
			}, func(ctx context.Context, nested *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
				if carrier == "storage only" {
					ctx = context.Background()
				} else if carrier == "context only" {
					nested = exec
				}
				return nested.Execute(ctx, read, nil)
			}))
			const query = `CREATE (:CallbackTxCache)
			WITH 1 AS keep CALL cacheprobe.readcount() YIELD c
			WITH c AS first CREATE (:CallbackTxCache)
			WITH first CALL cacheprobe.readcount() YIELD c RETURN first,c`
			for _, want := range [][][]interface{}{{{int64(1), int64(2)}}, {{int64(3), int64(4)}}} {
				result, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, want, result.Rows)
			}
			result, err := exec.Execute(context.Background(), read, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(4)}}, result.Rows)
		})
	}
}
