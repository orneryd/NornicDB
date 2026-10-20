package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/nestedstats"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNestedCypherUpdateStats(t *testing.T) {
	for _, tc := range nestedstats.Cases() {
		t.Run(tc.Name, func(t *testing.T) {
			run := func(query string) *QueryStats {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nestedstats"))
				if tc.Setup != "" {
					_, err := exec.Execute(context.Background(), tc.Setup, nil)
					require.NoError(t, err)
				}
				result, err := exec.Execute(context.Background(), query, tc.Params)
				require.NoError(t, err)
				if result.Stats == nil {
					return &QueryStats{}
				}
				return result.Stats
			}
			require.Equal(t, run(tc.Direct), run(tc.Query))
		})
	}
}

func BenchmarkNestedCypherRun(b *testing.B) {
	store := newTestMemoryEngine(b)
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	params := map[string]any{"body": "RETURN 1 AS value"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "CALL apoc.cypher.run($body,{}) YIELD value RETURN value", params); err != nil {
			b.Fatal(err)
		}
	}
}

func TestNestedCypherRollbackStats(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nestedrollback")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "CALL apoc.cypher.doIt($body,{}) YIELD value RETURN value", map[string]any{"body": "CREATE (:NestedRollback) RETURN 1 AS value"})
	require.NoError(t, err)
	require.Equal(t, 1, result.Stats.NodesCreated)
	rollback, err := exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	require.True(t, rollback.Stats == nil || *rollback.Stats == (QueryStats{}))
	result, err = exec.Execute(ctx, "MATCH (n:NestedRollback) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, result.Rows[0][0])
	require.True(t, result.Stats == nil || *result.Stats == (QueryStats{}))
}

func TestNestedCypherVoidNilTail(t *testing.T) {
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "nestedstats.nil", Mode: ProcedureModeWrite, MinArgs: 0, MaxArgs: 0}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return nil, nil
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nil_tail"))
	require.NotPanics(t, func() {
		result, err := exec.Execute(context.Background(), "CALL nestedstats.nil() RETURN 1 AS value", nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, result.Rows[0][0])
		require.True(t, result.Stats == nil || *result.Stats == (QueryStats{}))
	})
}

func TestNestedCypherVoidStatementStats(t *testing.T) {
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: "nestedstats.nilwrite", Mode: ProcedureModeWrite, MinArgs: 0, MaxArgs: 0}, func(ctx context.Context, exec *StorageExecutor, _ string, _ []interface{}) (*ExecuteResult, error) {
		_, err := exec.executeUserStatement(ctx, "CREATE (:VoidNestedStats)", nil)
		return nil, err
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nil_write"))
	result, err := exec.Execute(context.Background(), "CALL nestedstats.nilwrite() RETURN 1 AS value", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Rows[0][0])
	require.Equal(t, 1, result.Stats.NodesCreated)
}
