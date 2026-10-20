package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecutionOutputBudgetOwnsNativeResult(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	for _, query := range []string{
		"UNWIND range(1,200000) AS x RETURN x",
		"UNWIND range(1,200000) AS `x` RETURN `x`",
		"PROFILE UNWIND range(1,200000) AS x RETURN x",
		"CALL apoc.cypher.run('UNWIND range(1,200000) AS x RETURN x',{}) YIELD value RETURN value.x AS x",
	} {
		t.Run(query, func(t *testing.T) {
			ctx := WithResultBudget(WithStatementMemoryLimit(context.Background(), 16<<20), 1, 1<<20)
			result, err := executor.Execute(ctx, query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, result.Columns)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(1), result.Rows[0][0])
			require.Contains(t, resultstream.BudgetFromContext(ctx).Reasons, "max_rows")
			global, database := TransactionMemoryPoolUsage("test")
			require.Zero(t, global)
			require.Zero(t, database)
		})
	}
}
