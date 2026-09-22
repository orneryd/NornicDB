package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnionExecutesAllTopLevelBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "union_semantics"))
	ctx := context.Background()

	tests := []struct {
		name  string
		query string
		rows  [][]interface{}
	}{
		{
			name:  "distinct three branches",
			query: "RETURN 2 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x",
			rows:  [][]interface{}{{int64(2)}, {int64(1)}},
		},
		{
			name:  "all three branches",
			query: "RETURN 2 AS x UNION ALL RETURN 1 AS x UNION ALL RETURN 2 AS x",
			rows:  [][]interface{}{{int64(2)}, {int64(1)}, {int64(2)}},
		},
		{
			name:  "distinct unwind branches",
			query: "UNWIND [2, 1, 2, 3] AS x RETURN x UNION UNWIND [3, 4] AS x RETURN x",
			rows:  [][]interface{}{{int64(2)}, {int64(1)}, {int64(3)}, {int64(4)}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := exec.Execute(ctx, test.query, nil)
			require.NoError(t, err)
			require.Equal(t, []string{"x"}, result.Columns)
			require.Equal(t, test.rows, result.Rows)
		})
	}
}

func TestUnionRequiresIdenticalProjectedColumnNames(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "union_columns"))
	ctx := context.Background()

	for _, operator := range []string{"UNION", "UNION ALL"} {
		t.Run(operator, func(t *testing.T) {
			_, err := exec.Execute(ctx, "RETURN 1 AS a "+operator+" RETURN 2 AS b", nil)
			require.Error(t, err)

			var semanticError *SemanticError
			require.True(t, errors.As(err, &semanticError))
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
			require.Equal(t, "DifferentColumnsInUnion", semanticError.Detail)
		})
	}
}

func TestUnionRejectsMixedDistinctAndAllOperators(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "union_operator_mode"))
	ctx := context.Background()

	queries := []string{
		"RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a",
		"RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a",
	}
	for _, query := range queries {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err)

		var semanticError *SemanticError
		require.True(t, errors.As(err, &semanticError))
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semanticError.Code)
		require.Equal(t, "InvalidClauseComposition", semanticError.Detail)
	}
}
