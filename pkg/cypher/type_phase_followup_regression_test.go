package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTypePhaseFollowupRegressions(t *testing.T) {
	for _, tc := range []struct {
		name, query, code string
		rows              [][]interface{}
	}{
		{name: "size_nullable_map", query: "RETURN size(CASE WHEN true THEN {a: 1} ELSE null END)", code: "Neo.ClientError.Statement.TypeError"},
		{name: "float_product_list_index", query: "WITH {k: 1} AS n RETURN [10, 20][n.k * 1.5] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "size_static_map", query: "RETURN size({a: 1})", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "size_nullable_string", query: "RETURN size(CASE WHEN true THEN 'ä😊' ELSE null END)", rows: [][]interface{}{{int64(2)}}},
		{name: "size_nullable_list", query: "RETURN size(CASE WHEN true THEN [1, 2] ELSE null END)", rows: [][]interface{}{{int64(2)}}},
		{name: "size_null_branch", query: "RETURN size(CASE WHEN false THEN {a: 1} ELSE null END)", rows: [][]interface{}{{nil}}},
		{name: "float_left_product_list_index", query: "WITH {k: 1} AS n RETURN [10, 20][1.5 * n.k] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "null_product_list_index", query: "WITH null AS n RETURN [10, 20][n * 1.5] AS v", code: "Neo.ClientError.Statement.SyntaxError"},
		{name: "null_list_index", query: "WITH null AS n RETURN [10, 20][n] AS v", rows: [][]interface{}{{nil}}},
		{name: "integer_product_list_index", query: "WITH {k: 1} AS n RETURN [10, 20][n.k * 1] AS v", rows: [][]interface{}{{int64(20)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "type_phase"))
			result, err := executor.Execute(context.Background(), tc.query, nil)
			if tc.code != "" {
				require.Error(t, err)
				var classified interface{ BoltErrorCode() string }
				require.True(t, errors.As(err, &classified), "%v", err)
				require.Equal(t, tc.code, classified.BoltErrorCode(), "%v", err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.rows, result.Rows)
			}
		})
	}
}
