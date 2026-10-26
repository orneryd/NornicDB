package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/internal/callscopetest"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These public continuations pin the consumers found by the design check.
// Expected rows are independent of the scope walker and physical operator.
func TestCallScopeDesignConsumers(t *testing.T) {
	for _, tc := range callscopetest.DesignCases() {
		t.Run(tc.Name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-design"))
			result, err := exec.Execute(context.Background(), tc.Query, nil)
			require.NoError(t, err, tc.Query)
			require.Equal(t, tc.Columns, result.Columns)
			require.Equal(t, tc.Rows, result.Rows)
		})
	}
}

// A UNION makes rewriting inspect the enclosing frame. OPTIONAL is part of
// the CALL header, so the prefix must end before OPTIONAL rather than CALL.
func TestCallScopeOptionalUnionRewrite(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-design"))
	result, err := exec.Execute(context.Background(), "UNWIND [1,2] AS i OPTIONAL CALL (i) { WITH i WHERE i > 1 RETURN i AS j UNION WITH i WHERE i > 1 RETURN i * 10 AS j } RETURN i, j ORDER BY i,j", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"i", "j"}, result.Columns)
	require.Equal(t, [][]interface{}{{int64(1), nil}, {int64(2), int64(2)}, {int64(2), int64(20)}}, result.Rows)
}

// Frame discovery uses canonical clauses and expression bindings, including
// CASE keywords, soft keyword aliases, lexical locals, WITH tails and LET order.
func TestCallScopeRewriteExpressionFrames(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		columns     []string
		rows        [][]interface{}
	}{
		{"case", "RETURN CASE WHEN NOT EXISTS { MATCH (n:Nope) RETURN n.id AS i UNION RETURN 7 AS i } THEN 1 ELSE 2 END AS v", []string{"v"}, [][]interface{}{{int64(2)}}},
		{"keyword_alias", "WITH 1 AS when RETURN CASE WHEN EXISTS { RETURN when AS x UNION RETURN when AS x } THEN when ELSE 0 END AS v", []string{"v"}, [][]interface{}{{int64(1)}}},
		{"lexical", "WITH 1 AS a RETURN [x IN [a] | COUNT { RETURN x AS value UNION RETURN x AS value }] AS values", []string{"values"}, [][]interface{}{{[]interface{}{int64(1)}}}},
		{"with_tail", "WITH 1 AS a WITH a AS x WHERE EXISTS { RETURN x AS y UNION RETURN x AS y } RETURN x", []string{"x"}, [][]interface{}{{int64(1)}}},
		{"let_item", "CYPHER 25 LET a=1, b=COUNT { RETURN a AS x UNION RETURN a AS x } RETURN b", []string{"b"}, [][]interface{}{{int64(1)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "scope-rewrite"))
			result, err := exec.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, tc.columns, result.Columns)
			require.Equal(t, tc.rows, result.Rows)
		})
	}
}
