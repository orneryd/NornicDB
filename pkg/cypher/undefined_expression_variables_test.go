package cypher

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A variable read anywhere in a RETURN, WITH or UNWIND expression must be
// bound, inside a CASE, a list, a map projection or a comprehension too
// (Neo4j 5.26.30, #907). Label expressions, normal forms and the
// expression's own variables are not variables to bind.
func TestUndefinedVariablesInExpressionsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "undefined_expressions"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:A)", nil)
	require.NoError(t, err)

	for _, query := range []string{
		"UNWIND [zz{.a}] AS v RETURN v",
		"RETURN CASE WHEN zz{.a} THEN 1 ELSE 0 END AS v",
		"RETURN [x IN [1] | CASE zz WHEN 1 THEN 1 END] AS v",
		"UNWIND [[x IN [1] WHERE x = zz]] AS v RETURN v",
		"WITH [x IN [1] | x + zz] AS v RETURN v",
		"WITH 1 AS a RETURN {b: a, c: [x IN [a] | x * zz]} AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", code)
			require.Contains(t, err.Error(), "zz")
		})
	}

	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (n) RETURN n:A|B AS v LIMIT 1", true},
		{"MATCH (n) RETURN n:B&!A AS v LIMIT 1", false},
		{"RETURN 'a' IS NFC NORMALIZED AS v", true},
		{"WITH {a: 1} AS m RETURN m{.a, b: [y IN [1] | y]} AS v", map[string]interface{}{"a": int64(1), "b": []interface{}{int64(1)}}},
		{"RETURN reduce(acc = 0, x IN [1, 2] | acc + x) AS v", int64(3)},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}
}
