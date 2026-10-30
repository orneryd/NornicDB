package cypher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNestedSubqueryStaticValidationBeforeExecution(t *testing.T) {
	for _, query := range []string{
		"RETURN COUNT { MATCH (exists {.a}) } AS v",
		"RETURN {n: COUNT { MATCH (exists {.a}) }} AS v",
		"RETURN [COUNT { MATCH (exists {.a}) }] AS v",
		"WITH 1 AS i CALL (i) { UNWIND [2,1] AS x ORDER BY x LIMIT i RETURN collect(x) AS v } RETURN v",
		"WITH 1 AS i CALL (i) { UNWIND [2,1] AS x ORDER BY x SKIP i RETURN collect(x) AS v } RETURN v",
		"WITH 1 AS i CALL (i) { RETURN 1 AS x LIMIT i } RETURN x",
	} {
		t.Run(query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			var semantic *SemanticError
			require.True(t, errors.As(err, &semantic))
			require.Equal(t, "Neo.ClientError.Statement.SyntaxError", semantic.Code)
		})
	}
}

func TestNestedSubqueryStaticValidationControls(t *testing.T) {
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
		want   interface{}
	}{
		{"RETURN COUNT { MATCH (n {a: 1}) } AS v", nil, int64(0)},
		{"WITH {a:1} AS m RETURN COUNT { WITH m RETURN m {.a} } AS v", nil, int64(1)},
		{"WITH 1 AS i CALL (i) { UNWIND [2,1] AS x ORDER BY x LIMIT 1 RETURN collect(x) AS v } RETURN v", nil, []interface{}{int64(1)}},
		{"WITH 1 AS i CALL (i) { UNWIND [2,1] AS x ORDER BY x LIMIT $limit RETURN collect(x) AS v } RETURN v", map[string]interface{}{"limit": int64(1)}, []interface{}{int64(1)}},
		{"RETURN 'CALL (i) { RETURN 1 LIMIT i }' AS v", nil, "CALL (i) { RETURN 1 LIMIT i }"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			exec, ctx := newUnitExecutor(t)
			result, err := exec.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, result.Rows)
		})
	}
}
