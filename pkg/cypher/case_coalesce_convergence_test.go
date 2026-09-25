package cypher

// §5.4 verification: CASE/COALESCE short-circuiting, the null/undefined
// distinction, scope shadowing and parameter handling across the converged
// evaluator. Division by zero is the eager-evaluation canary: the row
// evaluator records a DivisionByZero statement failure, so any eager
// evaluation of a skipped branch would surface as an error.

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCaseExpression_ShortCircuitsSkippedBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_short_circuit"))
	ctx := context.Background()

	queries := []struct {
		name   string
		query  string
		params map[string]interface{}
		want   interface{}
	}{
		{
			name:  "searched CASE skips ELSE",
			query: "RETURN CASE WHEN true THEN 1 ELSE 1/0 END AS r",
			want:  int64(1),
		},
		{
			name:  "searched CASE skips later WHEN and ELSE",
			query: "RETURN CASE WHEN false THEN 1/0 WHEN true THEN 3 ELSE 1/0 END AS r",
			want:  int64(3),
		},
		{
			name:  "searched CASE evaluates ELSE when no WHEN matches",
			query: "RETURN CASE WHEN false THEN 1/0 ELSE 2 END AS r",
			want:  int64(2),
		},
		{
			name:  "simple CASE skips ELSE",
			query: "RETURN CASE 1 WHEN 1 THEN 'one' ELSE 1/0 END AS r",
			want:  "one",
		},
		{
			name:  "simple CASE skips unmatched later WHEN values",
			query: "RETURN CASE 1 WHEN 2 THEN 1/0 WHEN 1 THEN 'hit' END AS r",
			want:  "hit",
		},
		{
			name:  "coalesce skips after first non-null",
			query: "RETURN coalesce(1, 1/0) AS r",
			want:  int64(1),
		},
	}
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err, "short-circuit must not surface skipped branch errors")
			require.Equal(t, [][]interface{}{{tc.want}}, res.Rows)
		})
	}
}

func TestCaseExpression_NullAndUndefinedDistinction(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_null_distinct"))
	ctx := context.Background()

	queries := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "undefined identifier IS NULL",
			query: "RETURN CASE WHEN missing IS NULL THEN 'null' ELSE 'other' END AS r",
			want:  "null",
		},
		{
			name:  "undefined comparison is null, not truthy",
			query: "RETURN CASE WHEN missing = 1 THEN 1 ELSE 2 END AS r",
			want:  int64(2),
		},
		{
			name:  "undefined as bare condition is not truthy",
			query: "RETURN CASE WHEN missing THEN 1 ELSE 2 END AS r",
			want:  int64(2),
		},
		{
			name:  "null literal falls through coalesce",
			query: "RETURN coalesce(missing, null, 5) AS r",
			want:  int64(5),
		},
		{
			name:  "CASE without ELSE and no match is null",
			query: "RETURN CASE WHEN false THEN 1 END AS r",
			want:  nil,
		},
	}
	for _, tc := range queries {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, res.Rows)
		})
	}
}

func TestCaseExpression_ScopeShadowingAndReduce(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_scope"))
	ctx := context.Background()

	res, err := exec.Execute(ctx, "UNWIND [1, 2] AS x RETURN x, CASE WHEN x > 1 THEN 'big' ELSE 'small' END AS s ORDER BY x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1), "small"}, {int64(2), "big"}}, res.Rows)

	res, err = exec.Execute(ctx, "RETURN reduce(acc = 0, x IN [1,2,3] | acc + CASE WHEN x > 1 THEN x ELSE 0 END) AS r", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5)}}, res.Rows)
}

func TestCaseExpression_Parameters(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_params"))
	ctx := context.Background()

	tests := []struct {
		name   string
		query  string
		params map[string]interface{}
		want   interface{}
	}{
		{
			name:   "parameter in searched condition",
			query:  "RETURN CASE WHEN $flag THEN 1 ELSE 2 END AS r",
			params: map[string]interface{}{"flag": true},
			want:   int64(1),
		},
		{
			name:   "parameter in simple test expression",
			query:  "RETURN CASE $n WHEN 2 THEN 'two' ELSE 'other' END AS r",
			params: map[string]interface{}{"n": int64(2)},
			want:   "two",
		},
	}
	// A missing parameter is the statement's error, as in Neo4j (#657).
	_, err := exec.Execute(ctx, "RETURN coalesce($missing, 7) AS r", nil)
	require.ErrorContains(t, err, "Neo.ClientError.Statement.ParameterMissing: Expected parameter(s): missing")
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{tc.want}}, res.Rows)
		})
	}
}

func TestCaseExpression_SharedEvaluatorLevel(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "case_shared_level"))

	values := map[string]interface{}{"a": int64(3), "b": int64(2), "s": "x"}
	require.Equal(t, "three", exec.evaluateCaseExpressionFromValues("CASE a WHEN 3 THEN 'three' ELSE 'other' END", values))
	require.Equal(t, "open", exec.evaluateCaseExpressionFromValues("CASE WHEN a = 9 THEN 'closed' ELSE 'open' END", values))
	require.Nil(t, exec.evaluateCaseExpressionFromValues("CASE WHEN a < 0 THEN 2 END", values))
	require.Equal(t, int64(5), exec.evaluateExpressionFromValues("coalesce(missing, null, 5)", values))
	require.Equal(t, "x", exec.evaluateExpressionFromValues("coalesce(null, s, 'y')", values))
}
