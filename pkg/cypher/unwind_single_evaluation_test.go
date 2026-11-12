package cypher

// The statement's leading UNWIND evaluates its list expression once: the
// evaluation that prepares the statement's items is the evaluation the
// clause's rows read, and the general row operators no longer evaluate the
// same expression a second time. The statement gate counts those evaluations
// without PROFILE, so the count is the public behavior.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeadingUnwindEvaluatesItsListOnce(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		params      map[string]interface{}
		rows        [][]interface{}
		empty       bool
		calls       int64
	}{
		{name: "list", query: "UNWIND nornicdb.testgate([1, 2]) AS value RETURN value", rows: [][]interface{}{{int64(1)}, {int64(2)}}, calls: 1},
		{name: "null", query: "UNWIND nornicdb.testgate(null) AS value RETURN value", empty: true, calls: 1},
		{name: "scalar", query: "UNWIND nornicdb.testgate(7) AS value RETURN value", rows: [][]interface{}{{int64(7)}}, calls: 1},
		{name: "empty list", query: "UNWIND nornicdb.testgate([]) AS value RETURN value", empty: true, calls: 1},
		{name: "parameter in the list expression", query: "UNWIND nornicdb.testgate($items) AS value RETURN value", params: map[string]interface{}{"items": []interface{}{"a", "b"}}, rows: [][]interface{}{{"a"}, {"b"}}, calls: 1},
		{name: "parameter list", query: "UNWIND $items AS value RETURN value", params: map[string]interface{}{"items": []interface{}{int64(1), int64(2)}}, rows: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "second identical UNWIND", query: "UNWIND nornicdb.testgate([1]) AS a UNWIND nornicdb.testgate([1]) AS b RETURN a, b", rows: [][]interface{}{{int64(1), int64(1)}}, calls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := newStatementGate(nil).install(t)
			exec, ctx := newConvergenceExecutor(t)
			result, err := exec.Execute(ctx, tc.query, tc.params)
			require.NoError(t, err)
			if tc.empty {
				require.Empty(t, result.Rows, tc.query)
			} else {
				require.Equal(t, tc.rows, result.Rows, tc.query)
			}
			require.EqualValues(t, tc.calls, gate.Calls(), "%s evaluates its UNWIND list expression %d time(s)", tc.query, tc.calls)
			require.NoError(t, gate.Err())
		})
	}
}

// A body that imports a binding evaluates its own UNWIND for each invocation,
// a range stays a stream, and the batch operator keeps the rows it already
// produced: the handoff belongs to the one statement that prepared its items.
func TestLeadingUnwindEvaluationStaysWithItsStatement(t *testing.T) {
	t.Run("correlated body evaluates per invocation", func(t *testing.T) {
		gate := newStatementGate(nil).install(t)
		exec, ctx := newConvergenceExecutor(t)
		result, err := exec.Execute(ctx, "UNWIND [1, 2] AS seed CALL (seed) { UNWIND nornicdb.testgate([seed, seed * 10]) AS value RETURN value } RETURN seed, value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{
			{int64(1), int64(1)},
			{int64(1), int64(10)},
			{int64(2), int64(2)},
			{int64(2), int64(20)},
		}, result.Rows)
		require.EqualValues(t, 2, gate.Calls(), "a body importing a binding evaluates its UNWIND once per invocation")
		require.NoError(t, gate.Err())
	})

	t.Run("range streams", func(t *testing.T) {
		exec, ctx := newConvergenceExecutor(t)
		result, err := exec.Execute(ctx, "UNWIND range(1, 3) AS value RETURN value", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, result.Rows)
	})

	t.Run("independent create batch keeps its rows", func(t *testing.T) {
		gate := newStatementGate(nil).install(t)
		exec, ctx := newConvergenceExecutor(t)
		_, err := exec.Execute(ctx, "UNWIND nornicdb.testgate([1, 2]) AS x CREATE (n:UnwindOnceBatch {v: x})", nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, gate.Calls(), "the batch operator runs the items the statement prepared")
		require.NoError(t, gate.Err())
		require.Equal(t, [][]interface{}{{int64(2)}}, countRows(t, exec, "UnwindOnceBatch"))
	})
}
