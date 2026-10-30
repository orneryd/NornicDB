package cypher

// #514 / #656 core: a CREATE/MERGE property map (node or relationship) and its
// list items must evaluate every arithmetic operator — *, -, ^, %, unary minus
// — to a value. The previous parser only routed top-level '+' and '/' through
// the evaluator, so 2 * 3 was stored as the string '2 * 3' (silent wrong data).

import (
	"context"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func propertyValueRow(t *testing.T, exec *StorageExecutor, query string) interface{} {
	t.Helper()
	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	require.Len(t, res.Rows, 1, query)
	require.Len(t, res.Rows[0], 1, query)
	return res.Rows[0][0]
}

func TestPropertyMapArithmeticOperatorsEvaluateToValues(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prop_arith"))

	cases := []struct {
		query string
		want  interface{}
	}{
		{"CREATE (n:T {a: 2 * 3}) RETURN n.a", int64(6)},
		{"CREATE (n:T {a: 7 - 2}) RETURN n.a", int64(5)},
		{"CREATE (n:T {a: 2 ^ 3}) RETURN n.a", float64(8.0)},
		{"CREATE (n:T {a: 7 % 4}) RETURN n.a", int64(3)},
		{"CREATE (n:T {a: -(2 * 3)}) RETURN n.a", int64(-6)},
		{"CREATE (n:T {a: 1 + 1}) RETURN n.a", int64(2)},
		{"CREATE (n:T {a: 6 / 2}) RETURN n.a", int64(3)},
		{"CREATE (n:T {a: 2 * 3 + 1}) RETURN n.a", int64(7)},
		{"CREATE (n:T {a: [1 + 1, 2 * 2, 7 % 4, 2 ^ 2]}) RETURN n.a",
			// The float result of 2 ^ 2 makes the stored array all-floats, as
			// in Neo4j's array property rule (#643).
			[]interface{}{float64(2), float64(4), float64(3), float64(4)}},
		{"CREATE (:T)-[r:R {w: 2 * 3}]->(:T) RETURN r.w", int64(6)},
		{"MERGE (n:T {a: 2 * 3}) RETURN n.a", int64(6)},
		{"CREATE (n:T {a: 2.5 * 2}) RETURN n.a", float64(5.0)},
	}
	for _, tc := range cases {
		got := propertyValueRow(t, exec, tc.query)
		require.Equal(t, tc.want, got, tc.query)
	}
}

func TestPropertyMapArithmeticNullOperandOmitsProperty(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prop_arith_null"))
	res, err := exec.Execute(context.Background(), "CREATE (n:T {id: 1, a: 2 * null}) RETURN n.a", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{nil}, res.Rows[0])
	// The property is omitted, exactly like the existing '/' null rule.
	rows, err := exec.Execute(context.Background(), "MATCH (n:T {id: 1}) RETURN properties(n)", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{map[string]interface{}{"id": int64(1)}}, rows.Rows[0])
}

func TestPropertyMapArithmeticFailureFailsTheStatement(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prop_arith_fail"))
	_, err := exec.Execute(context.Background(), "CREATE (n:T {a: 1 / 0}) RETURN n.a", nil)
	require.Error(t, err)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", code)
	// Nothing was created.
	rows, err := exec.Execute(context.Background(), "MATCH (n:T) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), rows.Rows[0][0])
}

func TestPropertyMapFunctionCallsAndLiteralsUnchanged(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "prop_literals"))
	cases := []struct {
		query string
		want  interface{}
	}{
		{"CREATE (n:T {s: 'a' + 'b'}) RETURN n.s", "ab"},
		{"CREATE (n:T {s: toUpper('abc')}) RETURN n.s", "ABC"},
		{"CREATE (n:T {s: 'x', i: 3, f: 2.5, b: true}) RETURN [n.s, n.i, n.f, n.b]",
			[]interface{}{"x", int64(3), 2.5, true}},
	}
	for _, tc := range cases {
		got := propertyValueRow(t, exec, tc.query)
		require.Equal(t, tc.want, got, tc.query)
	}
	// An invalid temporal constructor still fails the statement.
	_, err := exec.Execute(context.Background(), "CREATE (n:TP {d: datetime('x')}) RETURN n.d", nil)
	require.Error(t, err)
}
