package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestCheckOperatorBranches pins Neo4j's operand rules for the operator
// combinations the statement tests don't reach (#657).
func TestCheckOperatorBranches(t *testing.T) {
	known := knownOperand
	for _, tc := range []struct {
		op          byte
		left, right staticOperand
		want        string
		fails       bool
	}{
		{op: '+', left: known("Date"), right: known("List<T>"), want: "List<T>"},
		{op: '+', left: known("Duration"), right: staticOperand{}, want: ""},
		{op: '/', left: known("Duration"), right: known("String"), fails: true},
		{op: '/', left: known("Duration"), right: known("Integer"), want: "Duration"},
		{op: '*', left: known("String"), right: known("Integer"), fails: true},
		{op: '%', left: known("Boolean"), right: known("Integer"), fails: true},
		{op: '^', left: known("Boolean"), right: known("Integer"), fails: true},
	} {
		got, err := checkOperator(tc.op, tc.left, tc.right)
		if tc.fails {
			require.Error(t, err, "%c %+v %+v", tc.op, tc.left, tc.right)
			continue
		}
		require.NoError(t, err, "%c %+v %+v", tc.op, tc.left, tc.right)
		require.Equal(t, tc.want, got.kind, "%c %+v %+v", tc.op, tc.left, tc.right)
	}
}

// TestStaticOperatorCheckerBranches covers how the checker walks an
// expression: the operands of boolean, comparison and arithmetic operators,
// unary minus, parameters and the items of lists, maps and function calls. A
// type error anywhere inside is reported.
func TestStaticOperatorCheckerBranches(t *testing.T) {
	checker := staticOperatorChecker{params: map[string]interface{}{"i": int64(1), "s": "a"}}
	for _, tc := range []struct {
		expr  string
		want  string
		fails bool
	}{
		{expr: "true * 2 OR x", fails: true},
		{expr: "x OR true * 2", fails: true},
		{expr: "NOT true * 2", fails: true},
		{expr: "NOT x", want: "Boolean"},
		{expr: "true * 2 IN [1]", fails: true},
		{expr: "1 IN [true * 2]", fails: true},
		{expr: "true * 2 = 1", fails: true},
		{expr: "1 = true * 2", fails: true},
		{expr: "(true * 2) + 1", fails: true},
		{expr: "1 + (true * 2)", fails: true},
		{expr: "-(true * 2)", fails: true},
		{expr: "-'a'", fails: true},
		{expr: "-$i", want: "Integer"},
		{expr: "$missing", want: ""},
		{expr: "$s", want: "String"},
		{expr: "[true * 2]", fails: true},
		{expr: "{a: true * 2}", fails: true},
		{expr: "abs(true * 2)", fails: true},
		{expr: "[x]", want: "List<T>"},
		{expr: "CASE WHEN true THEN 1 END", want: ""},
		{expr: "(a)-[:R]->(b)", want: ""},
	} {
		got, err := checker.check(tc.expr)
		if tc.fails {
			require.Error(t, err, tc.expr)
			continue
		}
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, got.kind, tc.expr)
	}
	noParams := staticOperatorChecker{}
	got, err := noParams.check("$i")
	require.NoError(t, err)
	require.False(t, got.known())
}

// TestStaticParameterOperandBranches pins the static type of each parameter
// value kind.
func TestStaticParameterOperandBranches(t *testing.T) {
	require.False(t, staticParameterOperand(nil).known())
	require.Equal(t, "Float", staticParameterOperand(1.5).kind)
	require.Equal(t, "Boolean", staticParameterOperand(true).kind)
	require.Equal(t, "List<T>", staticParameterOperand([]interface{}{int64(1)}).kind)
	require.Equal(t, "Map", staticParameterOperand(map[string]interface{}{"a": 1}).kind)
	require.False(t, staticParameterOperand(struct{}{}).known())
}

// TestStaticOperatorClauseBranches runs statements whose operator type errors
// sit in each clause form the validation visits: RETURN DISTINCT, ORDER BY, a
// CREATE pattern, SET and a MATCH pattern, next to quoted text with braces.
func TestStaticOperatorClauseBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "staticops"))
	ctx := context.Background()
	for _, query := range []string{
		"RETURN DISTINCT true * 2 AS x",
		"RETURN 1 AS x ORDER BY x + true",
		"WITH 1 AS x WHERE x > 0 AND true * 2 > 1 RETURN x",
		"CREATE (n:SO {v: true * 2})",
		"MATCH (n) SET n.v = true * 2",
		"MATCH (n:SO {s: '{', v: true * 2}) RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, err.Error(), "Type mismatch", query)
	}
	for _, query := range []string{
		"UNWIND [1, 1] AS i RETURN DISTINCT i + 1 AS x",
		"WITH 1 AS x WHERE x + 1 > 0 RETURN x ORDER BY x + 1 SKIP 0 LIMIT 1",
		"MATCH (n:SO {s: '{x}'}) WHERE n.v + 1 > 0 RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	_, err := exec.Execute(ctx, "RETURN $s + 1 AS x", map[string]interface{}{"s": true})
	require.Error(t, err)
}
