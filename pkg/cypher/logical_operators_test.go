package cypher

import (
	"context"
	"errors"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Results and errors of Neo4j 5.26.30 for AND, OR, XOR and NOT over
// operands whose type is only known at run time (#907).
func TestLogicalOperatorsMatchNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "logical"))
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Q {big: 9007199254740993})", nil)
	require.NoError(t, err)
	const values = "WITH {t: true, f: false, n: null, i: 1, s: 's', l: [1], e: []} AS m "
	const typeError = "Neo.ClientError.Statement.TypeError"

	for _, testCase := range []struct {
		expression string
		want       interface{}
	}{
		// A false operand decides an AND, a true one an OR, whatever the
		// others are; the chain is one operation.
		{"m.f AND m.i", false},
		{"m.i AND m.f", false},
		{"m.i AND m.t AND m.f", false},
		{"m.i AND (m.t AND m.f)", false},
		{"m.n AND m.i AND m.f", false},
		{"m.i AND m.f AND m.s", false},
		{"m.t OR m.i", true},
		{"m.i OR m.t", true},
		{"m.i OR m.f OR m.t", true},
		{"m.i AND m.f OR m.t", true},
		{"NOT (m.i AND m.f)", true},
		// Lists are predicates: true unless empty.
		{"m.t AND m.l", true},
		{"m.l OR m.f", true},
		{"m.n AND m.l", nil},
		{"m.e AND m.t", false},
		{"m.e OR m.f", false},
		{"m.l XOR m.t", false},
		{"m.l XOR m.n", nil},
		{"m.e XOR m.f", false},
		{"NOT m.l", false},
		{"NOT m.e", true},
		{"m.l AND m.l", []interface{}{int64(1)}},
		// Neo4j's simplification: true leaves an AND, false an OR or an
		// XOR, repeats count once, NOT NOT x is x; what is left is the
		// operand's value as it is.
		{"m.i AND true", int64(1)},
		{"true AND m.i", int64(1)},
		{"m.i OR false", int64(1)},
		{"false OR m.i", int64(1)},
		{"m.i XOR false", int64(1)},
		{"false XOR m.i", int64(1)},
		{"m.i AND m.i", int64(1)},
		{"m.i AND (m.i)", int64(1)},
		{"m.i AND m.i AND m.i", int64(1)},
		{"m.s OR m.s", "s"},
		{"NOT NOT m.i", int64(1)},
		{"NOT (NOT m.i)", int64(1)},
		{"(m.i AND true) OR false", int64(1)},
		{"m.e OR false", []interface{}{}},
		{"m.i AND false", false},
		{"m.i OR true", true},
		{"CASE WHEN m.i OR m.t THEN 1 ELSE 0 END", int64(1)},
		{"CASE WHEN m.l THEN 1 ELSE 0 END", int64(1)},
		{"CASE WHEN m.e THEN 1 ELSE 0 END", int64(0)},
	} {
		t.Run(testCase.expression, func(t *testing.T) {
			result, err := exec.Execute(ctx, values+"RETURN "+testCase.expression+" AS v", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}

	for _, expression := range []string{
		"m.t AND m.i",
		"m.n AND m.s",
		"m.f OR m.i",
		"m.n OR m.s",
		"m.i XOR m.t",
		"m.i XOR m.i",
		"m.l XOR m.i",
		"NOT m.i",
		"NOT m.s",
		"m.i XOR true",
		"m.i AND null",
		"null OR m.i",
		"m.i OR m.s",
		"m.i AND true AND m.t",
		"m.i AND m.i OR m.f",
		"(m.i OR m.i) AND m.t",
		"m.i AND m.i XOR m.f",
		"CASE WHEN m.f OR m.i THEN 1 ELSE 0 END",
		"[x IN [1, 2] WHERE m.i | x]",
	} {
		t.Run(expression, func(t *testing.T) {
			_, err := exec.Execute(ctx, values+"RETURN "+expression+" AS v", nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, typeError, code)
		})
	}

	// WHERE reads its value as a predicate.
	for _, testCase := range []struct {
		predicate string
		rows      int
	}{
		{"m.l", 1},
		{"m.e", 0},
		{"NOT m.l", 0},
		{"m.f AND m.i", 0},
		{"m.i OR m.t", 1},
		{"m.l XOR m.f", 1},
		{"m.n AND m.l", 0},
	} {
		t.Run("WHERE "+testCase.predicate, func(t *testing.T) {
			result, err := exec.Execute(ctx, values+"WITH m WHERE "+testCase.predicate+" RETURN 1 AS v", nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, testCase.rows)
		})
	}
	for _, predicate := range []string{"m.i", "NOT m.i", "m.t XOR m.i", "m.n AND m.i", "m.i OR false", "true AND m.i", "m.i AND m.i"} {
		t.Run("WHERE "+predicate, func(t *testing.T) {
			_, err := exec.Execute(ctx, values+"WITH m WHERE "+predicate+" RETURN 1 AS v", nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, typeError, code)
		})
	}

	// The differential sweep's shapes: a property over a matched node.
	const matched = "MATCH (n:Q) WITH n, true AS t, null AS z "
	for _, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{matched + "RETURN t OR n.big AS v", [][]interface{}{{true}}},
		{matched + "RETURN false AND n.big AS v", [][]interface{}{{false}}},
		{matched + "RETURN false OR n.big AS v", [][]interface{}{{int64(9007199254740993)}}},
		{matched + "RETURN n.big XOR false AS v", [][]interface{}{{int64(9007199254740993)}}},
		{matched + "RETURN n.big AND n.big AS v", [][]interface{}{{int64(9007199254740993)}}},
		{matched + "WITH n, t WHERE n.big OR t RETURN 1 AS v", [][]interface{}{{int64(1)}}},
		{matched + "WITH n WHERE n.big AND false RETURN 1 AS v", nil},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			if testCase.want == nil {
				require.Empty(t, result.Rows)
				return
			}
			require.Equal(t, testCase.want, result.Rows)
		})
	}
	for _, query := range []string{
		matched + "WITH n, t WHERE t XOR n.big RETURN 1 AS v",
		matched + "WITH n, z WHERE z AND n.big RETURN 1 AS v",
		matched + "WITH n WHERE NOT n.big RETURN 1 AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, typeError, code)
		})
	}
}

// Operands are evaluated in order up to the deciding one; an operand's
// error counts only when nothing decides, and beats a type error (Neo4j
// 5.26.30, #907). A comprehension's own AND / OR stay inside it.
func TestLogicalOperatorsEvaluationOrderMatchesNeo4j(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "logical_order"))
	ctx := context.Background()
	for _, testCase := range []struct {
		query string
		want  interface{}
	}{
		{"WITH 0 AS x RETURN x > 0 AND 1 / x > 0 AS v", false},
		{"WITH 0 AS x RETURN x = 0 OR 1 / x > 0 AS v", true},
		{"WITH 0 AS x RETURN x > 0 AND 1 / x > 0 AND false AS v", false},
		{"RETURN [x IN [1] WHERE x > 0 AND x < 5 OR x = 7] AS v", []interface{}{int64(1)}},
		{"RETURN [x IN [1, 0] WHERE x > 0 AND 1 / x > 0] AS v", []interface{}{int64(1)}},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			result, err := exec.Execute(ctx, testCase.query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{testCase.want}}, result.Rows)
		})
	}
	// Where an operand's error is recorded on the statement as it happens
	// (evaluateLogicalExpression), an error before the deciding operand
	// still fails it; Neo4j returns false / true for these.
	for _, query := range []string{
		"WITH 0 AS x RETURN 1 / x > 0 AND x > 0 AS v",
		"WITH 0 AS x RETURN 1 / x > 0 OR x = 0 AS v",
		"WITH 0 AS x, {i: 1} AS m RETURN 1 / x > 0 AND m.i AS v",
		"WITH 0 AS x, {i: 1} AS m RETURN m.i AND 1 / x > 0 AS v",
		"RETURN [x IN [1, 0] WHERE x > 0 AND 1 / x > 0 OR NOT 1 / x = 1] AS v",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := exec.Execute(ctx, query, nil)
			require.Error(t, err)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Statement.ArithmeticError", code)
		})
	}
}

// The predicate reading of a value (cypherPredicateTruth) and the condition
// evaluators that use it (#907).
func TestLogicalOperatorsPredicateReading(t *testing.T) {
	truth, err := cypherPredicateTruth([]interface{}{})
	require.NoError(t, err)
	require.Equal(t, truthFalse, truth)
	truth, err = cypherPredicateTruth([]int64{1})
	require.NoError(t, err)
	require.Equal(t, truthTrue, truth)
	// A byte array is not a list.
	_, err = cypherPredicateTruth([]byte{1})
	require.Error(t, err)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "logical_conditions"))
	ctx := withExpressionFailureSlot(withValueBindings(context.Background(), map[string]interface{}{"f": false, "i": int64(1), "l": []interface{}{int64(1), nil}}))
	require.False(t, exec.evaluateCondition(ctx, "2 IN l", nil, nil))
	require.Nil(t, exec.conditionValue(ctx, "2 IN l", nil, nil))
	require.NoError(t, getExpressionFailure(ctx))
	require.False(t, exec.evaluateCondition(ctx, "f OR i", nil, nil))
	code, _ := nornicerrors.Neo4jStatus(getExpressionFailure(ctx))
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)

	ctx = context.Background()
	_, err = exec.Execute(ctx, "CREATE (:C {v: 1})-[:T]->(:C {v: 2})", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{"WITH 1 AS a RETURN false XOR false AS v", [][]interface{}{{false}}},
		{"MATCH (n:C) RETURN n.v AS v, CASE WHEN EXISTS { (n)-->() } THEN 1 ELSE 0 END AS out ORDER BY v", [][]interface{}{{int64(1), int64(1)}, {int64(2), int64(0)}}},
	} {
		result, err := exec.Execute(ctx, testCase.query, nil)
		require.NoError(t, err, testCase.query)
		require.Equal(t, testCase.want, result.Rows, testCase.query)
	}
	_, err = exec.Execute(ctx, "MATCH (n:C) RETURN CASE WHEN n.v THEN 1 ELSE 0 END AS v", nil)
	require.Error(t, err)
	code, _ = nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)
}

// An evaluator that returns its operands' errors (evaluateRowValue) has them
// held back: a deciding operand wins, and an evaluation error beats a type
// error (#907).
func TestEvaluateLogicalExpressionHoldsOperandErrors(t *testing.T) {
	failed := errors.New("/ by zero")
	values := map[string]interface{}{"t": true, "f": false, "i": int64(1)}
	eval := func(operand string) (interface{}, bool, error) {
		if operand == "boom" {
			return nil, true, failed
		}
		value, known := values[operand]
		return value, known, nil
	}
	for _, testCase := range []struct {
		expr string
		want interface{}
		err  error
	}{
		{"boom AND f", false, nil},
		{"boom OR t", true, nil},
		{"i AND boom", nil, failed},
		{"boom AND i", nil, failed},
		{"boom XOR f", nil, failed},
		{"t XOR boom", nil, failed},
	} {
		value, logical, ok, err := evaluateLogicalExpression(testCase.expr, eval)
		require.True(t, logical, testCase.expr)
		require.True(t, ok, testCase.expr)
		require.Equal(t, testCase.want, value, testCase.expr)
		require.Equal(t, testCase.err, err, testCase.expr)
	}
	_, logical, ok, _ := evaluateLogicalExpression("t AND unknown", eval)
	require.True(t, logical)
	require.False(t, ok)

	truth, err := cypherPredicateTruth(&PathResult{})
	require.NoError(t, err)
	require.Equal(t, truthTrue, truth)

	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "logical_holds"))
	ctx := withExpressionFailureSlot(withValueBindings(context.Background(), map[string]interface{}{"f": false, "i": int64(1)}))
	require.Nil(t, exec.evaluateExpressionWithContextFull(ctx, "f OR i", nil, nil, nil, nil, nil, 0))
	code, _ := nornicerrors.Neo4jStatus(getExpressionFailure(ctx))
	require.Equal(t, "Neo.ClientError.Statement.TypeError", code)

	// A CASE WHEN the row evaluator can't read as a value (EXISTS { … })
	// is evaluated as a predicate.
	node := &storage.Node{ID: "lonely", Labels: []string{"C"}}
	value, ok, err := exec.evaluateRowCaseExpression("CASE WHEN EXISTS { (n)-->() } THEN 1 ELSE 0 END", map[string]interface{}{"n": node})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(0), value)
}

// NOT is also a variable name (Neo4j 5.26.30, #907).
func TestLogicalNotAsVariableName(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "not_name"))
	ctx := context.Background()
	for _, query := range []string{
		"WITH 1 AS not WITH not WHERE not = 1 RETURN not AS v",
		"WITH 1 AS NOT WITH NOT WHERE NOT = 1 RETURN NOT AS v",
		"WITH 1 AS not RETURN not AS v",
		"WITH {p: 1} AS not RETURN not.p AS v",
	} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.NotEmpty(t, result.Rows, query)
		require.Equal(t, int64(1), result.Rows[0][0], query)
	}
	for _, expr := range []string{"not IN l", "not AS x", "not = 1", "not.p"} {
		_, negation := logicalNotOperand(expr)
		require.False(t, negation, expr)
	}
	result, err := exec.Execute(ctx, "WITH false AS b RETURN NOT b AS v, NOT (b) AS w, NOT -1 IS NULL AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true, true, true}}, result.Rows)
}
