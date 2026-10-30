package knowledgepolicy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// evalCtxWith builds an onAccessEvalContext convenient for the parser
// tests. The expression engine takes nowNanos and entityProps via this
// struct, so tests parameterize each evaluation through it without
// reaching into the higher-level applyOnAccessMutations harness.
func evalCtxWith(props map[string]interface{}, params map[string]interface{}) onAccessEvalContext {
	if props == nil {
		props = map[string]interface{}{}
	}
	if params == nil {
		params = map[string]interface{}{}
	}
	return onAccessEvalContext{
		entityProps: props,
		params:      params,
		nowNanos:    1_700_000_000_000_000_000,
	}
}

func TestEvalOnAccess_ArithmeticAndComparisons(t *testing.T) {
	cases := []struct {
		expr string
		want interface{}
	}{
		{"1 + 2", int64(3)},
		{"3 * 4", int64(12)},
		{"10 - 3", int64(7)},
		{"7 / 2", float64(3.5)},
		{"-5 + 10", int64(5)},
		{"-5.5", float64(-5.5)},
		{"1.5 + 2", float64(3.5)},
		{"1.5 * 2.0", float64(3.0)},
		{"3.0 - 1", float64(2.0)},
		{"1 + 2 * 3", int64(7)},
		{"(1 + 2) * 3", int64(9)},
		{"5 > 3", true},
		{"5 < 3", false},
		{"5 = 5", true},
		{"5 <> 5", false},
		{"5 >= 5", true},
		{"5 <= 4", false},
	}
	for _, tc := range cases {
		got, err := evalOnAccessExpression(tc.expr, evalCtxWith(nil, nil))
		require.NoError(t, err, "expr=%q", tc.expr)
		require.Equal(t, tc.want, got, "expr=%q", tc.expr)
	}
}

func TestEvalOnAccess_BooleanLogic(t *testing.T) {
	cases := []struct {
		expr string
		want interface{}
	}{
		{"TRUE AND TRUE", true},
		{"TRUE AND FALSE", false},
		{"FALSE OR TRUE", true},
		{"FALSE OR FALSE", false},
		{"NULL OR TRUE", true},
		{"NULL AND TRUE", false},
		{"TRUE", true},
		{"FALSE", false},
		{"NULL = NULL", true},
		// NULL != NULL — production semantics return nil (the "right
		// side wins" branch when both operands are nil and the op is
		// neq returns the boolean result; observed value is nil from
		// the path that doesn't take the bool short-circuit).
		{"NULL > 1", false},
	}
	for _, tc := range cases {
		got, err := evalOnAccessExpression(tc.expr, evalCtxWith(nil, nil))
		require.NoError(t, err, "expr=%q", tc.expr)
		require.Equal(t, tc.want, got, "expr=%q", tc.expr)
	}
}

func TestEvalOnAccess_Strings(t *testing.T) {
	got, err := evalOnAccessExpression(`"hello"`, evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, "hello", got)

	// String comparison falls back to lexicographic.
	got, err = evalOnAccessExpression(`"a" < "b"`, evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, true, got)
}

func TestEvalOnAccess_Properties(t *testing.T) {
	ctx := evalCtxWith(map[string]interface{}{
		"score":   int64(7),
		"label":   "active",
		"missing": nil,
	}, nil)

	got, err := evalOnAccessExpression("score + 3", ctx)
	require.NoError(t, err)
	require.Equal(t, int64(10), got)

	got, err = evalOnAccessExpression("n.score", ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), got)

	got, err = evalOnAccessExpression(`label = "active"`, ctx)
	require.NoError(t, err)
	require.Equal(t, true, got)

	// Unknown property is nil.
	got, err = evalOnAccessExpression("nope", ctx)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestEvalOnAccess_Parameters(t *testing.T) {
	ctx := evalCtxWith(nil, map[string]interface{}{
		"threshold": int64(5),
		"name":      "active",
	})
	got, err := evalOnAccessExpression("$threshold + 2", ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), got)

	got, err = evalOnAccessExpression(`$name = "active"`, ctx)
	require.NoError(t, err)
	require.Equal(t, true, got)

	// Unknown param is nil.
	got, err = evalOnAccessExpression("$missing", ctx)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestEvalOnAccess_CASE(t *testing.T) {
	got, err := evalOnAccessExpression("CASE WHEN 1 = 1 THEN 100 ELSE 200 END", evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, int64(100), got)

	got, err = evalOnAccessExpression("CASE WHEN 1 = 2 THEN 100 ELSE 200 END", evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, int64(200), got)
}

func TestEvalOnAccess_CASE_Errors(t *testing.T) {
	cases := []string{
		"CASE 1 = 1 THEN 100 ELSE 200 END",  // missing WHEN
		"CASE WHEN 1 = 1 100 ELSE 200 END",  // missing THEN
		"CASE WHEN 1 = 1 THEN 100 200 END",  // missing ELSE
		"CASE WHEN 1 = 1 THEN 100 ELSE 200", // missing END
	}
	for _, expr := range cases {
		_, err := evalOnAccessExpression(expr, evalCtxWith(nil, nil))
		require.Error(t, err, "expr=%q", expr)
	}
}

func TestEvalOnAccess_BuiltinFunctions(t *testing.T) {
	got, err := evalOnAccessExpression("TIMESTAMP()", evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, int64(1_700_000_000_000_000_000), got)

	got, err = evalOnAccessExpression("COALESCE(NULL, NULL, 5)", evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Equal(t, int64(5), got)

	got, err = evalOnAccessExpression("COALESCE(NULL, NULL)", evalCtxWith(nil, nil))
	require.NoError(t, err)
	require.Nil(t, got)

	// Unknown function fails.
	_, err = evalOnAccessExpression("BOGUS(1, 2)", evalCtxWith(nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported function")
}

func TestEvalOnAccess_ParseErrors(t *testing.T) {
	cases := []string{
		"1 + ",  // trailing op
		"(1",    // unclosed paren
		"@",     // bad token
		"1.5.5", // malformed number
	}
	for _, expr := range cases {
		_, err := evalOnAccessExpression(expr, evalCtxWith(nil, nil))
		require.Error(t, err, "expr=%q", expr)
	}
}

func TestEvalOnAccess_DivisionByZero(t *testing.T) {
	_, err := evalOnAccessExpression("5 / 0", evalCtxWith(nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "division by zero")
}

func TestEvalOnAccess_NegationOfNonNumeric(t *testing.T) {
	_, err := evalOnAccessExpression(`-"hello"`, evalCtxWith(nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "negate")
}

func TestEvalOnAccess_DotAccessRequiresIdent(t *testing.T) {
	ctx := evalCtxWith(map[string]interface{}{"v": int64(1)}, nil)
	_, err := evalOnAccessExpression("n.123", ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "property name")
}

// TestEvalOnAccess_CypherArithmeticDispatch_Regression locks in the
// Cypher-style arithmetic semantics fixed in commit alongside this
// test: int op int → int (preserved by isIntKind), float op anything →
// float (truthy types decide, not value-coercion). Previously
// arithmetic() called toInt64 on both sides, which truncates floats,
// so 1.5 + 2 produced int64(3) and 1.5 * 2.0 produced int64(2). The
// fix replaces toInt64 with the type-discriminating isIntKind so
// floats stay floats.
func TestEvalOnAccess_CypherArithmeticDispatch_Regression(t *testing.T) {
	t.Run("int op int stays int", func(t *testing.T) {
		got, err := evalOnAccessExpression("3 + 4", evalCtxWith(nil, nil))
		require.NoError(t, err)
		require.IsType(t, int64(0), got)
	})
	t.Run("float either side promotes to float", func(t *testing.T) {
		got, err := evalOnAccessExpression("1.5 + 2", evalCtxWith(nil, nil))
		require.NoError(t, err)
		require.IsType(t, float64(0), got)
		require.Equal(t, float64(3.5), got)
	})
	t.Run("two float operands stay float", func(t *testing.T) {
		got, err := evalOnAccessExpression("1.5 * 2.0", evalCtxWith(nil, nil))
		require.NoError(t, err)
		require.IsType(t, float64(0), got)
		require.Equal(t, float64(3.0), got)
	})
	t.Run("division of two ints is float per existing engine rule", func(t *testing.T) {
		// The on-access engine has historically promoted int / int
		// to float (the int branch refuses tokenSlash). Pinning that
		// behavior alongside the dispatch regression so it doesn't
		// silently change.
		got, err := evalOnAccessExpression("7 / 2", evalCtxWith(nil, nil))
		require.NoError(t, err)
		require.Equal(t, float64(3.5), got)
	})
}

func TestEvalOnAccess_NonNumericArithmetic(t *testing.T) {
	_, err := evalOnAccessExpression(`"a" + "b"`, evalCtxWith(nil, nil))
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric operands")
}

func TestTruthy_Variants(t *testing.T) {
	require.False(t, truthy(nil))
	require.True(t, truthy(true))
	require.False(t, truthy(false))
	require.True(t, truthy(int64(1)))
	require.False(t, truthy(int64(0)))
	require.True(t, truthy(float64(1.5)))
	require.False(t, truthy(float64(0)))
	require.True(t, truthy("non-empty"))
	require.False(t, truthy(""))
}
