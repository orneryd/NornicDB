package cypher

// NOR-440 reproduction at the owners the issue enumerates: the arithmetic tier
// splitter itself, the row value evaluator's text and compiled-program owners,
// the row predicate planner's text and compiled owners, and the runtime
// type-error text consumers.
//
// These are reproduction rows: each asserts the product contract (a CASE …
// END is one unit, so an operator inside it is the CASE's and never the
// expression's). They fail while the defect is present; no row pins the
// current answer.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNOR440SplitRowArithmeticTierKeepsCaseWhole measures the splitter's
// contract directly: the split is at the rightmost top-level operator of the
// tier, and a CASE … END is one unit whatever operators it holds.
func TestNOR440SplitRowArithmeticTierKeepsCaseWhole(t *testing.T) {
	cases := []struct {
		id        string
		expr      string
		operators string
		wantLeft  string
		wantRight string
		wantOp    byte
		wantOK    bool
	}{
		{
			id:        "rightmost-additive-is-not-in-case",
			expr:      "1 + CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END",
			operators: "+-",
			wantLeft:  "1",
			wantRight: "CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END",
			wantOp:    '+',
			wantOK:    true,
		},
		{
			id:        "case-with-unary-minus",
			expr:      "x + CASE WHEN x > 1 THEN -1 ELSE 0 END",
			operators: "+-",
			wantLeft:  "x",
			wantRight: "CASE WHEN x > 1 THEN -1 ELSE 0 END",
			wantOp:    '+',
			wantOK:    true,
		},
		{
			id:        "nested-case-is-one-unit",
			expr:      "1 + CASE WHEN true THEN 2 + CASE WHEN true THEN 1 ELSE 0 END ELSE 0 END",
			operators: "+-",
			wantLeft:  "1",
			wantRight: "CASE WHEN true THEN 2 + CASE WHEN true THEN 1 ELSE 0 END ELSE 0 END",
			wantOp:    '+',
			wantOK:    true,
		},
		{
			id:        "multiplicative-tier-inside-case",
			expr:      "2 * CASE WHEN true THEN 3 * 2 ELSE 1 END",
			operators: "*/%",
			wantLeft:  "2",
			wantRight: "CASE WHEN true THEN 3 * 2 ELSE 1 END",
			wantOp:    '*',
			wantOK:    true,
		},
		{
			id:        "division-tier-inside-case",
			expr:      "12 / CASE WHEN true THEN 6 / 3 ELSE 1 END",
			operators: "*/%",
			wantLeft:  "12",
			wantRight: "CASE WHEN true THEN 6 / 3 ELSE 1 END",
			wantOp:    '/',
			wantOK:    true,
		},
		{
			id:        "power-tier-inside-case",
			expr:      "2 ^ CASE WHEN true THEN 2 ^ 2 ELSE 1 END",
			operators: "^",
			wantLeft:  "2",
			wantRight: "CASE WHEN true THEN 2 ^ 2 ELSE 1 END",
			wantOp:    '^',
			wantOK:    true,
		},
		{
			// Control: an operator inside a function call's parentheses is
			// already outside the top level.
			id:        "function-call-operand-unchanged",
			expr:      "1 + CASE WHEN true THEN toInteger('2' + '3') ELSE 0 END",
			operators: "+-",
			wantLeft:  "1",
			wantRight: "CASE WHEN true THEN toInteger('2' + '3') ELSE 0 END",
			wantOp:    '+',
			wantOK:    true,
		},
		{
			// Control: a parenthesised CASE hides its operators.
			id:        "parenthesized-case-unchanged",
			expr:      "1 + (CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END)",
			operators: "+-",
			wantLeft:  "1",
			wantRight: "(CASE WHEN 4 > 1 THEN 4 - 1 ELSE 0 END)",
			wantOp:    '+',
			wantOK:    true,
		},
		{
			// Contract: a CASE is not an arithmetic expression of its own.
			id:        "case-only-has-no-top-level-operator",
			expr:      "CASE WHEN true THEN 3 * 2 ELSE 1 END",
			operators: "*/%",
			wantOK:    false,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.id, func(t *testing.T) {
			left, right, operator, ok := splitRowArithmeticTier(tc.expr, tc.operators)
			require.Equalf(t, tc.wantOK, ok, "%s: split of %q on %q", tc.id, tc.expr, tc.operators)
			if !tc.wantOK {
				return
			}
			require.Equalf(t, tc.wantLeft, left, "%s: left of %q", tc.id, tc.expr)
			require.Equalf(t, tc.wantRight, right, "%s: right of %q", tc.id, tc.expr)
			require.Equalf(t, tc.wantOp, operator, "%s: operator of %q", tc.id, tc.expr)
		})
	}
}

// TestNOR440RowValueOwnersCaseLaterOperand measures both row value owners —
// the text evaluator and the compiled row value program — for a CASE that is
// not the first operand.
func TestNOR440RowValueOwnersCaseLaterOperand(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	cases := []struct {
		id    string
		class string
		expr  string
		want  interface{}
	}{
		{id: "trigger-add-case-subtract", class: "trigger",
			expr: "x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END", want: int64(9)},
		{id: "trigger-multiply-case-multiply", class: "trigger",
			expr: "x * CASE WHEN x > 1 THEN x * 2 ELSE 1 END", want: int64(50)},
		{id: "trigger-subtract-case-add", class: "trigger",
			expr: "x - CASE WHEN x > 1 THEN 1 + 1 ELSE 0 END", want: int64(3)},
		{id: "control-parenthesized", class: "control",
			expr: "x + (CASE WHEN x > 1 THEN x - 1 ELSE 0 END)", want: int64(9)},
		{id: "control-case-first", class: "control",
			expr: "CASE WHEN x > 1 THEN x - 1 ELSE 0 END + x", want: int64(9)},
	}
	for _, tc := range cases {
		for _, text := range []bool{true, false} {
			tc, text := tc, text
			owner := "compiled-program"
			if text {
				owner = "text"
			}
			t.Run(tc.id+"/"+owner, func(t *testing.T) {
				ctx := withExpressionFailureSlot(context.Background())
				values := map[string]interface{}{"x": int64(5), rowStatementContextKey: ctx}
				var value interface{}
				var ok bool
				var err error
				if text {
					value, ok, err = exec.evaluateRowValueText(t.Context(), tc.expr, values)
				} else {
					root, _ := (&rowValueCompiler{}).compile(ctx, expressionPart(tc.expr))
					value, ok, err = root.eval(t.Context(), exec, values, nil)
				}
				require.NoErrorf(t, err, "%s/%s: %s", tc.id, owner, tc.expr)
				require.Truef(t, ok, "%s/%s: %s", tc.id, owner, tc.expr)
				require.Equalf(t, tc.want, value, "%s/%s: %s", tc.id, owner, tc.expr)
			})
		}
	}
}

// TestNOR440RowPredicateOwnersCaseLaterOperand measures both row predicate
// owners — the text predicate and the compiled predicate plan — for a
// comparison whose left operand is arithmetic containing a later CASE.
func TestNOR440RowPredicateOwnersCaseLaterOperand(t *testing.T) {
	exec, _ := perRowTestExecutor(t)
	cases := []struct {
		id    string
		class string
		expr  string
		want  bool
	}{
		{id: "trigger-equality", class: "trigger",
			expr: "x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END = 9", want: true},
		{id: "trigger-greater-than", class: "trigger",
			expr: "x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END > 8", want: true},
		{id: "control-parenthesized", class: "control",
			expr: "x + (CASE WHEN x > 1 THEN x - 1 ELSE 0 END) = 9", want: true},
		{id: "control-case-first", class: "control",
			expr: "CASE WHEN x > 1 THEN x - 1 ELSE 0 END + x = 9", want: true},
		{id: "control-plain-comparison", class: "control",
			expr: "x = 5", want: true},
	}
	for _, tc := range cases {
		for _, text := range []bool{true, false} {
			tc, text := tc, text
			owner := "compiled-plan"
			if text {
				owner = "text"
			}
			t.Run(tc.id+"/"+owner, func(t *testing.T) {
				ctx := withExpressionFailureSlot(context.Background())
				values := map[string]interface{}{"x": int64(5), rowStatementContextKey: ctx}
				var holds bool
				if text {
					holds = exec.evaluateRowPredicateText(ctx, tc.expr, values)
				} else {
					holds = compileRowPredicateProgram(ctx, tc.expr).holds(exec, ctx, values)
				}
				require.NoErrorf(t, getExpressionFailure(ctx), "%s/%s: %s", tc.id, owner, tc.expr)
				require.Equalf(t, tc.want, holds, "%s/%s: %s", tc.id, owner, tc.expr)
			})
		}
	}
}

// TestNOR440RuntimeTypeErrorTextOwnersCaseLaterOperand measures the two
// runtime type-error owners the issue lists as raw-text splitter callers:
// expressionTextAccepted (operand admission) and predicateIsArithmetic
// (whether a predicate's top level is arithmetic).
func TestNOR440RuntimeTypeErrorTextOwnersCaseLaterOperand(t *testing.T) {
	ctx := context.Background()
	t.Run("operand-admission/trigger-later-case", func(t *testing.T) {
		require.True(t, expressionTextAccepted(ctx,
			"x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END", expressionTextOperand))
	})
	t.Run("operand-admission/control-parenthesized", func(t *testing.T) {
		require.True(t, expressionTextAccepted(ctx,
			"x + (CASE WHEN x > 1 THEN x - 1 ELSE 0 END)", expressionTextOperand))
	})
	t.Run("operand-admission/control-case-first", func(t *testing.T) {
		require.True(t, expressionTextAccepted(ctx,
			"CASE WHEN x > 1 THEN x - 1 ELSE 0 END + x", expressionTextOperand))
	})
	t.Run("predicate-arithmetic/control-later-case", func(t *testing.T) {
		// A CASE on the right of a top-level + is top-level arithmetic.
		require.True(t, predicateIsArithmetic("x + CASE WHEN x > 1 THEN x - 1 ELSE 0 END"))
	})
	t.Run("predicate-arithmetic/control-case-first", func(t *testing.T) {
		require.True(t, predicateIsArithmetic("CASE WHEN x > 1 THEN x - 1 ELSE 0 END + x"))
	})
	t.Run("predicate-arithmetic/case-only-is-not-arithmetic", func(t *testing.T) {
		require.False(t, predicateIsArithmetic("CASE WHEN x > 1 THEN x - 1 ELSE 0 END"))
	})
}
