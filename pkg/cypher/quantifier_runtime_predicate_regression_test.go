package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireQuantifierRuntimeTypeError(t *testing.T, err error) {
	t.Helper()
	var classified interface{ BoltErrorCode() string }
	require.ErrorAs(t, err, &classified)
	require.Equal(t, "Neo.ClientError.Statement.TypeError", classified.BoltErrorCode())
}

func TestQuantifiersRejectRuntimeNumericPredicates(t *testing.T) {
	for _, function := range []string{"all", "any", "none", "single"} {
		t.Run(function, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			query := fmt.Sprintf("UNWIND [{id:1}] AS n RETURN %s(x IN [1,2] WHERE x+n.id) AS v", function)
			_, err := exec.Execute(context.Background(), query, nil)
			requireQuantifierRuntimeTypeError(t, err)
		})
	}
}

func TestQuantifierRuntimePredicateEvaluatorParity(t *testing.T) {
	exec, _ := newTestExecutor(t)
	values := map[string]interface{}{"n": map[string]interface{}{"id": int64(1)}}
	for _, function := range []string{"all", "any", "none", "single"} {
		expression := fmt.Sprintf("%s(x IN [1,2] WHERE x+n.id)", function)
		t.Run(function+"/text", func(t *testing.T) {
			_, _, err := exec.evaluateRowValueText(expression, values)
			requireQuantifierRuntimeTypeError(t, err)
		})
		t.Run(function+"/compiled", func(t *testing.T) {
			program := rowValueProgramFor(expression)
			require.NotNil(t, program)
			_, _, err := program.root.eval(exec, values, nil, nil)
			requireQuantifierRuntimeTypeError(t, err)
		})
		t.Run(function+"/context", func(t *testing.T) {
			ctx := withExpressionFailureSlot(withValueBindings(context.Background(), values))
			result := exec.evaluateQuantifierWithContext(ctx, function, "x IN [1,2] WHERE x+n.id", nil, nil, nil, nil, nil, 0)
			require.Nil(t, result)
			requireQuantifierRuntimeTypeError(t, getExpressionFailure(ctx))
		})
	}
}

func TestQuantifierRuntimePredicateControlCases(t *testing.T) {
	exec, _ := newTestExecutor(t)
	for _, tc := range []struct {
		function                  string
		empty                     interface{}
		shortList, shortPredicate string
		short                     interface{}
	}{
		{"all", true, "[0,1]", "CASE WHEN x=0 THEN false ELSE x+n.id END", false},
		{"any", false, "[1,0]", "CASE WHEN x=1 THEN true ELSE x+n.id END", true},
		{"none", true, "[1,0]", "CASE WHEN x=1 THEN true ELSE x+n.id END", false},
		{"single", false, "[1,1,0]", "CASE WHEN x=1 THEN true ELSE x+n.id END", false},
	} {
		for _, control := range []struct {
			name, list, predicate string
			want                  interface{}
		}{
			{"empty", "[]", "x+n.id", tc.empty},
			{"null_list", "n.missing", "x+n.id", nil},
			{"null_predicate", "[1,2]", "n.missing", nil},
			{"short_circuit", tc.shortList, tc.shortPredicate, tc.short},
		} {
			t.Run(tc.function+"/"+control.name, func(t *testing.T) {
				query := fmt.Sprintf("UNWIND [{id:1}] AS n RETURN %s(x IN %s WHERE %s) AS v", tc.function, control.list, control.predicate)
				result, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{control.want}}, result.Rows)
			})
		}
	}
}

func TestQuantifierRuntimePredicatePreservesEvaluationFailures(t *testing.T) {
	failure := fmt.Errorf("predicate evaluation failed")
	for _, tc := range []struct {
		name     string
		resolved bool
		err      error
	}{
		{"unresolved", false, nil},
		{"failed", true, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			value, resolved, err := rowQuantifierLoop("any", []interface{}{1, 2}, func(interface{}) {}, func() (interface{}, bool, error) {
				calls++
				return nil, tc.resolved, tc.err
			})
			require.Nil(t, value)
			require.False(t, resolved)
			if tc.err != nil {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls, "unresolved or failed predicates must not evaluate later elements")
		})
	}
}
