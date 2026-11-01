package cypher

import (
	"context"
	"errors"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

func TestMathFunctionOwnerPreservesArgumentSemantics(t *testing.T) {
	failure := errors.New("controlled argument failure")
	for _, tc := range []struct {
		name      string
		function  string
		values    []interface{}
		want      interface{}
		invalid   bool
		evalError bool
	}{
		{name: "unary", function: "ceil", values: []interface{}{1.2}, want: float64(2)},
		{name: "binary", function: "power", values: []interface{}{int64(3), float64(2)}, want: float64(9)},
		{name: "unary null", function: "ceil", values: []interface{}{nil}},
		{name: "null before left type error", function: "power", values: []interface{}{"wrong", nil}},
		{name: "null before right type error", function: "power", values: []interface{}{nil, "wrong"}},
		{name: "left type error", function: "power", values: []interface{}{"wrong", float64(2)}, invalid: true},
		{name: "right type error", function: "power", values: []interface{}{float64(2), "wrong"}, invalid: true},
		{name: "unary type error", function: "ceil", values: []interface{}{"wrong"}, invalid: true},
		{name: "argument error", function: "power", values: []interface{}{float64(2), float64(3)}, evalError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"left"}
			if len(tc.values) == 2 {
				args = append(args, "right")
			}
			var seen []string
			scope := cypherfn.Context{Eval: func(expression string) (interface{}, error) {
				seen = append(seen, expression)
				if tc.evalError {
					return nil, failure
				}
				index := 0
				if expression == "right" {
					index = 1
				}
				return tc.values[index], nil
			}}
			got, found, err := cypherfn.EvaluateFunction(context.Background(), tc.function, args, scope)
			require.True(t, found)
			if tc.evalError {
				require.Same(t, failure, err)
				require.Equal(t, []string{"left"}, seen)
				require.Nil(t, got)
				return
			}
			require.Equal(t, args, seen, "all arguments are evaluated once before null/type decisions")
			if tc.invalid {
				var mismatch *cypherfn.TypeMismatchError
				require.ErrorAs(t, err, &mismatch)
				require.Equal(t, tc.function, mismatch.Function)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
