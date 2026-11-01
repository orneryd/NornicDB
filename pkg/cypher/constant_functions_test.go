package cypher

import (
	"context"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestConstantFunctionNamespaceContracts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    interface{}
		invalid bool
		gds     bool
	}{
		{name: "pi", want: math.Pi}, {name: "e", want: math.E},
		{name: "gds.version", want: gdsVersion, gds: true},
		{name: "gds.version", args: []string{"  "}, want: gdsVersion, gds: true},
		{name: "pi", args: []string{""}, invalid: true},
		{name: "e", args: []string{"1"}, invalid: true},
		{name: "gds.version", args: []string{"1"}, invalid: true, gds: true},
	} {
		t.Run(tc.name+"/"+fmtConstantArgs(tc.args), func(t *testing.T) {
			evaluated := false
			scope := cypherfn.Context{Eval: func(string) (interface{}, error) { evaluated = true; return nil, nil }}
			got, found, err := cypherfn.EvaluateFunction(context.Background(), tc.name, tc.args, scope)
			require.True(t, found)
			require.False(t, evaluated, "constant functions must not evaluate rejected arguments")
			if tc.invalid {
				require.Error(t, err)
				require.Nil(t, got)
				if tc.gds {
					var count *cypherfn.ParameterCountError
					require.ErrorAs(t, err, &count)
					require.True(t, count.TooMany)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
	for _, tc := range []struct {
		name    string
		wantNaN bool
	}{{"gds.util.NaN", true}, {"gds.util.infinity", false}} {
		got, found, err := cypherfn.EvaluateFunction(context.Background(), tc.name, nil, cypherfn.Context{Eval: func(string) (interface{}, error) { t.Fatal("constant evaluated an argument"); return nil, nil }})
		require.True(t, found)
		require.NoError(t, err)
		number, ok := got.(float64)
		require.True(t, ok)
		if tc.wantNaN {
			require.True(t, math.IsNaN(number))
		} else {
			require.True(t, math.IsInf(number, 1))
		}
	}
}

func fmtConstantArgs(args []string) string {
	if len(args) == 0 {
		return "empty"
	}
	if args[0] == "" {
		return "blank"
	}
	return args[0]
}
