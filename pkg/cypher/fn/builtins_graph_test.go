package fn

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// graphTestContext is a function context on a composite database whose
// graphs are graphs; composite false is any other database. Eval returns
// the value vars holds for an argument expression.
func graphTestContext(graphs []string, composite bool, vars map[string]interface{}) Context {
	return Context{
		Eval: func(expr string) (interface{}, error) {
			if expr == "boom" {
				return nil, errors.New("eval failed")
			}
			return vars[expr], nil
		},
		Graphs: func() ([]string, bool) { return graphs, composite },
	}
}

func TestGraphNamesListsCompositeGraphs(t *testing.T) {
	ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, nil)
	value, found, err := EvaluateFunction("graph.names", nil, ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []interface{}{"cmp.a", "cmp.b"}, value)

	// A composite database with no graphs lists none.
	value, found, err = EvaluateFunction("GRAPH.NAMES", nil, graphTestContext(nil, true, nil))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []interface{}{}, value)
}

func TestGraphFunctionsAreUnknownOutsideComposite(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  Context
	}{
		{"no graphs callback", Context{Eval: func(string) (interface{}, error) { return nil, nil }}},
		{"not a composite", graphTestContext(nil, false, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, function := range []string{"graph.names", "graph.propertiesByName"} {
				value, found, err := EvaluateFunction(function, []string{"'cmp.a'"}, tc.ctx)
				require.True(t, found)
				require.Nil(t, value)
				var unknown *UnknownFunctionError
				require.ErrorAs(t, err, &unknown)
				require.Equal(t, function, unknown.Function)
				require.EqualError(t, err, "Unknown function '"+function+"'")
			}
		})
	}
}

func TestGraphPropertiesByName(t *testing.T) {
	vars := map[string]interface{}{"a": "cmp.a", "upper": "CMP.B", "missing": "cmp.zz", "number": int64(1)}
	ctx := graphTestContext([]string{"cmp.a", "cmp.b"}, true, vars)

	t.Run("a graph of the composite is an empty map", func(t *testing.T) {
		for _, arg := range []string{"a", "upper"} {
			value, found, err := EvaluateFunction("graph.propertiesByName", []string{arg}, ctx)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, map[string]interface{}{}, value)
		}
	})

	t.Run("a name that is no graph of the composite is Graph not found", func(t *testing.T) {
		value, found, err := EvaluateFunction("graph.propertiesByName", []string{"missing"}, ctx)
		require.True(t, found)
		require.Nil(t, value)
		var notFound *GraphNotFoundError
		require.ErrorAs(t, err, &notFound)
		require.Equal(t, "cmp.zz", notFound.Name)
		require.EqualError(t, err, "Graph not found: cmp.zz")
	})

	t.Run("a non-string name is an argument type error", func(t *testing.T) {
		_, _, err := EvaluateFunction("graph.propertiesByName", []string{"number"}, ctx)
		var argErr *ArgumentTypeError
		require.ErrorAs(t, err, &argErr)
		require.Equal(t, "graph.propertiesByName", argErr.Function)
		require.Equal(t, int64(1), argErr.Value)
	})

	t.Run("wrong argument count", func(t *testing.T) {
		_, _, err := EvaluateFunction("graph.propertiesByName", nil, ctx)
		require.EqualError(t, err, "Insufficient parameters for function 'graph.propertiesByName'")
		_, _, err = EvaluateFunction("graph.propertiesByName", []string{"a", "a"}, ctx)
		require.EqualError(t, err, "Too many parameters for function 'graph.propertiesByName'")
		_, _, err = EvaluateFunction("graph.names", []string{"1"}, ctx)
		require.EqualError(t, err, "Too many parameters for function 'graph.names'")
		var count *ParameterCountError
		require.ErrorAs(t, err, &count)
		require.Equal(t, "graph.names", count.Function)
		require.True(t, count.TooMany)
	})

	t.Run("an argument that fails to evaluate fails the call", func(t *testing.T) {
		_, _, err := EvaluateFunction("graph.propertiesByName", []string{"boom"}, ctx)
		require.EqualError(t, err, "eval failed")
	})
}
