package cypher

import (
	"context"
	"errors"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/stretchr/testify/require"
)

// TestCaseFunctionOwnerPolicies pins the one case owner behind the four
// functions: upper and lower reject a wrong argument count with Neo4j's
// parameter-count error, toUpper and toLower keep their null result, and all
// four share the null argument, the type mismatch and the Unicode mapping.
func TestCaseFunctionOwnerPolicies(t *testing.T) {
	scope := neo5FunctionContext(map[string]interface{}{"v": "straße İ", "n": int64(1)})

	t.Run("upper and lower reject a wrong argument count", func(t *testing.T) {
		for _, name := range []string{"upper", "lower"} {
			for _, count := range []struct {
				args []string
				want string
			}{
				{args: nil, want: "expects 1 argument(s), got 0"},
				{args: []string{"v", "v"}, want: "expects 1 argument(s), got 2"},
			} {
				value, found, err := cypherfn.EvaluateFunction(context.Background(), name, count.args, scope)
				require.True(t, found, name)
				require.ErrorContains(t, err, count.want, name)
				require.Nil(t, value, name)
			}
		}
	})

	t.Run("toUpper and toLower keep their null result", func(t *testing.T) {
		for _, name := range []string{"toupper", "tolower"} {
			for _, args := range [][]string{nil, {"v", "v"}} {
				value, found, err := cypherfn.EvaluateFunction(context.Background(), name, args, scope)
				require.True(t, found, name)
				require.NoError(t, err, name)
				require.Nil(t, value, name)
			}
		}
	})

	t.Run("null argument is null", func(t *testing.T) {
		nullScope := cypherfn.Context{Eval: func(string) (interface{}, error) { return nil, nil }}
		for _, name := range []string{"upper", "lower", "toupper", "tolower"} {
			value, found, err := cypherfn.EvaluateFunction(context.Background(), name, []string{"v"}, nullScope)
			require.True(t, found, name)
			require.NoError(t, err, name)
			require.Nil(t, value, name)
		}
	})

	t.Run("non-string argument is a type mismatch", func(t *testing.T) {
		for _, name := range []string{"upper", "lower", "toupper", "tolower"} {
			value, found, err := cypherfn.EvaluateFunction(context.Background(), name, []string{"n"}, scope)
			require.True(t, found, name)
			var mismatch *cypherfn.TypeMismatchError
			require.ErrorAs(t, err, &mismatch, name)
			require.Equal(t, "String", mismatch.Expected, name)
			require.Nil(t, value, name)
		}
	})

	t.Run("argument evaluation errors pass through", func(t *testing.T) {
		want := errors.New("argument failed")
		failing := cypherfn.Context{Eval: func(string) (interface{}, error) { return nil, want }}
		for _, name := range []string{"upper", "lower", "toupper", "tolower"} {
			value, found, err := cypherfn.EvaluateFunction(context.Background(), name, []string{"v"}, failing)
			require.True(t, found, name)
			require.ErrorIs(t, err, want, name)
			require.Nil(t, value, name)
		}
	})

	t.Run("one Unicode mapping", func(t *testing.T) {
		for _, pair := range [][2]string{{"upper", "toupper"}, {"lower", "tolower"}} {
			mapped, found, err := cypherfn.EvaluateFunction(context.Background(), pair[0], []string{"v"}, scope)
			require.True(t, found)
			require.NoError(t, err)
			shared, found, err := cypherfn.EvaluateFunction(context.Background(), pair[1], []string{"v"}, scope)
			require.True(t, found)
			require.NoError(t, err)
			require.Equal(t, shared, mapped, pair[0]+" and "+pair[1])
		}
		require.Equal(t, "STRASSE İ", mustMapCase(t, "upper", "straße İ"))
		require.Equal(t, "i\u0307", mustMapCase(t, "lower", "\u0130"))
	})
}

// mustMapCase is one case function's result for text through the registry.
func mustMapCase(t *testing.T, name, text string) string {
	t.Helper()
	value, found, err := cypherfn.EvaluateFunction(context.Background(), name, []string{"v"}, neo5FunctionContext(map[string]interface{}{"v": text}))
	require.True(t, found)
	require.NoError(t, err)
	mapped, isString := value.(string)
	require.True(t, isString)
	return mapped
}
