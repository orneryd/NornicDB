package cypher

import (
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestNativeExpressionScopes(t *testing.T) {
	for _, sample := range []struct{ text, name, source string }{
		{"any /* scope */ (x IN [1,2] WHERE x=1)", "x", "[1,2]"},
		{"reduce /* scope */ (s=0,x IN [1,2] | s+x)", "x", "[1,2]"},
		{"filter /* scope */ (x IN [1,2] WHERE x=1)", "x", "[1,2]"},
		{"[x /* declaration */ IN [1,2] | x]", "x", "[1,2]"},
		{"[x IN [1,2]]", "x", "[1,2]"},
		{"{k:v IN {a:1} WHERE v=1 | k:v}", "v", "{a:1}"},
	} {
		text := sample.text
		for _, clause := range []bool{false, true} {
			if clause {
				text = "RETURN " + sample.text + " AS value"
			}
			found := false
			for _, scope := range fabric.ExpressionScopes(text) {
				for _, declaration := range scope.Declarations {
					if declaration.Name != sample.name {
						continue
					}
					found = true
					require.Equal(t, sample.source, text[declaration.Source.Start:declaration.Source.End], text)
					require.True(t, scope.Binds(sample.name, declaration.At), text)
				}
			}
			require.True(t, found, text)
		}
	}
	text := "[x IN ['fake IN [1] /* fake */'] | COUNT { WITH [y IN [x] | y] AS ys RETURN ys }]"
	names := expressionLocalBindings(text)
	require.Contains(t, names, "x")
	require.Contains(t, names, "y")
	require.NotContains(t, names, "fake")
	root := expressionLocalBindingNames(text, true)
	require.Contains(t, root, "x")
	require.NotContains(t, root, "y")
	_, _, bound := localBindingOf(text, strings.LastIndex(text, "x]"), "x")
	require.True(t, bound)
}
