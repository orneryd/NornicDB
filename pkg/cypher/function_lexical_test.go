package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionLexicalReaders(t *testing.T) {
	for _, text := range []string{
		"/* range('bad') count(*) shortestPath((a)-->(a)) */ 'ok'",
		"// range('bad') count(*) shortestPath((a)-->(a))\n'ok'",
		"'it''s range(1) count(*) shortestPath((a)-->(a))'",
		"\"range(1) count(*) shortestPath((a)-->(a))\"",
		"custom /* gap */ . range(1)",
		"custom . /* gap */ count(*)",
		"custom . shortestPath((a)-->(a))",
	} {
		t.Run(text, func(t *testing.T) {
			require.Empty(t, namedFunctionArguments(text, "range"))
			require.Empty(t, findAggregateSpans(text))
			require.False(t, containsFunctionCallNamed(text, "range"))
			_, _, _, found := extractShortestPathCall(text)
			require.False(t, found)
			require.Equal(t, text, maskPathFunctionCalls(text))
		})
	}
	// Quoted names are identifiers; quotes inside their arguments are data.
	text := "`range` /* gap ( */ (1, 2 /* ) */)"
	name, inner, ok := parseFunctionCallWS(text)
	require.True(t, ok)
	require.Equal(t, "range", name)
	require.Equal(t, []string{inner}, namedFunctionArguments(text, "range"))
	require.True(t, isFunctionCallWS(text, "range"))
	require.Equal(t, strings.TrimSpace(inner), extractFuncArgs(text, "range"))
	// A real nested call remains visible to the static walker.
	var names []string
	require.NoError(t, forEachFunctionCall("toUpper(/* range(1) */ toString(range(1, 2)))", func(name, _ string) error {
		names = append(names, name)
		return nil
	}))
	require.Equal(t, []string{"toUpper", "toString", "range"}, names)
	require.False(t, containsStatementClockCall("'date()' /* datetime() */"))
	require.True(t, containsStatementClockCall("`datetime` /* gap */ . statement ()"))
	name, arguments, call := parseFunctionCallWS("/* gap */ `uuid` /* gap */ ()")
	require.True(t, call)
	require.Equal(t, "UUID", staticFunctionResultType(name, arguments))
}
