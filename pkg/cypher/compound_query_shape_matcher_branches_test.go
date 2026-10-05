package cypher

import "testing"

import "github.com/stretchr/testify/require"

func TestCompoundQueryShapeMatcher_HelperParsers(t *testing.T) {

	id, trailing, ok := parseIdentifierToken("abc_1 tail")
	require.True(t, ok)
	require.Equal(t, "abc_1", id)
	require.Equal(t, " tail", trailing)
	_, _, ok = parseIdentifierToken("1abc")
	require.False(t, ok)

	inside, rest, ok := extractParenSection("(a(b))x")
	require.True(t, ok)
	require.Equal(t, "a(b)", inside)
	require.Equal(t, "x", rest)
	_, _, ok = extractParenSection("(a")
	require.False(t, ok)

	inside, rest, ok = extractBracketSection("[a['x']]y")
	require.True(t, ok)
	require.Equal(t, "a['x']", inside)
	require.Equal(t, "y", rest)
	_, _, ok = extractBracketSection("[a")
	require.False(t, ok)
}
