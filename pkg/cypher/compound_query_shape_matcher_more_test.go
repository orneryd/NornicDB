package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompoundQueryShapeMatcher_MoreHelperParserBranches(t *testing.T) {

	inside, rest, ok := extractParenSection("(')')tail")
	require.True(t, ok)
	require.Equal(t, "')'", inside)
	require.Equal(t, "tail", rest)
	_, _, ok = extractParenSection("nope")
	require.False(t, ok)
	inside, rest, ok = extractBracketSection("[\"]\"]tail")
	require.True(t, ok)
	require.Equal(t, "\"]\"", inside)
	require.Equal(t, "tail", rest)
	_, _, ok = extractBracketSection("oops")
	require.False(t, ok)
}

func TestSharedDelimiterSectionsUseCypherQuoting(t *testing.T) {
	for _, test := range []struct {
		text, inside string
		open, close  rune
	}{
		{"('a\\' ) b')tail", "'a\\' ) b'", '(', ')'},
		{"[\"a\\\" ] b\"]tail", "\"a\\\" ] b\"", '[', ']'},
		{"(`name)part`)tail", "`name)part`", '(', ')'},
		{"[`name]part`]tail", "`name]part`", '[', ']'},
		{"(a /* ) */ (b))tail", "a /* ) */ (b)", '(', ')'},
		{"[a /* ] */ [b]]tail", "a /* ] */ [b]", '[', ']'},
	} {
		t.Run(test.text, func(t *testing.T) {
			var inside, rest string
			var ok bool
			if test.open == '(' {
				inside, rest, ok = extractParenSection(test.text)
				require.Equal(t, len(test.inside)+1, findMatchingCallParen(test.text, 0))
			} else {
				inside, rest, ok = extractBracketSection(test.text)
				require.Equal(t, len(test.inside)+1, findMatchingBracket(test.text, 0))
			}
			require.True(t, ok)
			require.Equal(t, test.inside, inside)
			require.Equal(t, "tail", rest)
			require.Equal(t, len(test.inside)+1, findMatchingDelimiter(test.text, 0, test.open, test.close))
			callInside, callRest, callOK := parseCallTailDelimited("  "+test.text, byte(test.open), byte(test.close))
			require.True(t, callOK)
			require.Equal(t, test.inside, callInside)
			require.Equal(t, rest, callRest)
		})
	}
}

func TestSharedIdentifierTokenSupportsQuotedNames(t *testing.T) {
	for _, test := range []struct{ text, name, rest string }{
		{"alpha_2 tail", "alpha_2", " tail"},
		{"`two words` tail", "two words", " tail"},
		{"`a``b` tail", "a`b", " tail"},
		{"`name]part` tail", "name]part", " tail"},
	} {
		t.Run(test.text, func(t *testing.T) {
			name, rest, ok := parseIdentifierToken(test.text)
			require.True(t, ok)
			require.Equal(t, test.name, name)
			require.Equal(t, test.rest, rest)
			callName, callRest, callOK := parseCallTailIdentifier(test.text)
			require.True(t, callOK)
			require.Equal(t, name, callName)
			require.Equal(t, rest, callRest)
			written, end, symbolic := scanSymbolicName(test.text, 0)
			require.True(t, symbolic)
			require.Equal(t, len(test.text)-len(rest), end)
			require.Equal(t, test.text[:end], written)
		})
	}
}

func TestSharedLexicalBoundaries(t *testing.T) {
	for _, text := range []string{"", "1bad", "``", "`unclosed", "`a``"} {
		name, rest, ok := parseIdentifierToken(text)
		require.False(t, ok, text)
		require.Empty(t, name)
		require.Empty(t, rest)
	}
	for _, start := range []int{-1, 0, 2, 3} {
		name, end, ok := scanIdentifierToken("1a", start)
		require.False(t, ok)
		require.Empty(t, name)
		require.Equal(t, start, end)
		written, next, symbolic := scanSymbolicName("1a", start)
		require.False(t, symbolic)
		require.Empty(t, written)
		require.Equal(t, start, next)
	}
	for _, text := range []string{"", "x", "[", "['unclosed]", "[`unclosed]", "[/* unclosed]", "[[x]"} {
		inside, rest, ok := extractBracketSection(text)
		require.False(t, ok, text)
		require.Empty(t, inside)
		require.Empty(t, rest)
	}
	inside, rest, ok := extractBracketSection("[]tail")
	require.True(t, ok)
	require.Empty(t, inside)
	require.Equal(t, "tail", rest)
	for _, start := range []int{-1, 1, 2} {
		require.Equal(t, -1, findMatchingDelimiter("()", start, '(', ')'))
	}
	end, closed := scanCypherQuotedText("'x'", -1, '\'')
	require.False(t, closed)
	require.Equal(t, -1, end)
	end, closed = scanCypherQuotedText("'x'", 1, '\'')
	require.False(t, closed)
	require.Equal(t, 1, end)
}
