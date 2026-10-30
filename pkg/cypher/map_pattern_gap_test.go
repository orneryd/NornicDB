package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMapRelationshipShapeUsesCanonicalTrivia(t *testing.T) {
	for _, gap := range []string{"", " ", "\t\r\n", "\u00a0\u2003", "/* ] -> ( ignored */", "//ignored arrow -> (\n", " /*one*/ \u2028 /*two*/ "} {
		for _, arrow := range []string{"-", "-" + gap + ">"} {
			query := "MATCH (a)-[r:R {x:1}]" + gap + arrow + gap + "(b) RETURN r"
			brace := strings.IndexByte(query, '{')
			require.True(t, newMapPatternContext(query).insidePatternDelimiter(query, brace), "%q", query)
		}
	}
	for _, tail := range []string{"", "/*unfinished", "// EOF", "+(b)", "- 42", "- /*unfinished", "-> /*unfinished", "-'(b)'"} {
		query := "MATCH (a)-[r:R {x:1}]" + tail
		brace := strings.IndexByte(query, '{')
		require.False(t, newMapPatternContext(query).insidePatternDelimiter(query, brace), "%q", query)
	}
}

func TestMapNodeWhereTriviaRetainsPatternOwnership(t *testing.T) {
	for _, gap := range []string{" ", "\u00a0", " /*gap*/ ", " //gap\n"} {
		named := "MATCH (WHERE" + gap + "{x:1}) RETURN WHERE"
		require.True(t, newMapPatternContext(named).insidePatternDelimiter(named, strings.IndexByte(named, '{')))
		predicate := "MATCH (n WHERE" + gap + "n.value = {x:1}) RETURN n"
		require.False(t, newMapPatternContext(predicate).insidePatternDelimiter(predicate, strings.IndexByte(predicate, '{')))
	}
}
