package fabric

import (
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

// TestParseUseClauseSemicolonEndsTheStatement: a ';' after the graph ends
// the statement, as in Neo4j 5.26: with only whitespace and comments after
// it, USE has no clause ("Query cannot conclude with USE GRAPH"); another
// statement after it is "Expected exactly one statement per query but got:
// <n>". A ';' in quoted text is text.
func TestParseUseClauseSemicolonEndsTheStatement(t *testing.T) {
	for query, want := range map[string]localization.Message{
		"USE db;":                                  localization.CypherCommandRoutingUseQueryCannotConclude(),
		"USE db ; // nothing else":                 localization.CypherCommandRoutingUseQueryCannotConclude(),
		"USE db; MATCH (n) RETURN count(n) AS c":   localization.CypherCommandRoutingMultipleStatements(2),
		"USE db;MATCH (n) RETURN n":                localization.CypherCommandRoutingMultipleStatements(2),
		"USE db; RETURN 1 AS x; RETURN 2 AS y":     localization.CypherCommandRoutingMultipleStatements(3),
		"USE db; RETURN ';' AS x":                  localization.CypherCommandRoutingMultipleStatements(2),
		"USE db; RETURN 1 AS x; /* c */ ; ":        localization.CypherCommandRoutingMultipleStatements(2),
		"USE db; RETURN `a;b` AS x; RETURN 2 AS y": localization.CypherCommandRoutingMultipleStatements(3),
	} {
		_, _, hasUse, err := ParseUseClause(query, false)
		require.True(t, hasUse, query)
		var syntaxErr *UseSyntaxError
		require.True(t, errors.As(err, &syntaxErr), query)
		require.Equal(t, want, syntaxErr.Message, query)
	}
	require.Equal(t, "Expected exactly one statement per query but got: 2",
		localization.CypherCommandRoutingMultipleStatements(2).Fallback)

	clause, remaining, hasUse, err := ParseUseClause("USE db RETURN ';' AS x", false)
	require.NoError(t, err)
	require.True(t, hasUse)
	require.Equal(t, "db", clause.Name)
	require.Equal(t, "RETURN ';' AS x", remaining)
}
