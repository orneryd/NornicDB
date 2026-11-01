package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/stretchr/testify/require"
	"slices"
	"strings"
	"testing"
)

// Exercise the complete grammar keyword population and the public selector reader.
func TestUseFollowerWordsHaveOneOwner(t *testing.T) {
	query := ruleStartWords(CypherParserRULE_useQueryFollower)
	admin := ruleStartWords(CypherParserRULE_useAdministrationFollower)
	for _, literal := range CypherParserParserStaticData.LiteralNames {
		word := strings.Trim(literal, "'")
		if !grammarWord(word) {
			continue
		}
		_, inQuery := query[word]
		_, inAdmin := admin[word]
		for _, spelling := range []string{word, strings.ToLower(word)} {
			require.Equal(t, inQuery, IsUseClauseFollower(spelling, true), spelling)
			require.Equal(t, inQuery || inAdmin, IsUseClauseFollower(spelling, false), spelling)
		}
		if !inQuery && !inAdmin {
			continue
		}
		t.Run(word, func(t *testing.T) {
			for _, gap := range []string{" ", "/*gap*/", "\r\n"} {
				text := "USE GRAPH" + gap + word + " 1"
				ref, err := ReadUseReference(t.Context(), cyphertext.NewSource(t.Context(), text, cyphertext.AcceptedSource))
				require.NoError(t, err, text)
				require.Equal(t, "GRAPH", ref.Name, text)
				require.Equal(t, "USE GRAPH", text[:ref.End], text)
			}
		})
	}
	for _, word := range []string{"ALL", "CSV", "HEADERS", "INVALID"} {
		require.False(t, IsUseClauseFollower(word, false), word)
	}
	require.False(t, IsUseClauseFollower("GRANT", true))
	require.False(t, IsUseClauseFollower("ſHOW", false))
}

func TestClauseVocabularyComesFromGrammar(t *testing.T) {
	require.False(t, IsClauseWord("ſET"))
	require.False(t, IsStatementBodyStart("ſHOW"))
	words := ClauseWords()
	require.True(t, slices.IsSorted(words))
	require.Equal(t, len(words), len(slices.Compact(slices.Clone(words))))
	for _, literal := range CypherParserParserStaticData.LiteralNames {
		word := strings.Trim(literal, "'")
		if grammarWord(word) {
			require.Equal(t, slices.Contains(words, word), IsClauseWord(strings.ToLower(word)), word)
		}
	}
	words[0] = "CORRUPTED"
	require.NotContains(t, ClauseWords(), "CORRUPTED")
	for _, word := range []string{"LOAD", "LET", "FILTER", "FOR", "RETURN", "WHERE", "YIELD"} {
		require.True(t, IsClauseWord(word), word)
	}
}

// Routing-only words used to be ID tokens. Declaring their grammar role must
// preserve every identifier-valued context, not make them reserved names.
func TestRoutingVocabularyPreservesIdentifierRoles(t *testing.T) {
	for word := range ruleStartWords(CypherParserRULE_routingKeywordName) {
		for _, text := range []string{
			"WITH 1 AS " + word + " RETURN " + word,
			"MATCH (" + word + ") RETURN " + word,
			"RETURN $" + word,
			"RETURN {" + word + ": 1}",
			"CYPHER 5 " + strings.ToLower(word) + "=value RETURN 1",
		} {
			_, err := ParseSyntax(t.Context(), text, StatementEntry, false, AcceptedSource)
			require.NoError(t, err, text)
		}
	}
}

func TestUpdatingClauseRoleStopsAtQueryBody(t *testing.T) {
	syntax, err := ParseSyntax(t.Context(), "MATCH (n) SET n.x = COUNT { MATCH (m) } RETURN n", StatementEntry, false, AcceptedSource)
	require.NoError(t, err)
	reads, writes := 0, 0
	syntax.Walk(func(node SyntaxNode) func() {
		switch node.Rule() {
		case CypherParserRULE_matchSt:
			reads++
			require.False(t, node.IsUpdatingClause(), node.Text())
		case CypherParserRULE_setSt:
			writes++
			require.True(t, node.IsUpdatingClause(), node.Text())
		}
		return nil
	})
	require.Equal(t, 2, reads)
	require.Equal(t, 1, writes)
}
