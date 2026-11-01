package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestStatementVocabularyConsumersUseGrammar(t *testing.T) {
	for _, word := range cypherantlr.ClauseWords() {
		require.True(t, isNameableClauseKeyword(word), word)
		require.Contains(t, withImportClauseKeywords, word)
		text := "x " + word + " 1"
		require.Equal(t, 2, firstKeywordIndexFromDefault(text, 0, withImportClauseKeywords...), word)
	}
	for _, word := range cypherantlr.CompletionClauseWords() {
		require.Contains(t, validatorClauseKeywords, word)
	}
	for _, text := range []string{"LOAD CSV FROM 'file:///x' AS row RETURN row", "LET x = 1 RETURN x", "FOR x IN [1] RETURN x"} {
		_, starts := statementStartWord(text)
		require.True(t, starts, text)
		require.Equal(t, openingClause, readStatementOpening(readLeadingWords(text)), text)
	}
	for _, text := range []string{"SET n.x = 1", "REMOVE n.x", "FINISH", "UNION RETURN 1"} {
		require.Equal(t, openingNone, readStatementOpening(readLeadingWords(text)), text)
	}
}

// Cover the grammar population through the consumers exposed by the compiled
// inventory, including NEXT admission and CALL predicate insertion. These
// checks intentionally include future grammar additions without a second list.
func TestStatementVocabularyBoundaryConsumers(t *testing.T) {
	for _, word := range cypherantlr.ClauseWords() {
		t.Run(word, func(t *testing.T) {
			require.NoError(t, validateLeadingNodePatternTransition("MATCH (n) "+word+" 1"))
			require.Equal(t, "MATCH (n)", strings.TrimSpace(extractMatchClause("MATCH (n) "+word+" 1")))
			if word != "WHERE" {
				tail := "WHERE n.x = 1 " + word + " 1"
				require.Equal(t, "WHERE n.x = 1 AND n.y = 2 "+word+" 1", buildCallTailPredicateInjection(tail, []string{"n.y = 2"}))
			}
		})
	}
	for _, word := range cypherantlr.QueryPartStartWords() {
		r := structureRewriter{query: word + " 1"}
		require.True(t, r.startsQueryPart(0, len(r.query)), word)
		require.True(t, groupByKeysEnd(word, "WITH"), word)
	}
	for _, word := range []string{"WHEN", "notAClause"} {
		r := structureRewriter{query: word + " 1"}
		require.Equal(t, word == "WHEN", r.startsQueryPart(0, len(r.query)), word)
	}
	// Projection order is semantic; spellings, including multi-token ORDER BY,
	// must come from the productions in that order.
	require.Equal(t, []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"}, projectionModifierKeywords)
	require.Equal(t, [][]string{{"ORDER BY"}, {"SKIP"}, {"LIMIT"}, {"WHERE"}, {"RETURN"}}, showSegmentKeywords)
}

func TestStatementVocabularyPublicComposition(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, query := range []string{
			"CYPHER 25 RETURN 1 AS x NEXT LET y = x + 1 RETURN y",
			"CYPHER 25 RETURN 1 AS x NEXT FOR y IN [x + 1] RETURN y",
			"CALL () { RETURN 1 AS x } WITH x LET y = x + 1 RETURN y",
		} {
			result, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows, query)
		}
	})
}

// Grammar words remain names in identifier-valued roles. Exercise execution
// and SHOW continuation, rather than only comparing helper collections.
func TestStatementVocabularyPublicNameRoles(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t))
		for _, literal := range cypherantlr.CypherParserParserStaticData.LiteralNames {
			word := strings.Trim(literal, "'")
			if !cypherantlr.IsUseClauseFollower(word, false) {
				continue
			}
			name := strings.ToLower(word)
			t.Run(name, func(t *testing.T) {
				for _, query := range []string{
					"WITH 1 AS " + name + " RETURN " + name,
					"SHOW FUNCTIONS YIELD name AS " + name + " RETURN " + name + " LIMIT 1",
				} {
					result, err := exec.Execute(t.Context(), query, nil)
					require.NoError(t, err, query)
					require.Equal(t, []string{name}, result.Columns, query)
				}
			})
		}
	})
}
