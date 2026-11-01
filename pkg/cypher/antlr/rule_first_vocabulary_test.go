package antlr

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Simple rules, optional prefixes and nested alternatives exercise the
// distinction between rule-entry FIRST and internal decision lookahead.
// This fixture also compiles on the pre-correction published source.
func TestGrammarVocabularyRuleEntries(t *testing.T) {
	CypherParserInit()
	for _, test := range []struct {
		rule int
		want []string
	}{
		{CypherParserRULE_skipSt, []string{"SKIP"}},
		{CypherParserRULE_limitSt, []string{"LIMIT"}},
		{CypherParserRULE_orderSt, []string{"ORDER"}},
		{CypherParserRULE_where, []string{"WHERE"}},
		{CypherParserRULE_matchSt, []string{"MATCH", "OPTIONAL"}},
		{CypherParserRULE_deleteSt, []string{"DELETE", "DETACH"}},
	} {
		t.Run(CypherParserParserStaticData.RuleNames[test.rule], func(t *testing.T) {
			got := ruleStartWords(test.rule)
			require.Len(t, got, len(test.want))
			for _, word := range test.want {
				require.Contains(t, got, word)
			}
		})
	}
}
