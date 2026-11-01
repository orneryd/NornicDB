package antlr

import (
	"fmt"
	"slices"
	"sync"
	"testing"
)

// A caller may transform its returned vocabulary without changing any other
// caller's recognition. Exercise the complete exported slice population under
// concurrent reads and client mutation, including nested semantic groups.
func TestGrammarVocabularyViewIsolation(t *testing.T) {
	views := []struct {
		name string
		read func() []string
	}{
		{"clauses", ClauseWords}, {"completion", CompletionClauseWords},
		{"projection", ProjectionModifierWords}, {"show", ShowTailWords},
		{"call", CallTailBoundaryWords}, {"queryPart", QueryPartStartWords},
		{"queryBody", QueryBodyContainerWords}, {"expressionBody", ExpressionQueryBodyWords},
		{"ruleRoles", func() []string {
			return ClauseKeywordsForRules(CypherParserRULE_orderSt, CypherParserRULE_skipSt, CypherParserRULE_limitSt)
		}},
	}
	for _, view := range views {
		t.Run(view.name, func(t *testing.T) {
			want := view.read()
			if len(want) == 0 {
				t.Fatal("reviewed vocabulary view is empty")
			}
			var wg sync.WaitGroup
			failures := make(chan error, 16)
			for worker := range 16 {
				wg.Go(func() {
					own := view.read()
					own[0] = fmt.Sprintf("client-owned-%d", worker)
					if !slices.Equal(want, view.read()) {
						failures <- fmt.Errorf("client mutation escaped %s", view.name)
					}
				})
			}
			wg.Wait()
			close(failures)
			for err := range failures {
				t.Error(err)
			}
		})
	}
	rules := []int{CypherParserRULE_orderSt, CypherParserRULE_skipSt}
	want := ClauseKeywordGroupsForRules(rules...)
	for range 16 {
		own := ClauseKeywordGroupsForRules(rules...)
		own[0][0] = "client-owned"
		own[1] = nil
		got := ClauseKeywordGroupsForRules(rules...)
		for i := range want {
			if !slices.Equal(want[i], got[i]) {
				t.Fatalf("client mutation escaped semantic role %d", i)
			}
		}
	}
}
