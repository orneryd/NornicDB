package antlr

import (
	"strings"
	"sync"
)

// queryClauseStarts are the first words of the grammar's reading and updating
// clause productions. Their lookahead supplies USE routing and contextual GRAPH
// with the complete accepted vocabulary, including clauses added to the shared
// grammar. A second hand-maintained keyword list cannot narrow the statement
// language on entry.
var queryClauseStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_readingStatement, CypherParserRULE_updatingStatement)
})

// graphQueryStarts are the first words of the grammar's single query: every
// clause and projection a graph query can open with (MATCH, LET, FOR, CREATE,
// RETURN, FINISH, …).
var graphQueryStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_singleQuery)
})

// statementStarts are the first words of the grammar's script: every word a
// statement (graph query, command, transaction control or execution prefix)
// can start with.
var statementStarts = sync.OnceValue(func() map[string]struct{} {
	return ruleStartWords(CypherParserRULE_script)
})

// IsStatementStart reports whether word starts a statement in the shared
// grammar (statementStarts).
func IsStatementStart(word string) bool {
	_, found := statementStarts()[strings.ToUpper(word)]
	return found
}

// ruleStartWords reads the keyword lookahead of the rules' decisions from the
// generated ATN.
func ruleStartWords(rules ...int) map[string]struct{} {
	CypherParserInit()
	starts := make(map[string]struct{})
	atn := CypherParserParserStaticData.atn
	for _, decision := range atn.DecisionToState {
		found := false
		for _, rule := range rules {
			found = found || decision.GetRuleIndex() == rule
		}
		if !found {
			continue
		}
		for _, interval := range atn.NextTokensInContext(decision, nil).GetIntervals() {
			for token := max(1, interval.Start); token < interval.Stop && token < len(CypherParserParserStaticData.LiteralNames); token++ {
				if word := strings.Trim(CypherParserParserStaticData.LiteralNames[token], "'"); word != "" {
					starts[word] = struct{}{}
				}
			}
		}
	}
	return starts
}

func isQueryClauseStart(word string) bool {
	_, found := queryClauseStarts()[strings.ToUpper(word)]
	return found
}

// IsGraphQueryStart reports whether word opens a graph query in the shared
// grammar (graphQueryStarts).
func IsGraphQueryStart(word string) bool {
	_, found := graphQueryStarts()[strings.ToUpper(word)]
	return found
}
