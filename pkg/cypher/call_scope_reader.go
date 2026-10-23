package cypher

import "strings"

type callSubqueryScopeClause struct {
	names      []string
	scoped     bool
	text       string
	bodyOffset int
	invalid    bool
}

// readCallSubqueryScope is the shared static/runtime owner of CALL's scope
// clause. scoped distinguishes CALL (*) from legacy CALL { ... }; a nil name
// list means all outer variables, and a non-nil empty list means CALL ().
// text and bodyOffset let validation use the same parsed clause. Legacy
// importing-WITH and implicit imports are resolved by their existing owners.
func readCallSubqueryScope(cypher string) callSubqueryScopeClause {
	offset := queryGapEnd(cypher, 0)
	trimmed := strings.TrimSpace(cypher[offset:])
	if !startsWithKeywordFold(trimmed, "CALL") {
		return callSubqueryScopeClause{}
	}
	idx := queryGapEnd(trimmed, len("CALL"))
	scope := callSubqueryScopeClause{bodyOffset: offset + idx}
	if idx >= len(trimmed) || trimmed[idx] != '(' {
		return scope
	}
	close := findMatchingCallParen(trimmed, idx)
	if close < 0 {
		return scope
	}
	scope.scoped = true
	scope.bodyOffset = offset + queryGapEnd(trimmed, close+1)
	scope.text = strings.TrimSpace(trimmed[idx+1 : close])
	names, err := callScopeClauseVariables(scope.text)
	if err != nil {
		// An invalid clause imports nothing; validateCallSubqueryScopes
		// reports the reader's error.
		scope.invalid, scope.names = true, []string{}
		return scope
	}
	if len(names) == 1 && names[0] == "*" {
		return scope
	}
	scope.names = names
	return scope
}
