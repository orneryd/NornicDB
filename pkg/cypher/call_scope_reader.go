package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

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
func readCallSubqueryScope(ctx context.Context, source string) callSubqueryScopeClause {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25")
	if !syntax.Valid() {
		return callSubqueryScopeClause{invalid: err != nil}
	}
	first, _ := syntax.LeadingCall()
	if !first.Valid() {
		return callSubqueryScopeClause{invalid: err != nil}
	}
	if first.Rule() == cypherantlr.CypherParserRULE_callSubquery {
		scope := scopeClauseFromSyntax(first)
		scope.invalid = err != nil
		return scope
	}
	name := first.ChildRule(cypherantlr.CypherParserRULE_invocationName)
	if name.Valid() {
		start, _ := name.Span()
		return callSubqueryScopeClause{bodyOffset: start, invalid: err != nil}
	}
	return callSubqueryScopeClause{invalid: err != nil}
}
