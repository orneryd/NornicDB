package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.FragmentEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	invalidScope := err != nil
	if invalidScope && ctx.Err() == nil {
		// Refine only an existing grammar rejection. Procedure arguments and
		// a later syntax error do not constitute a malformed import scope.
		invalidScope = malformedCallScopeError(ctx, source) != nil
	}
	if !syntax.Valid() {
		return callSubqueryScopeClause{invalid: invalidScope}
	}
	first, _ := syntax.LeadingCall()
	if !first.Valid() {
		return callSubqueryScopeClause{invalid: invalidScope}
	}
	if first.Rule() == cypherantlr.CypherParserRULE_callSubquery {
		scope := scopeClauseFromSyntax(first)
		scope.invalid = invalidScope
		return scope
	}
	name := first.ChildRule(cypherantlr.CypherParserRULE_invocationName)
	if name.Valid() {
		start, _ := name.Span()
		return callSubqueryScopeClause{bodyOffset: start, invalid: invalidScope}
	}
	return callSubqueryScopeClause{invalid: invalidScope}
}

// malformedCallScopeError refines a grammar rejection using CALL's canonical
// scope reader. Only a parenthesized scope followed by a subquery body qualifies;
// procedure arguments and quoted/comment text cannot supply a scope clause.
func malformedCallScopeError(ctx context.Context, source string) error {
	opts := defaultKeywordScanOpts()
	opts.SkipParens, opts.SkipBrackets = false, false
	for from := 0; from < len(source); {
		if err := ctx.Err(); err != nil {
			return err
		}
		call := keywordIndexFrom(source, "CALL", from, opts)
		if call < 0 {
			return nil
		}
		from = call + len("CALL")
		open, unfinished := cyphertext.GapEnd(source, from)
		if unfinished >= 0 || open >= len(source) || source[open] != '(' {
			continue
		}
		close := cyphertext.FindMatchingDelimiter(source, open, '(', ')', cyphertext.OpeningDelimiter)
		if close < 0 {
			leading, unfinished := cyphertext.GapEnd(source, 0)
			if unfinished < 0 && call == leading {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherResultMetadataImportVariablesRequired())
			}
			continue
		}
		body, unfinished := cyphertext.GapEnd(source, close+1)
		if unfinished >= 0 || body >= len(source) || source[body] != '{' {
			continue
		}
		if _, err := callScopeClauseVariables(ctx, source[open+1:close]); err != nil {
			return err
		}
	}
	return nil
}
