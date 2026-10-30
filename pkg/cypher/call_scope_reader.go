// Author of retained CALL inspection: Codex R17 implementation agent.
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
func readCallSubqueryScope(clause pipelineClause) callSubqueryScopeClause {
	if clause.kind != pipelineClauseCallSubquery {
		return scopeClauseFromSyntax(cypherantlr.SyntaxNode{})
	}
	return scopeClauseFromSyntax(clause.syntax.Window().Root())
}

// readCallSubqueryScopeText is the explicit native/invalid source boundary;
// retained CALL consumers cannot substitute source text for their clause node.
func readCallSubqueryScopeText(ctx context.Context, source string) callSubqueryScopeClause {
	first, err := cypherantlr.LeadingCallSyntax(ctx, source, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	invalidScope := err != nil
	if invalidScope && ctx.Err() == nil {
		// Refine only an existing grammar rejection. Procedure arguments and
		// a later syntax error do not constitute a malformed import scope.
		invalidScope = malformedCallScopeError(ctx, source) != nil
	}
	scope := scopeClauseFromSyntax(first)
	scope.invalid = invalidScope
	return scope
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
