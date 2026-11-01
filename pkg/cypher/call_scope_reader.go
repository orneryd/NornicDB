// Author of retained CALL inspection: Codex R17 implementation agent.
package cypher

import (
	"context"

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
	first, err := cypherantlr.LeadingCallSyntax(ctx, cypherantlr.ReadStatement(source), cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
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
// The CALL, its parentheses and its body brace are read from the rejected
// statement's own lexer tokens (the parser's recovered product), and each
// list through callScopeClauseVariables, the scopeList production's reader.
// Author of the token-owned form: Claude R17 implementation agent, 30 October 2026.
func malformedCallScopeError(ctx context.Context, source string) error {
	syntax, _ := cypherantlr.InspectSyntax(ctx, cypherantlr.ReadStatement(source), cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return err
	}
	if !syntax.Valid() {
		return nil
	}
	var live []cypherantlr.SyntaxToken
	for index := 0; index < syntax.TokenCount(); index++ {
		if token := syntax.Token(index); token.Channel() == 0 && token.Kind() > 0 {
			live = append(live, token)
		}
	}
	for at, token := range live {
		if token.Kind() != cypherantlr.CypherLexerCALL || at+1 >= len(live) || live[at+1].Kind() != cypherantlr.CypherLexerLPAREN {
			continue
		}
		close, depth := -1, 0
		for index := at + 1; index < len(live) && close < 0; index++ {
			switch live[index].Kind() {
			case cypherantlr.CypherLexerLPAREN:
				depth++
			case cypherantlr.CypherLexerRPAREN:
				if depth--; depth == 0 {
					close = index
				}
			}
		}
		if close < 0 {
			if at == 0 {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherResultMetadataImportVariablesRequired())
			}
			continue
		}
		if close+1 >= len(live) || live[close+1].Kind() != cypherantlr.CypherLexerLBRACE {
			continue
		}
		_, start := live[at+1].Span()
		end, _ := live[close].Span()
		if _, err := callScopeClauseVariables(ctx, syntax.Source()[start:end]); err != nil {
			return err
		}
	}
	return nil
}
