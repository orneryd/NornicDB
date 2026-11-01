// Author: Claude R17 implementation agent, 30 October 2026.
package cypher

// Text fixtures for tests: each reads a test's statement text through the
// retained owner production uses (no production caller reads text this way).

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// forEachQueryBodyText preserves the independent source/admission boundary and
// its original brace-coordinate protocol. Retained readers use the typed entry.
func forEachQueryBodyText(ctx context.Context, source string, containers []string, visit func(string, int, int) error) error {
	if isNornicExtensionStatement(source) || isSchemaCommandStatement(source) {
		return nil
	}
	syntax, err := cypherantlr.InspectSyntax(ctx, cypherantlr.ReadStatement(source), cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		return syntaxAdmissionError(ctx, source, AcceptedSource, err)
	}
	return forEachQueryContainer(ctx, syntax.Root(), containers, func(kind string, node cypherantlr.SyntaxNode) error {
		open, hasOpen := node.DirectToken(cypherantlr.CypherLexerLBRACE)
		close, hasClose := node.DirectToken(cypherantlr.CypherLexerRBRACE)
		if !hasOpen || !hasClose {
			return nil
		}
		_, start := open.Span()
		end, _ := close.Span()
		return visit(kind, start, end)
	})
}

func callSubqueryBraces(ctx context.Context, source string) (open, end int, ok bool) {
	call, err := cypherantlr.LeadingCallSyntax(ctx, cypherantlr.ReadStatement(source), cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return -1, -1, false
	}
	if !call.Valid() || call.Rule() != cypherantlr.CypherParserRULE_callSubquery {
		return -1, -1, false
	}
	return querySyntaxBraces(call)
}

func querySyntaxBraces(node cypherantlr.SyntaxNode) (open, end int, ok bool) {
	left, hasLeft := node.DirectToken(cypherantlr.CypherLexerLBRACE)
	right, hasRight := node.DirectToken(cypherantlr.CypherLexerRBRACE)
	if !hasLeft || !hasRight {
		return -1, -1, false
	}
	open, _ = left.Span()
	end, _ = right.Span()
	return open, end, true
}

func (e *StorageExecutor) validatePipelineSizeArguments(ctx context.Context, rows []pipelineRow, clause pipelineClause, keyword string) error {
	return e.validatePipelineSizeExpressions(ctx, rows, projectionExpressions(ctx, clause, keyword))
}
