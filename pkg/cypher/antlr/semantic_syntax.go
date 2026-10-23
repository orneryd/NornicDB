package antlr

import (
	"context"

	antlr4 "github.com/antlr4-go/antlr/v4"
)

// ReadClauseSyntax exposes the grammar tree for one pipeline clause. Consumers
// walk every child, including expressions in patterns, projections and tails;
// this reader does not maintain a second list of expression-bearing clauses.
func ReadClauseSyntax(ctx context.Context, query string, consume func(antlr4.Tree, []rune)) error {
	return readBatchTree(ctx, query, batchReadClause, consume)
}

// ReadExpressionSyntax uses the same grammar for semantic expression admission.
// Parameters and row values are deliberately absent from this syntax layer.
func ReadExpressionSyntax(ctx context.Context, query string, consume func(antlr4.Tree, []rune)) error {
	return readBatchTree(ctx, query, batchReadExpression, consume)
}

// SyntaxText returns a parsed rule's original text, preserving token gaps.
func SyntaxText(node antlr4.ParserRuleContext, source []rune) string {
	return sourceExpression(node, source)
}

// ReadStatementSyntax exposes the same grammar tree with statement prefixes
// and UNION branches intact, for shared statement-level semantic admission.
func ReadStatementSyntax(ctx context.Context, query string, consume func(antlr4.Tree, []rune)) error {
	return readBatchTree(ctx, query, batchReadStatement, consume)
}
