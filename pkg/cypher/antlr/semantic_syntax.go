package antlr

import "context"

// ReadClauseSyntax is the clause-entry view of the canonical lexical product.
func ReadClauseSyntax(ctx context.Context, query string, consume func(StatementSyntax)) error {
	return readSyntax(ctx, query, ClauseEntry, consume)
}

// ReadExpressionSyntax uses the same product for structural scalar admission.
func ReadExpressionSyntax(ctx context.Context, query string, consume func(StatementSyntax)) error {
	return readSyntax(ctx, query, ExpressionEntry, consume)
}

// ReadStatementSyntax retains prefixes, UNION branches and nested clauses.
func ReadStatementSyntax(ctx context.Context, query string, consume func(StatementSyntax)) error {
	return readSyntax(ctx, query, StatementEntry, consume)
}

func readSyntax(ctx context.Context, query string, entry SyntaxEntry, consume func(StatementSyntax)) error {
	syntax, err := ParseSyntax(ctx, query, entry, false, RawSource)
	if err == nil {
		consume(syntax)
	}
	return err
}
