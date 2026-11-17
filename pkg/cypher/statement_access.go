package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
)

type readOnlyStatementsKey struct{}

// WithReadOnlyStatements restricts this statement and dynamically executed child
// statements to read-only effects. It narrows, never replaces, authorization.
// For example a Bolt READ transaction keeps this scope on each RUN context.
func WithReadOnlyStatements(ctx context.Context) context.Context {
	if ctx.Value(readOnlyStatementsKey{}) != nil {
		return ctx
	}
	return context.WithValue(ctx, readOnlyStatementsKey{}, true)
}

// CheckStatementAccess admits statement effects against the caller's requested
// read restriction. Dynamic statement procedures are admitted here; their resolved
// text re-enters this same owner before execution. EXPLAIN has no write effects.
// The source stage prevents client escapes from being interpreted twice.
func CheckStatementAccess(ctx context.Context, query string, stage SourceStage) error {
	if ctx.Value(readOnlyStatementsKey{}) == nil {
		return nil
	}
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return source.Err
	}
	contract, err := readStatementExecutionContract(ctx, source.Text)
	if err != nil {
		return err
	}
	return readStatementAccessError(ctx, contract)
}

func readStatementAccessError(ctx context.Context, contract statementExecutionContract) error {
	// Unknown procedures retain their ProcedureNotFound diagnostic. Only known
	// updating effects require a read-mode refusal before handler resolution.
	if ctx.Value(readOnlyStatementsKey{}) != nil && (contract.writes || contract.stateChange) {
		return newSemanticError("Neo.ClientError.Statement.AccessMode", "AccessMode", "Writing in read access mode not allowed")
	}
	return nil
}
