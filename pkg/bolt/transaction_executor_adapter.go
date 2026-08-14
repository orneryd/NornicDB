package bolt

import (
	"context"
)

// transactionalBoltQueryExecutorAdapter owns one database-scoped explicit
// transaction executor. The session lifecycle serializes RUN and terminal
// operations for every TransactionalExecutor, so this storage adapter needs no
// second lock or polling loop. It is intentionally not cached across sessions.
type transactionalBoltQueryExecutorAdapter struct {
	boltQueryExecutorAdapter

	inTx bool
}

// Execute delegates transaction-scoped Cypher to the database executor chosen
// for this session.
func (a *transactionalBoltQueryExecutorAdapter) Execute(
	ctx context.Context,
	query string,
	params map[string]any,
) (*QueryResult, error) {
	return a.boltQueryExecutorAdapter.Execute(ctx, query, params)
}

// HasPendingTransactionWrites reports storage truth for commit cache handling.
func (a *transactionalBoltQueryExecutorAdapter) HasPendingTransactionWrites() bool {
	return a.executor != nil && a.executor.HasPendingTransactionWrites()
}

// BeginTransaction preclaims adapter ownership so panic compensation can reach
// a downstream transaction allocated before the panic.
func (a *transactionalBoltQueryExecutorAdapter) BeginTransaction(ctx context.Context, _ map[string]any) error {
	if a.inTx {
		return nil
	}
	// Claim ownership before issuing BEGIN so lifecycle best-effort cleanup can
	// reach a storage transaction if the downstream executor allocates it and
	// then returns an error or panics.
	a.inTx = true
	if _, err := a.boltQueryExecutorAdapter.Execute(ctx, "BEGIN", nil); err != nil {
		// StorageExecutor's returned-error contract leaves no active storage
		// transaction. A panic does not cross this branch, so the preclaim stays
		// set and lifecycle recovery can still issue the compensating ROLLBACK.
		a.inTx = false
		return err
	}
	return nil
}

// CommitTransaction releases adapter ownership after the terminal storage call;
// lifecycle treats returned errors and panics as uncertain outcomes.
func (a *transactionalBoltQueryExecutorAdapter) CommitTransaction(ctx context.Context) error {
	if !a.inTx {
		return nil
	}
	_, err := a.boltQueryExecutorAdapter.Execute(ctx, "COMMIT", nil)
	a.inTx = false
	return err
}

// RollbackTransaction issues at most one compensating storage rollback for the
// adapter-owned transaction.
func (a *transactionalBoltQueryExecutorAdapter) RollbackTransaction(ctx context.Context) error {
	if !a.inTx {
		return nil
	}
	_, err := a.boltQueryExecutorAdapter.Execute(ctx, "ROLLBACK", nil)
	a.inTx = false
	return err
}
