package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Preserve database admission/accounting wrappers when committing the raw
// transaction opened by unwrapping the storage engine. Both explicit and
// implicit transactions must use this boundary.
func (e *StorageExecutor) commitStorageTransaction(ctx context.Context, tx *storage.BadgerTransaction) (err error) {
	// This is the last abortable gate before storage may publish a write.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := memoryFailureFromContext(ctx); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			publishStatementCommit(ctx)
		}
	}()
	engine := e.storage
	if e.txContext != nil && e.txContext.tx == tx && e.txContext.engine != nil {
		// Administration can choose system after BEGIN. Commit through that
		// transaction's managed owner, not the request's original user graph.
		engine = e.txContext.engine
		if e.txContext.commitOwner != nil {
			return e.txContext.commitOwner.CommitTransaction(ctx, tx)
		}
	}
	var owner storage.TransactionCommitter
	if engine == e.storage && e.storageExecution != nil {
		owner = e.storageExecution.committer
	} else {
		owner, _ = storage.CommitOwnerForEngine(engine)
	}
	ok := owner != nil
	if !ok {
		return storage.ErrNotImplemented
	}
	return owner.CommitTransaction(ctx, tx)
}
