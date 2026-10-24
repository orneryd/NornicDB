package cypher

import "context"

// executeTransactionBatch owns only the batch's commits. The outer statement
// keeps its transaction, so ordinary writes roll back with that statement.
// Admitted batches precede outer writes; after a child commit the still-empty
// outer transaction advances through storage's existing snapshot owner.
func (e *StorageExecutor) executeTransactionBatch(ctx context.Context, clause string, execute func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	// A composite statement commits as one (compositeStatement): a statement
	// nested in it, such as a procedure's, runs no batch of its own.
	if compositeStatementFrom(ctx) != nil {
		return nil, transactionalCallContextError("transactional CALL on composite targets is not supported")
	}
	base := e
	outer, _ := e.storage.(*transactionStorageWrapper)
	if outer != nil {
		if tx := outer.transaction(); tx.OperationCount() > 0 || tx.HasKnowledgePolicyChanges() {
			return nil, transactionalCallContextError("transactional CALL cannot follow statement writes")
		}
		base = e.cloneWithStorage(outer.GetInnerEngine())
	}
	changed := false
	result, err := base.executeWithImplicitTransactionCallback(ctx, clause, upperASCII(clause), func(batchCtx context.Context, batchExec *StorageExecutor) (*ExecuteResult, error) {
		result, err := execute(batchCtx, batchExec)
		if wrapper, ok := batchExec.storage.(*transactionStorageWrapper); ok {
			tx := wrapper.transaction()
			changed = tx.OperationCount() > 0 || tx.HasKnowledgePolicyChanges()
		}
		return result, err
	})
	if err == nil && changed && outer != nil {
		err = outer.transaction().AdvanceImplicitSnapshot()
		e.invalidateNodeLookupCache()
	}
	return result, err
}
