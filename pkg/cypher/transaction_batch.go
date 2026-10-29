package cypher

import "context"

// executeTransactionBatch owns only the batch's commits. The outer statement
// keeps its transaction, so ordinary writes roll back with that statement.
// Admitted batches precede outer writes; after a child commit the still-empty
// outer transaction advances through storage's existing snapshot owner.
func (e *StorageExecutor) executeTransactionBatch(ctx context.Context, clause string, execute func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	base := e
	outer, _ := e.storage.(*transactionStorageWrapper)
	if outer != nil {
		if outer.tx.OperationCount() > 0 || outer.tx.HasKnowledgePolicyChanges() {
			return nil, transactionalCallContextError("transactional CALL cannot follow statement writes")
		}
		base = e.cloneWithStorage(outer.underlying)
		// This admitted child batch owns its commit. Do not rejoin the outer
		// implicit transaction through context after selecting its base engine.
		ctx = periodicBatchContext{Context: ctx}
	}
	changed := false
	result, err := base.executeWithImplicitTransactionCallback(ctx, clause, upperASCII(clause), func(batchCtx context.Context, batchExec *StorageExecutor) (*ExecuteResult, error) {
		result, err := execute(batchCtx, batchExec)
		if wrapper, ok := batchExec.storage.(*transactionStorageWrapper); ok {
			changed = wrapper.tx.OperationCount() > 0 || wrapper.tx.HasKnowledgePolicyChanges()
		}
		return result, err
	})
	if err == nil && changed && outer != nil {
		err = outer.tx.AdvanceImplicitSnapshot()
		e.invalidateNodeLookupCache()
	}
	return result, err
}
