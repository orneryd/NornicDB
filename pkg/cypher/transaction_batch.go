package cypher

import "context"

// executeTransactionBatch owns only the batch's commits. The outer statement
// keeps its transaction, so ordinary writes roll back with that statement.
// Admitted batches precede outer writes; after a child commit the still-empty
// outer transaction advances through storage's existing snapshot owner.
func (e *StorageExecutor) executeTransactionBatch(ctx context.Context, clause string, execute func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
	base := e
	outer, _ := viewMetadataForExecutor[*transactionStorageWrapper](e)
	if outer != nil {
		if outer.tx.OperationCount() > 0 || outer.tx.HasKnowledgePolicyChanges() {
			return nil, transactionalCallContextError("transactional CALL cannot follow statement writes")
		}
		base = e.cloneWithStorage(outer.underlying)
	}
	changed := false
	result, err := base.executeWithImplicitTransactionCallback(ctx, clause, upperASCII(clause), func(batchCtx context.Context, batchExec *StorageExecutor) (*ExecuteResult, error) {
		result, err := execute(batchCtx, batchExec)
		if wrapper, ok := viewMetadataForExecutor[*transactionStorageWrapper](batchExec); ok {
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
