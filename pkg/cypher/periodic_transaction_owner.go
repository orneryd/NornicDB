package cypher

import (
 "context"

 "github.com/orneryd/nornicdb/pkg/fabric"
 "github.com/orneryd/nornicdb/pkg/localization"
)

// Independent commits cannot be absorbed by a caller or undone by a remote
// coordinator after execution. Refuse the actual resolved batch-owning call
// before invoking its handler when an atomic transaction already owns it.
func (e *StorageExecutor) checkPeriodicTransactionScope(ctx context.Context, procedure string) error {
 _,distributed:=fabric.FabricTransactionFromContext(ctx)
 if e.inTransaction(ctx) || distributed {
  return localizedStatusError("Neo.ClientError.Transaction.TransactionTypeNotSupported","PeriodicRequiresAutocommit",localization.CypherTransactionsQueryInTransactionUnsupported("CALL "+procedure))
 }
 return nil
}

// Each native autocommit batch uses the existing implicit transaction owner.
// A failed batch has its own expression-error slot; previous batches remain
// committed and its error cannot poison the next batch or caller's summary.
func (e *StorageExecutor) executePeriodicBatch(ctx context.Context, procedure string, run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
 if err:=e.checkPeriodicTransactionScope(ctx,procedure); err!=nil { return nil,err }
 batch:=e.cloneForStorage(e.storage)
 batch.txContext=nil
 ctx=context.WithValue(ctx,expressionFailureKey{},&expressionFailure{})
 ctx=context.WithValue(ctx,expressionOperandFailureKey{},&expressionFailure{})
 return batch.executeWithImplicitTransactionCallback(ctx,"CALL "+procedure,"CALL "+procedure,run)
}
