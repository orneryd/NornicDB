package cypher

import (
 "context"

 "github.com/orneryd/nornicdb/pkg/fabric"
 "github.com/orneryd/nornicdb/pkg/localization"
 "github.com/orneryd/nornicdb/pkg/storage"
)

// A Fabric coordinator cannot undo independently committed batches. Reject
// the actual resolved handler before any action, also on marked remote peers.
// Native explicit transactions retain v1.4.3's accepted, folded semantics.
func (e *StorageExecutor) checkPeriodicTransactionScope(ctx context.Context, procedure string) error {
 _,distributed:=fabric.FabricTransactionFromContext(ctx)
 coordinatorOwned:=storage.CoordinatorOwnsTransaction(ctx)
 if e.txContext!=nil {
  _,composite:=e.txContext.tx.(*fabric.FabricTransaction)
  coordinatorOwned=coordinatorOwned||e.txContext.coordinatorOwned||composite
 }
 if distributed || coordinatorOwned {
  return localizedStatusError("Neo.ClientError.Transaction.TransactionTypeNotSupported","PeriodicRequiresAutocommit",localization.CypherTransactionsQueryInTransactionUnsupported("CALL "+procedure))
 }
 return nil
}

// Preserve shipped explicit-transaction folding. An autocommit statement's
// rollback wrapper, including a dynamic CALL's wrapper, must not absorb the
// independently owned batches. Auth, cancellation and statement values stay
// on the same context; only the caller's pending storage is hidden.
func (e *StorageExecutor) executePeriodicBatch(ctx context.Context, procedure string, run func(context.Context, *StorageExecutor) (*ExecuteResult, error)) (*ExecuteResult, error) {
 if err:=e.checkPeriodicTransactionScope(ctx,procedure); err!=nil { return nil,err }
 if e.txContext!=nil && e.txContext.active { return run(ctx,e) }
 store:=e.storage
 for {
  wrapper,ok:=store.(*transactionStorageWrapper)
  if !ok {break}
  store=wrapper.underlying
 }
 batch:=e.cloneForStorage(store)
 batch.txContext=nil
 ctx=periodicBatchContext{Context:ctx}
 ctx=context.WithValue(ctx,expressionFailureKey{},&expressionFailure{})
 ctx=context.WithValue(ctx,expressionOperandFailureKey{},&expressionFailure{})
 return batch.executeWithImplicitTransactionCallback(ctx,"CALL "+procedure,"CALL "+procedure,run)
}

type periodicBatchContext struct{context.Context}
func (ctx periodicBatchContext) Value(key interface{}) interface{} {
 if key==ctxKeyTxStorage { return nil }
 return ctx.Context.Value(key)
}