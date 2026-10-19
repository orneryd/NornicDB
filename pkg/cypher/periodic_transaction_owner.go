package cypher

import (
 "context"


)

// A periodic operation owns each batch, independently of its caller. Preserve
// auth/cancellation and Fabric admission, but never adopt the caller's pending
// storage or poison its expression-error slot when a batch fails.
func (e *StorageExecutor) executePeriodicBatch(ctx context.Context, procedure string, run func(context.Context,*StorageExecutor)(*ExecuteResult,error)) (*ExecuteResult,error) {
 store:=e.storage
 for {
  wrapper,ok:=store.(*transactionStorageWrapper)
  if !ok { break }
  store=wrapper.underlying
 }
 batch:=e.cloneForStorage(store)
 batch.txContext=nil
 ctx=periodicBatchContext{Context:ctx}
 ctx=context.WithValue(ctx,expressionFailureKey{},&expressionFailure{})
 ctx=context.WithValue(ctx,expressionOperandFailureKey{},&expressionFailure{})
 if admit,ok:=ctx.Value(dynamicWriteAdmissionKey{}).(dynamicWriteAdmission); ok {
  ctx=context.WithValue(ctx,dynamicWriteAdmissionKey{},dynamicWriteAdmission(func(inner context.Context,_ *StorageExecutor,statement string) error {
   return admit(inner,e,statement)
  }))
 }
 return batch.executeWithImplicitTransactionCallback(ctx,"CALL "+procedure,"CALL "+procedure,run)
}

type periodicBatchContext struct{ context.Context }
func (ctx periodicBatchContext) Value(key interface{}) interface{} {
 if key==ctxKeyTxStorage { return nil }
 return ctx.Context.Value(key)
}

