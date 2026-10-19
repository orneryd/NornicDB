package cypher

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type dynamicWriteAdmissionKey struct{}
type dynamicWriteAdmission func(context.Context,*StorageExecutor,string) error

// Both public callback reentry and internal statement execution enter here
// after normalization and parameter installation. Procedure handlers have
// already evaluated their arguments using the normal evaluator. Nested dynamic
// calls execute through the same gate, with no argument-shape prediction.
func (e *StorageExecutor) admitResolvedStatementWrite(ctx context.Context,info *QueryInfo,statement string) error {
	admit,ok := ctx.Value(dynamicWriteAdmissionKey{}).(dynamicWriteAdmission)
	if !ok { return nil }
	if info == nil { info = e.analyzer.Analyze(statement) }
	if !statementWritesGraph(info,statement) { return nil }
	return admit(ctx,e,statement)
}

// promoteResolvedStatement changes only the existing participant's admission.
// GetOrOpen enforces the one-write-shard rule; the existing callback/phase owner
// still controls COMMIT and rollback. The fragment adopts that participant's
// canonical storage adapter so later clauses see its own pending writes.
func (c *cypherFabricExecutor) promoteResolvedStatement(ctx context.Context,caller *StorageExecutor,dbName string,engine storage.Engine,statement string) error {
	sub,ok := fabric.SubTransactionFromContext(ctx)
	tx := c.fabricTransaction()
	if !ok || tx == nil { return fmt.Errorf("dynamic statement has no Fabric participant") }
	if err := c.base.authorizeSelectedDatabase(ctx,dbName,statement); err != nil { return err }
	if _,err := tx.GetOrOpen(sub.ShardName,true); err != nil { return err }
	participant,err := c.ensureLocalShardTxExecutor(ctx,sub,dbName,engine)
	if err != nil { return err }
	storageTx,ok := participant.txContext.tx.(*storage.BadgerTransaction)
	if !ok { return localizedError(localization.CypherTransactionsUnknownType(),nil) }
	wrapper := participant.transactionStorageFor(storageTx)
	if caller.storage == wrapper && caller.txContext == participant.txContext { return nil }
	wrapper.ensureNodeLookupCacheLocked(caller)
	caller.storage,caller.txContext = wrapper,participant.txContext
	caller.nodeLookupCache,caller.nodeLookupCacheMu = wrapper.txNodeLookupCache,wrapper.txNodeLookupCacheMu
	caller.propertyDecayEngine = unwrapBadgerEngine(wrapper)
	caller.propertyDecayEngineKnown = true
	return nil
}
