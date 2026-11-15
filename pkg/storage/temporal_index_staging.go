package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// A temporal constraint's history index becomes complete when the entries of
// the nodes that existed before the constraint are written. When the constraint
// is published by a schema statement, those entries belong to that statement's
// own transaction (MarkNewTemporalConstraintsForEngine): they are staged here
// and publish with the constraint attributes, so a statement that rolls back
// leaves neither the constraint nor an entry behind. The engine's own
// backfills (a direct caller, the start-up rebuild) keep their maintenance
// write; this file is only the transaction-staged owner.

// stageTemporalBackfillForLabel stages the temporal history and
// current-version entries of every stored node of label in the schema
// statement's transaction, from that transaction's snapshot: the same per-node
// work as RebuildTemporalIndexes and backfillTemporalIndexForLabel, staged
// instead of committed on its own. Its writes join the transaction's buffered
// writes and its reads are the transaction's own (stagedTemporalRebuild), so
// nothing it derives reaches storage before the statement commits. engine is
// that transaction's view: the population and each stored body are read through
// it, in its namespace.
func (b *BadgerEngine) stageTemporalBackfillForLabel(ctx context.Context, tx *BadgerTransaction, engine Engine, label string) error {
	// The population is the transaction's own label stream, read as IDs: the
	// indexed entities are then read once, without vectors, through the same
	// view. The view hands back the namespace-free IDs of its database; the
	// entries are keyed by the stored ID the transaction commits under.
	var ids []NodeID
	if err := StreamStoredNodesByLabelProjected(ctx, engine, label, []string{}, func(node *Node) error {
		if node != nil {
			ids = append(ids, EnsureNodeIDDatabasePrefixForEngine(engine, node.ID))
		}
		return nil
	}); err != nil {
		return err
	}
	view := tx.stagedTemporalRebuild()
	asOf := time.Now().UTC()
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := view.stored(ctx, id)
		if err != nil {
			return err
		}
		if err := b.rebuildTemporalNodeInTxn(ctx, view, node, asOf); err != nil {
			return err
		}
	}
	return nil
}

// stagedSchemaTransactionFor returns the schema statement's transaction when
// engine is that statement's own view, so its backfill stages its derived
// writes there. A view that is not a statement transaction (ok is false) leaves
// the backfill to the engine's maintenance pass. A view that reports a
// transaction which does not own it is refused rather than trusted: staging
// derived entries in another transaction would publish state this statement
// never read, in a transaction it does not own.
func stagedSchemaTransactionFor(engine Engine, schema *SchemaManager) (*BadgerTransaction, error) {
	view, ok := engine.(interface{ NativeSchemaTransaction() *BadgerTransaction })
	if !ok {
		return nil, nil
	}
	tx := view.NativeSchemaTransaction()
	if tx == nil || tx.ownsSchemaView(engine, schema) {
		return tx, nil
	}
	return nil, fmt.Errorf("temporal constraint backfill requires the statement's own transaction: %w", ErrInvalidData)
}

// ownsSchemaView reports whether tx is the native transaction behind engine's
// view: the engine the view serves, the schema the statement built its change
// on (the transaction's isolated view), and the database the view serves when it
// serves one. An unscoped view serves the whole store and its statement pinned
// this transaction to the store's own database, so it decides no namespace here.
func (tx *BadgerTransaction) ownsSchemaView(engine Engine, schema *SchemaManager) bool {
	// Wrapper identity methods may themselves consult transaction metadata.
	// Resolve foreign callbacks before taking the transaction's own lock.
	underlying := UnwrapEngine(engine)
	scope := engineScope(engine)
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.engine == nil || tx.engine != underlying {
		return false
	}
	if tx.knowledgeSchema == nil || tx.knowledgeSchema != schema {
		return false
	}
	return len(scope) != 1 || scope[0] == "" || scope[0] == tx.namespace
}

// stagedTemporalRebuild is the view of a temporal backfill whose derived writes
// belong to this transaction: they join its buffered writes, and its reads are
// its own snapshot with its pending writes over it. The schema is the isolated
// view holding the constraint the statement is creating.
func (tx *BadgerTransaction) stagedTemporalRebuild() temporalRebuildView {
	return temporalRebuildView{
		write:   tx.stageTemporalRebuildKey,
		schema:  tx.temporalRebuildSchema,
		stored:  tx.storedNodeWithoutVectors,
		current: tx.temporalRebuildCurrentPointer,
	}
}

// temporalRebuildSchema is the staged backfill's schema for a node's database:
// the transaction's isolated view, which is the schema the statement applies its
// change to, and only for the database this transaction is pinned to. A node of
// another database has no schema in this statement's view, and its derived keys
// belong to a database the statement does not own.
func (tx *BadgerTransaction) temporalRebuildSchema(namespace string) *SchemaManager {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace != tx.namespace {
		return nil
	}
	return tx.knowledgeSchema
}

// stageTemporalRebuildKey stages one derived key in the transaction's buffered
// writes. The operation check keeps a staging pass from outliving its
// transaction: a rolled-back transaction is no longer active, so the write is
// refused instead of buffered, and none of what the pass derived can reach
// storage.
func (tx *BadgerTransaction) stageTemporalRebuildKey(key, value []byte) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionSchemaWrite); err != nil {
		return err
	}
	tx.bufferSet(key, value)
	return nil
}

// temporalRebuildCurrentPointer reads the node a current-version key holds as
// this transaction sees it: its own staged writes and deletes first, then its
// snapshot.
func (tx *BadgerTransaction) temporalRebuildCurrentPointer(key []byte) (NodeID, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionSchemaWrite); err != nil {
		return "", false, err
	}
	if staged, ok := tx.pendingWrites[string(key)]; ok {
		return NodeID(append([]byte(nil), staged...)), true, nil
	}
	if tx.pendingDeletes[string(key)] {
		return "", false, nil
	}
	item, err := tx.badgerTx.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var current NodeID
	if err := item.Value(func(val []byte) error {
		current = NodeID(append([]byte(nil), val...))
		return nil
	}); err != nil {
		return "", false, err
	}
	return current, true, nil
}
