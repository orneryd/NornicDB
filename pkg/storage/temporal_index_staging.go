package storage

import (
	"context"
	"fmt"
	"time"
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
func stageTemporalBackfillForLabel(ctx context.Context, tx *BadgerTransaction, engine Engine, label string) error {
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
		if err := tx.stageTemporalRebuildNodeInTxn(ctx, node, asOf); err != nil {
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
	return temporalRebuildView{transaction: tx}
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

// stageTemporalRebuildNodeInTxn stages one stored node's derived temporal
// entries in this statement's transaction. The entries are the plan's
// derivation, the same one the maintenance backfill writes
// (rebuildTemporalNodeInTxn); this owner's writes are the statement's buffered
// writes, so the staged key writes below are reachable only from schema
// transaction staging, and they publish with the statement or not at all.
func (tx *BadgerTransaction) stageTemporalRebuildNodeInTxn(ctx context.Context, node *Node, asOf time.Time) error {
	plan, err := tx.stagedTemporalRebuild().plan(ctx, node, asOf)
	if err != nil {
		return err
	}
	for {
		entry, ok, err := plan.nextEntry(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := tx.stageTemporalRebuildKey(entry.history, []byte{}); err != nil {
			return err
		}
		if entry.current == nil {
			continue
		}
		if err := tx.stageTemporalRebuildKey(entry.current, entry.currentID); err != nil {
			return err
		}
	}
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
	return readTemporalCurrentPointer(tx.badgerTx, key)
}
