package storage

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// MergeNodePreparer is the capability of a transactional storage view that
// admits a MERGE node's unique keys before the MERGE looks the node up: the
// transaction itself, and the views that route a statement's writes to one.
// Callers discover it on the storage a statement runs against instead of
// asserting a concrete engine type, so every view that owns a transaction
// queues concurrent MERGEs of one key the same way.
type MergeNodePreparer interface {
	PrepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}) error
	PrepareMergeNodeForWrite(ctx context.Context, labels []string, props map[string]interface{}) error
}

var _ MergeNodePreparer = (*BadgerTransaction)(nil)

// PrepareMergeNode admits every complete UNIQUE / NODE KEY tuple in a MERGE
// node's property map before its snapshot lookup. It uses commit's canonical
// key projection and registry, retaining acquired keys until transaction end.
// A peer committed after an unobserved snapshot is caught up; a snapshot that
// has already been read or written returns retryable ErrConflict instead.
// A wait for a key another transaction holds ends as PrepareMergeKey says:
// with ErrDeadlock, ctx's error, or ErrLockWaitTimeout at the server's
// transaction timeout; a wait past the slow-query threshold is logged.
func (tx *BadgerTransaction) PrepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}) error {
	return tx.prepareMergeNode(ctx, labels, props, false)
}

// PrepareMergeNodeForWrite admits a MERGE node like PrepareMergeNode, for a
// statement that writes the node the MERGE matches (ON MATCH SET, or a later
// SET, REMOVE or DELETE of it). It takes each key's lock even when the key's
// node exists, so concurrent MERGE writers of one node serialize as in Neo4j:
// each waits for the one before it to end, then catches up to its commit and
// writes on top of it, instead of failing at commit because both wrote the
// node from the same snapshot (NOR-117). A commit of the node still in the
// write-behind buffer is landed first (settleBufferedNode).
func (tx *BadgerTransaction) PrepareMergeNodeForWrite(ctx context.Context, labels []string, props map[string]interface{}) error {
	return tx.prepareMergeNode(ctx, labels, props, true)
}

func (tx *BadgerTransaction) prepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}, write bool) error {
	if err := tx.clientOwnershipError(transactionDataWrite); err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	if tx.Status != TxStatusActive || tx.namespace == "" || tx.snapshotTx == nil {
		tx.mu.Unlock()
		return nil
	}
	namespace, engine := tx.namespace, tx.engine
	tx.mu.Unlock()
	if err := engine.initializeManagedEntityNamespace(ctx, namespace); err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	if err := tx.clientOwnershipError(transactionDataWrite); err != nil {
		tx.mu.Unlock()
		return err
	}
	if tx.Status != TxStatusActive {
		tx.mu.Unlock()
		return nil
	}
	if tx.commitReleaseWrite == nil {
		lease, err := engine.beginBarrierWrite(ctx, false, false)
		if err != nil {
			tx.mu.Unlock()
			return err
		}
		tx.commitReleaseWrite = lease.releaseCallback()
	}
	if err := engine.pendingNamespaceConflict(namespace); err != nil {
		tx.releaseWriteBarrierLocked()
		tx.mu.Unlock()
		return err
	}
	schema := engine.GetSchemaForNamespace(namespace)
	tx.mu.Unlock()

	keys, err := schema.uniqueMergeKeys(ctx, labels, props)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := tx.prepareMergeAdmissionKey(ctx, schema, namespace, key, props, write); err != nil {
			return err
		}
	}
	return nil
}

func (tx *BadgerTransaction) prepareMergeAdmissionKey(ctx context.Context, schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}, write bool) error {
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	_, held := tx.mergeKeyReleases[key.lock]
	active := tx.Status == TxStatusActive
	tx.mu.Unlock()
	if !active || held {
		// No peer can commit a key this transaction already holds.
		return nil
	}
	nodeID, found, complete, err := tx.mergeAdmissionHolder(ctx, schema, namespace, key, props)
	if err != nil {
		return err
	}
	if found && !write {
		return tx.catchUpToNode(ctx, nodeID)
	}
	if !found && !complete {
		return nil
	}
	wait := tx.mergeKeyWait(namespace)
	release, err := schema.acquireUniqueConstraintLocks(ctx, tx.ID, []uniqueConstraintLockKey{key.lock}, &wait)
	if err != nil {
		return err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		release()
		return err
	}
	if _, held = tx.mergeKeyReleases[key.lock]; tx.Status != TxStatusActive || held {
		tx.mu.Unlock()
		release()
		return nil
	}
	if tx.mergeKeyReleases == nil {
		tx.mergeKeyReleases = make(map[uniqueConstraintLockKey]func(), 1)
	}
	tx.mergeKeyReleases[key.lock] = release
	tx.mu.Unlock()
	nodeID, found, _, err = tx.mergeAdmissionHolder(ctx, schema, namespace, key, props)
	if err != nil {
		return err
	}
	if found {
		if write {
			if err := tx.settleBufferedNode(ctx, nodeID); err != nil {
				return err
			}
		}
		return tx.catchUpToNode(ctx, nodeID)
	}
	return nil
}

// settleBufferedNode lands a write-behind buffered commit of nodeID in Badger
// before a MERGE writer that holds the node's key catches up to it. Until the
// flusher lands it, the node's MVCC head doesn't show that commit, and the
// flush would later write the node behind the writer's Badger snapshot, so
// the writer's commit would lose to it. Once landed, a writer that has
// observed nothing moves to a snapshot taken now, even if the buffer has
// already drained: its ACK may precede the transaction while the physical
// flush follows it. Any observed transaction with a pending buffered node
// fails with ErrConflict, as catchUpToNode does. Without write-behind it
// does nothing.
func (tx *BadgerTransaction) settleBufferedNode(ctx context.Context, nodeID NodeID) error {
	buffer := tx.engine.writeBehind
	if buffer == nil {
		return nil
	}
	_, pending, _ := buffer.GetNode(nodeID)
	if pending {
		if err := buffer.flushContext(ctx, true, false); err != nil {
			return err
		}
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()
	if tx.Status != TxStatusActive {
		return nil
	}
	if tx.observedLocked() {
		if pending {
			return localizedError(localization.StorageTransactionNodeChanged(string(nodeID)), ErrConflict)
		}
		return nil
	}
	return tx.advanceSnapshotLocked(ctx, false)
}

// mergeAdmissionHolder reads key's committed holder, outside the transaction
// snapshot. Scalar admission retains its existing cache contract; composite
// admission takes the candidate of the constraint's complete equality index
// that commit validation would reject the tuple for (committedKeyHolder).
func (tx *BadgerTransaction) mergeAdmissionHolder(ctx context.Context, schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}) (NodeID, bool, bool, error) {
	label, properties := key.lock.label, key.properties
	if len(properties) == 1 {
		nodeID, found, _, complete, err := schema.LookupUniqueConstraintValue(ctx, label, properties[0], props[properties[0]])
		return nodeID, found, complete, err
	}
	idx, err := schema.filledKeyIndex(ctx, label, properties)
	if err != nil {
		return "", false, false, err
	}
	if idx == nil {
		return "", false, false, nil
	}
	hash, _ := key.lock.value.(string)
	ids, err := idx.lookupFullHash(ctx, hash)
	if err != nil {
		return "", false, false, err
	}
	var cancellation bulkReadCancellation
	for _, id := range ids {
		if err := cancellation.check(ctx); err != nil {
			return "", false, false, err
		}
		holder, err := tx.engine.committedKeyHolder(ctx, id, namespace, label, properties, props)
		if err != nil {
			return "", false, false, err
		}
		if holder != nil {
			return id, true, true, nil
		}
	}
	return "", false, true, ctx.Err()
}
