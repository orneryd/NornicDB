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
	if tx.clientDetached.Load() {
		return ErrTransactionClosed
	}
	tx.mu.Lock()
	if tx.Status != TxStatusActive || tx.namespace == "" || tx.snapshotTx == nil {
		tx.mu.Unlock()
		return nil
	}
	namespace, engine := tx.namespace, tx.engine
	tx.mu.Unlock()
	if err := engine.initializeManagedEntityNamespace(ctx, namespace); err != nil {
		return err
	}
	tx.mu.Lock()
	if tx.clientDetached.Load() {
		tx.mu.Unlock()
		return ErrTransactionClosed
	}
	if tx.Status != TxStatusActive {
		tx.mu.Unlock()
		return nil
	}
	if tx.commitReleaseWrite == nil {
		lease, err := engine.beginBarrierWrite(false, false)
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

	for _, key := range schema.uniqueMergeKeys(labels, props) {
		if err := tx.prepareMergeAdmissionKey(ctx, schema, namespace, key, props, write); err != nil {
			return err
		}
	}
	return nil
}

func (tx *BadgerTransaction) prepareMergeAdmissionKey(ctx context.Context, schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}, write bool) error {
	tx.mu.Lock()
	_, held := tx.mergeKeyReleases[key.lock]
	active := tx.Status == TxStatusActive
	tx.mu.Unlock()
	if !active || held {
		// No peer can commit a key this transaction already holds.
		return nil
	}
	nodeID, found, complete, holderErr := tx.mergeAdmissionHolderWithContext(ctx, schema, namespace, key, props)
	if holderErr != nil {
		return holderErr
	}
	if found && !write {
		return tx.catchUpToNode(nodeID)
	}
	if !found && !complete {
		return nil
	}
	release, err := schema.acquireUniqueConstraintCommitLocks(ctx, tx.ID, []uniqueConstraintLockKey{key.lock})
	if err != nil {
		return err
	}
	tx.mu.Lock()
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
	nodeID, found, _, holderErr = tx.mergeAdmissionHolderWithContext(ctx, schema, namespace, key, props)
	if holderErr != nil {
		return holderErr
	}
	if found {
		if write {
			if err := tx.settleBufferedNode(nodeID); err != nil {
				return err
			}
		}
		return tx.catchUpToNode(nodeID)
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
func (tx *BadgerTransaction) settleBufferedNode(nodeID NodeID) error {
	buffer := tx.engine.writeBehind
	if buffer == nil {
		return nil
	}
	_, pending, _ := buffer.GetNode(nodeID)
	if pending {
		if err := buffer.Flush(); err != nil {
			return err
		}
	}
	tx.mu.Lock()
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
	return tx.refreshSnapshotLocked()
}

// mergeAdmissionHolder reads key's committed holder, outside the transaction
// snapshot. Scalar and composite indexes supply candidates; the visible point
// owner decides whether a candidate is an admission holder. Physical uniqueness
// remains the commit validator's separate responsibility.
func (tx *BadgerTransaction) mergeAdmissionHolderWithContext(ctx context.Context, schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}) (NodeID, bool, bool, error) {
	label, properties := key.lock.label, key.properties
	if len(properties) == 1 {
		id, found, _, complete := schema.LookupUniqueConstraintValueForPlanning(label, properties[0], props[properties[0]])
		if !found {
			return id, false, complete, nil
		}
		holder, err := tx.engine.committedKeyHolder(id, namespace, label, properties, props)
		if err != nil || holder == nil {
			return "", false, complete, err
		}
		return id, true, complete, nil
	}
	idx := schema.filledKeyIndex(label, properties)
	if idx == nil {
		return "", false, false, nil
	}
	hash, _ := key.lock.value.(string)
	ids := idx.lookupFullHash(hash)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return "", false, true, err
		}
		holder, err := tx.engine.committedKeyHolder(id, namespace, label, properties, props)
		if err != nil {
			return "", false, true, err
		}
		if holder != nil {
			return id, true, true, nil
		}
	}
	return "", false, true, nil
}
