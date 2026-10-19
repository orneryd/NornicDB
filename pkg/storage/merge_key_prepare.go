package storage

import "context"

// PrepareMergeNode admits every complete UNIQUE / NODE KEY tuple in a MERGE
// node's property map before its snapshot lookup. It uses commit's canonical
// key projection and registry, retaining acquired keys until transaction end.
// A peer committed after an unobserved snapshot is caught up; a snapshot that
// has already been read or written returns retryable ErrConflict instead.
func (tx *BadgerTransaction) PrepareMergeNode(ctx context.Context, labels []string, props map[string]interface{}) error {
	tx.mu.Lock()
	if tx.Status != TxStatusActive || tx.namespace == "" || tx.snapshotTx == nil {
		tx.mu.Unlock()
		return nil
	}
	schema := tx.engine.GetSchemaForNamespace(tx.namespace)
	tx.mu.Unlock()
	for _, key := range schema.uniqueMergeKeys(labels, props) {
		if err := tx.prepareMergeAdmissionKey(ctx, schema, key, props); err != nil {
			return err
		}
	}
	return nil
}

func (tx *BadgerTransaction) prepareMergeAdmissionKey(ctx context.Context, schema *SchemaManager, key mergeAdmissionKey, props map[string]interface{}) error {
	tx.mu.Lock()
	_, held := tx.mergeKeyReleases[key.lock]
	active := tx.Status == TxStatusActive
	tx.mu.Unlock()
	if !active || held {
		// No peer can commit a key this transaction already holds.
		return nil
	}
	nodeID, found, complete := schema.lookupMergeAdmissionNode(key, props)
	if found {
		return tx.catchUpToNode(nodeID)
	}
	if !complete {
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
	if nodeID, found, _ := schema.lookupMergeAdmissionNode(key, props); found {
		return tx.catchUpToNode(nodeID)
	}
	return nil
}
