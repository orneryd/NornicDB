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
	namespace := tx.namespace
	schema := tx.engine.GetSchemaForNamespace(namespace)
	tx.mu.Unlock()
	for _, key := range schema.uniqueMergeKeys(labels, props) {
		if err := tx.prepareMergeAdmissionKey(ctx, schema, namespace, key, props); err != nil {
			return err
		}
	}
	return nil
}

func (tx *BadgerTransaction) prepareMergeAdmissionKey(ctx context.Context, schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}) error {
	tx.mu.Lock()
	_, held := tx.mergeKeyReleases[key.lock]
	active := tx.Status == TxStatusActive
	tx.mu.Unlock()
	if !active || held {
		// No peer can commit a key this transaction already holds.
		return nil
	}
	nodeID, found, complete := tx.mergeAdmissionHolder(schema, namespace, key, props)
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
	if nodeID, found, _ := tx.mergeAdmissionHolder(schema, namespace, key, props); found {
		return tx.catchUpToNode(nodeID)
	}
	return nil
}

// mergeAdmissionHolder reads key's committed holder, outside the transaction
// snapshot. Scalar admission retains its existing cache contract; composite
// admission takes the candidate of the constraint's complete equality index
// that commit validation would reject the tuple for (committedKeyHolder).
func (tx *BadgerTransaction) mergeAdmissionHolder(schema *SchemaManager, namespace string, key mergeAdmissionKey, props map[string]interface{}) (NodeID, bool, bool) {
	label, properties := key.lock.label, key.properties
	if len(properties) == 1 {
		nodeID, found, _, complete := schema.LookupUniqueConstraintValueForPlanning(label, properties[0], props[properties[0]])
		return nodeID, found, complete
	}
	idx := schema.filledKeyIndex(label, properties)
	if idx == nil {
		return "", false, false
	}
	hash, _ := key.lock.value.(string)
	for _, id := range idx.lookupFullHash(hash) {
		if tx.engine.committedKeyHolder(id, namespace, label, properties, props) != nil {
			return id, true, true
		}
	}
	return "", false, true
}
