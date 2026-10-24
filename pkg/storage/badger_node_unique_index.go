package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// A reservation is stored with the node commit, never published by a later
// cache callback. 0x27 is a new key domain; no existing prefix is reassigned.
const prefixNodeUnique = byte(0x27)

func nodeUniqueNamespacePrefix(namespace string) []byte {
	key := []byte{prefixNodeUnique, 0}
	key = binary.AppendUvarint(key, uint64(len(namespace)))
	return append(key, namespace...)
}

func nodeUniquePrefix(namespace, label, property string) []byte {
	key := nodeUniqueNamespacePrefix(namespace)
	for _, part := range []string{label, property} {
		key = binary.AppendUvarint(key, uint64(len(part)))
		key = append(key, part...)
	}
	return key
}

func nodeUniqueKey(namespace, label, property string, value interface{}) ([]byte, bool) {
	canonical, ok := indexValueKey(value)
	if !ok || !lockableUniqueConstraintKey(uniqueConstraintLockKey{value: canonical}) {
		return nil, false
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%T:%v", canonical, canonical)))
	key := append(nodeUniquePrefix(namespace, label, property), 1)
	return append(key, digest[:]...), true
}

func readNodeUniqueKey(txn *badger.Txn, key []byte) (NodeID, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return "", nil
	}
	if err != nil {
		return "", &CommitFailureError{Cause: err}
	}
	value, err := item.ValueCopy(nil)
	if err != nil {
		return "", &CommitFailureError{Cause: err}
	}
	return NodeID(value), err
}

// readNodeUniqueValue reads the same stored key that commits mutate. Pending
// data overlays are merged by the schema reader at its existing read boundary.
func (b *BadgerEngine) readNodeUniqueValue(namespace, label, property string, value interface{}) (holder NodeID, complete bool, err error) {
	key, indexed := nodeUniqueKey(namespace, label, property, value)
	if !indexed {
		return "", false, nil
	}
	err = b.withView(func(txn *badger.Txn) error {
		var readErr error
		holder, readErr = readNodeUniqueKey(txn, key)
		if readErr != nil {
			return readErr
		}
		_, readErr = txn.Get(append(nodeUniquePrefix(namespace, label, property), 0))
		if readErr == badger.ErrKeyNotFound {
			// An unbackfilled constraint uses the actual stored bodies until
			// its DDL publishes a complete index. No side cache supplies claims.
			holder = ""
			if hook := getUniqueConstraintScanHook(); hook != nil {
				hook()
			}
			options := badgerIteratorOptions()
			options.Prefix = append([]byte{prefixNode}, []byte(namespace+":")...)
			it := txn.NewIterator(options)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				var node *Node
				if err := it.Item().Value(func(data []byte) error { var err error; node, err = b.decodeNode(namespace, data); return err }); err != nil {
					return err
				}
				if hasLabel(node.Labels, label) && compareValues(node.Properties[property], value) {
					holder = node.ID
					break
				}
			}
			complete = true
			return nil
		}
		complete = readErr == nil
		return readErr
	})
	if err != nil {
		err = &CommitFailureError{Cause: err}
	}
	return
}

func nodeUniqueReverseKey(id NodeID) []byte {
	return append([]byte{prefixNodeUnique, 1}, []byte(id)...)
}

func readNodeUniqueReverse(txn *badger.Txn, id NodeID) ([][]byte, error) {
	item, err := txn.Get(nodeUniqueReverseKey(id))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, &CommitFailureError{Cause: err}
	}
	var keys [][]byte
	err = item.Value(func(data []byte) error { return json.Unmarshal(data, &keys) })
	if err != nil {
		return nil, &CommitFailureError{Cause: err}
	}
	return keys, err
}

func writeNodeUniqueReverse(txn *badger.Txn, id NodeID, keys [][]byte) error {
	if len(keys) == 0 {
		return txn.Delete(nodeUniqueReverseKey(id))
	}
	data, err := json.Marshal(keys)
	if err != nil {
		return err
	}
	return txn.Set(nodeUniqueReverseKey(id), data)
}

// writeNodeUniqueImageInTxn is part of the primary-node write, not a commit
// callback. The reverse entry is the previous committed index image. It changes
// atomically with both the reservation keys and the node body. Repeated writes
// in one batch see their own prior image; no early identity can become stale.
func (b *BadgerEngine) writeNodeUniqueImageInTxn(txn *badger.Txn, id NodeID, after *Node, removalsOnly bool) error {
	namespace := namespaceForNodeID(id)
	schema := b.GetSchemaForNamespace(namespace)
	if !schema.HasUniqueValueTracking() {
		return nil
	}
	previous, err := readNodeUniqueReverse(txn, id)
	if err != nil {
		return err
	}
	desired := make(map[string][]byte)
	definitions := make(map[string]Constraint)
	if after != nil {
		for _, c := range schema.GetConstraintsForLabels(after.Labels) {
			if c.Type != ConstraintUnique || len(c.Properties) != 1 || c.EffectiveEntityType() == ConstraintEntityRelationship {
				continue
			}
			key, ok := nodeUniqueKey(namespace, c.Label, c.Properties[0], after.Properties[c.Properties[0]])
			if ok {
				desired[string(key)] = key
				definitions[string(key)] = c
			}
		}
	}
	old := make(map[string]bool, len(previous))
	for _, key := range previous {
		old[string(key)] = true
		if _, keep := desired[string(key)]; keep {
			continue
		}
		holder, err := readNodeUniqueKey(txn, key)
		if err != nil {
			return err
		}
		if holder == id {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
	}
	if removalsOnly {
		return nil
	}
	for encoded, key := range desired {
		if old[encoded] {
			continue
		}
		// Reading the key enrolls it in native conflict detection. All new logical
		// claims have already been checked under their commit locks. A competing
		// physical publication makes the whole data/index commit conflict.
		holder, err := readNodeUniqueKey(txn, key)
		if err != nil {
			return err
		}
		if holder != "" && holder != id {
			c := definitions[encoded]
			value, _ := indexValueKey(after.Properties[c.Properties[0]])
			view, source := schema.beginPendingRead()
			currentNode, currentHolder := view.keep(id), view.keep(holder)
			for _, pending := range view.valueMatches(nil, c.Label, c.Properties[0], value) {
				if pending == id {
					currentNode = true
				}
				if pending == holder {
					currentHolder = true
				}
			}
			endPendingRead(source)
			if currentHolder {
				if currentNode {
					return ErrConflict
				}
				// This physical replay is hidden by a newer acknowledged node image.
				// It cannot replace the reservation of a currently visible holder.
				continue
			}
		}
		if err := txn.Set(key, []byte(id)); err != nil {
			return err
		}
	}
	if len(previous) == len(desired) {
		same := true
		for _, key := range previous {
			if _, ok := desired[string(key)]; !ok {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}
	keys := make([][]byte, 0, len(desired))
	for _, key := range desired {
		keys = append(keys, key)
	}
	return writeNodeUniqueReverse(txn, id, keys)
}

// Release every departing value before inserting any new node image in the
// statement. Both phases belong to the same commit writer, so rename/create
// and swaps do not depend on map iteration order and cannot expose free keys.
func (b *BadgerEngine) prepareNodeUniqueRemovals(w *batchWriter, ops []Operation, writes map[string][]byte, deletes map[string]bool) error {
	seen := make(map[NodeID]bool)
	for _, op := range ops {
		if (op.Type != OpUpdateNode && op.Type != OpCreateNode) || seen[op.NodeID] {
			continue
		}
		seen[op.NodeID] = true
		key := nodeKey(op.NodeID)
		value, written := writes[string(key)]
		if !written || deletes[string(key)] {
			continue
		}
		if err := w.write(func(txn *badger.Txn) error { return b.nodeUniqueRecordInTxn(txn, key, value, true) }); err != nil {
			return err
		}
	}
	return nil
}

// Bulk prefix deletion uses one native DropPrefix boundary for bodies and
// their index entries. Reverse records identify only the affected nodes;
// a key reused by another node is retained. The caller holds the native drop
// gate across this selection and the deletion of data and index entries.
func (b *BadgerEngine) nodeUniqueDropPrefixes(txn *badger.Txn, prefix, namespace string, wholeNamespace bool) ([][]byte, error) {
	reversePrefix := nodeUniqueReverseKey(NodeID(prefix))
	keys := [][]byte{reversePrefix}
	if wholeNamespace {
		return append(keys, nodeUniqueNamespacePrefix(namespace)), nil
	}
	it := txn.NewIterator(badgerPrefixIteratorOptions(reversePrefix))
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		id := NodeID(it.Item().Key()[2:])
		previous, err := readNodeUniqueReverse(txn, id)
		if err != nil {
			return nil, err
		}
		for _, key := range previous {
			holder, err := readNodeUniqueKey(txn, key)
			if err != nil {
				return nil, err
			}
			if holder == id {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

// stageNodeUniqueIndexes installs the complete DDL backfill in the schema
// transaction. The temporary values belong to that backfill, not to commits.
func stageNodeUniqueIndexes(w *batchWriter, namespace string, schema *SchemaManager) error {
	for _, uc := range schema.uniqueBackfillConstraints() {
		uc.mu.RLock()
		complete := uc.valuesCacheComplete
		uc.mu.RUnlock()
		if !complete {
			continue
		}
		prefix := nodeUniquePrefix(namespace, uc.Label, uc.Property)
		if err := clearNodeUniqueConstraint(w, namespace, prefix); err != nil {
			return err
		}
		uc.mu.RLock()
		for value, id := range uc.values {
			key, ok := nodeUniqueKey(namespace, uc.Label, uc.Property, value)
			if !ok {
				continue
			}
			if err := w.write(func(txn *badger.Txn) error {
				previous, err := readNodeUniqueReverse(txn, id)
				if err != nil {
					return err
				}
				found := false
				for _, old := range previous {
					if string(old) == string(key) {
						found = true
						break
					}
				}
				if !found {
					if err := writeNodeUniqueReverse(txn, id, append(previous, key)); err != nil {
						return err
					}
				}
				return txn.Set(key, []byte(id))
			}); err != nil {
				uc.mu.RUnlock()
				return err
			}
		}
		uc.mu.RUnlock()
		if complete {
			if err := w.write(func(txn *badger.Txn) error { return txn.Set(append(prefix, 0), nil) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *BadgerEngine) deleteKeyWithUniqueIndex(txn *badger.Txn, key []byte) error {
	if len(key) > 1 && key[0] == prefixNode {
		if err := b.writeNodeUniqueImageInTxn(txn, NodeID(key[1:]), nil, false); err != nil {
			return err
		}
	}
	return txn.Delete(key)
}

func clearUniqueBackfillValues(schema *SchemaManager) {
	schema.mu.RLock()
	defer schema.mu.RUnlock()
	clearUniqueBackfillValuesLocked(schema)
}

// The schema publication owner already holds schema.mu exclusively.
func clearUniqueBackfillValuesLocked(schema *SchemaManager) {
	for _, uc := range schema.uniqueConstraints {
		uc.mu.Lock()
		uc.values = make(map[interface{}]NodeID)
		uc.valuesCacheComplete = false
		uc.mu.Unlock()
	}
}

// Copy the definitions before native writes: a batch may commit while staging,
// so it must not retain the schema lock across that storage boundary.
func (sm *SchemaManager) uniqueBackfillConstraints() []*UniqueConstraint {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	constraints := make([]*UniqueConstraint, 0, len(sm.uniqueConstraints))
	for _, constraint := range sm.uniqueConstraints {
		constraints = append(constraints, constraint)
	}
	return constraints
}

// Schema loading also runs under Restore's read hold. Use that loader's read
// privilege with the ordinary commit writer, and initialize only missing
// indexes. An already complete stored index is never repaired by restart.
func (b *BadgerEngine) initializeNodeUniqueIndexes(namespace string, schema *SchemaManager) error {
	constraints := schema.uniqueBackfillConstraints()
	if len(constraints) == 0 {
		return nil
	}
	readTS := b.db.oracle.beginReadHeld()
	defer b.db.oracle.endRead(readTS)
	cw := b.newCommitWriter(b.db, b.db.DB.NewTransactionAt(readTS, true))
	defer cw.discard()
	missing := NewSchemaManager()
	for _, uc := range constraints {
		_, err := cw.txn.Get(append(nodeUniquePrefix(namespace, uc.Label, uc.Property), 0))
		if err == badger.ErrKeyNotFound {
			missing.uniqueConstraints[uc.Label+":"+uc.Property] = uc
		} else if err != nil {
			return &CommitFailureError{Cause: err}
		}
	}
	if len(missing.uniqueConstraints) == 0 {
		return nil
	}
	if err := stageNodeUniqueIndexes(&cw.batchWriter, namespace, missing); err != nil {
		if abortErr := cw.abort(); abortErr != nil {
			return fmt.Errorf("%w: %w", err, abortErr)
		}
		return err
	}
	if err := cw.finish(); err != nil {
		if abortErr := cw.abort(); abortErr != nil {
			return fmt.Errorf("%w: %w", err, abortErr)
		}
		return err
	}
	return nil
}

// A dropped/reintroduced definition cannot inherit an old completeness marker.
// Clear its derived entries in the schema commit; the normal DDL backfill then
// installs the index from its validated snapshot.
func resetChangedNodeUniqueDefinitions(w *batchWriter, namespace string, before, after *SchemaDefinition) error {
	definitions := func(def *SchemaDefinition) map[string][]byte {
		out := make(map[string][]byte)
		if def == nil {
			return out
		}
		for _, c := range def.Constraints {
			if c.Type != ConstraintUnique || len(c.Properties) != 1 || c.EffectiveEntityType() == ConstraintEntityRelationship {
				continue
			}
			prefix := nodeUniquePrefix(namespace, c.Label, c.Properties[0])
			out[string(prefix)] = prefix
		}
		return out
	}
	old, next := definitions(before), definitions(after)
	changed := make(map[string][]byte)
	for key, prefix := range old {
		if _, retained := next[key]; !retained {
			changed[key] = prefix
		}
	}
	for key, prefix := range next {
		if _, existed := old[key]; !existed {
			changed[key] = prefix
		}
	}
	for _, prefix := range changed {
		if err := clearNodeUniqueConstraint(w, namespace, prefix); err != nil {
			return err
		}
	}
	return nil
}

// A definition change or backfill replaces both directions of that index in
// the same schema commit. Retaining old reverse keys would make a later rename
// back to an old value incorrectly look like an already registered claim.
func clearNodeUniqueConstraint(w *batchWriter, namespace string, prefix []byte) error {
	type reverseUpdate struct {
		id   NodeID
		keys [][]byte
	}
	var updates []reverseUpdate
	reversePrefix := nodeUniqueReverseKey(NodeID(namespace + ":"))
	it := w.txn.NewIterator(badgerPrefixIteratorOptions(reversePrefix))
	for it.Rewind(); it.Valid(); it.Next() {
		id := NodeID(it.Item().Key()[2:])
		previous, err := readNodeUniqueReverse(w.txn, id)
		if err != nil {
			it.Close()
			return err
		}
		kept := previous[:0]
		for _, key := range previous {
			if !bytes.HasPrefix(key, prefix) {
				kept = append(kept, key)
			}
		}
		if len(kept) != len(previous) {
			updates = append(updates, reverseUpdate{id, kept})
		}
	}
	it.Close()
	var obsolete [][]byte
	it = w.txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	for it.Rewind(); it.Valid(); it.Next() {
		obsolete = append(obsolete, it.Item().KeyCopy(nil))
	}
	it.Close()
	// Iterators close before the commit writer can rotate a large batch.
	for _, update := range updates {
		if err := w.write(func(txn *badger.Txn) error { return writeNodeUniqueReverse(txn, update.id, update.keys) }); err != nil {
			return err
		}
	}
	for _, key := range obsolete {
		if err := w.write(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
			return err
		}
	}
	return nil
}

// Direct schema persistence and transaction DDL share the same transition
// reader. The old definition comes from this commit's native transaction.
func prepareNodeUniqueSchemaTransition(w *batchWriter, namespace string, after *SchemaDefinition) error {
	var before SchemaDefinition
	item, err := w.txn.Get(schemaKey(namespace))
	if err != nil && err != badger.ErrKeyNotFound {
		return &CommitFailureError{Cause: err}
	}
	if err == nil {
		if err := item.Value(func(data []byte) error { return json.Unmarshal(data, &before) }); err != nil {
			return &CommitFailureError{Cause: err}
		}
	}
	return resetChangedNodeUniqueDefinitions(w, namespace, &before, after)
}

func (b *BadgerEngine) nodeUniqueRecordInTxn(txn *badger.Txn, key, value []byte, removalsOnly bool) error {
	if len(key) > 1 && key[0] == prefixNode {
		id := NodeID(key[1:])
		schema := b.GetSchemaForNamespace(namespaceForNodeID(id))
		schema.mu.RLock()
		var properties map[string]struct{}
		if len(schema.uniqueConstraints) > 0 {
			properties = make(map[string]struct{}, len(schema.uniqueConstraints))
			for _, c := range schema.uniqueConstraints {
				properties[c.Property] = struct{}{}
			}
		}
		schema.mu.RUnlock()
		if properties != nil {
			node, err := b.decodeNodeProjected(namespaceForNodeID(id), value, properties)
			if err != nil {
				return &CommitFailureError{Cause: err}
			}
			if err := b.writeNodeUniqueImageInTxn(txn, id, node, removalsOnly); err != nil {
				return err
			}
		}
	}
	return nil
}
