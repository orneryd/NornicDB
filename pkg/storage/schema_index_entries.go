package storage

// Equality-index entry maintenance: the one owner of how a node (or, for the
// relationship uniqueness index, an edge) is filed in a CompositeIndex.
//
// Every write moves an entity from its old entries to its new ones with
// ReplaceNode, under one hold of idx.mu. A reader therefore sees either the
// old entries or the new ones, never neither: removing and re-inserting in two
// lock sections let a concurrent seek miss a node whose key never changed.
// An entry whose key is unchanged is never removed.

// IndexNode files a node under its entries in the index: an arity-1 index
// under its canonical single-property key (values, the encoding ORDER BY /
// range / equality seeks read), an arity >= 2 index under its full and every
// leading-prefix composite key. It is ReplaceNode with no previous entries.
func (idx *CompositeIndex) IndexNode(nodeID NodeID, properties map[string]interface{}) error {
	idx.ReplaceNode(nodeID, nil, properties)
	return nil
}

// RemoveNode removes a node's entries for properties from the index. It is
// ReplaceNode with no new entries.
func (idx *CompositeIndex) RemoveNode(nodeID NodeID, properties map[string]interface{}) {
	idx.ReplaceNode(nodeID, properties, nil)
}

// ReplaceNode moves a node from the entries of oldProperties to those of
// newProperties atomically for readers. Nil properties file nothing: pass nil
// old properties for a node the index did not cover, and nil new properties
// for one it no longer covers. Entries both sides share stay in place; the new
// entries are always (re)filed, so a node missing from the index is repaired.
func (idx *CompositeIndex) ReplaceNode(nodeID NodeID, oldProperties, newProperties map[string]interface{}) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(idx.Properties) == 1 {
		oldKey, hadOld := idx.singleValueKey(oldProperties)
		newKey, hasNew := idx.singleValueKey(newProperties)
		if hadOld && (!hasNew || oldKey != newKey) {
			idx.unfileValueLocked(nodeID, oldKey)
		}
		if hasNew {
			idx.fileValueLocked(nodeID, newKey)
		}
		return
	}

	oldValues := compositeIndexValues(idx, oldProperties)
	newValues := compositeIndexValues(idx, newProperties)
	shared := 0
	for shared < len(oldValues) && shared < len(newValues) && oldValues[shared] == newValues[shared] {
		shared++
	}
	// Prefixes up to shared, and the full key when every value is shared, are
	// the same entries on both sides.
	if len(oldValues) == len(idx.Properties) && shared < len(idx.Properties) {
		hash := NewCompositeKey(oldValues...).Hash
		idx.fullIndex[hash] = removeNodeID(idx.fullIndex[hash], nodeID)
		if len(idx.fullIndex[hash]) == 0 {
			delete(idx.fullIndex, hash)
		}
	}
	for i := shared + 1; i <= len(oldValues); i++ {
		hash := NewCompositeKey(oldValues[:i]...).Hash
		idx.prefixIndex[hash] = removeNodeID(idx.prefixIndex[hash], nodeID)
		if len(idx.prefixIndex[hash]) == 0 {
			delete(idx.prefixIndex, hash)
		}
	}
	if len(newValues) == len(idx.Properties) {
		hash := NewCompositeKey(newValues...).Hash
		idx.fullIndex[hash] = appendUnique(idx.fullIndex[hash], nodeID)
	}
	for i := 1; i <= len(newValues); i++ {
		hash := NewCompositeKey(newValues[:i]...).Hash
		idx.prefixIndex[hash] = appendUnique(idx.prefixIndex[hash], nodeID)
	}
}

// singleValueKey returns the arity-1 index key properties file under, false
// when the property is absent or has no key (null, byte arrays, ...).
func (idx *CompositeIndex) singleValueKey(properties map[string]interface{}) (interface{}, bool) {
	value, exists := properties[idx.Properties[0]]
	if !exists {
		return nil, false
	}
	return indexValueKey(value)
}

// fileValueLocked files nodeID under an arity-1 key. The caller holds idx.mu.
func (idx *CompositeIndex) fileValueLocked(nodeID NodeID, valueKey interface{}) {
	if idx.values == nil {
		idx.values = make(map[interface{}][]NodeID)
	}
	if _, exists := idx.values[valueKey]; !exists {
		idx.keysDirty = true
	}
	idx.values[valueKey] = appendUnique(idx.values[valueKey], nodeID)
}

// unfileValueLocked removes nodeID from an arity-1 key. The caller holds idx.mu.
func (idx *CompositeIndex) unfileValueLocked(nodeID NodeID, valueKey interface{}) {
	ids, exists := idx.values[valueKey]
	if !exists {
		return
	}
	if ids = removeNodeID(ids, nodeID); len(ids) > 0 {
		idx.values[valueKey] = ids
		return
	}
	delete(idx.values, valueKey)
	idx.keysDirty = true
}
