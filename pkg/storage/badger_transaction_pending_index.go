package storage

// The property indexes describe committed state: a transaction's own node
// writes reach them at commit. A property-index lookup made inside the
// transaction therefore merges the transaction's pending nodes in, exactly
// as SchemaManager merges an AsyncEngine's pending writes (pendingWriteView):
// committed entries of nodes the transaction has rewritten or deleted are
// dropped, and the transaction's pending nodes are listed by their current
// values.

// pendingIndexMinNodes is the number of pending nodes above which a
// transaction indexes them by value for its property-index lookups.
const pendingIndexMinNodes = 8

// pendingNodeChangedLocked moves the pending-index entries of a node from
// prev (its previous pending version, nil if none) to node (nil when the
// transaction deletes it). It is a no-op until a lookup has built the index,
// so a transaction that never reads through a property index pays nothing.
// Caller holds tx.mu.
func (tx *BadgerTransaction) pendingNodeChangedLocked(prev, node *Node) {
	if tx.pendingIndex == nil {
		return
	}
	tx.pendingIndex.remove(prev)
	tx.pendingIndex.add(node)
}

// pendingValueMatchesLocked returns the transaction's pending nodes with
// label whose property has valueKey (an indexValueKey). The first lookup
// made with more than pendingIndexMinNodes pending nodes builds the index
// from them; the first lookup of a (label, property) pair indexes that pair
// by value. The result may be the index's own slice: callers copy it.
// Caller holds tx.mu.
func (tx *BadgerTransaction) pendingValueMatchesLocked(label, property string, valueKey interface{}) []NodeID {
	if tx.pendingIndex == nil && len(tx.pendingNodes) <= pendingIndexMinNodes {
		// A few pending nodes are compared directly: an index of them costs
		// more to build than it saves (a single-statement transaction that
		// writes one node and looks one up).
		var matches []NodeID
		for id, node := range tx.pendingNodes {
			if !hasLabel(node.Labels, label) {
				continue
			}
			if key, ok := indexValueKey(node.Properties[property]); ok && key == valueKey {
				matches = append(matches, id)
			}
		}
		return matches
	}
	if tx.pendingIndex == nil {
		tx.pendingIndex = newPendingNodeIndex()
		for _, node := range tx.pendingNodes {
			tx.pendingIndex.add(node)
		}
	}
	tx.pendingIndex.track(label, property, func(id NodeID) *Node { return tx.pendingNodes[id] })
	return tx.pendingIndex.byValue[pendingValueKey{label: label, property: property, value: valueKey}]
}

// MergePendingPropertyMatches turns ids, a property-index lookup of label's
// property = value against committed state, into what this transaction
// sees: the IDs of nodes the transaction has rewritten or deleted are
// dropped, and the transaction's own pending nodes that have label and the
// value are added. ids is filtered in place. A transaction without node
// writes returns ids unchanged.
func (tx *BadgerTransaction) MergePendingPropertyMatches(ids []NodeID, label, property string, value interface{}) []NodeID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		return ids
	}
	kept := ids[:0]
	for _, id := range ids {
		if _, rewritten := tx.pendingNodes[id]; rewritten {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted {
			continue
		}
		kept = append(kept, id)
	}
	valueKey, ok := indexValueKey(value)
	if !ok || len(tx.pendingNodes) == 0 {
		return kept
	}
	return append(kept, tx.pendingValueMatchesLocked(label, property, valueKey)...)
}

// MergePendingCompositeMatches is MergePendingPropertyMatches for a composite
// index: committed IDs the transaction rewrote or deleted are dropped, and
// pending nodes that carry label and match the leading values of the index's
// properties (full or prefix) are added. values are raw pattern values; they
// are canonicalized the same way IndexNode/LookupFull canonicalize, so a
// pending node equals a committed entry exactly when it keys identically.
func (tx *BadgerTransaction) MergePendingCompositeMatches(ids []NodeID, label string, properties []string, values []interface{}) []NodeID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		return ids
	}
	kept := ids[:0]
	for _, id := range ids {
		if _, rewritten := tx.pendingNodes[id]; rewritten {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted {
			continue
		}
		kept = append(kept, id)
	}
	if len(tx.pendingNodes) == 0 {
		return kept
	}
	for id, node := range tx.pendingNodes {
		if compositeNodeMatches(node, label, properties, values) {
			kept = append(kept, id)
		}
	}
	return kept
}

// compositeNodeMatches reports whether a pending node carries label and its
// properties equal the given leading index values under composite-index key
// semantics (the same canonicalization as IndexNode).
func compositeNodeMatches(node *Node, label string, properties []string, values []interface{}) bool {
	if node == nil || !hasLabel(node.Labels, label) || len(values) == 0 || len(values) > len(properties) {
		return false
	}
	for i, value := range values {
		propValue, exists := node.Properties[properties[i]]
		if !exists {
			return false
		}
		want, ok := indexValueKey(value)
		if !ok {
			return false
		}
		got, ok := indexValueKey(propValue)
		if !ok || got != want {
			return false
		}
	}
	return true
}
