package storage

import "context"

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
// The write is already staged: index maintenance must finish with it.
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

func (tx *BadgerTransaction) pendingValueMatchesLocked(ctx context.Context, label, property string, valueKey interface{}) ([]NodeID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx.pendingIndex == nil && len(tx.pendingNodes) <= pendingIndexMinNodes {
		var matches []NodeID
		for id, node := range tx.pendingNodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if !hasLabel(node.Labels, label) {
				continue
			}
			key, ok, err := indexValueKey(ctx, node.Properties[property])
			if err != nil {
				return nil, err
			}
			if ok && key == valueKey {
				matches = append(matches, id)
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return matches, nil
	}
	index := tx.pendingIndex
	if index == nil {
		index = newPendingNodeIndex()
		for _, node := range tx.pendingNodes {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for _, nodeLabel := range node.Labels {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				ids := index.byLabel[nodeLabel]
				if ids == nil {
					ids = make(map[NodeID]bool)
					index.byLabel[nodeLabel] = ids
				}
				ids[node.ID] = true
			}
		}
	}
	if err := index.track(ctx, label, property, func(id NodeID) *Node { return tx.pendingNodes[id] }); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx.pendingIndex = index
	return index.byValue[pendingValueKey{label: label, property: property, value: valueKey}], nil
}

// MergePendingPropertyMatches turns ids, a property-index lookup of label's
// property = value against committed state, into what this transaction
// sees: the IDs of nodes the transaction has rewritten or deleted are
// dropped, and the transaction's own pending nodes that have label and the
// value are added. ids is filtered in place. A transaction without node
// writes returns ids unchanged.
func (tx *BadgerTransaction) MergePendingPropertyMatches(ctx context.Context, ids []NodeID, label, property string, value interface{}) ([]NodeID, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return ids, nil
	}
	kept := ids[:0]
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, rewritten := tx.pendingNodes[id]; rewritten {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted {
			continue
		}
		kept = append(kept, id)
	}
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil {
		return nil, err
	}
	if !ok || len(tx.pendingNodes) == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return kept, nil
	}
	pending, err := tx.pendingValueMatchesLocked(ctx, label, property, valueKey)
	if err != nil {
		return nil, err
	}
	for _, id := range pending {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		kept = append(kept, id)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return kept, nil
}

// MergePendingCompositeMatches is MergePendingPropertyMatches for a composite
// index: committed IDs the transaction rewrote or deleted are dropped, and
// pending nodes that carry label and match the leading values of the index's
// properties (full or prefix) are added. values are raw pattern values; they
// are canonicalized the same way IndexNode/LookupFull canonicalize, so a
// pending node equals a committed entry exactly when it keys identically.
func (tx *BadgerTransaction) MergePendingCompositeMatches(ctx context.Context, ids []NodeID, label string, properties []string, values []interface{}) ([]NodeID, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 && len(tx.deletedNodes) == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return ids, nil
	}
	kept := ids[:0]
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, rewritten := tx.pendingNodes[id]; rewritten {
			continue
		}
		if _, deleted := tx.deletedNodes[id]; deleted {
			continue
		}
		kept = append(kept, id)
	}
	for id, node := range tx.pendingNodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		matched, err := compositeNodeMatches(ctx, node, label, properties, values)
		if err != nil {
			return nil, err
		}
		if matched {
			kept = append(kept, id)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return kept, nil
}

// compositeNodeMatches reports whether a pending node carries label and its
// properties equal the given leading index values under composite-index key
// semantics (the same canonicalization as IndexNode). ctx is the read's
// lifetime, which both values' preparation observes.
func compositeNodeMatches(ctx context.Context, node *Node, label string, properties []string, values []interface{}) (bool, error) {
	if node == nil || !hasLabel(node.Labels, label) || len(values) == 0 || len(values) > len(properties) {
		return false, nil
	}
	for i, value := range values {
		propValue, exists := node.Properties[properties[i]]
		if !exists {
			return false, nil
		}
		want, ok, err := indexValueKey(ctx, value)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		got, ok, err := indexValueKey(ctx, propValue)
		if err != nil {
			return false, err
		}
		if !ok || got != want {
			return false, nil
		}
	}
	return true, nil
}
