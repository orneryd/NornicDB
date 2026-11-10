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

// pendingIndexLocked returns the transaction's pending-node index, building
// it from the pending nodes the first time there are more than
// pendingIndexMinNodes of them; nil until then, while callers check the few
// pending nodes directly. Once built, pendingNodeChangedLocked keeps it
// current. Every pending-node read by value goes through it: the scalar
// (pendingValueMatchesLocked) and the composite (pendingCompositeMatchesLocked)
// lookups and their estimates.
// Caller holds tx.mu.
func (tx *BadgerTransaction) pendingIndexLocked(ctx context.Context) (*pendingNodeIndex, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx.pendingIndex != nil {
		return tx.pendingIndex, nil
	}
	if len(tx.pendingNodes) <= pendingIndexMinNodes {
		return nil, nil
	}
	index := newPendingNodeIndex()
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
	tx.pendingIndex = index
	return index, nil
}

// trackPendingLocked has index list label's property by value: the first
// lookup of a (label, property) pair indexes that pair.
// Caller holds tx.mu.
func (tx *BadgerTransaction) trackPendingLocked(ctx context.Context, index *pendingNodeIndex, label, property string) error {
	return index.track(ctx, label, property, func(id NodeID) *Node { return tx.pendingNodes[id] })
}

// pendingValueMatchesLocked returns the transaction's pending nodes with
// label whose property has valueKey (an indexValueKey), from the pending
// index (pendingIndexLocked) once there is one. The result may be the index's
// own slice: callers copy it.
// Caller holds tx.mu.
func (tx *BadgerTransaction) pendingValueMatchesLocked(ctx context.Context, label, property string, valueKey interface{}) ([]NodeID, error) {
	index, err := tx.pendingIndexLocked(ctx)
	if err != nil {
		return nil, err
	}
	if index == nil {
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
	if err := tx.trackPendingLocked(ctx, index, label, property); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return index.byValue[pendingValueKey{label: label, property: property, value: valueKey}], nil
}

// pendingCompositeMatchesLocked is pendingValueMatchesLocked for a composite
// index read: the transaction's pending nodes with label whose leading
// properties equal values, appended to out, and how many. From the pending
// index it reads only the shortest value bucket of those properties
// (compositeBucket), as the write-behind overlay's composite read does;
// countOnly returns that bucket's length, the most nodes the read can add,
// without reading it.
// Caller holds tx.mu.
func (tx *BadgerTransaction) pendingCompositeMatchesLocked(ctx context.Context, out []NodeID, label string, properties []string, values []interface{}, countOnly bool) ([]NodeID, int, error) {
	if len(values) == 0 || len(values) > len(properties) {
		return out, 0, ctx.Err()
	}
	index, err := tx.pendingIndexLocked(ctx)
	if err != nil {
		return nil, 0, err
	}
	if index == nil {
		count := 0
		for id, node := range tx.pendingNodes {
			if err := ctx.Err(); err != nil {
				return nil, 0, err
			}
			matched, err := compositeNodeMatches(ctx, node, label, properties, values)
			if err != nil {
				return nil, 0, err
			}
			if matched {
				count++
				if !countOnly {
					out = append(out, id)
				}
			}
		}
		return out, count, ctx.Err()
	}
	for _, property := range properties[:len(values)] {
		if err := tx.trackPendingLocked(ctx, index, label, property); err != nil {
			return nil, 0, err
		}
	}
	if countOnly {
		bucket, err := index.compositeBucket(ctx, label, properties, values, nil)
		return out, len(bucket), err
	}
	var small [8]interface{}
	canonical := small[:min(len(values), len(small))]
	if len(values) > len(small) {
		canonical = make([]interface{}, len(values))
	}
	bucket, err := index.compositeBucket(ctx, label, properties, values, canonical)
	if err != nil {
		return nil, 0, err
	}
	count := 0
	var cancellation bulkReadCancellation
	for _, id := range bucket {
		if err := cancellation.check(ctx); err != nil {
			return nil, 0, err
		}
		matched, err := index.compositeMember(ctx, &cancellation, id, label, properties, canonical)
		if err != nil {
			return nil, 0, err
		}
		if matched {
			count++
			out = append(out, id)
		}
	}
	return out, count, ctx.Err()
}

// PendingPropertyMatchCount is how many of this transaction's pending nodes
// have label and property = value: the nodes MergePendingPropertyMatches adds
// to a committed lookup, counted from the same pending index without copying
// them. A planner adds it to the committed posting's length for an upper
// bound of what the transaction's lookup returns.
func (tx *BadgerTransaction) PendingPropertyMatchCount(ctx context.Context, label, property string, value interface{}) (int, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 {
		return 0, ctx.Err()
	}
	valueKey, ok, err := indexValueKey(ctx, value)
	if err != nil || !ok {
		return 0, err
	}
	pending, err := tx.pendingValueMatchesLocked(ctx, label, property, valueKey)
	return len(pending), err
}

// PendingCompositeMatchCount is PendingPropertyMatchCount for a composite
// index read: at most how many pending nodes MergePendingCompositeMatches adds
// for values (the leading properties' values), from the same pending read
// (pendingCompositeMatchesLocked) without listing them. Once the transaction
// indexes its pending nodes it reads one value bucket per property, not the
// pending nodes.
func (tx *BadgerTransaction) PendingCompositeMatchCount(ctx context.Context, label string, properties []string, values []interface{}) (int, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return 0, err
	}
	defer tx.mu.Unlock()
	if len(tx.pendingNodes) == 0 {
		return 0, ctx.Err()
	}
	_, count, err := tx.pendingCompositeMatchesLocked(ctx, nil, label, properties, values, true)
	return count, err
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
// properties (full or prefix) are added (pendingCompositeMatchesLocked). values are raw pattern values; they
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
	if len(tx.pendingNodes) == 0 {
		return kept, ctx.Err()
	}
	kept, _, err := tx.pendingCompositeMatchesLocked(ctx, kept, label, properties, values, false)
	if err != nil {
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
