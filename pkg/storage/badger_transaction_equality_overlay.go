package storage

import "sort"

// TransactionEqualityLookup exposes complete candidates from a transaction's
// pinned label population. False asks the caller to use its normal scan (for
// read-committed views, unkeyable values or populations outside the cache bound).
// Callers still apply their language's complete property/label predicate.
type TransactionEqualityLookup interface {
	LookupNodesByEquality(label string, properties map[string]interface{}) ([]*Node, bool, error)
}

type snapshotEqualityPopulation struct {
	index *pendingNodeIndex
	nodes map[NodeID]*Node
}

// LookupNodesByEquality reuses the pinned label reader and the existing
// equality-key index. Only matching candidates escape as copies. Transaction
// writes shadow committed IDs and join through the live pending-node index.
func (tx *BadgerTransaction) LookupNodesByEquality(label string, properties map[string]interface{}) ([]*Node, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, false, err
	}
	if tx.namespace == "" {
		if err := tx.engine.pendingNamespaceConflict(""); err != nil {
			return nil, false, err
		}
	}

	if tx.snapshotTx == nil || tx.readTS.IsZero() || tx.engine.writeBehind != nil || len(properties) == 0 {
		return nil, false, nil
	}
	key := snapshotLabelCacheKey{projectionKey: label, reveal: tx.engine.revealAll.Load()}
	population := tx.snapshotEquality[key]
	if population != nil && population.index == nil {
		return nil, false, nil
	}
	if population == nil {
		if len(tx.snapshotEquality) >= maxSnapshotProjectedLabelStreams {
			return nil, false, nil
		}
		// Use the same pinned physical label reader as label streaming, but
		// retain only a bounded population. A large/embedded population answers
		// this first probe while scanning, then declines future cached probes.
		population = &snapshotEqualityPopulation{nodes: make(map[NodeID]*Node)}
		var first []*Node
		retainedBytes := 0
		cacheable := true
		keyable := false
		for _, value := range properties {
			if _, ok := transactionEqualityKey(value); ok {
				keyable = true
				break
			}
		}
		if !keyable {
			return nil, false, nil
		}
		matches := func(node *Node) bool {
			if !hasLabel(node.Labels, label) {
				return false
			}
			for property, value := range properties {
				expected, ok := transactionEqualityKey(value)
				if !ok {
					continue
				}
				actual, ok := transactionEqualityKey(node.Properties[property])
				if !ok || actual != expected {
					return false
				}
			}
			return true
		}
		err := tx.engine.streamNodesByLabelFromPhysicalSnapshotAfterRead(tx.labelScanScopeLocked(), label, tx.withSnapshotViewLocked, nil, "", visibleRead, func(node *Node) error {
			if cacheable {
				bytes, ok := snapshotLabelPrefixNodeBytes(node)
				if !ok || len(population.nodes) >= maxSnapshotNodeLabelCacheNodes || bytes > maxSnapshotLabelPrefixBytes-retainedBytes || tx.nodeHasEmbeddingSidecar(tx.snapshotTx, node.ID) {
					cacheable = false
					population.nodes = nil
				} else {
					population.nodes[node.ID] = node
					retainedBytes += bytes
				}
			}
			if !tx.committedNodeSupersededLocked(node.ID) && matches(node) {
				first = append(first, copyNode(node))
			}
			return nil
		})
		if err != nil {
			return nil, false, err
		}
		if tx.snapshotEquality == nil {
			tx.snapshotEquality = make(map[snapshotLabelCacheKey]*snapshotEqualityPopulation)
		}
		tx.snapshotEquality[key] = population
		if !cacheable {
			for id, node := range tx.pendingNodes {
				if _, deleted := tx.deletedNodes[id]; !deleted && matches(node) {
					first = append(first, copyNode(node))
				}
			}
			return first, true, nil
		}
		population.index = newPendingNodeIndex()
		for _, node := range population.nodes {
			population.index.add(node)
		}
	}
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	var committed, pending []NodeID
	found := false
	for _, property := range names {
		value, ok := transactionEqualityKey(properties[property])
		if !ok {
			continue
		}
		if _, tracked := population.index.tracked[label][property]; !tracked && len(population.index.tracked[label]) >= maxSnapshotProjectedLabelStreams {
			continue
		}
		population.index.track(label, property, func(id NodeID) *Node { return population.nodes[id] })
		ids := population.index.byValue[pendingValueKey{label: label, property: property, value: value}]
		staged := tx.pendingValueMatchesLocked(label, property, value)
		if !found || len(ids)+len(staged) < len(committed)+len(pending) {
			committed, pending, found = ids, staged, true
		}
	}
	if !found {
		return nil, false, nil
	}
	result := make([]*Node, 0, len(committed)+len(pending))
	for _, id := range committed {
		if tx.committedNodeSupersededLocked(id) {
			continue
		}
		result = append(result, copyNode(population.nodes[id]))
	}
	for _, id := range pending {
		if _, deleted := tx.deletedNodes[id]; !deleted {
			result = append(result, copyNode(tx.pendingNodes[id]))
		}
	}
	return result, true, nil
}

// transactionEqualityKey limits pruning to scalar keys whose equality agrees
// with Cypher. Comparable temporal structs and composite values may have a
// different language equality contract, so the caller must evaluate them.
func transactionEqualityKey(value interface{}) (interface{}, bool) {
	switch value.(type) {
	case string, bool:
		return value, true
	default:
		return numericIndexKey(value)
	}
}
