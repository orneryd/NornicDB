package storage

import (
	"cmp"
	"context"

	"github.com/orneryd/nornicdb/internal/ctxsort"
)

// TransactionEqualityLookup exposes complete candidates from a transaction's
// pinned label population. False asks the caller to use its normal scan (for
// read-committed views, unkeyable values or populations outside the cache bound).
// Callers still apply their language's complete property/label predicate.
type TransactionEqualityLookup interface {
	LookupNodesByEquality(ctx context.Context, label string, properties map[string]interface{}) ([]*Node, bool, error)
}

type snapshotEqualityPopulation struct {
	index *pendingNodeIndex
	nodes map[NodeID]*Node
}

// LookupNodesByEquality reuses the pinned label reader and the existing
// equality-key index. Only matching candidates escape as copies. Transaction
// writes shadow committed IDs and join through the live pending-node index.
func (tx *BadgerTransaction) LookupNodesByEquality(ctx context.Context, label string, properties map[string]interface{}) ([]*Node, bool, error) {
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, false, err
	}
	defer tx.mu.Unlock()
	var cancellation bulkReadCancellation
	nowNanos := DecayScoringTime()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
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
	pendingNodes, overlayErr := writeOwnedReadImagesLocked(ctx, tx.pendingNodes, tx.deletedNodes, &tx.entityLocks, nodeWriteLockKind, func(ctx context.Context, id NodeID) (*Node, error) {
		return tx.engine.readNodeMetadata(ctx, id, storedRead, nodeItemQueryMetadata)
	})
	if overlayErr != nil {
		return nil, false, overlayErr
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
	key := snapshotLabelCacheKey{projectionKey: label}
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
			if err := cancellation.check(ctx); err != nil {
				return nil, false, err
			}
			if _, ok := transactionEqualityKey(value); ok {
				keyable = true
				break
			}
		}
		if !keyable {
			return nil, false, nil
		}

		// Retain physical membership, then apply the live visibility owner at
		// delivery. A suppressed member may become visible again without any
		// change to the pinned data snapshot. The scan and candidate preparation
		// share the statement lifetime.
		err := tx.engine.streamNodesByLabelFromPhysicalSnapshotAfterRead(ctx, tx.labelScanScopeLocked(), label, tx.withSnapshotViewLocked, nil, "", storedRead, func(node *Node) error {
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
			if !tx.committedNodeSupersededLocked(node.ID) && !tx.engine.filterEntityByDecay(visibleRead, node, nowNanos) && matches(node) {
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

		if !cacheable {
			for id, node := range pendingNodes {
				if err := cancellation.check(ctx); err != nil {
					return nil, false, err
				}
				if _, deleted := tx.deletedNodes[id]; !deleted && !tx.engine.filterEntityByDecay(visibleRead, node, nowNanos) && matches(node) {
					first = append(first, copyNode(node))
				}
			}
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			tx.snapshotEquality[key] = population
			return first, true, nil
		}
		population.index = newPendingNodeIndex()
		for _, node := range population.nodes {
			if err := cancellation.check(ctx); err != nil {
				return nil, false, err
			}
			population.index.add(node)
		}
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		tx.snapshotEquality[key] = population
	}
	names := make([]string, 0, len(properties))
	for name := range properties {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		names = append(names, name)
	}
	if err := ctxsort.Sort(ctx, names, cmp.Compare[string], false); err != nil {
		return nil, false, err
	}
	var committed, pending []NodeID
	found := false
	for _, property := range names {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		value, ok := transactionEqualityKey(properties[property])
		if !ok {
			continue
		}
		if _, tracked := population.index.tracked[label][property]; !tracked && len(population.index.tracked[label]) >= maxSnapshotProjectedLabelStreams {
			continue
		}
		if err := population.index.track(ctx, label, property, func(id NodeID) *Node { return population.nodes[id] }); err != nil {
			return nil, false, err
		}
		ids := population.index.byValue[pendingValueKey{label: label, property: property, value: value}]
		staged, err := tx.pendingValueMatchesLocked(ctx, label, property, value)
		if err != nil {
			return nil, false, err
		}
		if !found || len(ids)+len(staged) < len(committed)+len(pending) {
			committed, pending, found = ids, staged, true
		}
	}
	if !found {
		return nil, false, nil
	}
	result := make([]*Node, 0, len(committed)+len(pending))
	for _, id := range committed {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		if tx.committedNodeSupersededLocked(id) || tx.engine.filterEntityByDecay(visibleRead, population.nodes[id], nowNanos) {
			continue
		}
		result = append(result, copyNode(population.nodes[id]))
	}
	for _, id := range pending {
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		if _, deleted := tx.deletedNodes[id]; !deleted && !tx.engine.filterEntityByDecay(visibleRead, tx.pendingNodes[id], nowNanos) {
			result = append(result, copyNode(tx.pendingNodes[id]))
		}
	}
	// Refreshed no-op images are read overlays, not staged writes; the
	// pending-value index deliberately contains only actual staged images.
	for id, node := range pendingNodes {
		if _, staged := tx.pendingNodes[id]; staged {
			continue
		}
		if err := cancellation.check(ctx); err != nil {
			return nil, false, err
		}
		if matches(node) && !tx.engine.filterEntityByDecay(visibleRead, node, nowNanos) {
			result = append(result, copyNode(node))
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, false, err
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
