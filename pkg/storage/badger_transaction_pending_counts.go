package storage

import (
	"context"
	"strings"
)

// Transaction-visible counts (#683). A transaction stages its changes to the
// derived counters (node label counts and relationship type counts, issue
// #638) as it writes, and applies them after commit. What the transaction sees for a count is therefore the
// committed counter plus its own staged delta, which is Neo4j's
// read-committed view: committed data and the transaction's own writes.
// These accessors expose the staged deltas so a count inside a transaction
// stays O(1) instead of scanning every node or relationship it counts.
//
// ok is false when the deltas don't describe the requested counter: the
// transaction isn't pinned to namespace (it keeps no deltas for it). The
// caller then counts by scanning.

// PendingNodeLabelCountDelta returns what the transaction's staged writes
// add to the count of nodes with label in namespace.
func (tx *BadgerTransaction) PendingNodeLabelCountDelta(namespace, label string) (delta int64, ok bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace == "" || tx.namespace != namespace {
		return 0, false
	}
	return tx.pendingLabelCountDeltas[namespaceLabel{namespace: namespace, label: label}], true
}

// PendingEdgeTypeCountDelta returns what the transaction's staged writes add
// to the count of relationships of edgeType in namespace.
func (tx *BadgerTransaction) PendingEdgeTypeCountDelta(namespace, edgeType string) (delta int64, ok bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace == "" || tx.namespace != namespace {
		return 0, false
	}
	return tx.pendingEdgeTypeCountDeltas[namespaceEdgeType{namespace: namespace, edgeType: edgeType}], true
}

// PendingCountStoreDelta returns the transaction's staged contribution and
// visibility for one count request under its staging lock. Positional counts
// with staged writes retain the existing row-scan contract.
func (tx *BadgerTransaction) PendingCountStoreDelta(ctx context.Context, request CountStoreReadRequest) (CountStoreRead, bool, error) {
	if err := tx.mu.RLockContext(ctx); err != nil {
		return CountStoreRead{}, false, err
	}
	defer tx.mu.RUnlock()
	var cancel bulkReadCancellation
	if request.Namespace == "" || tx.namespace != request.Namespace {
		return CountStoreRead{}, false, nil
	}
	if request.HiddenLabelPrefix != "" {
		for key, delta := range tx.pendingLabelCountDeltas {
			if err := cancel.check(ctx); err != nil {
				return CountStoreRead{}, false, err
			}
			if key.namespace == request.Namespace && delta > 0 && strings.HasPrefix(key.label, request.HiddenLabelPrefix) {
				return CountStoreRead{}, true, nil
			}
		}
	}
	answer := CountStoreRead{DescribesRows: true}
	switch request.Kind {
	case CountStoreNodesByLabel:
		answer.Count = tx.pendingLabelCountDeltas[namespaceLabel{namespace: request.Namespace, label: request.Name}]
	case CountStoreRelationshipsByType:
		for key, delta := range tx.pendingEdgeTypeCountDeltas {
			if err := cancel.check(ctx); err != nil {
				return CountStoreRead{}, false, err
			}
			if key.namespace != request.Namespace {
				continue
			}
			if len(request.Types) == 0 {
				answer.Count += delta
				continue
			}
			for _, kind := range request.Types {
				if err := cancel.check(ctx); err != nil {
					return CountStoreRead{}, false, err
				}
				if key.edgeType == kind {
					answer.Count += delta
					break
				}
			}
		}
	default:
		if len(tx.pendingLabelCountDeltas) > 0 || len(tx.pendingEdgeTypeCountDeltas) > 0 {
			return CountStoreRead{}, true, nil
		}
		// The positional counts are derived only at commit, from the
		// transaction's changes: any change that can move one counts rows.
		pending, err := tx.positionalChangesPendingLocked(ctx)
		if err != nil {
			return CountStoreRead{}, false, err
		}
		if pending {
			return CountStoreRead{}, true, nil
		}
	}
	return answer, true, nil
}
