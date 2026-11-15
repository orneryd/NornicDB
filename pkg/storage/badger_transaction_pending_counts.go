package storage

import (
	"context"
	"strings"
)

// Transaction-visible counts (#683). A transaction stages its changes to the
// derived counters (node label counts and relationship type counts, issue
// #638) as it writes, and applies them after commit. What the transaction sees for a count is therefore the
// committed counter plus its own staged delta: committed data as of the
// transaction's own read version, and the transaction's own writes.
// These accessors expose the staged deltas so a count inside a transaction
// stays O(1) instead of scanning every node or relationship it counts.
//
// ok is false when the deltas don't describe the requested counter: the
// transaction isn't pinned to namespace (it keeps no deltas for it), or a
// commit of that namespace has moved past the transaction's read version so
// the committed counter now counts rows the transaction's snapshot does not
// hold (committedCountersDescribeRowsLocked). The caller then counts by
// scanning the rows it reads. A read whose counter comes from the transaction's
// own snapshot instead (ScopeCountStoreRead) does not need that last
// certification: the counter it reads cannot move.

// committedCountersDescribeRowsLocked reports whether the committed derived
// counters count the same rows this transaction reads.
//
// The counters hold every committed change as of the moment they are read: a
// reader without a pinned snapshot (no read version, or the write-behind
// read-committed view) reads exactly that state. A reader with a pinned
// snapshot reads its namespace's rows as they were at its begin boundary, so
// the counters describe them only while no commit of that namespace has moved
// past that boundary. A committed peer relationship or node the transaction
// cannot see would otherwise be counted (or a peer deletion it still sees
// would be left out), disagreeing with the same transaction's point reads,
// adjacency scans and label scans.
//
// The caller holds tx.mu and has already established that namespace is the
// transaction's own, non-empty namespace.
func (tx *BadgerTransaction) committedCountersDescribeRowsLocked() bool {
	if tx.engine == nil || tx.readTS.IsZero() || tx.engine.writeBehind != nil {
		return true
	}
	return tx.namespaceSnapshotCurrentLocked()
}

// PendingNodeLabelCountDelta returns what the transaction's staged writes
// add to the count of nodes with label in namespace.
func (tx *BadgerTransaction) PendingNodeLabelCountDelta(namespace, label string) (delta int64, ok bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace == "" || tx.namespace != namespace || !tx.committedCountersDescribeRowsLocked() {
		return 0, false
	}
	return tx.pendingLabelCountDeltas[namespaceLabel{namespace: namespace, label: label}], true
}

// PendingEdgeTypeCountDelta returns what the transaction's staged writes add
// to the count of relationships of edgeType in namespace.
func (tx *BadgerTransaction) PendingEdgeTypeCountDelta(namespace, edgeType string) (delta int64, ok bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace == "" || tx.namespace != namespace || !tx.committedCountersDescribeRowsLocked() {
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
	if request.Namespace == "" || tx.namespace != request.Namespace || !tx.committedCountersDescribeRowsLocked() {
		return CountStoreRead{}, false, nil
	}
	return tx.pendingCountStoreContributionLocked(ctx, request)
}

// pendingCountStoreContributionLocked is the one owner of what a transaction's
// staged writes add to one count-store read, and of whether that contribution
// leaves the answer to those rows.
//
// tracked is false when the request is not this transaction's to answer: an
// empty or foreign namespace. A tracked contribution with DescribesRows false
// means the transaction holds a staged change the contribution cannot express -
// a staged internal label, or a change that can move a positional (label, type)
// count, which is derived only at commit - so the count stays with the rows.
//
// The certification that the committed counters still describe the
// transaction's rows does not live here: the live counter read owns it
// (PendingCountStoreDelta), and a snapshot counter read does not need it. The
// caller holds tx.mu.
func (tx *BadgerTransaction) pendingCountStoreContributionLocked(ctx context.Context, request CountStoreReadRequest) (answer CountStoreRead, tracked bool, err error) {
	if request.Namespace == "" || tx.namespace != request.Namespace {
		return CountStoreRead{}, false, nil
	}
	var cancel bulkReadCancellation
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
	answer = CountStoreRead{DescribesRows: true}
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
