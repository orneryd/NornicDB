package storage

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
	return tx.pendingLabelCountDeltas[namespaceLabel{namespace: namespace, label: normalizeCountLabel(label)}], true
}

// PendingEdgeTypeCountDelta returns what the transaction's staged writes add
// to the count of relationships of edgeType in namespace.
func (tx *BadgerTransaction) PendingEdgeTypeCountDelta(namespace, edgeType string) (delta int64, ok bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if namespace == "" || tx.namespace != namespace {
		return 0, false
	}
	return tx.pendingEdgeTypeCountDeltas[namespaceEdgeType{namespace: namespace, edgeType: normalizeCountEdgeType(edgeType)}], true
}
