package storage

import "context"

// LookupEdgesByPropertyEquality answers a relationship-property equality lookup
// inside the transaction: the index's committed postings merged with the
// transaction's own pending relationship writes and minus its deletions, so a
// match sees the same relationships this transaction's GetEdgesByType returns.
// handled is false when the committed index cannot answer the lookup, and the
// caller keeps its complete scan.
func (tx *BadgerTransaction) LookupEdgesByPropertyEquality(ctx context.Context, relationshipType string, properties []string, values []interface{}) ([]*Edge, bool, error) {
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return nil, false, err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return nil, false, err
	}
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return nil, false, err
	}
	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return nil, false, err
	}
	if tx.engine.writeBehind != nil || !tx.namespaceSnapshotCurrentLocked() {
		// Current-only postings cannot answer an older snapshot. Its existing
		// versioned type reader remains authoritative, as does write-behind.
		return nil, false, nil
	}
	committed, handled, err := tx.engine.LookupEdgesByPropertyEqualityInNamespace(ctx, tx.namespace, relationshipType, properties, values)
	if err != nil || !handled {
		return nil, handled, err
	}
	if !tx.namespaceSnapshotCurrentLocked() {
		// A peer may publish while the index/read-back runs. Discard that
		// answer rather than admitting a current image into the pinned cut.
		return nil, false, nil
	}
	tx.observed = true
	edges, err := tx.mergePendingEdgesLocked(ctx, committed, func(edge *Edge) bool {
		return relationshipEdgeCarriesValues(edge, relationshipType, properties, values)
	})
	if err != nil {
		return nil, false, err
	}
	return edges, true, nil
}
