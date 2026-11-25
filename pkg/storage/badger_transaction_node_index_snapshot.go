package storage

import "context"

// CurrentNodeIndexMatchesSnapshot reports whether current node postings still
// describe this transaction's read view. Callers must check again after an
// index read: a peer may commit while the posting is being acquired. The
// write-behind read-committed view follows current state, like derived counts.
func (tx *BadgerTransaction) CurrentNodeIndexMatchesSnapshot(ctx context.Context) (bool, error) {
	if err := tx.clientOwnershipError(transactionRead); err != nil {
		return false, err
	}
	if err := tx.mu.LockContext(ctx); err != nil {
		return false, err
	}
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return false, err
	}
	if err := tx.engine.pendingNamespaceConflict(""); err != nil {
		return false, err
	}
	return tx.committedCountersDescribeRowsLocked(), nil
}
