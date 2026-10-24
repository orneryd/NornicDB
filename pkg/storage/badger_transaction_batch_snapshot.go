package storage

import "fmt"

// AdvanceImplicitSnapshot advances an unwritten implicit statement after an
// independently committed child batch. For example, CALL IN TRANSACTIONS uses
// it before its next outer clause reads the child's committed nodes. Explicit
// transactions and transactions holding mutations retain their pinned snapshot.
func (tx *BadgerTransaction) AdvanceImplicitSnapshot() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}
	if !tx.implicit || len(tx.operations) > 0 || len(tx.pendingWrites) > 0 || len(tx.pendingDeletes) > 0 || tx.knowledgeSchemaDirty || len(tx.mergeKeyReleases) > 0 {
		return fmt.Errorf("cannot advance a written or explicit transaction snapshot: %w", ErrConflict)
	}
	if err := tx.refreshSnapshotLocked(); err != nil {
		return err
	}
	tx.observed = false
	return nil
}
