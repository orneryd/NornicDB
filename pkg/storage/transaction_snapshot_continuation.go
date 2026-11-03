package storage

import (
	"context"
	"fmt"
)

// ResumeAfterIndependentCommit advances an implicit caller's read view while
// retaining its entire pending write set and rollback ownership. Explicit
// transactions never enter this path. Before advancing, old pending mutations
// are checked against their old snapshot so a batch cannot mask a conflict.
func (tx *BadgerTransaction) ResumeAfterIndependentCommit(ctx context.Context) error {
	if err := tx.mu.LockContext(ctx); err != nil {
		return err
	}
	defer tx.mu.Unlock()
	if err := tx.ensureOperationAllowedLocked(transactionRead); err != nil {
		return err
	}
	if tx.commitMode != CommitModeImplicit {
		return fmt.Errorf("independent-commit continuation requires an implicit transaction")
	}
	return tx.advanceSnapshotLocked(ctx, true)
}

func (tx *BadgerTransaction) advanceSnapshotLocked(ctx context.Context, preserveWrites bool) error {
	lifecycleSeq := tx.engine.prefixDrops.seq.Load()
	var bufferEpoch uint64
	if tx.engine.writeBehind != nil {
		bufferEpoch = tx.engine.writeBehind.readEpoch.Load()
	}
	snapshotTx, badgerReadTs, beginPresence, err := tx.engine.beginTransactionSnapshot(ctx, tx.badgerDB)
	if err != nil {
		return err
	}
	// Pin the next view before validating the old footprint. A publication
	// after validation then remains newer than the next view and is checked
	// again at commit; no interval can silently erase a write conflict.
	beginSnapshot, err := tx.engine.snapshotNamespaceVersions(ctx)
	if err != nil {
		snapshotTx.Discard()
		beginPresence.release()
		tx.badgerDB.endRead(badgerReadTs)
		return err
	}
	if preserveWrites {
		if len(tx.operations) > 0 && tx.engine.prefixDroppedSince(tx.lifecycleSeq, tx.namespace) {
			err = fmt.Errorf("%w: namespace %q was deleted or rewritten by maintenance before continuation", ErrConflict, tx.namespace)
		} else if tx.schemaRuntime != nil && tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence != tx.readTS.CommitSequence {
			err = fmt.Errorf("schema snapshot changed before continuation: %w", ErrConflict)
		} else {
			err = tx.validateSnapshotIsolationConflicts(ctx)
		}
		if err != nil {
			snapshotTx.Discard()
			beginPresence.release()
			tx.badgerDB.endRead(badgerReadTs)
			return err
		}
	}
	nextWriter := tx.badgerDB.newTxnAt(badgerReadTs, true)
	if tx.badgerTx != nil && tx.engine.idDict != nil {
		if preserveWrites {
			tx.engine.idDict.moveTxnCounters(tx.badgerTx, nextWriter)
		} else {
			tx.engine.idDict.discardTxnCounters(tx.badgerTx)
		}
	}
	if tx.badgerTx != nil && tx.engine.propKeyDict != nil {
		if preserveWrites {
			tx.engine.propKeyDict.moveTxnCounters(tx.badgerTx, nextWriter)
		} else {
			tx.engine.propKeyDict.discardTxnCounters(tx.badgerTx)
		}
	}
	if tx.badgerTx != nil {
		tx.badgerTx.Discard()
	}
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
	}
	if tx.badgerReadHeld {
		tx.badgerDB.endRead(tx.badgerReadTs)
		tx.badgerReadHeld = false
	}
	tx.beginPresence.release()
	tx.beginPresence = beginPresence
	tx.snapshotTx = snapshotTx
	tx.badgerReadTs = badgerReadTs
	tx.badgerReadHeld = true
	tx.badgerTx = nextWriter
	tx.lifecycleSeq = lifecycleSeq
	tx.bufferEpoch = bufferEpoch
	tx.beginSnapshot = beginSnapshot
	tx.clearSnapshotCachesLocked()
	return tx.refreshReadVersionForNamespaceLocked()
}
