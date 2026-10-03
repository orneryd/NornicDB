package storage

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/dgraph-io/badger/v4"
)

// kvWriter is the write target of the helpers that allocate numeric IDs
// (resolveOrAllocate*NumIDInTxn and the index-key builders on top of them)
// and archive superseded MVCC bodies. It is either a *badger.Txn, which the
// engine's own writes and the commit phase pass, or a transaction's stagedKV,
// which its statement-time operations pass.
type kvWriter interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	NewIterator(opt badger.IteratorOptions) *badger.Iterator
}

// stagedKV is a BadgerTransaction seen as a kvWriter. Its writes join the
// transaction's buffered writes (pendingWrites / pendingDeletes), which
// Commit writes in as many Badger batches as the transaction needs; reads
// and iteration go to the transaction's Badger transaction. Staging keeps
// statement-time writes out of that Badger transaction, so no statement can
// outgrow Badger's per-batch limit before commit (#703). Converting a
// *BadgerTransaction to *stagedKV is free.
type stagedKV BadgerTransaction

func (tx *BadgerTransaction) staged() *stagedKV { return (*stagedKV)(tx) }

// Set stages key = value. Like Badger's Txn.Set, it keeps value without
// copying it; the caller must not modify it afterwards.
func (s *stagedKV) Set(key, value []byte) error {
	tx := (*BadgerTransaction)(s)
	keyStr := string(key)
	delete(tx.pendingDeletes, keyStr)
	tx.pendingWrites[keyStr] = value
	return nil
}

// Delete stages the deletion of key.
func (s *stagedKV) Delete(key []byte) error {
	(*BadgerTransaction)(s).bufferDelete(key)
	return nil
}

// NewIterator iterates the transaction's Badger transaction. Staged writes
// are not visible to it; the only staged-write-aware iteration is the
// numeric-ID freelist pop, which skips entries this transaction already
// popped (kvStagedDeleted).
func (s *stagedKV) NewIterator(opt badger.IteratorOptions) *badger.Iterator {
	return (*BadgerTransaction)(s).badgerTx.NewIterator(opt)
}

// kvWriterTxn returns the Badger transaction w belongs to: the key under
// which the ID and property-key dictionaries stage counters for it.
func kvWriterTxn(w kvWriter) *badger.Txn {
	if s, ok := w.(*stagedKV); ok {
		return s.badgerTx
	}
	return w.(*badger.Txn)
}

// kvStagedDeleted reports whether w has staged the deletion of key, which
// its iterator does not reflect. A *badger.Txn's iterator already hides its
// own deletions.
func kvStagedDeleted(w kvWriter, key []byte) bool {
	if s, ok := w.(*stagedKV); ok {
		return s.pendingDeletes[string(key)]
	}
	return false
}

// kvHas reports whether key exists for w, staged writes included.
func kvHas(w kvWriter, key []byte) (bool, error) {
	var txn *badger.Txn
	if s, ok := w.(*stagedKV); ok {
		if s.pendingDeletes[string(key)] {
			return false, nil
		}
		if _, ok := s.pendingWrites[string(key)]; ok {
			return true, nil
		}
		txn = s.badgerTx
	} else {
		txn = w.(*badger.Txn)
	}
	_, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return false, nil
	}
	return err == nil, err
}

// acquireCommitPublicationLocked takes the count locks a commit holds from
// the moment its writes can reach Badger until they are published:
// labelCountWriteMu / edgeTypeCountWriteMu when the commit changes those
// derived counts. The count keys are not in badgerTx, so this does not
// create optimistic conflicts; holding the locks through the follow-up delta
// writes preserves mutation order and keeps count readers from observing the
// committed entities without their derived counts.
//
// The engine's write barrier is taken earlier, at the start of Commit
// (commitReleaseWrite). An ordinary commit takes the count locks just before
// its Badger commit; a large commit before its first batch, ahead of the
// exclusive commit gate (lock order: write barrier, count locks, commit
// gate). Idempotent.
func (tx *BadgerTransaction) acquireCommitPublicationLocked() {
	if tx.commitCountsHeld {
		return
	}
	tx.commitCountsHeld = true
	if len(tx.pendingLabelCountDeltas) > 0 {
		tx.engine.labelCountWriteMu.Lock()
		tx.commitLabelCounts = true
	}
	if len(tx.pendingEdgeTypeCountDeltas) > 0 || len(tx.pendingEdgeTypeLabelCountDeltas) > 0 {
		tx.engine.edgeTypeCountWriteMu.Lock()
		tx.commitEdgeTypeCounts = true
	}
}

func (tx *BadgerTransaction) releaseLabelCountLockLocked() {
	if tx.commitLabelCounts {
		tx.commitLabelCounts = false
		tx.engine.labelCountWriteMu.Unlock()
	}
}

func (tx *BadgerTransaction) releaseEdgeTypeCountLockLocked() {
	if tx.commitEdgeTypeCounts {
		tx.commitEdgeTypeCounts = false
		tx.engine.edgeTypeCountWriteMu.Unlock()
	}
}

func (tx *BadgerTransaction) releaseWriteBarrierLocked() {
	if tx.commitReleaseWrite != nil {
		release := tx.commitReleaseWrite
		tx.commitReleaseWrite = nil
		release()
	}
}

// abortCommitLocked ends a commit that failed: a large commit's written
// batches are rolled back, the count locks are released, and the
// transaction is closed as rolled back. It is the one place a Badger write
// conflict found while committing (a peer committed between the snapshot
// validation and the write, whether on an ordinary commit or a large
// commit's first batch) becomes the commit-conflict error.
func (tx *BadgerTransaction) abortCommitLocked(err error) error {
	if errors.Is(err, badger.ErrConflict) && !errors.Is(err, ErrConflict) {
		err = commitConflictError(err)
	}
	if tx.commitW != nil {
		if abortErr := tx.commitW.abort(); abortErr != nil {
			tx.engine.log.Error("rolling back a failed large commit failed; storage refuses writes until restart",
				"subsystem", "transaction",
				"transaction_id", tx.ID,
				slog.Any("commit_error", err),
				slog.Any("rollback_error", abortErr),
			)
			err = fmt.Errorf("%w (%w)", err, abortErr)
		}
		tx.commitW = nil
	}
	tx.releaseLabelCountLockLocked()
	tx.releaseEdgeTypeCountLockLocked()
	tx.closeLocked(TxStatusRolledBack, true, nil)
	return err
}
