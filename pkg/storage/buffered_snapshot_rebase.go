package storage

import "github.com/dgraph-io/badger/v4"

// rebaseBadgerAfterDrainLocked retains the logical snapshot and staged writes
// while moving only its physical Badger view past already-acknowledged replay.
func (tx *BadgerTransaction) rebaseBadgerAfterDrainLocked() error {
	snapshotTx, readTS, err := tx.badgerDB.beginTxn(false)
	if err != nil {
		return err
	}
	next := tx.badgerDB.newTxnAt(readTS, true)
	tx.engine.idDict.moveTxnCounters(tx.badgerTx, next)
	tx.engine.propKeyDict.moveTxnCounters(tx.badgerTx, next)
	tx.badgerTx.Discard()
	if tx.snapshotTx != nil {
		tx.snapshotTx.Discard()
	}
	if tx.badgerReadHeld {
		tx.badgerDB.endRead(tx.badgerReadTs)
	}
	tx.snapshotTx, tx.badgerTx = snapshotTx, next
	tx.badgerReadTs, tx.badgerReadHeld = readTS, true
	tx.clearSnapshotCachesLocked()
	return nil
}

// Preserve the canonical property dictionary's staged tokens across that view
// change, just as its existing counter transfer preserves numeric identities.
func (d *propertyKeyDictionary) moveTxnCounters(from, to *badger.Txn) {
	d.txnMu.Lock()
	if counters, ok := d.txnCounters[from]; ok {
		d.txnCounters[to] = counters
		delete(d.txnCounters, from)
	}
	if pending, ok := d.txnPendingForward[from]; ok {
		d.txnPendingForward[to] = pending
		delete(d.txnPendingForward, from)
	}
	d.txnMu.Unlock()
}
