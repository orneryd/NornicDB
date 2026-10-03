package storage

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

func (b *BadgerEngine) ensureOpen() error {
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		return ErrStorageClosed
	}
	return nil
}

// beginWrite marks a durable write in flight so Close waits for it before
// releasing engine state. It returns the release func the caller must defer,
// or ErrStorageClosed when the engine has already closed (the caller must
// not touch Badger in that case). The closed check runs under the barrier so
// a Close that has completed is always observed and a Close that has not
// started yet cannot slip in between the check and the write.
func (b *BadgerEngine) beginWrite() (func(), error) {
	b.writeBarrier.RLock()
	if err := b.ensureOpen(); err != nil {
		b.writeBarrier.RUnlock()
		return nil, err
	}
	return b.writeBarrier.RUnlock, nil
}

func (b *BadgerEngine) beginSchemaWrite() (func(), error) {
	b.writeBarrier.Lock()
	if err := b.ensureOpen(); err != nil {
		b.writeBarrier.Unlock()
		return nil, err
	}
	return b.writeBarrier.Unlock, nil
}

func (b *BadgerEngine) withView(fn func(txn *badger.Txn) error) error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	return recoverBadgerClosedPanic(func() error {
		return db.View(fn)
	})
}

func (b *BadgerEngine) beginHelperTxn() (*managedBadgerDB, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrStorageClosed
	}
	b.txnWG.Add(1)
	return b.db, nil
}

// withUpdate runs fn as one atomic engine write, committed through the same
// commit writer as a transaction. fn reads and writes in one Badger
// transaction and cannot be repeated, so its writes must fit one Badger
// batch (badger.ErrTxnTooBig otherwise).
//
// Property-key tokens are made durable before the entity bytes that
// reference them (a later failure leaves only harmless orphaned tokens,
// matching Neo4j's token-before-entity invariant), and the numeric-ID
// counter high-water marks are persisted after the commit, in a separate
// transaction, so concurrent writers never conflict on the shared keys.
func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error) error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	var nodeMax, edgeMax uint64
	err = recoverBadgerClosedPanic(func() error {
		txn, readTs := db.beginTxn(true)
		defer db.endRead(readTs)
		cw := b.newCommitWriter(db, txn)
		defer cw.discard()
		fail := func(err error) error {
			b.idDict.discardTxnCounters(cw.txn)
			b.propKeyDict.discardTxnCounters(cw.txn)
			return err
		}
		if err := cw.writeOnce(fn); err != nil {
			return fail(err)
		}
		nodeMax, edgeMax = b.idDict.flushTxnCounters(cw.txn)
		if err := cw.finish(); err != nil {
			return fail(err)
		}
		return nil
	})
	if err == nil {
		runCommitTailHook()
		b.idDict.persistCounters(db, nodeMax, edgeMax)
	}
	return err
}

func recoverBadgerClosedPanic(fn func() error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if isBadgerClosedPanic(recovered) {
				err = ErrStorageClosed
				return
			}
			panic(recovered)
		}
	}()

	return fn()
}

func isBadgerClosedPanic(recovered interface{}) bool {
	message := fmt.Sprint(recovered)
	return strings.Contains(message, "DB Closed")
}
