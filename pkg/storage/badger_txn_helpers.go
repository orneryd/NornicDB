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

func (b *BadgerEngine) beginHelperTxn() (*badger.DB, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrStorageClosed
	}
	b.txnWG.Add(1)
	return b.db, nil
}

func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error) error {
	db, err := b.beginHelperTxn()
	if err != nil {
		return err
	}
	defer b.txnWG.Done()
	var nodeMax, edgeMax uint64
	var propKeyDrain propKeyTxnDrain
	err = recoverBadgerClosedPanic(func() error {
		return db.Update(func(txn *badger.Txn) error {
			if err := fn(txn); err != nil {
				if b.idDict != nil {
					b.idDict.discardTxnCounters(txn)
				}
				if b.propKeyDict != nil {
					b.propKeyDict.discardTxnCounters(txn)
				}
				return err
			}
			// Property-key tokens must be durable before entity bytes can
			// reference them. Persist them in a separate transaction first;
			// a later user-transaction failure leaves only harmless orphaned
			// tokens, matching Neo4j's token-before-entity invariant.
			if b.idDict != nil {
				nodeMax, edgeMax = b.idDict.flushTxnCounters(txn)
			}
			if b.propKeyDict != nil {
				propKeyDrain = b.propKeyDict.flushTxnCounters(txn)
				if err := b.propKeyDict.persistTxnCounters(db, propKeyDrain); err != nil {
					return fmt.Errorf("persisting property key dictionary: %w", err)
				}
			}
			return nil
		})
	})
	if err == nil {
		runCommitTailHook()
		if b.idDict != nil {
			b.idDict.persistCounters(db, nodeMax, edgeMax)
		}
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
