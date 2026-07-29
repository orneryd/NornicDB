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

func (b *BadgerEngine) withView(fn func(txn *badger.Txn) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return recoverBadgerClosedPanic(func() error {
		return b.db.View(fn)
	})
}

func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	var nodeMax, edgeMax uint64
	var propKeyDrain propKeyTxnDrain
	err := recoverBadgerClosedPanic(func() error {
		return b.db.Update(func(txn *badger.Txn) error {
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
				if err := b.propKeyDict.persistTxnCounters(b.db, propKeyDrain); err != nil {
					return fmt.Errorf("persisting property key dictionary: %w", err)
				}
			}
			return nil
		})
	})
	if err == nil {
		if b.idDict != nil {
			b.idDict.persistCounters(b.db, nodeMax, edgeMax)
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
