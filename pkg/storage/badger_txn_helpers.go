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
	return recoverBadgerClosedPanic(func() error {
		return b.db.Update(func(txn *badger.Txn) error {
			if err := fn(txn); err != nil {
				if b.idDict != nil {
					b.idDict.discardTxnCounters(txn)
				}
				return err
			}
			if b.idDict != nil {
				if err := b.idDict.flushTxnCounters(txn); err != nil {
					return err
				}
			}
			return nil
		})
	})
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
