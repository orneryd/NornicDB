package storage

import "github.com/dgraph-io/badger/v4"

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
	return b.db.View(fn)
}

func (b *BadgerEngine) withUpdate(fn func(txn *badger.Txn) error) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	return b.db.Update(fn)
}
