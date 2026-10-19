package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// One physical counter and one latest retirement per namespace. These are
// namespace-lifetime metadata, retained across DROP; never per-entity side data.
const prefixMVCCMetaNamespaceLifetime = byte(0x14)

func namespaceLifetimeKey(namespace string) []byte {
	return append([]byte{prefixMVCCMeta, prefixMVCCMetaNamespaceLifetime}, namespace...)
}

func (b *BadgerEngine) loadNamespaceLifetimes(reader badgerViewer, restoring bool) error {
	retired := make(map[string]uint64)
	var revision uint64
	prefix := namespaceLifetimeKey("")
	if err := reader.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := it.Item().Value(func(value []byte) error {
				if len(value) != 8 {
					return fmt.Errorf("invalid namespace lifetime counter %x: %d bytes", key, len(value))
				}
				current := binary.BigEndian.Uint64(value)
				if current > revision {
					revision = current
				}
				if len(key) > len(prefix) {
					retired[string(key[len(prefix):])] = current
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	b.observeCatalogRevision(revision)
	next := &namespaceLifetimes{retired: make(map[string]uint64), catalogRetired: retired, active: make(map[string]*NamespaceDrop)}
	if restoring {
		// Restore replaces catalog history but must not revive a transaction
		// that already predates a DROP in this live engine incarnation.
		prior := b.copyNamespaceLifetimes()
		prior.catalogRetired = retired
		if current := b.catalogRevision.Load(); current > revision {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, current)
			if err := b.db.updateHeld(func(txn *badger.Txn) error { return txn.Set(namespaceLifetimeKey(""), value) }); err != nil {
				return err
			}
		}
		next = prior
	}
	b.namespaceLifetimes.Store(next)
	return nil
}

func (b *BadgerEngine) persistNamespaceLifetime(namespace string, revision uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, revision)
	counter := make([]byte, 8)
	binary.BigEndian.PutUint64(counter, b.catalogRevision.Load())
	return b.withUpdate(func(txn *badger.Txn) error {
		if err := txn.Set(namespaceLifetimeKey(""), counter); err != nil {
			return err
		}
		return txn.Set(namespaceLifetimeKey(namespace), value)
	})
}
