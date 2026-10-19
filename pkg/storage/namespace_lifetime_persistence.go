package storage

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// NamespaceCatalogState is lifecycle metadata carried by replication snapshots,
// which otherwise contain graph entities only. Runtime transaction fences stay
// local to their engine and are never replaced by a remote catalog revision.
type NamespaceCatalogState struct {
	Revision uint64
	Retired  map[string]uint64
}

func CaptureNamespaceCatalogState(engine Engine) NamespaceCatalogState {
	state := NamespaceCatalogState{Retired: make(map[string]uint64)}
	if b := namespaceLifecycleOwner(engine); b != nil {
		state.Revision = b.catalogRevision.Load()
		if lifetime := b.namespaceLifetimes.Load(); lifetime != nil {
			for namespace, revision := range lifetime.catalogRetired {
				state.Retired[namespace] = revision
			}
		}
	}
	return state
}

func RestoreNamespaceCatalogState(engine Engine, state NamespaceCatalogState) error {
	b := namespaceLifecycleOwner(engine)
	if b == nil {
		return nil
	}
	release, err := AdmitNamespaceLifecycle(context.Background(), engine, "")
	if err != nil {
		return err
	}
	defer release()
	next := b.copyNamespaceLifetimes()
	next.catalogRetired = make(map[string]uint64, len(state.Retired))
	b.observeCatalogRevision(state.Revision)
	for namespace, revision := range state.Retired {
		next.catalogRetired[namespace] = revision
		b.observeCatalogRevision(revision)
	}
	if err := b.db.DropPrefix(namespaceLifetimeKey("")); err != nil {
		return err
	}
	if err := b.withUpdate(func(txn *badger.Txn) error {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, b.catalogRevision.Load())
		if err := txn.Set(namespaceLifetimeKey(""), value); err != nil {
			return err
		}
		for namespace, revision := range next.catalogRetired {
			value := make([]byte, 8)
			binary.BigEndian.PutUint64(value, revision)
			if err := txn.Set(namespaceLifetimeKey(namespace), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	b.namespaceLifetimes.Store(next)
	return nil
}

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
