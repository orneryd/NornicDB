package storage

import "github.com/dgraph-io/badger/v4"

// ReviewSetDropTailFailure is available only to the external public-path probe.
func ReviewSetDropTailFailure(b *BadgerEngine, hook func(string) error) {
	b.writeBarrier.Lock()
	defer b.writeBarrier.Unlock()
	b.dropPrefixTailHook = hook
}

// ReviewRawEntityExists bypasses entity caches and reads the physical body key.
func ReviewRawEntityExists(b *BadgerEngine, kind StoredEntityKind, id string) (bool, error) {
	key := nodeKey(NodeID(id))
	if kind == StoredEntityEdgeKind {
		key = edgeKey(EdgeID(id))
	}
	exists := false
	err := b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}
