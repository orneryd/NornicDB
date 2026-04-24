package storage

import (
	badger "github.com/dgraph-io/badger/v4"
)

// indexTombstoneKey constructs the tombstone key for an original index key.
// Format: [0x17][originalKey]. The original prefix byte is preserved so
// the read path can reconstruct the original key if needed.
func indexTombstoneKey(originalIndexKey []byte) []byte {
	key := make([]byte, 1+len(originalIndexKey))
	key[0] = prefixIndexTombstone
	copy(key[1:], originalIndexKey)
	return key
}

// hasIndexTombstone checks whether a tombstone exists for the given original
// index key within the provided transaction. Cost: one Badger point lookup,
// rejected in <50ns by bloom filter when no tombstone exists.
func hasIndexTombstone(txn *badger.Txn, originalIndexKey []byte) bool {
	_, err := txn.Get(indexTombstoneKey(originalIndexKey))
	return err == nil
}

// WriteIndexTombstones writes zero-length presence markers for all given
// original index keys in a single batched transaction.
func (b *BadgerEngine) WriteIndexTombstones(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Set(indexTombstoneKey(k), []byte{}); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteIndexTombstones removes tombstones for the given original index keys.
// Used when an entity recovers visibility (score rises above threshold) or
// when reveal() restores an entity.
func (b *BadgerEngine) DeleteIndexTombstones(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Delete(indexTombstoneKey(k)); err != nil && err != badger.ErrKeyNotFound {
				return err
			}
		}
		return nil
	})
}
