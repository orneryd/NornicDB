package storage

import (
	badger "github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
)

// indexTombstoneKey constructs the tombstone key for an original index key.
// It uses the canonical AccessMetaStore keyspace, preserving the original key.
func indexTombstoneKey(originalIndexKey []byte) []byte {
	return accessMetaKey(policyIndexMetaIDPrefix + string(originalIndexKey))
}

// hasIndexTombstone checks whether a tombstone exists for the given original
// index key within the provided transaction. Cost: one Badger point lookup,
// rejected in <50ns by bloom filter when no tombstone exists.
func hasIndexTombstone(txn *badger.Txn, originalIndexKey []byte) bool {
	entry, err := getAccessMetaInTxn(txn, policyIndexMetaIDPrefix+string(originalIndexKey))
	return err == nil && entry != nil && entry.IndexTombstone
}

func putIndexTombstoneInTxn(txn *badger.Txn, originalKey []byte) error {
	return putAccessMetaInTxn(txn, policyIndexMetaIDPrefix+string(originalKey), &knowledgepolicy.AccessMetaEntry{IndexTombstone: true})
}

// WriteIndexTombstones writes zero-length presence markers for all given
// original index keys in a single batched transaction.
func (b *BadgerEngine) WriteIndexTombstones(keys [][]byte) error {
	if len(keys) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := putIndexTombstoneInTxn(txn, k); err != nil {
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
