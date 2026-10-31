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
	entry, err := accessMetaInTxn(txn, policyIndexMetaIDPrefix+string(originalIndexKey), accessMetaRead, nil)
	return err == nil && entry != nil && entry.IndexTombstone
}

func putIndexTombstoneInTxn(txn *badger.Txn, originalKey []byte) error {
	_, err := accessMetaInTxn(txn, policyIndexMetaIDPrefix+string(originalKey), accessMetaWrite, &knowledgepolicy.AccessMetaEntry{IndexTombstone: true})
	return err
}

// SetIndexTombstones changes visibility markers for the original index keys
// atomically. present=true hides entries; present=false removes their markers.
// An empty batch performs no write. Keys and stored marker bytes are unchanged.
func (b *BadgerEngine) SetIndexTombstones(keys [][]byte, present bool) error {
	if len(keys) == 0 {
		return nil
	}
	return b.withUpdate(updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		for _, key := range keys {
			var err error
			if present {
				err = putIndexTombstoneInTxn(txn, key)
			} else {
				err = txn.Delete(indexTombstoneKey(key))
				if err == badger.ErrKeyNotFound {
					err = nil
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}
