package storage

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/util"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	policyWorkMetaIDPrefix  = "\x00work\x00"
	policyIndexMetaIDPrefix = "\x00index\x00"
)

func accessMetaKey(entityID string) []byte {
	return append([]byte{prefixMVCCMeta}, []byte("accessmeta:"+entityID)...)
}

func getAccessMetaInTxn(txn *badger.Txn, entityID string) (*knowledgepolicy.AccessMetaEntry, error) {
	item, err := txn.Get(accessMetaKey(entityID))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var entry knowledgepolicy.AccessMetaEntry
	err = item.Value(func(value []byte) error {
		return util.DecodeMsgpackBytes(value, &entry)
	})
	return &entry, err
}

func putAccessMetaInTxn(txn *badger.Txn, entityID string, entry *knowledgepolicy.AccessMetaEntry) error {
	data, err := msgpack.Marshal(entry)
	if err != nil {
		return err
	}
	return txn.Set(accessMetaKey(entityID), data)
}

func hasAccessState(entry *knowledgepolicy.AccessMetaEntry) bool {
	return entry != nil && (entry.HasAccessState || entry.Fixed != (knowledgepolicy.AccessMetaFixedFields{}) ||
		entry.LastMutatedAt != 0 || entry.MutationCount != 0 || len(entry.Overflow) != 0 || len(entry.KalmanFilters) != 0)
}

func (b *BadgerEngine) GetAccessMeta(entityID string) (*knowledgepolicy.AccessMetaEntry, error) {
	var entry *knowledgepolicy.AccessMetaEntry
	err := b.withView(func(txn *badger.Txn) error {
		var err error
		entry, err = getAccessMetaInTxn(txn, entityID)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err == nil && entry != nil && !hasAccessState(entry) && entry.DeindexWork == nil && !entry.IndexTombstone {
		return nil, nil
	}
	return entry, err
}

func (b *BadgerEngine) PutAccessMeta(entityID string, entry *knowledgepolicy.AccessMetaEntry) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		current, err := getAccessMetaInTxn(txn, entityID)
		if err != nil {
			return err
		}
		updated := *entry
		updated.HasAccessState = len(entityID) > 0 && entityID[0] != 0
		if current != nil {
			updated.IndexKeys = current.IndexKeys
			updated.HasIndexKeys = current.HasIndexKeys
			updated.Deindexed = current.Deindexed
		}
		return putAccessMetaInTxn(txn, entityID, &updated)
	})
}

func (b *BadgerEngine) DeleteAccessMeta(entityID string) error {
	return b.withUpdate(func(txn *badger.Txn) error {
		entry, err := getAccessMetaInTxn(txn, entityID)
		if err != nil {
			return err
		}
		if entry != nil && entry.HasIndexKeys {
			retained := &knowledgepolicy.AccessMetaEntry{
				TargetID: entry.TargetID, TargetScope: entry.TargetScope,
				IndexKeys: entry.IndexKeys, HasIndexKeys: true, Deindexed: entry.Deindexed,
			}
			return putAccessMetaInTxn(txn, entityID, retained)
		}
		return txn.Delete(accessMetaKey(entityID))
	})
}

func (b *BadgerEngine) ScanAccessMeta() ([]*knowledgepolicy.AccessMetaEntry, error) {
	var entries []*knowledgepolicy.AccessMetaEntry

	err := b.withView(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = accessMetaKey("")
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if len(item.Key()) > len(opts.Prefix) && item.Key()[len(opts.Prefix)] == 0 {
				continue
			}
			err := item.Value(func(val []byte) error {
				var entry knowledgepolicy.AccessMetaEntry
				if err := util.DecodeMsgpackBytes(val, &entry); err != nil {
					return err
				}
				if hasAccessState(&entry) {
					entries = append(entries, &entry)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// RecordMaterializedAccess records an access only after the query executor has
// fully materialized the entity into a result row.
func (b *BadgerEngine) RecordMaterializedAccess(entityID string) {
	if b == nil || b.accumulator == nil || entityID == "" {
		return
	}
	b.accumulator.IncrementAccess(entityID)
}
