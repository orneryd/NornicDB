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

// accessMetaMode selects what accessMetaInTxn does with the entry.
type accessMetaMode int

const (
	// accessMetaRead returns the entry stored for the entity: nil when there is
	// none, a decode error when the stored value does not decode.
	accessMetaRead accessMetaMode = iota
	// accessMetaWrite stores value for the entity and returns it with a nil
	// error. It performs no read.
	accessMetaWrite
)

// accessMetaInTxn is the one owner of the access-metadata keyspace: it reads
// (accessMetaRead) or writes (accessMetaWrite) an entity's entry in w. value
// is the entry to write; a read ignores it.
//
// A read projects a *stagedKV's buffered deletes and writes before it reads
// the transaction's Badger transaction, so a statement sees the access
// metadata it staged.
func accessMetaInTxn(w kvWriter, entityID string, mode accessMetaMode, value *knowledgepolicy.AccessMetaEntry) (*knowledgepolicy.AccessMetaEntry, error) {
	if mode == accessMetaWrite {
		data, err := msgpack.Marshal(value)
		if err != nil {
			return nil, err
		}
		if err := w.Set(accessMetaKey(entityID), data); err != nil {
			return nil, err
		}
		return value, nil
	}
	key := accessMetaKey(entityID)
	if staged, ok := w.(*stagedKV); ok {
		if staged.pendingDeletes[string(key)] {
			return nil, nil
		}
		if data, ok := staged.pendingWrites[string(key)]; ok {
			var entry knowledgepolicy.AccessMetaEntry
			err := util.DecodeMsgpackBytes(data, &entry)
			return &entry, err
		}
	}
	item, err := kvWriterTxn(w).Get(key)
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

func hasAccessState(entry *knowledgepolicy.AccessMetaEntry) bool {
	return entry != nil && (entry.HasAccessState || entry.Fixed != (knowledgepolicy.AccessMetaFixedFields{}) ||
		entry.LastMutatedAt != 0 || entry.MutationCount != 0 || len(entry.Overflow) != 0 || len(entry.KalmanFilters) != 0)
}

func (b *BadgerEngine) PutAccessMeta(entityID string, entry *knowledgepolicy.AccessMetaEntry) error {
	target := entityID
	if len(entityID) > 0 && entityID[0] == 0 {
		target = entry.TargetID
	}
	return b.withPublishingUpdate(func(txn *badger.Txn) error {
		current, err := accessMetaInTxn(txn, entityID, accessMetaRead, nil)
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
		_, err = accessMetaInTxn(txn, entityID, accessMetaWrite, &updated)
		return err
	}, nil, func() []any { return []any{namespaceWriteKey(namespaceForNodeID(NodeID(target)))} }, nil)
}

func (b *BadgerEngine) DeleteAccessMeta(entityID string) error {
	target := entityID
	return b.withPublishingUpdate(func(txn *badger.Txn) error {
		entry, err := accessMetaInTxn(txn, entityID, accessMetaRead, nil)
		if err != nil {
			return err
		}
		if entry != nil && len(entityID) > 0 && entityID[0] == 0 {
			target = entry.TargetID
		}
		if entry != nil && entry.HasIndexKeys {
			retained := &knowledgepolicy.AccessMetaEntry{
				TargetID: entry.TargetID, TargetScope: entry.TargetScope,
				IndexKeys: entry.IndexKeys, HasIndexKeys: true, Deindexed: entry.Deindexed,
			}
			_, err = accessMetaInTxn(txn, entityID, accessMetaWrite, retained)
			return err
		}
		return txn.Delete(accessMetaKey(entityID))
	}, nil, func() []any { return []any{namespaceWriteKey(namespaceForNodeID(NodeID(target)))} }, nil)
}

// ReadAccessMeta is the one implementation of committed access metadata reads
// and their DROP admission. A point read returns the entry stored for the
// request's entity ID; it retains work and tombstone entries. An all read
// exposes only entity access state, skipping internal work keys, from one
// captured transaction and a single iterator. The read mode is validated
// before any read happens.
func (b *BadgerEngine) ReadAccessMeta(request knowledgepolicy.AccessMetaReadRequest) (knowledgepolicy.AccessMetaReadResult, error) {
	var result knowledgepolicy.AccessMetaReadResult
	switch request.Mode {
	case knowledgepolicy.AccessMetaReadPoint, knowledgepolicy.AccessMetaReadAll:
	default:
		return result, ErrInvalidData
	}

	entityID := request.EntityID
	if request.Mode == knowledgepolicy.AccessMetaReadAll {
		if err := b.pendingNamespaceConflict(""); err != nil {
			return result, err
		}
	} else if len(entityID) > 0 && entityID[0] != 0 {
		if err := b.pendingPrefixConflict(entityID); err != nil {
			return result, err
		}
	}
	err := b.withView(func(txn *badger.Txn) error {
		if request.Mode == knowledgepolicy.AccessMetaReadPoint {
			var err error
			result.Entry, err = accessMetaInTxn(txn, entityID, accessMetaRead, nil)
			return err
		}
		opts := badgerIteratorOptions()
		opts.Prefix = accessMetaKey("")
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if len(item.Key()) > len(opts.Prefix) && item.Key()[len(opts.Prefix)] == 0 {
				continue
			}
			if err := item.Value(func(val []byte) error {
				var entry knowledgepolicy.AccessMetaEntry
				if err := util.DecodeMsgpackBytes(val, &entry); err != nil {
					return err
				}
				if hasAccessState(&entry) {
					result.Entries = append(result.Entries, &entry)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return knowledgepolicy.AccessMetaReadResult{}, err
	}
	if request.Mode != knowledgepolicy.AccessMetaReadPoint {
		return result, nil
	}
	if entry := result.Entry; entry != nil {
		if entry.TargetID != "" && entry.TargetID != entityID {
			if err := b.pendingPrefixConflict(entry.TargetID); err != nil {
				return knowledgepolicy.AccessMetaReadResult{}, err
			}
		}
		if !hasAccessState(entry) && entry.DeindexWork == nil && !entry.IndexTombstone {
			result.Entry = nil
		}
	}
	return result, nil
}

// RecordMaterializedAccess records an access only after the query executor has
// fully materialized the entity into a result row.
func (b *BadgerEngine) RecordMaterializedAccess(entityID string) {
	if b == nil || b.accumulator == nil || entityID == "" {
		return
	}
	b.accumulator.IncrementAccess(entityID)
}
