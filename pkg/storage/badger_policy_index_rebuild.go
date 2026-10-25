package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

func (b *BadgerEngine) migrateLegacyPolicyMetadata(ctx context.Context) error {
	for _, prefix := range []byte{0x11, 0x12, 0x13, 0x17} {
		scan := storedRecordScan{prefix: prefix, batchSize: indexEntryCatalogRewriteBatchSize}
		_, err := b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
			serializedMap := len(value) > 0 && (msgpcode.IsFixedMap(value[0]) || value[0] == msgpcode.Map16 || value[0] == msgpcode.Map32)
			if (prefix == 0x11 || prefix == 0x12) && bytes.IndexByte(key[1:], 0) >= 0 && !serializedMap {
				return 0, false, nil
			}
			entityID := string(key[1:])
			switch prefix {
			case 0x11:
				var entry knowledgepolicy.AccessMetaEntry
				if err := msgpack.Unmarshal(value, &entry); err != nil {
					return 0, false, fmt.Errorf("decode legacy access metadata %q: %w", entityID, err)
				}
				entry.HasAccessState = true
				current, err := getAccessMetaInTxn(txn, entityID)
				if err != nil {
					return 0, false, err
				}
				if current != nil {
					entry.IndexKeys, entry.HasIndexKeys, entry.Deindexed = current.IndexKeys, current.HasIndexKeys, current.Deindexed
				}
				if err := putAccessMetaInTxn(txn, entityID, &entry); err != nil {
					return 0, false, err
				}
			case 0x12:
				var catalog IndexEntryCatalog
				if err := msgpack.Unmarshal(value, &catalog); err != nil {
					return 0, false, fmt.Errorf("decode legacy catalog %q: %w", entityID, err)
				}
				for _, indexKey := range catalog.IndexKeys {
					restoreRelationshipPrefix(indexKey)
				}
				if err := putIndexEntryCatalogInTxn(txn, entityID, &catalog); err != nil {
					return 0, false, err
				}
			case 0x13:
				var work DeindexWorkItem
				if err := msgpack.Unmarshal(value, &work); err != nil {
					return 0, false, fmt.Errorf("decode legacy deindex work %q: %w", entityID, err)
				}
				if err := putDeindexWorkItemInTxn(txn, &work); err != nil {
					return 0, false, err
				}
			case 0x17:
				indexKey := append([]byte(nil), key[1:]...)
				restoreRelationshipPrefix(indexKey)
				if err := putIndexTombstoneInTxn(txn, indexKey); err != nil {
					return 0, false, err
				}
			}
			return 2, true, txn.Delete(key)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func restoreRelationshipPrefix(key []byte) {
	if len(key) == 0 {
		return
	}
	if key[0] == 0x18 {
		key[0] = prefixEdgeBetweenIndex
	} else if key[0] == 0x19 {
		key[0] = prefixEdgeBetweenHead
	}
}

// indexEntryCatalogRewriteBatchSize bounds the writes of one rewrite
// transaction.
const indexEntryCatalogRewriteBatchSize = 5000

// rewriteIndexEntryCatalogs sets every deindex catalog's keys to its entity's
// current index keys, and moves a deindexed entity's tombstones to them. A
// catalog whose entity no longer exists is left as it is.
func (b *BadgerEngine) rewriteIndexEntryCatalogs(ctx context.Context) error {
	scan := storedRecordScan{keyPrefix: accessMetaKey(""), batchSize: indexEntryCatalogRewriteBatchSize}
	_, err := b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
		var entry knowledgepolicy.AccessMetaEntry
		if err := msgpack.Unmarshal(value, &entry); err != nil {
			return 0, false, fmt.Errorf("decode index entry catalog %q: %w", key[len(accessMetaKey("")):], err)
		}
		if !entry.HasIndexKeys && entry.IndexKeys == nil {
			return 0, false, nil
		}
		cat := IndexEntryCatalog{TargetID: entry.TargetID, TargetScope: string(entry.TargetScope), IndexKeys: entry.IndexKeys, Deindexed: entry.Deindexed}
		keys, found, err := b.currentIndexKeysInTxn(txn, &cat)
		if err != nil || !found || sameIndexKeys(cat.IndexKeys, keys) {
			return 0, false, err
		}
		if cat.Deindexed {
			err = moveIndexTombstonesInTxn(txn, cat.IndexKeys, keys)
		}
		if err == nil {
			cat.IndexKeys = keys
			err = putIndexEntryCatalogInTxn(txn, cat.TargetID, &cat)
		}
		return 1 + 2*len(keys), err == nil, err
	})
	return err
}

// moveIndexTombstonesInTxn replaces the tombstones of old index keys with
// tombstones of current ones.
func moveIndexTombstonesInTxn(txn *badger.Txn, old, current [][]byte) error {
	for _, key := range old {
		if err := txn.Delete(indexTombstoneKey(key)); err != nil {
			return err
		}
	}
	for _, key := range current {
		if err := putIndexTombstoneInTxn(txn, key); err != nil {
			return err
		}
	}
	return nil
}

// currentIndexKeysInTxn returns the index keys of a catalog's entity as it is
// stored now; found is false when the entity no longer exists.
func (b *BadgerEngine) currentIndexKeysInTxn(txn *badger.Txn, cat *IndexEntryCatalog) (keys [][]byte, found bool, err error) {
	switch cat.TargetScope {
	case "NODE":
		id := NodeID(cat.TargetID)
		namespace, _, ok := ParseDatabasePrefix(cat.TargetID)
		if !ok {
			return nil, false, nil
		}
		err = readStoredBody(txn, nodeKey(id), func(value []byte) error {
			node, err := b.decodeNode(namespace, value)
			if err != nil {
				return fmt.Errorf("decode node %q: %w", id, err)
			}
			keys, found = b.collectNodeIndexKeys(id, node.Labels), true
			return nil
		})
	case "EDGE":
		id := EdgeID(cat.TargetID)
		err = readStoredBody(txn, edgeKey(id), func(value []byte) error {
			edge, err := b.decodeEdgeBodyByID(value, id)
			if err != nil {
				return fmt.Errorf("decode edge %q: %w", id, err)
			}
			keys, found = b.collectEdgeIndexKeys(id, edge.StartNode, edge.EndNode, edge.Type), true
			return nil
		})
	}
	return keys, found, err
}

// readStoredBody calls read with the value stored at key; nothing when there
// is none.
func readStoredBody(txn *badger.Txn, key []byte, read func(value []byte) error) error {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	return item.Value(read)
}

func sameIndexKeys(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
