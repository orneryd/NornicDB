package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// migrateV3ToV4 makes the label- and relationship-type-keyed data exact-case
// (#862). Up to version 3, label and type names were lower-cased in the label
// index, the relationship-type index, the relationship-between set and heads,
// the label and type counts and the temporal index, so :Person and :person
// shared entries and counts; Neo4j's names are case-sensitive. Node and edge
// bodies keep names as written, so every structure is rebuilt from them:
//
//   - the label index (0x03), the relationship-type index (0x06) and the
//     relationship-between set and heads (0x18 / 0x19) are rebuilt here;
//   - each deindex catalog (0x12) gets its entity's current index keys, and a
//     deindexed entity's tombstones (0x17) move to those keys, so it stays
//     hidden;
//   - the clean-shutdown marker is removed, so the temporal index and MVCC
//     heads are rebuilt before the database serves traffic
//     (prepareDerivedIndexes);
//   - the label and type counts are rebuilt at open by ensureLabelCounts and
//     ensureEdgeTypeCounts, which compare them with counts taken from the
//     bodies.
//
// The version is written last, so an interrupted run starts again.
func (b *BadgerEngine) migrateV3ToV4() error {
	ctx := context.Background()
	if _, err := b.rebuildLabelIndex(ctx); err != nil {
		return fmt.Errorf("rebuild label index: %w", err)
	}
	if _, err := b.rebuildEdgeTypeIndex(ctx); err != nil {
		return fmt.Errorf("rebuild edge-type index: %w", err)
	}
	if _, err := b.rebuildEdgeBetweenIndex(ctx); err != nil {
		return fmt.Errorf("rebuild edge-between index: %w", err)
	}
	if err := b.rewriteIndexEntryCatalogs(ctx); err != nil {
		return fmt.Errorf("rewrite index entry catalogs: %w", err)
	}
	// Clearing the clean-shutdown marker and writing the version commit
	// together.
	version := make([]byte, 8)
	binary.BigEndian.PutUint64(version, storageVersionLabelCaseV4)
	return b.withUpdate(func(txn *badger.Txn) error {
		return errors.Join(txn.Delete(cleanShutdownMarkerKey), txn.Set(mvccSchemaVersionKey(), version))
	})
}

// indexEntryCatalogRewriteBatchSize bounds the writes of one rewrite
// transaction.
const indexEntryCatalogRewriteBatchSize = 5000

// rewriteIndexEntryCatalogs sets every deindex catalog's keys to its entity's
// current index keys, and moves a deindexed entity's tombstones to them. A
// catalog whose entity no longer exists is left as it is.
func (b *BadgerEngine) rewriteIndexEntryCatalogs(ctx context.Context) error {
	scan := storedRecordScan{prefix: prefixIndexEntryCatalog, batchSize: indexEntryCatalogRewriteBatchSize}
	_, err := b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, value []byte) (int, bool, error) {
		var cat IndexEntryCatalog
		if err := msgpack.Unmarshal(value, &cat); err != nil {
			return 0, false, fmt.Errorf("decode index entry catalog %q: %w", key[1:], err)
		}
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
		if err := txn.Set(indexTombstoneKey(key), []byte{}); err != nil {
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
