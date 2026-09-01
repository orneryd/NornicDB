// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bufio"
	"bytes"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
)

// Backup creates a backup of the database to the specified file path.
// Uses BadgerDB's streaming backup which creates a consistent snapshot.
// The backup file is a self-contained, portable copy of the database.
func (b *BadgerEngine) Backup(path string) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return localizedError(localization.StorageClientStorageClosed(), ErrStorageClosed)
	}

	// Create backup file
	f, err := security.CreateRootedFile(path, 0o600)
	if err != nil {
		return localizedError(localization.StorageClientBackupFileCreateFailed(path, err), err)
	}
	defer f.Close()

	// Use BufferedWriter for better performance
	buf := bufio.NewWriterSize(f, 16*1024*1024) // 16MB buffer

	// Stream backup (since=0 means full backup)
	_, err = b.db.Backup(buf, 0)
	if err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}

	// Flush buffer
	if err := buf.Flush(); err != nil {
		return localizedError(localization.StorageClientBackupFlushFailed(err), err)
	}

	// Sync to disk
	if err := f.Sync(); err != nil {
		return localizedError(localization.StorageClientBackupSyncFailed(err), err)
	}

	return nil
}

// DeleteByPrefix deletes all nodes and edges with IDs starting with the given prefix.
// Used for DROP DATABASE operations to delete all data in a namespace.
//
// This uses Badger's native prefix drop for db-scoped keyspaces to avoid per-node
// decoding and DeleteNode/DeleteEdge loops. Secondary indexes that don't begin
// with the node/edge ID (label and edge-type indexes) are cleaned up by scanning
// those index keyspaces and deleting entries whose suffix IDs match the prefix.
func (b *BadgerEngine) DeleteByPrefix(prefix string) (nodesDeleted int64, edgesDeleted int64, err error) {
	if prefix == "" {
		return 0, 0, localizedError(localization.StorageClientDeletePrefixRequired(), nil)
	}

	if err := b.ensureOpen(); err != nil {
		return 0, 0, err
	}

	prefixBytes := []byte(prefix)

	countKeys := func(keyPrefix []byte) (int64, error) {
		var count int64
		if err := b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerIterOptsKeyOnly(keyPrefix))
			defer it.Close()

			for it.Rewind(); it.ValidForPrefix(keyPrefix); it.Next() {
				count++
			}
			return nil
		}); err != nil {
			return 0, err
		}
		return count, nil
	}

	nodeKeyPrefix := append([]byte{prefixNode}, prefixBytes...)
	edgeKeyPrefix := append([]byte{prefixEdge}, prefixBytes...)

	nodesDeleted, err = countKeys(nodeKeyPrefix)
	if err != nil {
		return 0, 0, err
	}
	edgesDeleted, err = countKeys(edgeKeyPrefix)
	if err != nil {
		return 0, 0, err
	}

	// Drop db-scoped keyspaces (these key formats all begin with nodeID/edgeID).
	dropPrefixes := [][]byte{
		nodeKeyPrefix,
		edgeKeyPrefix,
		append([]byte{prefixOutgoingIndex}, prefixBytes...),
		append([]byte{prefixIncomingIndex}, prefixBytes...),
		append([]byte{prefixEdgeBetweenIndex}, prefixBytes...),
		append([]byte{prefixEdgeBetweenHead}, prefixBytes...),
		append([]byte{prefixPendingEmbed}, prefixBytes...),
		append([]byte{prefixEmbedding}, prefixBytes...),
	}
	for _, p := range dropPrefixes {
		if err := b.db.DropPrefix(p); err != nil {
			return 0, 0, localizedError(localization.StorageClientDropPrefixFailed(p[0], err), err)
		}
	}

	deleteIndexEntriesBySuffixPrefix := func(indexPrefix byte) error {
		indexKeyPrefix := []byte{indexPrefix}
		wb := b.db.NewWriteBatch()
		defer wb.Cancel()

		const flushEvery = 50_000
		pending := 0

		if err := b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerIterOptsKeyOnly(indexKeyPrefix))
			defer it.Close()

			for it.Rewind(); it.ValidForPrefix(indexKeyPrefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) < 3 {
					continue
				}
				sep := bytes.IndexByte(key[1:], 0x00)
				if sep < 0 || 1+sep+1 >= len(key) {
					continue
				}
				suffixID := key[1+sep+1:]
				if !bytes.HasPrefix(suffixID, prefixBytes) {
					continue
				}

				if err := wb.Delete(item.KeyCopy(nil)); err != nil {
					return err
				}
				pending++
				if pending >= flushEvery {
					if err := wb.Flush(); err != nil {
						return err
					}
					pending = 0
				}
			}
			return nil
		}); err != nil {
			return err
		}

		if pending > 0 {
			if err := wb.Flush(); err != nil {
				return err
			}
		}
		return nil
	}

	// Clean up secondary indexes where the db prefix appears in the suffix.
	if err := deleteIndexEntriesBySuffixPrefix(prefixLabelIndex); err != nil {
		return 0, 0, localizedError(localization.StorageClientCleanLabelIndexFailed(err), err)
	}
	if err := deleteIndexEntriesBySuffixPrefix(prefixEdgeTypeIndex); err != nil {
		return 0, 0, localizedError(localization.StorageClientCleanEdgeTypeIndexFailed(err), err)
	}

	// Clear/adjust caches and cached counters.
	b.nodeCacheMu.Lock()
	for id := range b.nodeCache {
		if strings.HasPrefix(string(id), prefix) {
			delete(b.nodeCache, id)
		}
	}
	b.nodeCacheMu.Unlock()

	b.edgeTypeCacheMu.Lock()
	b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	b.edgeTypeCacheMu.Unlock()

	if nodesDeleted != 0 {
		b.nodeCount.Add(-nodesDeleted)
	}
	if edgesDeleted != 0 {
		b.edgeCount.Add(-edgesDeleted)
	}

	b.namespaceCountsMu.Lock()
	delete(b.namespaceNodeCounts, prefix)
	delete(b.namespaceEdgeCounts, prefix)
	b.namespaceCountsMu.Unlock()

	return nodesDeleted, edgesDeleted, nil
}

// Verify BadgerEngine implements Engine interface
var _ Engine = (*BadgerEngine)(nil)
