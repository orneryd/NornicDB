// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/security"
)

// Backup creates a backup of the database to the specified file path.
// Uses BadgerDB's streaming backup which creates a consistent snapshot.
// The backup file is a self-contained, portable copy of the database.
func (b *BadgerEngine) Backup(path string) error {
	// Every write acknowledged before the backup is in it.
	if err := b.FlushWriteBehind(nil); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
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

// restoreReadDrainTimeout bounds how long Restore waits for the reads and
// transactions open when it starts to end. A variable for tests.
var restoreReadDrainTimeout = 30 * time.Second

// Restore loads a Badger streaming backup into the engine. The backup format
// is the protobuf stream emitted by Backup, not the legacy JSON export.
//
// The store is offline while it is replaced (#1020). Restore stops the
// background index backfills, holds off writes (writeBarrier) and reads
// (holdReads: a read or write that starts meanwhile fails at once with
// ErrStorageRestoring, which is transient), and waits up to
// restoreReadDrainTimeout for the reads and transactions already open. If
// they are still open then, Restore fails and changes nothing. Otherwise no
// reader sees a half-restored store or decodes with a half-built
// dictionary: the in-memory state (ID and property-key dictionaries, MVCC
// floors and sequence, schemas, caches, counts) is rebuilt from the restored
// store before reads resume. The backfills then start again if the restored
// store needs them.
func (b *BadgerEngine) Restore(path string) (restoreErr error) {
	b.stopIndexBackfill(startupEdgeBetweenIndex)
	b.stopIndexBackfill(startupLabelIndex)
	defer func() { restoreErr = errors.Join(restoreErr, b.restartIndexBackfills()) }()

	lifecycle, err := b.beginLifecycleWrite(context.Background(), "")
	if err != nil {
		if errors.Is(err, ErrStorageClosed) {
			return localizedError(localization.StorageClientStorageClosed(), ErrStorageClosed)
		}
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	defer lifecycle.finish()

	b.mu.RLock()
	db := b.db
	b.mu.RUnlock()
	if db == nil {
		return localizedError(localization.StorageClientStorageClosed(), ErrStorageClosed)
	}

	file, err := security.OpenRootedFile(path, os.O_RDONLY, 0)
	if err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	defer file.Close()

	if !db.oracle.holdReads(restoreReadDrainTimeout) {
		return localizedError(localization.StorageClientRestoreBusy(restoreReadDrainTimeout.String()), nil)
	}
	defer db.oracle.releaseReads()

	if err := db.DropAll(context.Background()); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	b.InvalidatePendingEmbeddingsIndex()
	if err := db.Load(context.Background(), bufio.NewReaderSize(file, 16*1024), 1000); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	// The restored derived-count deltas are folded, as open folds them:
	// the engine that took the backup numbered them, and a new delta
	// reusing one's suffix would overwrite it.
	if err := b.foldRestoredCountDeltas(db); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	loader := heldViewer{db: db}
	ids, keys := newIDDictionary(), newPropertyKeyDictionary()
	if err := ids.loadFromBadger(loader); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	if err := keys.loadFromBadger(loader); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	b.idDict.replaceWith(ids)
	b.propKeyDict.replaceWith(keys)
	if err := b.initializeWALPropertyIdentity(db, true, true); err != nil {
		return err
	}
	if err := b.loadManualSuppressions(loader); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	b.mvccByNamespaceMu.Lock()
	b.mvccByNamespace = make(map[string]*namespaceMVCCState)
	b.mvccByNamespaceMu.Unlock()
	if err := b.initializeMVCCSequence(); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	if err := b.recoverNamespaceDrops(&lifecycle); err != nil {
		return err
	}
	if err := b.loadMVCCFloorKeys(loader); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	if err := b.loadPersistedSchemas(); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}

	b.invalidateCachesAfterRestore()
	if err := b.initializeCounts(); err != nil {
		return localizedError(localization.StorageClientBackupFailed(err), err)
	}
	b.invalidateManagedEntityUsage()
	return nil

}

// restartIndexBackfills starts the index backfills the store needs, as open
// does. It holds the write barrier so Close, which stops the backfills before
// taking it, can't run in between; a closed engine has nothing to backfill.
func (b *BadgerEngine) restartIndexBackfills() error {
	b.writeBarrier.RLock()
	defer b.writeBarrier.RUnlock()
	if b.ensureOpen(false) != nil {
		return nil
	}
	return errors.Join(b.startIndexBackfill(startupEdgeBetweenIndex, initializeIndexBackfill), b.startIndexBackfill(startupLabelIndex, initializeIndexBackfill))
}

func backupEngine(engine Engine, path string) error {
	if backupable, ok := engine.(interface{ Backup(string) error }); ok {
		return backupable.Backup(path)
	}
	return ErrNotImplemented
}

func restoreEngine(engine Engine, path string) error {
	if restorable, ok := engine.(interface{ Restore(string) error }); ok {
		return restorable.Restore(path)
	}
	return ErrNotImplemented
}

func (b *BadgerEngine) invalidateCachesAfterRestore() {
	b.nodeCacheGen.lockInvalidation(&b.nodeCacheMu)
	b.nodeCache = make(map[NodeID]*Node, b.nodeCacheMaxEntries)
	b.queryNodeCache = nil
	b.nodeCacheMu.Unlock()

	b.nodeBodyCacheMu.Lock()
	b.nodeBodyCache = make(map[NodeID]*nodeBodyCacheEntry)
	b.nodeBodyCacheLRU.Init()
	b.nodeBodyCacheBytes = 0
	b.nodeBodyCacheMu.Unlock()

	b.edgeTypeCacheGen.lockInvalidation(&b.edgeTypeCacheMu)
	b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
	b.edgeTypeCacheMu.Unlock()

	b.edgeCacheGen.lockInvalidation(&b.edgeCacheMu)
	b.edgeCache = make(map[EdgeID]*Edge, b.edgeCacheMaxItems)
	b.edgeCacheVersions = nil
	b.edgeCacheSources = nil
	b.edgeCacheMu.Unlock()

	b.adjCacheGen.lockInvalidation(&b.adjCacheMu)
	b.outgoingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
	b.incomingAdjCache = make(map[NodeID][]EdgeID, b.adjCacheMaxNodes)
	b.adjCacheMu.Unlock()

	b.labelFirstNodeCacheMu.Lock()
	b.labelFirstCacheGen.advance()
	b.labelFirstNodeCache = make(map[string]NodeID, b.labelFirstCacheMax)
	b.labelFirstNodeCacheMu.Unlock()
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

	// Runs after the locks below are released: the reset's consumers may write.
	var reset *commitNotification
	defer func() { b.awaitCommitNotification(reset) }()
	// Every write acknowledged before the delete is applied first, no write
	// runs during it, and a transaction open across it that writes under the
	// prefix fails its commit (beginLifecycleWrite).
	lifecycle, err := b.beginLifecycleWrite(context.Background(), prefix)
	if err != nil {
		return 0, 0, err
	}
	defer lifecycle.finish()
	if conflict := b.pendingPrefixConflict(prefix); conflict != nil {
		namespace, whole := wholeNamespaceFromPrefix(prefix)
		if !whole || b.pendingNamespaceDrop(namespace) == nil {
			return 0, 0, conflict
		}
	}
	b.labelCountWriteMu.Lock()
	defer b.labelCountWriteMu.Unlock()

	nodesDeleted, edgesDeleted, reset, err = b.deleteByPrefixHeld(prefix, &lifecycle, false)
	return nodesDeleted, edgesDeleted, err
}

func (b *BadgerEngine) deleteByPrefixHeld(prefix string, lifecycle *lifecycleWrite, recovery bool) (nodesDeleted int64, edgesDeleted int64, reset *commitNotification, err error) {
	prefixBytes := []byte(prefix)
	namespace, wholeNamespace := wholeNamespaceFromPrefix(prefix)

	countKeys := func(keyPrefix []byte) (int64, error) {
		var count int64
		if err := b.db.viewHeld(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions(keyPrefix))
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

	var deletedLabelCounts map[namespaceLabel]int64
	var deletedEdgeTypeCounts map[namespaceEdgeType]int64
	var deletedStartLabelCounts map[namespaceEdgeTypeLabel]int64
	var deletedEndLabelCounts map[namespaceEdgeTypeLabel]int64
	var intent *namespaceDropIntent
	nodeNums, edgeNums := make(map[uint64]struct{}), make(map[uint64]struct{})
	if wholeNamespace {
		intent = b.pendingNamespaceDrop(namespace)
	}
	if intent != nil {
		nodesDeleted, edgesDeleted = intent.nodesDeleted, intent.edgesDeleted
		nodeNums, edgeNums = intent.nodes, intent.edges
	} else {
		if wholeNamespace {
			nodesDeleted, err = countKeys(nodeKeyPrefix)
		} else {
			nodesDeleted, deletedLabelCounts, err = b.collectNodeLabelCountsByPrefix(nodeKeyPrefix)
		}
		if err != nil {
			return 0, 0, nil, err
		}
		if !wholeNamespace {
			deletedEdgeTypeCounts, deletedStartLabelCounts, deletedEndLabelCounts, err = b.collectEdgeTypeCountsByPrefixSnapshot(edgeKeyPrefix)
			if err != nil {
				return 0, 0, nil, err
			}
		}
		edgesDeleted, err = countKeys(edgeKeyPrefix)
		if err != nil {
			return 0, 0, nil, err
		}
		for _, entry := range []struct {
			kind byte
			ids  map[uint64]struct{}
		}{
			{prefixIDDictNodeForward, nodeNums}, {prefixIDDictEdgeForward, edgeNums},
		} {
			if err := b.db.viewHeld(func(txn *badger.Txn) error {
				it := txn.NewIterator(badgerPrefixIteratorOptions(append([]byte{entry.kind}, prefixBytes...)))
				defer it.Close()
				for it.Rewind(); it.ValidForPrefix(append([]byte{entry.kind}, prefixBytes...)); it.Next() {
					if err := it.Item().Value(func(value []byte) error {
						if len(value) == 8 {
							entry.ids[binary.BigEndian.Uint64(value)] = struct{}{}
						}
						return nil
					}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return 0, 0, nil, err
			}
		}

		if wholeNamespace {
			intent = &namespaceDropIntent{namespace: namespace, nodesDeleted: nodesDeleted, edgesDeleted: edgesDeleted, nodes: nodeNums, edges: edgeNums}
			if err := b.writeNamespaceDropIntent(intent, lifecycle); err != nil {
				return 0, 0, nil, err
			}
		}
	}

	var publishUsage func()
	if !wholeNamespace {
		publishUsage, err = b.prepareDeletedEntityPublication(nodeNums, edgeNums)
		if err != nil {
			return 0, 0, nil, err
		}
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
		manualSuppressionKey(manualSuppressionNode, prefix),
		manualSuppressionKey(manualSuppressionEdge, prefix),
	}
	if wholeNamespace {
		dropPrefixes = append(dropPrefixes, labelCountNamespacePrefix(namespace))
		dropPrefixes = append(dropPrefixes, edgeTypeCountNamespacePrefix(namespace))
		dropPrefixes = append(dropPrefixes, edgeTypeLabelCountNamespacePrefix(prefixMVCCMetaEdgeTypeStartLabelCount, namespace))
		dropPrefixes = append(dropPrefixes, edgeTypeLabelCountNamespacePrefix(prefixMVCCMetaEdgeTypeEndLabelCount, namespace))
		dropPrefixes = append(dropPrefixes, countDeltaNamespacePrefix(countDeltaKindLabel, namespace))
		dropPrefixes = append(dropPrefixes, countDeltaNamespacePrefix(countDeltaKindEdgeType, namespace))
	}
	// Bulk deletion bypasses per-node marker writes. Invalidate even on a
	// partial drop failure so future claims rebuild from the durable index.
	defer b.InvalidatePendingEmbeddingsIndex()
	if err := b.db.DropPrefix(context.Background(), prefixDropOptions{}, dropPrefixes...); err != nil {
		return 0, 0, nil, localizedError(localization.StorageClientDropPrefixFailed(prefixNode, err), err)
	}
	// A whole-namespace drop keeps admission fenced until its schema owner
	// is retired after successful cleanup. Do not take that owner's mutex
	// here: a pre-fence metadata writer may still hold it in its committed
	// tail. Partial-prefix deletion retains the schema, so retire just its
	// deleted holders under the existing lifecycle authority.
	if !wholeNamespace {
		// Primary bodies are gone. Retire their UNIQUE claims before any later
		// cleanup can fail and before lifecycle authority admits another writer.
		// Snapshot manager references so schema registry admission is not held
		// while the individual constraint owners publish their removals.
		var affectedSchemas []*SchemaManager
		b.schemasMu.RLock()
		for name, schema := range b.schemas {
			scoped := name + ":"
			if strings.HasPrefix(scoped, prefix) || strings.HasPrefix(prefix, scoped) {
				affectedSchemas = append(affectedSchemas, schema)
			}
		}
		b.schemasMu.RUnlock()
		for _, schema := range affectedSchemas {
			schema.retireUniqueValuesByNodePrefix(prefix)
		}
	}
	if b.dropPrefixTailHook != nil {
		if err := b.dropPrefixTailHook(prefix); err != nil {
			return 0, 0, nil, err
		}
	}
	if err := b.deleteNamespaceKeyFamilies(prefixBytes, namespace, wholeNamespace, nodeNums, edgeNums); err != nil {
		return 0, 0, nil, err
	}

	deleteIndexEntriesBySuffixPrefix := func(indexPrefix byte) error {
		indexKeyPrefix := []byte{indexPrefix}
		wb := b.db.NewWriteBatch(context.Background())
		defer wb.Cancel()

		const flushEvery = 50_000
		pending := 0

		if err := b.db.viewHeld(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions(indexKeyPrefix))
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
		return 0, 0, nil, localizedError(localization.StorageClientCleanLabelIndexFailed(err), err)
	}
	if err := deleteIndexEntriesBySuffixPrefix(prefixEdgeTypeIndex); err != nil {
		return 0, 0, nil, localizedError(localization.StorageClientCleanEdgeTypeIndexFailed(err), err)
	}
	if len(deletedLabelCounts) > 0 {
		if err := b.decrementLabelCounts(deletedLabelCounts); err != nil {
			return 0, 0, nil, err
		}
	}
	if len(deletedEdgeTypeCounts) > 0 {
		if err := b.decrementEdgeTypeCounts(deletedEdgeTypeCounts); err != nil {
			return 0, 0, nil, err
		}
	}
	if len(deletedStartLabelCounts) > 0 {
		if err := b.decrementEdgeTypeLabelCounts(prefixMVCCMetaEdgeTypeStartLabelCount, deletedStartLabelCounts); err != nil {
			return 0, 0, nil, err
		}
	}
	if len(deletedEndLabelCounts) > 0 {
		if err := b.decrementEdgeTypeLabelCounts(prefixMVCCMetaEdgeTypeEndLabelCount, deletedEndLabelCounts); err != nil {
			return 0, 0, nil, err
		}
	}

	if wholeNamespace {
		if err := b.completeNamespaceDrop(intent, lifecycle); err != nil {
			return 0, 0, nil, err
		}
	}
	b.dropManualSuppressions(prefix)
	// Node and edge dict entries are sharded: collect the victim IDs shard
	// by shard, then remove each pair with the sharded helpers (the drop is
	// exclusive, so the passes race with nothing).
	var nodeVictims []NodeID
	for i := range b.idDict.nodes {
		sh := &b.idDict.nodes[i]
		sh.mu.RLock()
		for id := range sh.forward {
			if strings.HasPrefix(string(id), prefix) {
				nodeVictims = append(nodeVictims, id)
			}
		}
		sh.mu.RUnlock()
	}
	for _, id := range nodeVictims {
		removeIDPair(&b.idDict.nodes, nodeForwardShard, nodeReverseShard, id, 0, idPairByString)
	}
	var edgeVictims []EdgeID
	for i := range b.idDict.edges {
		sh := &b.idDict.edges[i]
		sh.mu.RLock()
		for id := range sh.forward {
			if strings.HasPrefix(string(id), prefix) {
				edgeVictims = append(edgeVictims, id)
			}
		}
		sh.mu.RUnlock()
	}
	for _, id := range edgeVictims {
		removeIDPair(&b.idDict.edges, edgeForwardShard, edgeReverseShard, id, 0, idPairByString)
	}
	if wholeNamespace {
		b.transitionNamespaceSchema(namespace, schemaNamespaceRetire, nil)
		b.propKeyDict.mu.Lock()
		b.propKeyDict.walBindingDomain++
		b.propKeyDict.walBindingRevision++
		b.propKeyDict.walEpochRevision.Add(1)
		delete(b.propKeyDict.forward, namespace)
		delete(b.propKeyDict.reverse, namespace)
		delete(b.propKeyDict.persisted, namespace)
		delete(b.propKeyDict.nextID, namespace)
		b.propKeyDict.mu.Unlock()
	}
	if nodesDeleted > 0 || edgesDeleted > 0 {
		defer b.graphMutationVersions.changedPrefix(prefix)
	}
	// Clear/adjust caches and cached counters.
	if wholeNamespace {
		b.invalidateCachesAfterRestore()
	} else {
		b.nodeCacheGen.lockInvalidation(&b.nodeCacheMu)
		for id := range b.nodeCache {
			if strings.HasPrefix(string(id), prefix) {
				delete(b.nodeCache, id)
			}
		}
		for id := range b.queryNodeCache {
			if strings.HasPrefix(string(id), prefix) {
				delete(b.queryNodeCache, id)
			}
		}
		b.nodeCacheMu.Unlock()

		b.edgeTypeCacheGen.lockInvalidation(&b.edgeTypeCacheMu)
		b.edgeTypeCache = make(map[string][]*Edge, b.edgeTypeCacheMaxTypes)
		b.edgeTypeCacheMu.Unlock()

	}

	if !recovery && nodesDeleted != 0 {
		b.nodeCount.Add(-nodesDeleted)
	}
	if !recovery && edgesDeleted != 0 {
		b.edgeCount.Add(-edgesDeleted)
	}

	b.namespaceCountsMu.Lock()
	delete(b.namespaceNodeCounts, prefix)
	delete(b.namespaceEdgeCounts, prefix)
	b.namespaceCountsMu.Unlock()

	// No per-entity event was published for what this removed.
	if publishUsage != nil {
		publishUsage()
	}
	reset = b.publishNamespaceReset(prefix)
	if wholeNamespace {
		b.transitionPending(namespaceDropRetire, namespace, nil)
	}
	return nodesDeleted, edgesDeleted, reset, nil
}

func (b *BadgerEngine) deleteNamespaceKeyFamilies(prefix []byte, namespace string, wholeNamespace bool, nodes, edges map[uint64]struct{}) error {
	batch := b.db.NewWriteBatch(context.Background())
	defer batch.Cancel()
	pending := 0
	for _, family := range badgerKeyFamilies {
		err := b.db.viewHeld(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{family.prefix}))
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix([]byte{family.prefix}); it.Next() {
				key := it.Item().Key()
				if !namespaceOwnsBadgerKey(key, prefix, namespace, wholeNamespace, nodes, edges) {
					continue
				}
				if err := batch.Delete(it.Item().KeyCopy(nil)); err != nil {
					return err
				}
				pending++
				if pending >= 50_000 {
					if err := batch.Flush(); err != nil {
						return err
					}
					pending = 0
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if pending > 0 {
		return batch.Flush()
	}
	return nil
}

// Verify BadgerEngine implements Engine interface
var _ Engine = (*BadgerEngine)(nil)
