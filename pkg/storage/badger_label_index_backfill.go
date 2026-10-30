// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Label-index startup backfill.
//
// The label index (prefixLabelIndex = 0x03, key shape
// `[0x03][label][0x00][nodeNumID]`) is the index every
// `MATCH (n:Label)` resolves through. Production write paths
// (CreateNode / BulkCreateNodes / UpdateNode) write the index
// transactionally with the node body, so on a freshly-created store
// the index is always consistent.
//
// However: stores carried over from older binaries, or stores
// reconstructed by file-level copy of a Badger directory mid-write,
// can end up with node bodies present and the corresponding
// prefixLabelIndex keys absent (or stale numIDs no longer in the dict).
// `MATCH (n:Label)` on such a store silently returns 0 rows for every
// pre-existing node — the symptom reported on v1.1.1 and v1.1.2
// against a 5026-node restored corpus.
//
// This file mirrors the edge-between index backfill (see
// badger_edge_between_index.go and #125): a one-time, marker-gated,
// non-blocking startup pass that walks every node body and re-emits
// the `(label, nodeNumID)` entries from authoritative source — the
// node's own Labels slice on disk.
//
// Cost model: O(N) walk over node bodies, batched into Badger txn-
// sized chunks so each batch's dict allocations + label-index writes
// commit together. Runs in the background after engine open so
// non-empty stores don't block server startup; reads can fall through
// to AllNodes-and-filter (the workaround the bug report noted) until
// the rebuild completes, but in practice the rebuild finishes before
// most workloads warm up because every batch commits durably.

var labelIndexReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaLabelIndexReady}

const (
	labelIndexSchemeVersion    = 3
	labelIndexRebuildBatchSize = 50_000
	labelIndexRebuildLogEvery  = 100_000
)

// indexBackfillReady reads the selected family marker. Labels require the exact
// current scheme byte; relationship backfill retains key-presence semantics.
func (b *BadgerEngine) indexBackfillReady(kind startupIndexKind) (bool, error) {
	key := labelIndexReadyKey
	if kind == startupEdgeBetweenIndex {
		key = edgeBetweenIndexReadyKey
	}
	var ready bool
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			if kind == startupEdgeBetweenIndex {
				ready = true
				return nil
			}
			return item.Value(func(value []byte) error {
				ready = len(value) == 1 && value[0] == labelIndexSchemeVersion
				return nil
			})
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return ready, err
}

// markIndexBackfillReady records the selected family's established marker image.
func (b *BadgerEngine) markIndexBackfillReady(ctx context.Context, kind startupIndexKind) error {
	key, version := labelIndexReadyKey, byte(labelIndexSchemeVersion)
	if kind == startupEdgeBetweenIndex {
		key, version = edgeBetweenIndexReadyKey, 1
	}
	return b.withUpdate(updateRequest{ctx: ctx, scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		return txn.Set(key, []byte{version})
	})
}

type startupIndexKind uint8

const (
	startupLabelIndex startupIndexKind = iota
	startupEdgeBetweenIndex
)

// indexBackfillStartMode distinguishes marker-gated initialization from an
// explicitly requested rebuild. Both use one lifecycle registration protocol.
type indexBackfillStartMode uint8

const (
	initializeIndexBackfill indexBackfillStartMode = iota
	forceIndexBackfill
)

// startIndexBackfill owns each index family's cancellable startup lifecycle.
// The selected family keeps its independent state, rebuild and diagnostic fields.
func (b *BadgerEngine) startIndexBackfill(kind startupIndexKind, mode indexBackfillStartMode) error {
	if mode == initializeIndexBackfill {
		ready, err := b.indexBackfillReady(kind)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		prefix := []byte{prefixNode}
		if kind == startupEdgeBetweenIndex {
			prefix[0] = prefixEdge
		}
		var hasStored bool
		if err := b.db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			defer it.Close()
			it.Rewind()
			hasStored = it.ValidForPrefix(prefix)
			return nil
		}); err != nil {
			return err
		}
		if !hasStored {
			return b.markIndexBackfillReady(b.work.Context(), kind)
		}
	}

	mu := &b.labelIndexBackfillMu
	slot := &b.labelIndexBackfill
	indexName, message, entity := "label", "label index backfill", "nodes"
	rebuild := b.rebuildLabelIndex
	if kind == startupEdgeBetweenIndex {
		mu = &b.edgeBetweenIndexBackfillMu
		slot = &b.edgeBetweenIndexBackfill
		indexName, message, entity = "edge_between", "edge-between index backfill", "edges"
		rebuild = func(ctx context.Context) (int, error) {
			return b.rebuildRelationshipIndexes(ctx, rebuildEdgeBetweenOnly)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if *slot != nil {
		return nil
	}
	// The backfill is accepted engine work; its completion is what
	// stopIndexBackfill joins.
	*slot = b.startEngineWorker(b.work.Context(), func(ctx context.Context) {
		b.runIndexBackfill(ctx, rebuild, indexName, message, entity)
	})
	return nil
}

func (b *BadgerEngine) runIndexBackfill(ctx context.Context, rebuild func(context.Context) (int, error), indexName, message, entity string) {
	{
		start := time.Now()
		idxLog := b.log.With("subsystem", "index_rebuild", "index", indexName)
		idxLog.Info(message + " started")
		processed, err := rebuild(ctx)
		switch {
		case err == nil:
			idxLog.Info(message+" completed",
				entity, processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		case errors.Is(err, context.Canceled), errors.Is(err, ErrStorageClosed):
			idxLog.Info(message+" canceled",
				entity, processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		default:
			idxLog.Error(message+" failed",
				entity, processed,
				"duration_ms", time.Since(start).Milliseconds(),
				slog.Any("error", err),
			)
		}
	}
}

// stopIndexBackfill detaches, cancels and waits for the selected startup rebuild.
func (b *BadgerEngine) stopIndexBackfill(kind startupIndexKind) {
	mu := &b.labelIndexBackfillMu
	slot := &b.labelIndexBackfill
	if kind == startupEdgeBetweenIndex {
		mu = &b.edgeBetweenIndexBackfillMu
		slot = &b.edgeBetweenIndexBackfill
	}
	mu.Lock()
	worker := *slot
	*slot = nil
	mu.Unlock()
	worker.stopAndJoin()
}

// rebuildLabelIndex scans every stored node and re-emits the
// `(label, nodeNumID)` index entries from the node body's Labels slice
// (the authoritative source). The existing prefixLabelIndex range is
// dropped first so any stale entries from past dict allocations or
// half-applied writes don't shadow the rebuilt set.
func (b *BadgerEngine) rebuildLabelIndex(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.db.DropPrefix(ctx, prefixDropOptions{engine: b}, []byte{prefixLabelIndex}); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}
	scan := storedRecordScan{
		prefix:    prefixNode,
		batchSize: labelIndexRebuildBatchSize,
		logEvery:  labelIndexRebuildLogEvery,
		log:       b.log.With("subsystem", "index_rebuild", "index", "label"),
		message:   "label index backfill progress",
		unit:      "nodes",
	}
	processed, err := b.forEachStoredRecordInChunks(ctx, scan, func(txn *badger.Txn, key, val []byte) (int, bool, error) {
		if len(key) <= 1 {
			return 0, false, nil
		}
		nodeID := NodeID(key[1:])
		namespace, _, ok := ParseDatabasePrefix(string(nodeID))
		if !ok {
			// Nodes without a namespace prefix predate multi-DB support.
			// Skip — they can't participate in the per-namespace dict
			// either.
			return 0, false, nil
		}
		node, err := b.decodeNode(namespace, val)
		if err != nil {
			return 0, false, fmt.Errorf("decode node %q for label rebuild: %w", nodeID, err)
		}
		for i, label := range node.Labels {
			lblKey, err := b.labelIndexKeyString(txn, label, nodeID)
			if err == nil {
				err = txn.Set(lblKey, []byte{})
			}
			if err != nil {
				return i, false, fmt.Errorf("write label index for %q/%q: %w", nodeID, label, err)
			}
		}
		return len(node.Labels), true, nil
	})
	if err != nil {
		return processed, err
	}
	if err := ctx.Err(); err != nil {
		return processed, err
	}
	return processed, b.markIndexBackfillReady(ctx, startupLabelIndex)
}
