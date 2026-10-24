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
// non-empty stores don't block server startup. Label readers use a
// complete node-body scan until the ready marker is committed.

const (
	labelIndexRebuildBatchSize = 50_000
	labelIndexRebuildLogEvery  = 100_000
)

// ensureLabelIndex schedules the label-index startup backfill once.
//
// Empty stores mark the backfill complete immediately. Non-empty
// stores rebuild in the background; label-index readers use a complete
// node-body scan until the ready marker is committed. This keeps startup
// non-blocking without exposing partial index contents to queries.
func (b *BadgerEngine) ensureLabelIndex() error {
	ready, err := b.labelIndexReady()
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	hasNodes, err := b.hasAnyStoredNodes()
	if err != nil {
		return err
	}
	if !hasNodes {
		return b.markEmptyLabelIndexReady()
	}

	b.startLabelIndexBackfill()
	return nil
}

// hasAnyStoredNodes detects whether a startup rebuild has work to do.
func (b *BadgerEngine) hasAnyStoredNodes() (bool, error) {
	var hasNodes bool
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixNode}))
		defer it.Close()
		it.Rewind()
		hasNodes = it.ValidForPrefix([]byte{prefixNode})
		return nil
	})
	return hasNodes, err
}

// startLabelIndexBackfill launches one cancellable background rebuild.
func (b *BadgerEngine) startLabelIndexBackfill() {
	b.labelIndexBackfillMu.Lock()
	if b.labelIndexBackfillDone != nil {
		b.labelIndexBackfillMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.labelIndexBackfillCancel = cancel
	b.labelIndexBackfillDone = done
	b.labelIndexBackfillMu.Unlock()

	go func() {
		defer close(done)
		start := time.Now()
		idxLog := b.log.With("subsystem", "index_rebuild", "index", "label")
		idxLog.Info("label index backfill started")
		processed, err := b.rebuildLabelIndex(ctx)
		switch {
		case err == nil:
			idxLog.Info("label index backfill completed",
				"nodes", processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		case errors.Is(err, context.Canceled), errors.Is(err, ErrStorageClosed):
			idxLog.Info("label index backfill canceled",
				"nodes", processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		default:
			idxLog.Error("label index backfill failed",
				"nodes", processed,
				"duration_ms", time.Since(start).Milliseconds(),
				slog.Any("error", err),
			)
		}
	}()
}

// stopLabelIndexBackfill cancels and waits for a running rebuild.
func (b *BadgerEngine) stopLabelIndexBackfill() {
	b.labelIndexBackfillMu.Lock()
	cancel := b.labelIndexBackfillCancel
	done := b.labelIndexBackfillDone
	b.labelIndexBackfillCancel = nil
	b.labelIndexBackfillDone = nil
	b.labelIndexBackfillMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// rebuildLabelIndex scans every stored node and re-emits the
// `(label, nodeNumID)` index entries from the node body's Labels slice.
// Clearing and rebuilding uses versioned writes, so a reader admitted before
// the building marker keeps its complete index generation in its snapshot.
func (b *BadgerEngine) rebuildLabelIndex(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.ensureOpen(); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}
	generation, err := b.beginLabelIndexRebuild(ctx)
	if err != nil {
		return 0, err
	}
	if err := b.clearLabelIndexPrefixVersioned(ctx); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}
	if hook := b.labelIndexAfterClearHook; hook != nil {
		hook()
	}
	idxLog := b.log.With("subsystem", "index_rebuild", "index", "label")
	idxLog.Debug("label index backfill index cleared")
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
	if err := b.completeLabelIndexGeneration(generation); err != nil {
		return processed, err
	}
	return processed, nil
}

// clearLabelIndexPrefixVersioned writes tombstones through managed Badger
// transactions. The old values remain available to every reader whose
// snapshot predates this generation's building marker; DropPrefix would
// physically remove those values and truncate already-admitted streams.
func (b *BadgerEngine) clearLabelIndexPrefixVersioned(ctx context.Context) error {
	prefix := []byte{prefixLabelIndex}
	const maxBatch = 5000
	batchSize := maxBatch
	var cursor []byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		nextCursor := []byte(nil)
		done := false
		err := b.withUpdate(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.IteratorOptions{Prefix: prefix, PrefetchValues: false})
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = prefix
			}
			deleted := 0
			for it.Seek(start); it.ValidForPrefix(prefix); it.Next() {
				if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
					return err
				}
				deleted++
				if deleted >= batchSize {
					it.Next()
					if it.ValidForPrefix(prefix) {
						nextCursor = it.Item().KeyCopy(nil)
					} else {
						done = true
					}
					return nil
				}
			}
			done = true
			return nil
		})
		if errors.Is(err, badger.ErrConflict) && batchSize > 1 {
			batchSize /= 2
			continue
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		cursor = nextCursor
		batchSize = maxBatch
	}
}
