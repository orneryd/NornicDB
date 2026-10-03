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
// `[0x03][lowercase(label)][0x00][nodeNumID]`) is the index every
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
	labelIndexRebuildBatchSize = 50_000
	labelIndexRebuildLogEvery  = 100_000
)

// ensureLabelIndex schedules the label-index startup backfill once.
//
// Empty stores mark the backfill complete immediately. Non-empty
// stores rebuild in the background; until the rebuild marker key is
// written, MATCH (n:Label) reads still hit the on-disk index — they
// just see a partial view that will be made whole on next read once
// the rebuild advances past those entries. This matches the same
// "non-blocking, opportunistic self-heal" stance as
// ensureEdgeBetweenIndex.
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
		return b.markLabelIndexReady()
	}

	b.startLabelIndexBackfill()
	return nil
}

// labelIndexReady reports whether the compatibility backfill completed.
func (b *BadgerEngine) labelIndexReady() (bool, error) {
	var ready bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(labelIndexReadyKey)
		if err == nil {
			ready = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		return err
	})
	return ready, err
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

// markLabelIndexReady records that no compatibility rebuild remains.
func (b *BadgerEngine) markLabelIndexReady() error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, []byte{1})
	})
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
	if err := b.ensureOpen(); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}
	if err := recoverBadgerClosedPanic(func() error { return b.db.DropPrefix([]byte{prefixLabelIndex}) }); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}

	idxLog := b.log.With("subsystem", "index_rebuild", "index", "label")
	processed := 0

	// Rebuild in txn-scoped chunks so each batch's dict allocations +
	// index writes commit together. Cursors across the node prefix
	// advance between chunks.
	var cursor []byte
	for {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		done := false
		err := b.withUpdate(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixNode}))
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = []byte{prefixNode}
			}
			writes := 0
			for it.Seek(start); it.ValidForPrefix([]byte{prefixNode}); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				if len(key) <= 1 {
					continue
				}
				nodeID := NodeID(key[1:])
				namespace, _, ok := ParseDatabasePrefix(string(nodeID))
				if !ok {
					// Nodes without a namespace prefix predate multi-DB
					// support. Skip — they can't participate in the
					// per-namespace dict either.
					continue
				}
				if err := item.Value(func(val []byte) error {
					node, err := b.decodeNode(namespace, val)
					if err != nil {
						return fmt.Errorf("decode node %q for label rebuild: %w", nodeID, err)
					}
					for _, label := range node.Labels {
						lblKey, err := b.labelIndexKeyString(txn, label, nodeID)
						if err != nil {
							return fmt.Errorf("label key for %q/%q: %w", nodeID, label, err)
						}
						if err := txn.Set(lblKey, []byte{}); err != nil {
							return fmt.Errorf("write label index for %q/%q: %w", nodeID, label, err)
						}
						writes++
					}
					processed++
					if processed%labelIndexRebuildLogEvery == 0 {
						idxLog.Info("label index backfill progress",
							"nodes", processed,
						)
					}
					return nil
				}); err != nil {
					return err
				}
				if writes >= labelIndexRebuildBatchSize {
					it.Next()
					if it.ValidForPrefix([]byte{prefixNode}) {
						cursor = append([]byte(nil), it.Item().Key()...)
					} else {
						done = true
					}
					return nil
				}
			}
			done = true
			return nil
		})
		if err != nil {
			return processed, err
		}
		if done {
			break
		}
	}

	if err := ctx.Err(); err != nil {
		return processed, err
	}
	return processed, b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(labelIndexReadyKey, []byte{1})
	})
}
