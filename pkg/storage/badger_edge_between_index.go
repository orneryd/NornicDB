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

var edgeBetweenIndexReadyKey = []byte{prefixMVCCMeta, prefixMVCCMetaEdgeBetweenIndexReady}

const (
	edgeBetweenIndexRebuildBatchSize = 50_000
	edgeBetweenIndexRebuildLogEvery  = 100_000
)

// ensureEdgeBetweenIndex schedules direct relationship lookup backfill once.
//
// Older Badger stores predate the edge-between indexes. Existing latest-state
// reads can fall back to the outgoing-edge index and self-heal opportunistically,
// so non-empty stores rebuild in the background instead of blocking open.
func (b *BadgerEngine) ensureEdgeBetweenIndex() error {
	ready, err := b.edgeBetweenIndexReady()
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	hasEdges, err := b.hasAnyStoredEdges()
	if err != nil {
		return err
	}
	if !hasEdges {
		return b.markEdgeBetweenIndexReady()
	}

	b.startEdgeBetweenIndexBackfill()
	return nil
}

// edgeBetweenIndexReady reports whether the compatibility backfill completed.
func (b *BadgerEngine) edgeBetweenIndexReady() (bool, error) {
	var ready bool
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(edgeBetweenIndexReadyKey)
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

// hasAnyStoredEdges detects whether a startup rebuild has work to do.
func (b *BadgerEngine) hasAnyStoredEdges() (bool, error) {
	var hasEdges bool
	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerIterOptsKeyOnly([]byte{prefixEdge}))
		defer it.Close()
		it.Rewind()
		hasEdges = it.ValidForPrefix([]byte{prefixEdge})
		return nil
	})
	return hasEdges, err
}

// markEdgeBetweenIndexReady records that no compatibility rebuild remains.
func (b *BadgerEngine) markEdgeBetweenIndexReady() error {
	return b.withUpdate(func(txn *badger.Txn) error {
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
	})
}

// startEdgeBetweenIndexBackfill launches one cancellable background rebuild.
func (b *BadgerEngine) startEdgeBetweenIndexBackfill() {
	b.edgeBetweenIndexBackfillMu.Lock()
	if b.edgeBetweenIndexBackfillDone != nil {
		b.edgeBetweenIndexBackfillMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	b.edgeBetweenIndexBackfillCancel = cancel
	b.edgeBetweenIndexBackfillDone = done
	b.edgeBetweenIndexBackfillMu.Unlock()

	go func() {
		defer close(done)
		start := time.Now()
		// D-07: subsystem-tag once at goroutine entry; reuse for the entire
		// backfill lifecycle so attribute payload allocates once.
		idxLog := b.log.With("subsystem", "index_rebuild", "index", "edge_between")
		idxLog.Info("edge-between index backfill started")
		processed, err := b.rebuildEdgeBetweenIndex(ctx)
		switch {
		case err == nil:
			idxLog.Info("edge-between index backfill completed",
				"edges", processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		case errors.Is(err, context.Canceled), errors.Is(err, ErrStorageClosed):
			idxLog.Info("edge-between index backfill canceled",
				"edges", processed,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		default:
			idxLog.Error("edge-between index backfill failed",
				"edges", processed,
				"duration_ms", time.Since(start).Milliseconds(),
				slog.Any("error", err),
			)
		}
	}()
}

// stopEdgeBetweenIndexBackfill cancels and waits for a running rebuild.
func (b *BadgerEngine) stopEdgeBetweenIndexBackfill() {
	b.edgeBetweenIndexBackfillMu.Lock()
	cancel := b.edgeBetweenIndexBackfillCancel
	done := b.edgeBetweenIndexBackfillDone
	b.edgeBetweenIndexBackfillCancel = nil
	b.edgeBetweenIndexBackfillDone = nil
	b.edgeBetweenIndexBackfillMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// rebuildEdgeBetweenIndex scans stored edges and writes direct lookup entries.
func (b *BadgerEngine) rebuildEdgeBetweenIndex(ctx context.Context) (int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.db.DropPrefix([]byte{prefixEdgeBetweenIndex}); err != nil {
		return 0, fmt.Errorf("clear edge-between set index before rebuild: %w", err)
	}
	if err := b.db.DropPrefix([]byte{prefixEdgeBetweenHead}); err != nil {
		return 0, fmt.Errorf("clear edge-between head index before rebuild: %w", err)
	}

	// D-07 single-allocation: pre-bind subsystem attributes once for the
	// rebuild's progress emissions (every edgeBetweenIndexRebuildLogEvery
	// edges) so the steady-state path adds no .With(...) allocations.
	idxLog := b.log.With("subsystem", "index_rebuild", "index", "edge_between")
	processed := 0

	// Rebuild in txn-scoped chunks so each batch's dict allocations +
	// index writes commit together. Scan cursors across the edge prefix
	// advance between chunks.
	var cursor []byte
	for {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		done := false
		err := b.withUpdate(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerIterOptsPrefetchValues([]byte{prefixEdge}, 100))
			defer it.Close()
			start := cursor
			if len(start) == 0 {
				start = []byte{prefixEdge}
			}
			writes := 0
			for it.Seek(start); it.ValidForPrefix([]byte{prefixEdge}); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				var edgeID EdgeID
				if len(key) > 1 {
					edgeID = EdgeID(key[1:])
				}
				if err := item.Value(func(val []byte) error {
					edge, err := b.decodeEdgeBodyByID(val, edgeID)
					if err != nil {
						return fmt.Errorf("decode edge for edge-between index: %w", err)
					}
					if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
						return err
					}
					writes++
					processed++
					if processed%edgeBetweenIndexRebuildLogEvery == 0 {
						idxLog.Info("edge-between index backfill progress",
							"edges", processed,
						)
					}
					return nil
				}); err != nil {
					return err
				}
				if writes >= edgeBetweenIndexRebuildBatchSize {
					it.Next()
					if it.ValidForPrefix([]byte{prefixEdge}) {
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
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
	})
}
