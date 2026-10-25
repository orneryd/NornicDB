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

// indexBackfillReady reads the selected family marker. The label index's
// marker belongs to its lifecycle owner (labelIndexReady); relationship
// backfill retains key-presence semantics.
func (b *BadgerEngine) indexBackfillReady(kind startupIndexKind) (bool, error) {
	if kind == startupLabelIndex {
		return b.labelIndexReady()
	}
	var ready bool
	err := b.withView(func(txn *badger.Txn) error {
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

// markIndexBackfillReady records that an empty store needs no backfill. The
// label index publishes through its lifecycle owner.
func (b *BadgerEngine) markIndexBackfillReady(kind startupIndexKind) error {
	if kind == startupLabelIndex {
		return b.markEmptyLabelIndexReady()
	}
	return b.withUpdate( func(txn *badger.Txn) error {
		return txn.Set(edgeBetweenIndexReadyKey, []byte{1})
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
// Admission pins engine state through the probes and, if one is registered,
// the worker. Registration shares Close's shutdown mutex, so Close either
// rejects the start or sees its task before stopping the backfills.
func (b *BadgerEngine) startIndexBackfill(kind startupIndexKind, mode indexBackfillStartMode) error {
	if _, err := b.beginHelperTxnWithAdmission(true); err != nil {
		return err
	}
	workerOwnsLifetime := false
	defer func() {
		if !workerOwnsLifetime {
			b.txnWG.Done()
		}
	}()
	maintenancePause(pauseIndexBackfillStartCheck)
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
		if err := b.withView(func(txn *badger.Txn) error {
			it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
			defer it.Close()
			it.Rewind()
			hasStored = it.ValidForPrefix(prefix)
			return nil
		}); err != nil {
			return err
		}
		if !hasStored {
			return b.markIndexBackfillReady(kind)
		}
	}

	mu := &b.labelIndexBackfillMu
	cancelSlot, doneSlot := &b.labelIndexBackfillCancel, &b.labelIndexBackfillDone
	indexName, message, entity := "label", "label index backfill", "nodes"
	rebuild := b.rebuildLabelIndex
	if kind == startupEdgeBetweenIndex {
		mu = &b.edgeBetweenIndexBackfillMu
		cancelSlot, doneSlot = &b.edgeBetweenIndexBackfillCancel, &b.edgeBetweenIndexBackfillDone
		indexName, message, entity = "edge_between", "edge-between index backfill", "edges"
		rebuild = func(ctx context.Context) (int, error) {
			return b.rebuildRelationshipIndexes(ctx, rebuildEdgeBetweenOnly)
		}
	}
	// A probe can run across Close's stop calls. Refuse registration then;
	// the helper lease still protects every handle access the probe made.
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed || b.closing {
		return ErrStorageClosed
	}
	mu.Lock()
	if *doneSlot != nil {
		mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	*cancelSlot = cancel
	*doneSlot = done
	mu.Unlock()

	workerOwnsLifetime = true
	go func() {
		defer b.txnWG.Done()
		defer close(done)
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
	}()
	return nil
}

// resumeIndexBackfill recovers a stopped family through the same lifetime
// admission as startup. A closing or closed engine has nothing to resume.
// Admission takes no barrier, so DROP can also call it inside its barrier.
func (b *BadgerEngine) resumeIndexBackfill(kind startupIndexKind) error {
	err := b.startIndexBackfill(kind, initializeIndexBackfill)
	if errors.Is(err, ErrStorageClosed) {
		return nil
	}
	return err
}

// stopIndexBackfill detaches, cancels and waits for the selected startup rebuild.
func (b *BadgerEngine) stopIndexBackfill(kind startupIndexKind) {
	mu := &b.labelIndexBackfillMu
	cancelSlot, doneSlot := &b.labelIndexBackfillCancel, &b.labelIndexBackfillDone
	if kind == startupEdgeBetweenIndex {
		mu = &b.edgeBetweenIndexBackfillMu
		cancelSlot, doneSlot = &b.edgeBetweenIndexBackfillCancel, &b.edgeBetweenIndexBackfillDone
	}
	mu.Lock()
	cancel := *cancelSlot
	done := *doneSlot
	*cancelSlot = nil
	*doneSlot = nil
	mu.Unlock()

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
	if _, err := b.beginHelperTxnWithAdmission(true); err != nil {
		return 0, fmt.Errorf("clear label index before rebuild: %w", err)
	}
	defer b.txnWG.Done()
	maintenancePause(pauseIndexBackfillStartCheck)
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
		_, _, ok := ParseDatabasePrefix(string(nodeID))
		if !ok {
			// Nodes without a namespace prefix predate multi-DB support.
			// Skip — they can't participate in the per-namespace dict
			// either.
			return 0, false, nil
		}
		node, err := decodeNodeMetadata(val)
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
		err := b.withUpdate( func(txn *badger.Txn) error {
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
