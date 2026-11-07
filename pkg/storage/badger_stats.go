// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// Stats and Lifecycle
// ============================================================================

// NodeCount returns the total number of valid, decodable nodes.
// This is consistent with AllNodes() - only counts nodes that can be successfully decoded.
// Corrupted or incompatible node entries are not counted.
// initializeCounts scans the database once to initialize the cached node and edge counts.
// This is called only on engine startup to enable O(1) stats lookups.
func (b *BadgerEngine) initializeCounts() error {
	var nodeCount, edgeCount int64
	namespaceNodeCounts := make(map[string]int64)
	namespaceEdgeCounts := make(map[string]int64)

	// A loader: it reads past a restore's hold (viewHeld).
	err := b.db.viewHeld(func(txn *badger.Txn) error {
		// Count nodes
		nodePrefix := []byte{prefixNode}
		nodeIt := txn.NewIterator(badgerPrefixIteratorOptions(nodePrefix))
		defer nodeIt.Close()

		for nodeIt.Seek(nodePrefix); nodeIt.ValidForPrefix(nodePrefix); nodeIt.Next() {
			nodeCount++
			key := nodeIt.Item().Key()
			if len(key) <= 1 {
				continue
			}
			if ns, ok := namespacePrefixFromID(string(key[1:])); ok {
				namespaceNodeCounts[ns]++
			}
		}

		// Count edges
		edgePrefix := []byte{prefixEdge}
		edgeIt := txn.NewIterator(badgerPrefixIteratorOptions(edgePrefix))
		defer edgeIt.Close()

		for edgeIt.Seek(edgePrefix); edgeIt.ValidForPrefix(edgePrefix); edgeIt.Next() {
			edgeCount++
			key := edgeIt.Item().Key()
			if len(key) <= 1 {
				continue
			}
			if ns, ok := namespacePrefixFromID(string(key[1:])); ok {
				namespaceEdgeCounts[ns]++
			}
		}

		return nil
	})

	if err != nil {
		return err
	}

	// Initialize atomic counters
	b.nodeCount.Store(nodeCount)
	b.edgeCount.Store(edgeCount)
	b.namespaceCountsMu.Lock()
	b.namespaceNodeCounts = namespaceNodeCounts
	b.namespaceEdgeCounts = namespaceEdgeCounts
	b.namespaceCountsMu.Unlock()

	return nil
}

func (b *BadgerEngine) NodeCount(ctx context.Context) (int64, error) {
	// A counter read has no materialization step to cancel, but a canceled
	// request must not be answered with a successful count: it already owns no
	// result, and reporting one would let a canceled statement publish a total.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.pendingNamespaceConflict(""); err != nil {
		return 0, err
	}

	if err := b.totalCountWriteMu.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer b.totalCountWriteMu.RUnlock()
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}

	count := b.nodeCount.Load()
	// The atomic counter advances when the flusher lands a generation, not
	// at ACK: fold in the buffered delta so the count is read-your-writes
	// (PR #1043 read-overlay parity).
	if b.writeBehind != nil {
		delta, err := b.writeBehind.totalCountDeltaForPrefix(ctx, "", false)
		if err != nil {
			return 0, err
		}
		count += delta
	}
	return count, nil
}

// adjacencyCountRequest binds direction and ID without constructing an index
// key before lifecycle admission. It is used only by the public degree adapters.
type adjacencyCountRequest struct {
	nodeID   NodeID
	incoming bool
}

// countByPrefix counts records whose ID key starts with the given ID prefix.
// NodeCountByPrefix and EdgeCountByPrefix are thin wrappers over this kernel;
// the two differ only in the key-type byte and the namespace count cache.
// An explicitly supplied adjacency request admits and counts the raw index;
// it neither consults namespace caches nor changes
// the degree API convention of returning the count while discarding view errors
// (HARD_CONVERGENCE.md item 5: node/edge twin consolidation).
// Author: storage-format-owners lane (Codex), 1 November 2026.
func (b *BadgerEngine) countByPrefix(ctx context.Context, prefix string, keyType byte, cache map[string]int64, adjacency ...adjacencyCountRequest) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	readPrefix := prefix
	if len(adjacency) > 0 {
		readPrefix = string(adjacency[0].nodeID)
	}
	if err := b.pendingPrefixConflict(readPrefix); err != nil {
		return 0, err
	}

	if len(adjacency) == 0 {
		if err := b.ensureOpen(false); err != nil {
			return 0, err
		}

		b.namespaceCountsMu.RLock()
		if count, ok := cache[prefix]; ok {
			b.namespaceCountsMu.RUnlock()
			return count, nil
		}
		b.namespaceCountsMu.RUnlock()
	}
	var keyPrefix []byte
	if len(adjacency) != 0 {
		request := adjacency[0]
		if request.nodeID == "" {
			return 0, nil
		}
		if err := b.ensureOpen(false); err != nil {
			return 0, err
		}
		if request.incoming {
			keyPrefix = b.incomingIndexPrefixString(request.nodeID)
		} else {
			keyPrefix = b.outgoingIndexPrefixString(request.nodeID)
		}
		if keyPrefix == nil {
			return 0, nil
		}
	}
	var count int64
	err := b.withView(func(txn *badger.Txn) error {
		if len(adjacency) == 0 {
			keyPrefix = make([]byte, 0, 1+len(prefix))
			keyPrefix = append(keyPrefix, keyType)
			keyPrefix = append(keyPrefix, []byte(prefix)...)
		}

		it := txn.NewIterator(badgerPrefixIteratorOptions(keyPrefix))
		defer it.Close()
		var cancel bulkReadCancellation
		for it.Rewind(); it.Valid(); it.Next() {
			if err := cancel.check(ctx); err != nil {
				return err
			}
			count++
		}
		return nil
	})
	if len(adjacency) != 0 {
		return count, err
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}

// NodeCountByPrefix counts nodes whose NodeID begins with the provided prefix.
// The prefix refers to the NodeID string prefix (e.g., database namespace "nornic:").
//
// This is an optional fast-path used by NamespacedEngine to provide accurate
// per-database counts without decoding values.
func (b *BadgerEngine) NodeCountByPrefix(ctx context.Context, prefix string) (int64, error) {
	return b.entityCountByPrefix(ctx, prefix, prefixNode)
}

// EdgeCount returns the total number of valid, decodable edges.
// This is consistent with AllEdges() - only counts edges that can be successfully decoded.
func (b *BadgerEngine) EdgeCount(ctx context.Context) (int64, error) {
	// A counter read has no materialization step to cancel, but a canceled
	// request must not be answered with a successful count: it already owns no
	// result, and reporting one would let a canceled statement publish a total.
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := b.pendingNamespaceConflict(""); err != nil {
		return 0, err
	}

	if err := b.totalCountWriteMu.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer b.totalCountWriteMu.RUnlock()
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}

	count := b.edgeCount.Load()
	// The atomic counter advances when the flusher lands a generation, not
	// at ACK: fold in the buffered delta so the count is read-your-writes
	// (PR #1043 read-overlay parity).
	if b.writeBehind != nil {
		delta, err := b.writeBehind.totalCountDeltaForPrefix(ctx, "", true)
		if err != nil {
			return 0, err
		}
		count += delta
	}
	return count, nil
}

// EdgeCountByPrefix counts edges whose EdgeID begins with the provided prefix.
// The prefix refers to the EdgeID string prefix (e.g., database namespace "nornic:").
func (b *BadgerEngine) EdgeCountByPrefix(ctx context.Context, prefix string) (int64, error) {
	return b.entityCountByPrefix(ctx, prefix, prefixEdge)
}

// GetSchema returns the schema manager.
func (b *BadgerEngine) GetSchema() *SchemaManager {
	// Backwards-compat: callers without an explicit namespace get the default DB schema.
	// Most production callers should use NamespacedEngine which routes correctly.
	return b.GetSchemaForNamespace("nornic")
}

// Close closes the BadgerDB database. It is the engine owner's one stop: the
// first caller stops the producers and seals admission, and every caller,
// first or repeated, joins the same retirement and receives the same outcome.
func (b *BadgerEngine) Close() error {
	b.closeMu.Lock()
	defer b.closeMu.Unlock()
	if !b.work.Closing() {
		b.sealAdmission()
	}
	b.work.BeginStop(b.retire)
	return b.work.JoinStopped()
}

// sealAdmission stops the engine's producers while admission is still open
// (their final writes are admitted work), then seals admission once every
// durable write published. Outcomes go to the owner for every Close caller.
func (b *BadgerEngine) sealAdmission() {
	// Reject new writers first, then wait for every admitted one to finish,
	// so the final drain below meets every acknowledged commit: a listener
	// it delivers that writes again observes ErrStorageClosed instead of
	// being acknowledged into a generation nothing drains.
	b.mu.Lock()
	b.closing = true
	b.mu.Unlock()
	b.writeBarrier.Lock()
	b.writeBarrier.Unlock()
	// Stop the background backfills before taking the write barrier: they
	// write through withUpdate and must not be left waiting on a barrier
	// this goroutine holds while it waits for them to stop.
	b.stopIndexBackfill(startupEdgeBetweenIndex)
	b.stopIndexBackfill(startupLabelIndex)
	b.stopDeleteCleanup()
	b.stopChangeRetention()
	b.stopCountDeltaFoldWorker()

	// Drain acknowledged write-behind commits before tearing down Badger so
	// a shutdown never loses writes that were acknowledged to clients. The
	// drain keeps the engine's own work context without its cancellation:
	// Close seals admission before BeginStop cancels that context, and the
	// final drain must stay the uncancellable waiter that performs the drain
	// instead of depending on a background flusher that may already be gone.
	if b.writeBehind != nil {
		drainErr := b.FlushWriteBehind(context.WithoutCancel(b.work.Context()))
		if drainErr != nil && b.log != nil {
			b.log.Error("write-behind drain on close failed", "subsystem", "storage", "error", drainErr)
		}
		b.work.RecordError("", drainErr)
		b.writeBehind.Close()
		b.writeBehind.releaseTransactionBegins()
	}
	// The suppressions the deletes so far ended are deleted before the store
	// closes (forgetManualSuppression). The read hold is taken through the same
	// engine work context this phase already runs on, so a stopping owner ends
	// the pass instead of leaving it on an unowned root; the error stays visible
	// in the owner's outcomes and the ends are not deleted without the barrier.
	if err := b.writeBarrier.RLockContext(b.work.Context()); err != nil {
		b.work.RecordError("", fmt.Errorf("suppression ends before the write barrier: %w", err))
	} else {
		b.work.RecordError("", b.endSuppressions(&entityWriteContext{ctx: b.work.Context(), engine: b, owner: newEngineWriteLockOwner()}))
		b.writeBarrier.RUnlock()
	}
	// The MVCC lifecycle worker is stopped here for the same reason:
	// StopLifecycle waits for the worker to exit, and the worker must not
	// be parked behind a barrier this goroutine is about to hold. The call
	// Controller injection is part of the unpublished DB construction frame;
	// invoke its one stop outside the engine publication lock.
	b.mu.RLock()
	lifecycle := b.lifecycleController
	b.mu.RUnlock()
	if lifecycle != nil {
		lifecycle.StopLifecycle()
	}

	// Wait for every in-flight durable write (see writeBarrier) so no
	// commit that already reached Badger has its publication tail torn by
	// the release below. Seal before releasing the barrier; later helpers
	// and writers fail before reaching Badger, while listeners can re-enter.
	b.writeBarrier.Lock()
	b.mu.Lock()
	b.work.CloseAdmission()
	b.mu.Unlock()
	// Do not retain the barrier while listeners re-enter storage; the owner
	// keeps state until their notifications' tickets return.
	b.writeBarrier.Unlock()
	// Deliver what is ready but undelivered (a replay queues its events for
	// whoever retired it); each delivery returns its notification's ticket.
	b.drainCommitNotifications()
}

// retire is the owner's dispose: it runs once, after every admitted helper
// and reserved notification returned, so nothing runs against the state it
// releases (#726).
func (b *BadgerEngine) retire() error {
	b.mu.RLock()
	db := b.db
	b.mu.RUnlock()
	// Fold any derived-count deltas the last commits left behind so a clean
	// reopen has no open-time write to perform (PR #1043 item #2).
	countErr := b.foldRemainingCountDeltasOnClose(context.Background(), db)
	b.releasePendingEmbeddingCandidates()
	propertyErr := b.finishPropertyWALOnClose(db)

	b.mu.Lock()
	b.releaseClosedStateLocked()
	b.mu.Unlock()

	if db == nil {
		return errors.Join(countErr, propertyErr)
	}
	return errors.Join(countErr, propertyErr, db.Close())
}

// releaseClosedStateLocked drops large heap graphs that are not needed after
// Close. Test fixtures often keep executor and engine shells reachable until
// process exit; retaining the closed Badger handle behind those shells pins
// in-memory memtable arenas and caches long after the engine is unusable.
//
// b.mu must be held and the engine owner must have sealed admission.
func (b *BadgerEngine) releaseClosedStateLocked() {
	b.acceptedApplications = nil
	b.transitionPending(namespaceDropRelease, "", nil)
	b.db = nil
	b.nodeCache = nil
	b.queryNodeCache = nil
	b.nodeBodyCache = nil
	b.nodeBodyCacheLRU.Init()
	b.nodeBodyCacheBytes = 0
	b.edgeTypeCache = nil
	b.edgeCache = nil
	b.edgeCacheVersions = nil
	b.edgeCacheSources = nil
	b.durableCaches.mu.Lock()
	b.durableCaches.advanceFloorLocked()
	b.durableCaches.mu.Unlock()
	b.outgoingAdjCache = nil
	b.incomingAdjCache = nil
	b.labelFirstNodeCache = nil
	b.namespaceNodeCounts = nil
	b.namespaceEdgeCounts = nil
	b.mvccByNamespace = nil
	b.lifecycleController = nil
	b.callbackMu.Lock()
	b.onNodeCreated = nil
	b.onNodeUpdated = nil
	b.onNodeDeleted = nil
	b.onEdgeCreated = nil
	b.onEdgeUpdated = nil
	b.onEdgeDeleted = nil
	b.callbackMu.Unlock()
	b.storageMetrics = nil
	b.mvccMetrics = nil
}

// Sync forces a sync of all data to disk.
// This is useful for ensuring durability before a crash.
// A commit's settlement can call it after its worker left the lifecycle owner,
// so it takes its own short admission: Close joins a Sync in progress and
// refuses a later one, and it holds nothing a settlement that closes the engine
// would wait for. Author: NOR-288 lane (Claude), 2026-10-31.
func (b *BadgerEngine) Sync() error {
	if !b.work.Enter() {
		return ErrStorageClosed
	}
	defer b.work.Leave()
	if b.nativeSyncHook != nil {
		b.nativeSyncHook()
	}
	return b.db.Sync()
}

// RunGC runs garbage collection on the BadgerDB value log.
// Should be called periodically for long-running applications.
func (b *BadgerEngine) RunGC() error {
	if !b.work.Enter() {
		return ErrStorageClosed
	}
	defer b.work.Leave()
	return b.db.RunValueLogGC(0.5)
}

// Size returns the approximate size of the database in bytes.
func (b *BadgerEngine) Size() (lsm, vlog int64) {
	if !b.work.Enter() {
		return 0, 0
	}
	defer b.work.Leave()
	return b.db.Size()
}

// EstimateSize returns the approximate key and value bytes under prefix, or
// zero once Close sealed admission.
func (b *BadgerEngine) EstimateSize(prefix []byte) (keys, values uint64) {
	if !b.work.Enter() {
		return 0, 0
	}
	defer b.work.Leave()
	return b.db.EstimateSize(prefix)
}

// FindNodeNeedingEmbedding returns a node that needs embedding.
// The durable prefixPendingEmbed index is authoritative. A derived candidate
// heap avoids reopening/rewinding that range for every claim: Badger's rewind
// and read-ahead revisit deleted marker versions even with a prefix bound.
// After one lazy index load, a live candidate needs only point validation;
// stale candidates are removed once. Each returned node is validated in the
// engine write that cleans the markers before it; cleanup larger than one
// Badger batch commits batch by batch, so it always makes progress. Claim
// serialization remains with the worker. This exported optional-interface
// method carries the worker job context through catalog admission, iteration
// and cleanup preparation. An actual storage error is returned, while no
// eligible candidate is (nil, nil).
func (b *BadgerEngine) FindNodeNeedingEmbedding(ctx context.Context) (*Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.pendingNamespaceConflict(""); err != nil {
		return nil, err
	}

	if err := b.ensureOpen(false); err != nil {
		return nil, err
	}
	p := &b.pendingCandidates
	if err := p.mu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer p.mu.Unlock()
	// Hints can be lost: an empty catalog cannot prove an empty durable
	// index. Reconcile at the empty boundary; a previously verified empty
	// snapshot is reusable until storage publishes another commit. Live
	// claims keep their point-read path; worker scheduling owns idle backoff.
	reconciled := len(p.ids) == 0 || !p.loaded || p.generation != b.pendingCandidatesGeneration.Load()
	if err := p.load(ctx, b); err != nil {
		return nil, err
	}
	if len(p.ids) == 0 {
		return nil, nil
	}

	var found *Node
	removedStale := 0
	removedNoLongerNeeds := 0

	for len(p.ids) > 0 {
		// Cleanup deletions are committed one Badger batch at a time: a
		// prefix of stale markers larger than one batch must still make
		// durable progress toward the live candidates behind it.
		batchFull := false
		err := b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
			for len(p.ids) > 0 && !batchFull {
				if err := ctx.Err(); err != nil {
					return err
				}
				nodeID := p.ids[0]
				if _, err := txn.Get(pendingEmbedKey(nodeID)); err == badger.ErrKeyNotFound {
					p.removeFirst()
					continue
				} else if err != nil {
					return err
				}
				remove := func(counter *int) error {
					if err := txn.Delete(pendingEmbedKey(nodeID)); errors.Is(err, badger.ErrTxnTooBig) {
						// Commit the deletions that fit; this marker stays
						// first and is revalidated in the next batch.
						batchFull = true
						return nil
					} else if err != nil {
						return err
					}
					p.removeFirst()
					*counter++
					return nil
				}

				// Never embed the system database; remove any stale/system entries.
				if isSystemNamespaceID(string(nodeID)) {
					if err := remove(&removedNoLongerNeeds); err != nil {
						return err
					}
					continue
				}

				// Verify node exists in the same transaction for consistency.
				item, err := txn.Get(nodeKey(nodeID))
				if err == badger.ErrKeyNotFound {
					if err := remove(&removedStale); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}

				var node *Node
				if err := item.Value(func(val []byte) error {
					var decErr error
					node, decErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
					return decErr
				}); err != nil || node == nil {
					if err := remove(&removedStale); err != nil {
						return err
					}
					continue
				}

				// If node no longer needs embedding, remove it from the pending index.
				if !b.shouldIndexPendingEmbed(node) {
					if err := remove(&removedNoLongerNeeds); err != nil {
						return err
					}
					continue
				}

				found = node
				break
			}
			return nil
		})

		if err != nil {
			// A failed cleanup commit must not discard still-durable candidates.
			b.InvalidatePendingEmbeddingsIndex()
			return nil, err
		}
		if batchFull {
			continue
		}
		if found != nil || reconciled {
			break
		}
		// The last cached hints were stale. A committed marker whose hint was
		// lost may still exist, so check the durable index before returning nil.
		reconciled = true
		if err := p.load(ctx, b); err != nil {
			return nil, err
		}
	}
	// Log at most once per call (avoid per-entry spam on large stale indexes).
	if removedStale > 0 || removedNoLongerNeeds > 0 {
		b.log.Info("pending embeddings cleanup",
			"subsystem", "embeddings_index",
			"removed_stale", removedStale,
			"removed_no_longer_needed", removedNoLongerNeeds,
		)
	}

	return found, nil
}

// MarkNodeEmbedded removes a node from the pending embeddings index.
// Call this after successfully embedding a node.
func (b *BadgerEngine) MarkNodeEmbedded(nodeID NodeID) {
	_ = b.withPublishingUpdate(context.Background(), func(txn *badger.Txn) error {
		return txn.Delete(pendingEmbedKey(nodeID))
	}, nil, func(context.Context) ([]any, error) { return []any{namespaceWriteKey(namespaceForNodeID(nodeID))}, nil }, nil, nil)
}

// AddToPendingEmbeddings adds a node to the pending embeddings index.
// Call this when creating a node that needs embedding.
func (b *BadgerEngine) AddToPendingEmbeddings(nodeID NodeID) {
	_ = b.withPublishingUpdate(context.Background(), func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(nodeID))
		if err == nil {
			var node *Node
			if err = item.Value(func(value []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeProjected(txn, namespaceForNodeID(nodeID), value, nil)
				return decodeErr
			}); err != nil {
				return err
			}
			if !b.embeddingLabelsAllowed(node) {
				return nil
			}
		} else if err != badger.ErrKeyNotFound {
			return err
		}
		return b.setPendingEmbeddingInTxn(txn, nodeID)
	}, nil, func(context.Context) ([]any, error) { return []any{namespaceWriteKey(namespaceForNodeID(nodeID))}, nil }, nil, nil)
}

// PendingEmbeddingsCount returns the number of nodes waiting for embedding.
// Note: This requires a scan of the pending index, so use sparingly.
func (b *BadgerEngine) PendingEmbeddingsCount() int {
	return b.CountPendingEmbeddingIDs(b.work.Context(), nil)
}

// InvalidatePendingEmbeddingsIndex discards only the derived candidate catalog.
// The durable pending index is unchanged. The generation needs no queue lock,
// including during Restore while new read transactions are held off.
func (b *BadgerEngine) InvalidatePendingEmbeddingsIndex() {
	b.pendingCandidatesGeneration.Add(1)
}

// RefreshPendingEmbeddingsIndex rebuilds the pending embeddings index.
// This scans all nodes and adds any missing ones to the index.
// It also removes stale entries for nodes that no longer exist or already have embeddings.
// Use this on startup or after bulk imports. Its caller context is erased by
// the exported optional interface, so this entry passes its own explicit
// unbounded lifetime for the index rebuild's writes.
func (b *BadgerEngine) RefreshPendingEmbeddingsIndex() int {
	b.InvalidatePendingEmbeddingsIndex()
	added := 0
	removed := 0

	// First pass: Clean up stale entries in the pending index
	// Remove entries for nodes that don't exist or already have embeddings
	cleanupErr := b.withUpdate(context.Background(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		pendingPrefix := []byte{prefixPendingEmbed}
		it := txn.NewIterator(badgerPrefixIteratorOptions(pendingPrefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			// Extract nodeID from key (skip prefix byte)
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])

			// Never embed the system database; remove from pending index if present.
			if isSystemNamespaceID(string(nodeID)) {
				txn.Delete(key)
				removed++
				continue
			}

			// Check if node exists and still needs embedding
			// ALL node IDs must be prefixed - no unprefixed IDs allowed
			keyToCheck := nodeKey(nodeID)
			item, err := txn.Get(keyToCheck)

			if err == badger.ErrKeyNotFound {
				// Node doesn't exist - remove from pending index
				// This is a stale entry - log it for debugging.
				// Cap the per-call log volume so a corrupted/oversized
				// pending index does not flood the log pipeline.
				if removed < 10 {
					b.log.Debug("refresh pending embeddings: removing stale entry",
						"subsystem", "embeddings_index",
						"reason", "node_not_found",
						"node_id", string(nodeID),
					)
				}
				txn.Delete(key)
				removed++
				continue
			}
			if err != nil {
				// Error reading - mark as stale.
				if removed < 10 {
					b.log.Debug("refresh pending embeddings: removing corrupted entry",
						"subsystem", "embeddings_index",
						"reason", "read_error",
						"node_id", string(nodeID),
						slog.Any("error", err),
					)
				}
				txn.Delete(key)
				removed++
				continue
			}

			// Check if node already has embedding
			// nodeID is already extracted above from the pending embed key
			// item already contains the node data from above
			var node *Node
			if err := item.Value(func(val []byte) error {
				var decodeErr error
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
				return decodeErr
			}); err != nil {
				// Can't decode - remove stale entry
				txn.Delete(key)
				removed++
				continue
			}

			// Remove from index if node no longer qualifies for managed embedding.
			if !b.shouldIndexPendingEmbed(node) {
				txn.Delete(key)
				removed++
			}
		}
		return nil
	})
	if cleanupErr != nil {
		b.log.Warn("refresh pending embeddings: cleanup failed", slog.Any("error", cleanupErr))
	}

	// Second pass: Add missing nodes to the index
	_ = b.withUpdate(context.Background(), updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
		prefix := []byte{prefixNode}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// Extract nodeID from key (skip prefix byte)
			key := item.Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if isSystemNamespaceID(string(nodeID)) {
				continue
			}

			item.Value(func(val []byte) error {
				node, err := b.decodeNodeWithEmbeddings(txn, val, nodeID)
				if err != nil {
					return nil
				}

				// Skip internal nodes
				for _, label := range node.Labels {
					if len(label) > 0 && label[0] == '_' {
						return nil
					}
				}

				// Check if needs embedding and not already in index
				if b.shouldIndexPendingEmbed(node) {
					// Check if already in pending index
					_, err := txn.Get(pendingEmbedKey(node.ID))
					if err == badger.ErrKeyNotFound {
						b.setPendingEmbeddingInTxn(txn, node.ID)
						added++
					}
				}
				return nil
			})
		}
		return nil
	})

	// Always log if there were changes, or if we're cleaning up stale entries.
	if added > 0 || removed > 0 {
		b.log.Info("pending embeddings index refreshed",
			"subsystem", "embeddings_index",
			"added", added,
			"removed_stale", removed,
		)
	}
	// No changes case: previously a commented-out verbose-mode print; left
	// silent so steady-state refreshes do not contribute log volume.
	return added
}

// IterateNodes iterates through all nodes one at a time without loading all into memory.
// The callback returns true to continue, false to stop.
func (b *BadgerEngine) IterateNodes(fn func(*Node) bool) error {
	return b.StreamNodesWithOptions(context.Background(), StreamNodesOptions{WithEmbeddings: true}, func(n *Node) error {
		if !fn(n) {
			return ErrIterationStopped
		}
		return nil
	})
}

// StreamNodesOptions controls a streaming node scan. The five legacy stream
// variants (StreamNodes, StreamNodesWithoutEmbeddings, StreamNodesByPrefix,
// StreamNodesByPrefixProjected, StreamNodesByPrefixWithoutEmbeddings) are thin
// wrappers over StreamNodesWithOptions; consolidating their ~620 duplicated
// lines across four engines into one kernel per engine (HARD_CONVERGENCE.md
// item 4) means future scan fixes land in one place.
type StreamNodesOptions struct {
	// Label is an optional index hint. Callers still check label membership.
	Label string
	// QueryNodes settles embedding metadata without loading stored vectors,
	// as QueryNodeReader does. It applies to whole-node reads without
	// WithEmbeddings; resident vectors may be shared, never copied.
	QueryNodes bool
	// Prefix limits the scan to IDs starting with this string ("" scans all).
	Prefix string
	// Projection, when non-nil, decodes only these properties.
	Projection []string
	// WithEmbeddings loads separately stored chunk vectors.
	WithEmbeddings bool
	// ApplyDecayFilter drops nodes failing decay scoring, including label scans.
	ApplyDecayFilter bool
	// StripEmbeddings removes inline/legacy embedding payloads from results.
	StripEmbeddings bool
	// PropertyFilter, with a Projection, lets the engine skip a node whose
	// projected properties it rejects before decoding the rest of the node
	// (#824). It is a hint: an engine may ignore it, and pending writes are
	// not filtered, so the caller still tests every node it receives.
	PropertyFilter func(properties map[string]interface{}) bool
	// PropertyFilterProjection names the properties PropertyFilter reads, so an
	// engine can settle the filter before it decodes the rest of a body. It
	// applies to a scan without a Projection, whose row is the whole node: a
	// Projection already names what the filter reads. Like PropertyFilter it
	// is a hint an engine may ignore, and pending writes are not filtered,
	// so the caller still tests every node it receives.
	PropertyFilterProjection []string
	// PropertyStringEquals, for projected properties, lets the engine skip a
	// node whose stored value of a property is not that string by comparing
	// the stored bytes, without decoding the node (#857). A stored string
	// equals a Cypher string exactly when its bytes do, and a value of any
	// other type never equals a string. Like PropertyFilter it is a hint.
	PropertyStringEquals map[string]string
}

// fullNodeStreamOptions is the one read policy of a full node stream a
// consumer sees: StreamNodes, StreamNodeChunks and StreamNodesByPrefix, on the
// engine and on every wrapper. It reads complete bodies with their stored
// vectors and applies the visibility layer, as a visible read does. A stored
// read of every node, hidden ones included, sets its own options
// (StreamStoredNodes).
func fullNodeStreamOptions(prefix string) StreamNodesOptions {
	return StreamNodesOptions{Prefix: prefix, WithEmbeddings: true, ApplyDecayFilter: true}
}

// StreamNodesWithOptions iterates nodes one at a time without materializing
// them, honoring prefix scoping, property projection, embedding and decay
// options. Full scans use value prefetching; prefix scans use key-only seeks.
func (b *BadgerEngine) StreamNodesWithOptions(ctx context.Context, opts StreamNodesOptions, fn func(node *Node) error) error {
	if fn == nil {
		return ErrInvalidData
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	// A property filter does not disqualify the label route: the scoped label
	// owner decodes each candidate through the scan's own decoder and settles the
	// filter before it materializes the node (#1682), so this route keeps its
	// label index and skips another label's bodies as well.
	if opts.Projection == nil && opts.Label != "" && ((opts.QueryNodes && !opts.WithEmbeddings) || opts.scanDecodesFilter()) {
		visit := func(node *Node) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if opts.StripEmbeddings {
				node = copyNodeWithoutEmbeddings(node)
			}
			return fn(node)
		}
		err := b.streamNodesByLabelProjectedInScope(ctx, opts.Prefix, opts.Label, nil, opts.readVisibility(), visit, opts)
		if err == ErrIterationStopped {
			return nil
		}
		return err
	}
	// Write-behind overlay: acknowledged-but-unflushed nodes shadow
	// committed rows for their IDs and are visited after the scan.
	var overlay []*Node
	var touched map[NodeID]bool
	v := storedRead
	if opts.ApplyDecayFilter {
		v = visibleRead
	}
	var captureErr error
	capture := func(w *WriteBehindBuffer) {
		pendingNodes, err := b.readBufferedEntities(ctx, w, v, bufferedEntityRead{kind: bufferedNodes, prefix: opts.Prefix})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingNodes.nodes, pendingNodes.nodesTouched
	}
	inScope := func(id NodeID) bool {
		return opts.Prefix == "" || strings.HasPrefix(string(id), opts.Prefix)
	}
	// streamNodesWithOptionsInTxn already applies the projection, decay and
	// strip filters; this wrapper only shadows rows the overlay replaces.
	stopped := false
	emit := func(node *Node) error {
		if node == nil {
			return nil
		}
		if len(touched) > 0 && touched[node.ID] {
			return nil // the write-behind overlay shadows this committed row
		}
		err := fn(node)
		if err == ErrIterationStopped {
			stopped = true
		}
		return err
	}
	err := b.withReadView(ctx, capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan
		}
		return b.streamNodesWithOptionsInTxn(ctx, txn, opts, emit)
	}, readViewOptions{scope: opts.Prefix, scopeKind: readPrefixScope})
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	for _, node := range overlay {
		if !inScope(node.ID) {
			continue
		}
		projected := opts.copyResidentNode(node)
		if err := fn(projected); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// streamNodesWithOptionsInTxn is StreamNodesWithOptions over one read
// transaction: the engine's current view, or a transaction's pinned physical
// snapshot (BadgerTransaction.StreamNodesWithOptions, #824). ErrIterationStopped
// from fn ends the scan and is not returned.
func (b *BadgerEngine) streamNodesWithOptionsInTxn(ctx context.Context, txn *badger.Txn, opts StreamNodesOptions, fn func(node *Node) error) error {
	nowNanos := DecayScoringTime()
	// A projected or filtered scan decodes through one scan-scoped decoder
	// (#857). It resolves each stored property token through this read
	// snapshot, so the names it selects are the ones the bodies were written
	// with, and it applies the property filter before a node is materialized.
	var projected *projectedNodeDecoder
	if opts.scanDecodesFilter() || (!opts.WithEmbeddings && opts.Projection != nil) {
		projected = newProjectedNodeDecoder(b, txn, opts)
	}
	// A single prefix check in the same snapshot proves that no sidecar or
	// chunk exists for any node in this scan. Avoid one missing-key lookup per
	// node when a database (or scoped prefix) has no embedding records.
	embeddingRecordsAbsent := false
	if opts.WithEmbeddings && projected == nil {
		prefix := append([]byte{prefixEmbedding}, opts.Prefix...)
		iteratorOptions := badger.DefaultIteratorOptions
		iteratorOptions.Prefix = prefix
		iteratorOptions.PrefetchValues = false
		it := txn.NewIterator(iteratorOptions)
		it.Seek(prefix)
		embeddingRecordsAbsent = !it.ValidForPrefix(prefix)
		it.Close()
	}

	visit := func(txn *badger.Txn, item *badger.Item) error {
		node, err := b.readStreamNodeItem(ctx, txn, item, opts, projected, nowNanos, embeddingRecordsAbsent)
		if err != nil {
			// A record this scan cannot decode fails the scan: skipping it would
			// return the rest of the population as if it were all of it.
			return fmt.Errorf("scanning node %q: %w", NodeID(item.Key()[1:]), err)
		}
		if node == nil {
			return nil // the scan's decoder rejected this node
		}
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return ErrIterationStopped
			}
			return err
		}
		return nil
	}

	if opts.Prefix == "" {
		it := txn.NewIterator(badgerPrefixIteratorOptions([]byte{prefixNode}))
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := visit(txn, it.Item()); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	}
	seekPrefix := append([]byte{prefixNode}, []byte(opts.Prefix)...)
	it := txn.NewIterator(badgerPrefixIteratorOptions(seekPrefix))
	defer it.Close()
	for it.Seek(seekPrefix); it.ValidForPrefix(seekPrefix); it.Next() {
		if err := visit(txn, it.Item()); err != nil {
			if err == ErrIterationStopped {
				return nil // Normal stop
			}
			return err
		}
	}
	return nil
}

// readStreamNodeItem is the one native node hydration, visibility and projection
// policy. The streaming scan applies it to every committed row, and captured
// output materialization applies the same reader to the row it selects, so an
// emitted entity image is the one the native scan would emit. A nil node with no
// error is a row the decoder rejected; a decode failure is returned to the
// caller, which fails its read rather than publishing a partial population.
func (b *BadgerEngine) readStreamNodeItem(ctx context.Context, txn *badger.Txn, item *badger.Item, opts StreamNodesOptions, projected *projectedNodeDecoder, nowNanos int64, embeddingRecordsAbsent bool) (*Node, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var node *Node
	var err error
	if opts.QueryNodes && opts.Projection == nil && !opts.WithEmbeddings && !opts.scanDecodesFilter() {
		node, err = b.readNodeItemInTxn(txn, item, NodeID(item.Key()[1:]), nodeItemQueryMetadata)
	} else if opts.WithEmbeddings && projected == nil && len(item.Key()) > 1 {
		// Reuse the version-keyed body cache through the same snapshot owner
		// as point reads. Unlike the current-value cache, it never requires
		// replaying a user callback when concurrent writes advance the view.
		node, err = b.readNodeItemInTxnWithEmbeddingAbsence(txn, item, NodeID(item.Key()[1:]), nodeItemFullEmbeddings, embeddingRecordsAbsent)
	} else {
		err = item.Value(func(val []byte) error {
			key := item.Key()
			if len(key) <= 1 {
				return nil
			}
			if projected != nil {
				var decodeErr error
				node, decodeErr = projected.decode(key, val)
				return decodeErr
			}
			nodeID := NodeID(key[1:])
			var decodeErr error
			if opts.WithEmbeddings {
				node, decodeErr = b.decodeNodeWithEmbeddings(txn, val, nodeID)
			} else {
				node, decodeErr = b.decodeNodeProjected(txn, namespaceForNodeID(nodeID), val, nil)
			}
			return decodeErr
		})
	}
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}
	if opts.ApplyDecayFilter && b.filterEntityByDecay(visibleRead, node, nowNanos) {
		return nil, nil
	}
	if opts.StripEmbeddings {
		node = copyNodeWithoutEmbeddings(node)
	}
	return node, nil
}

// StreamNodes implements StreamingEngine.StreamNodes for memory-efficient iteration.
// Iterates through all nodes one at a time without loading all into memory.
func (b *BadgerEngine) StreamNodes(ctx context.Context, fn func(node *Node) error) error {
	return b.StreamNodesWithOptions(ctx, fullNodeStreamOptions(""), fn)
}

// StreamNodesWithoutEmbeddings iterates complete node records without reading
// the separate vector keyspace. User properties are retained for snapshots.
func (b *BadgerEngine) StreamNodesWithoutEmbeddings(ctx context.Context, fn func(node *Node) error) error {
	return b.StreamNodesWithOptions(ctx, StreamNodesOptions{}, fn)
}

// StreamNodesByPrefix streams nodes whose IDs start with prefix.
// This is significantly faster than full StreamNodes + callback filtering when
// tenants/databases share a physical store.
func (b *BadgerEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return b.StreamNodesWithOptions(ctx, fullNodeStreamOptions(prefix), fn)
}

// StreamNodesByPrefixProjected streams nodes whose IDs start with prefix while
// decoding only the requested user properties. It preserves StreamNodesByPrefix
// semantics: nil properties reads full nodes, and ErrIterationStopped ends the
// scan successfully.
func (b *BadgerEngine) StreamNodesByPrefixProjected(ctx context.Context, prefix string, properties []string, fn func(node *Node) error) error {
	if properties == nil {
		return b.StreamNodesByPrefix(ctx, prefix, fn)
	}
	return b.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: properties}, fn)
}

// StreamNodesByPrefixWithoutEmbeddings streams node metadata without loading
// separate or legacy inline vector payloads.
func (b *BadgerEngine) StreamNodesByPrefixWithoutEmbeddings(ctx context.Context, prefix string, fn func(node *Node) error) error {
	return b.StreamNodesWithOptions(ctx, StreamNodesOptions{Prefix: prefix, Projection: []string{}, StripEmbeddings: true}, fn)
}

// StreamEdges implements StreamingEngine.StreamEdges for memory-efficient iteration.
// Iterates through all edges one at a time without loading all into memory.
func (b *BadgerEngine) StreamEdges(ctx context.Context, fn func(edge *Edge) error) error {
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	// Write-behind overlay: acknowledged-but-unflushed edges shadow
	// committed rows for their IDs and are visited after the scan.
	var overlay []*Edge
	var touched map[EdgeID]bool
	var captureErr error
	capture := func(w *WriteBehindBuffer) {
		pendingEdges, err := b.readBufferedEntities(ctx, w, visibleRead, bufferedEntityRead{kind: bufferedEdges})
		if err != nil {
			captureErr = err
			return
		}
		overlay, touched = pendingEdges.edges, pendingEdges.edgesTouched
	}
	nowNanos := DecayScoringTime()
	stopped := false
	emit := func(edge *Edge) error {
		if edge == nil {
			return nil
		}
		if len(touched) > 0 && touched[edge.ID] {
			return nil // the write-behind overlay shadows this committed row
		}
		if b.filterEntityByDecay(visibleRead, edge, nowNanos) {
			return nil
		}
		err := fn(edge)
		if err == ErrIterationStopped {
			stopped = true
		}
		return err
	}
	err := b.withReadView(ctx, capture, func(txn *badger.Txn) error {
		if captureErr != nil {
			return captureErr // the canceled capture retires the scan
		}
		prefix := []byte{prefixEdge}
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			edge, err := b.readStreamEdgeItem(txn, it.Item())
			if err != nil {
				continue // Skip invalid edges
			}
			if err := emit(edge); err != nil {
				if err == ErrIterationStopped {
					return nil // Normal stop
				}
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if stopped {
		return nil
	}
	for _, edge := range overlay {
		if err := fn(copyEdge(edge)); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return nil
}

// StreamNodeChunks implements StreamingEngine.StreamNodeChunks for batch
// processing. It batches the node stream, so a chunk holds exactly the nodes
// StreamNodes publishes: the same bodies, vectors and visibility.
func (b *BadgerEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func(nodes []*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	chunk := make([]*Node, 0, chunkSize)
	stopped := false
	err := b.StreamNodes(ctx, func(node *Node) error {
		chunk = append(chunk, node)
		if len(chunk) < chunkSize {
			return nil
		}
		err := fn(chunk)
		chunk = chunk[:0]
		stopped = err == ErrIterationStopped
		return err
	})
	if err != nil || stopped {
		return err
	}
	if len(chunk) > 0 {
		if err := fn(chunk); err != ErrIterationStopped {
			return err
		}
	}
	return nil
}

// ============================================================================
// Utility functions for compatibility
// ============================================================================

// HasPrefix checks if a byte slice has the given prefix.
func hasPrefix(s, prefix []byte) bool {
	return len(s) >= len(prefix) && bytes.Equal(s[:len(prefix)], prefix)
}

// ClearAllEmbeddings removes embeddings from all nodes, allowing them to be regenerated.
// Returns the number of nodes that had their embeddings cleared.
func (b *BadgerEngine) ClearAllEmbeddings() (int, error) {
	return b.ClearAllEmbeddingsForPrefix("")
}

// ClearAllEmbeddingsForPrefix removes embeddings from nodes whose IDs start with the given prefix.
// This is used to clear embeddings for a single logical database namespace (e.g., "nornic:").
//
// If idPrefix is empty, clears embeddings for all nodes.
func (b *BadgerEngine) ClearAllEmbeddingsForPrefix(idPrefix string) (int, error) {
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}

	cleared := 0

	// First, collect all node IDs that have embeddings
	var nodeIDs []NodeID
	err := b.withView(func(txn *badger.Txn) error {
		keyPrefix := []byte{prefixNode}
		it := txn.NewIterator(badgerPrefixIteratorOptions(keyPrefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			// Extract nodeID from key (skip prefix byte)
			key := item.Key()
			if len(key) <= 1 {
				continue
			}
			nodeID := NodeID(key[1:])
			if idPrefix != "" && !strings.HasPrefix(string(nodeID), idPrefix) {
				continue
			}

			err := item.Value(func(val []byte) error {
				node, err := b.decodeNodeWithEmbeddings(txn, val, nodeID)
				if err != nil {
					return nil // Skip invalid nodes
				}
				if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
					nodeIDs = append(nodeIDs, node.ID)
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("error scanning nodes: %w", err)
	}

	// Now update each node to clear its embedding, hidden ones included: they
	// must not keep an embedding of the model being replaced.
	for _, id := range nodeIDs {
		node, err := b.GetStoredNode(context.Background(), id)
		if err != nil {
			continue // Skip if node no longer exists
		}
		before := *node
		node.ChunkEmbeddings = nil
		if err := b.UpdateNode(context.Background(), NodeUpdateRequest{Before: &before, After: node, Policy: StoredNodeImages}); err != nil {
			b.log.Warn("failed to clear embedding for node",
				"subsystem", "embeddings_index",
				"node_id", string(id),
				slog.Any("error", err),
			)
			continue
		}
		cleared++
	}

	b.log.Info("cleared embeddings",
		"subsystem", "embeddings_index",
		"cleared", cleared,
	)

	// Refresh the pending embeddings index so nodes get re-processed
	added := b.RefreshPendingEmbeddingsIndex()
	if added > 0 {
		b.log.Info("added nodes to pending embeddings queue",
			"subsystem", "embeddings_index",
			"added", added,
		)
	}

	return cleared, nil
}

// entityCountByPrefix keeps namespace-cache and committed-buffer visibility in
// one owner; total-count synchronization precedes lifecycle admission as before.
func (b *BadgerEngine) entityCountByPrefix(ctx context.Context, prefix string, keyType byte) (int64, error) {
	if err := b.totalCountWriteMu.RLockContext(ctx); err != nil {
		return 0, err
	}
	defer b.totalCountWriteMu.RUnlock()
	if err := b.ensureOpen(false); err != nil {
		return 0, err
	}
	cache := b.namespaceNodeCounts
	if keyType == prefixEdge {
		cache = b.namespaceEdgeCounts
	}
	count, err := b.countByPrefix(ctx, prefix, keyType, cache)
	if err != nil {
		return 0, err
	}
	if b.writeBehind != nil {
		delta, err := b.writeBehind.totalCountDeltaForPrefix(ctx, prefix, keyType == prefixEdge)
		if err != nil {
			return 0, err
		}
		count += delta
	}
	return count, nil
}

// readStreamEdgeItem is the one native relationship hydration policy: the
// relationship stream and captured output materialization both read a committed
// row through it. Renderable rows come back whole; a row this reader cannot
// decode is absent from both populations, as the relationship stream omits it
// today, so a captured image never differs from the streamed one.
func (b *BadgerEngine) readStreamEdgeItem(txn *badger.Txn, item *badger.Item) (*Edge, error) {
	key := item.Key()
	var edgeID EdgeID
	if len(key) > 1 {
		edgeID = EdgeID(key[1:])
	}
	var edge *Edge
	err := item.Value(func(value []byte) error {
		var decodeErr error
		edge, decodeErr = b.decodeEdgeBodyWithID(txn, namespaceForEdgeID(edgeID), value, edgeID)
		return decodeErr
	})
	if err != nil {
		return nil, nil // Skip invalid edges, as the release stream does.
	}
	return edge, nil
}
