package storage

// Write-behind integration for BadgerEngine.
//
// When BadgerOptions.WriteBehind is set, implicit (autocommit) transaction
// commits are captured into the engine's rotating commit buffer after every
// commit-time validation succeeds, and acknowledged immediately. A background
// flusher replays drained generations into Badger inside one real Badger
// transaction — the amortization that recovers ingest throughput. Explicit
// transactions and snapshot opens drain the buffer first.

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
)

// bufferedMemoryAdmissionError keeps the shared application-pressure status
// through storage, Cypher, Bolt and HTTP while retaining the limiting cause.
type bufferedMemoryAdmissionError struct{ cause error }

func (e *bufferedMemoryAdmissionError) Error() string { return e.StatusMessage() }
func (e *bufferedMemoryAdmissionError) Unwrap() error { return e.cause }
func (e *bufferedMemoryAdmissionError) BoltErrorCode() string {
	return "Neo.TransientError.General.MemoryPoolOutOfMemoryError"
}
func (e *bufferedMemoryAdmissionError) StatusMessage() string {
	if errors.Is(e.cause, memory.ErrStaleSample) {
		return localization.ApplicationMemoryUnavailable().Fallback
	}
	return localization.ApplicationMemoryPressure().Fallback
}

func bufferedMemoryError(cause error) error {
	message := localization.ApplicationMemoryPressure()
	if cause == memory.ErrStaleSample {
		message = localization.ApplicationMemoryUnavailable()
	}
	return &bufferedMemoryAdmissionError{cause: localization.NewLocalizedError(
		"Neo.TransientError.General.MemoryPoolOutOfMemoryError", message, cause)}
}

// estimateBufferedCommitBytes forecasts payload plus read-overlay growth
// before the replayable WAL boundary. The physical cgroup sample remains the
// final pressure check; this deliberately overestimates shared references.
func estimateBufferedCommitBytes(c *bufferedCommit) int64 {
	bytes := int64(256 + len(c.namespace) + len(c.txID) + 256*len(c.ops))
	for _, op := range c.ops {
		bytes += int64(len(op.NodeID) + len(op.EdgeID) + 16*len(op.DeletedEdgeIDs))
		for _, node := range [2]*Node{op.Node, op.OldNode} {
			bytes += estimateRetainedNodeBytes(node)
		}
		for _, edge := range [2]*Edge{op.Edge, op.OldEdge} {
			bytes += estimateRetainedEdgeBytes(edge)
		}
	}
	for key, value := range c.writes {
		bytes += int64(64 + len(key) + len(value))
	}
	for key := range c.deletes {
		bytes += int64(48 + len(key))
	}
	if c.idEntries != nil {
		bytes += 64 + int64(16*(len(c.idEntries.nodes)+len(c.idEntries.edges)))
		for _, id := range c.idEntries.nodes {
			bytes += int64(len(id))
		}
		for _, id := range c.idEntries.edges {
			bytes += int64(len(id))
		}
	}
	for namespace := range c.propKeyDrain.counters {
		bytes += int64(32 + len(namespace))
	}
	for _, entry := range c.propKeyDrain.pending {
		bytes += int64(48 + len(entry.namespace) + len(entry.name))
	}
	bytes += int64(64*len(c.labelDeltas) + 64*len(c.edgeTypeDeltas) + 80*len(c.edgeTypeLabelDeltas))
	return bytes
}

// SetBufferedCommitWAL installs the WAL that receives the durable payload of
// every acknowledged write-behind commit. It is called once, at construction,
// by NewWALEngine before the engine serves traffic.
func (b *BadgerEngine) SetBufferedCommitWAL(w *WAL) {
	b.bufferedCommitWAL = w
}

// bufferedFlushKey is the Badger key that marks one write-behind commit as
// having reached Badger. It is written inside the same transaction that
// applies the generation's data, so the marker is durable exactly when the
// data is — replay at open skips any commit whose marker exists.
func bufferedFlushKey(txID string) []byte {
	key := make([]byte, 0, 1+len(txID))
	key = append(key, prefixBufferedFlush)
	key = append(key, txID...)
	return key
}

// bufferedFlushMarkerExists reports whether a buffered commit's data has
// already been applied to Badger (marker present).
func (b *BadgerEngine) bufferedFlushMarkerExists(txID string) (bool, error) {
	if txID == "" {
		return false, nil
	}
	key := bufferedFlushKey(txID)
	exists := false
	err := b.withView(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		exists = true
		return nil
	})
	return exists, err
}

// commitBufferedLocked captures the transaction's validated, materialized
// commit state into the engine's write-behind buffer and closes the
// transaction as committed without writing to Badger. The caller has already
// run every validation phase (constraints, snapshot isolation, connected
// deletes) and holds the engine write barrier.
func (tx *BadgerTransaction) commitBufferedLocked() error {
	// The commit is acknowledged under its entities' write-order stripes, so
	// a write of the same entities that commits to Badger itself
	// (orderAfterBufferedCommits) falls wholly before or after it. They are
	// taken first, as such a write takes them before any commit lock.
	// A node it deletes is held exclusively, so no commit writing a
	// relationship of that node is acknowledged alongside it.
	var stripes, deletions orderStripeSet
	stripes.operations(tx.operations)
	deletions.deletions(tx.operations)
	tx.engine.writeBehind.order.lockMixed(&stripes, &deletions)
	defer tx.engine.writeBehind.order.unlockMixed(&stripes, &deletions)
	if err := tx.revalidateEndpointsLocked(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}
	physicalOps := tx.physicalOperationsLocked()
	if len(physicalOps) == 0 && len(tx.pendingWrites) == 0 && len(tx.pendingDeletes) == 0 {
		tx.releaseSnapshotReaderLocked()
		tx.closeLocked(TxStatusCommitted, true, nil)
		return nil
	}
	if tx.namespace == "" {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return localizedError(localization.StorageTransactionCommitNamespaceMissing(), nil)
	}

	// Ownership transfer, not copies: this is an implicit autocommit, the
	// transaction closes right after the ACK and closeLocked replaces the
	// staged maps with fresh ones, so nothing else can touch these again.
	// The tx keeps its slice header (OperationCount reads it post-commit);
	// the backing arrays are exclusively consumed by the buffer from here.
	c := bufferedCommit{
		namespace: tx.namespace,
		txID:      tx.ID,
		ops:       physicalOps,
		writes:    tx.pendingWrites,
		deletes:   tx.pendingDeletes,
	}
	// Allocate the commit's MVCC version NOW, at ACK time: the DB-local
	// sequence counter pins the commit's place in history, so the flusher
	// materializes heads that are already ordered against every later
	// transaction's snapshot — no backdating or flush-time ordering games.
	if version, err := tx.engine.allocateMVCCVersion(tx.badgerTx, tx.namespace, time.Now()); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	} else {
		c.version = version
	}
	c.counterNodeMax, c.counterEdgeMax = tx.engine.idDict.flushTxnCounters(tx.badgerTx)
	c.idEntries = tx.engine.idDict.takeTxnCommitting(tx.badgerTx)
	// The dictionary no longer owns these entries. A rejected pre-WAL commit
	// must clear their pending marks, which otherwise retain the whole state.
	transferred := false
	defer func() {
		if transferred {
			return
		}
		tx.engine.idDict.clearPending(c.idEntries)
		if c.idEntries != nil {
			clear(c.idEntries.nodes)
			clear(c.idEntries.edges)
			c.idEntries.nodes = nil
			c.idEntries.edges = nil
		}
		if c.memoryLease != nil {
			c.memoryLease.Release()
		}
	}()
	c.hasCounters = true
	c.propKeyDrain = tx.engine.propKeyDict.flushTxnCounters(tx.badgerTx)
	// Capture the derived-count deltas the sync path applies after commit;
	// replay merges and applies them under the count locks. Ownership
	// transfer, not copies: closeLocked nils these exact maps on the
	// buffered path (bufferedStagingTransferred), so the transaction can
	// never mutate them after the ACK, and the replay only reads them.
	c.labelDeltas = tx.pendingLabelCountDeltas
	c.edgeTypeDeltas = tx.pendingEdgeTypeCountDeltas
	c.edgeTypeLabelDeltas = tx.pendingEdgeTypeLabelCountDeltas
	// The overlay serves these bodies to every reader until the flush lands
	// them, so they enter it as the image storage returns (as the synchronous
	// commit publishes them).
	if err := tx.engine.storeWrittenBodies(c.writes, c.ops); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}

	forecast := estimateBufferedCommitBytes(&c)
	if forecast > maxBufferedWALPayloadBytes {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("wal: buffered commit exceeds decoded payload limit: %w", badger.ErrTxnTooBig)
	}
	if policy := tx.engine.memoryPolicy; policy != nil {
		lease, err := policy.Reserve(forecast)
		if err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return bufferedMemoryError(err)
		}
		c.memoryLease = lease
	}
	// Durability boundary: write the commit's full payload to the WAL and
	// push it to the kernel page cache BEFORE acknowledging. If the process
	// is killed before the background flusher lands the generation, the WAL
	// still holds every acknowledged write and RecoverBufferedCommits
	// replays it at the next open. write(2), not fsync: this closes the
	// kill -9 loss window without reintroducing the per-commit fsync the
	// buffer exists to avoid; power-loss durability remains the durable
	// synchronous path's contract.
	if tx.engine.bufferedCommitWAL != nil {
		if err := tx.engine.bufferedCommitWAL.AppendBufferedCommit(&c); err != nil {
			// The ACK must never outrun the durable record: failing to log
			// the payload fails the commit.
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return err
		}
	}

	tx.releaseSnapshotReaderLocked()
	tx.bufferedStagingTransferred = true
	tx.engine.writeBehind.AppendCommit(c)
	transferred = true
	// The write is committed and visible to every later read the moment it is
	// buffered, so the query-result cache's revision token must advance NOW,
	// not at flush. The sync path advances it per-op through the notify*
	// callbacks in its commit tail; the buffered path defers those callbacks
	// to the flusher, so without this a cached read that shares no label with
	// the write (e.g. a whole-type relationship count) stays stale until the
	// generation flushes (PR #1043 retest @39f0f03f, issue A).
	tx.engine.graphMutationVersions.changed(tx.namespace)
	tx.bufferedAck.Store(true)
	tx.closeLocked(TxStatusCommitted, true, nil)
	return nil
}

// persistReplayTxnCounters makes one generation's property-key tokens
// durable through the generation's commit writer: through the large commit
// once it has turned large (the flusher holds the commit gate exclusively
// and must not re-enter it), and through ordinary commits otherwise,
// mirroring the sync commit path's finish.
func (b *BadgerEngine) persistReplayTxnCounters(cw *commitWriter, drain propKeyTxnDrain) error {
	if drain.empty() {
		return nil
	}
	var to batchTarget = b.db
	if cw.large != nil {
		to = cw.large
	}
	w := &batchWriter{to: to}
	defer w.discard()
	err := drain.writeTo(w)
	if err == nil {
		err = w.flush()
	}
	if err != nil {
		return err
	}
	b.propKeyDict.markPersisted(drain)
	return nil
}

// replayWriteBehind replays one drained generation into Badger. It marks
// the generation applied on success so the buffer retires it.
func (b *BadgerEngine) replayWriteBehind(buf *CommitBuffer) error {
	commits := buf.Commits()
	if len(commits) == 0 {
		return nil
	}
	if err := b.applyBufferedCommitsWithCountPublication(commits, func() { buf.countsApplied.Store(true) }); err != nil {
		return err
	}
	buf.markApplied()
	return nil
}

// applyBufferedCommits replays buffered commits into Badger: a single Badger
// transaction applies every buffered statement's physical operations and raw
// KV writes, then the commit tail runs (MVCC sequence persistence, ID
// counters, derived counts, caches, callbacks). Before the transaction
// commits it also writes each commit's flush marker key, so the marker is
// durable exactly when the data is — recovery at open skips any commit whose
// marker exists.
func (b *BadgerEngine) applyBufferedCommits(commits []bufferedCommit) error {
	defer b.drainCommitNotifications()
	return b.applyBufferedCommitsWithCountPublication(commits, nil)
}

// publishCounts retires the generation count overlay under the same locks that
// publish persisted counters. Entity/cache callbacks run afterward without locks.
func (b *BadgerEngine) applyBufferedCommitsWithCountPublication(commits []bufferedCommit, publishCounts func()) error {
	if len(commits) == 0 {
		return nil
	}

	txn := b.db.DB.NewTransactionAt(b.db.oracle.published.Load(), true)
	cw := b.newCommitWriter(b.db, txn)
	// Write through the commit writer's own batch writer, exactly like the
	// sync commit path: cw.finish() commits cw.batchWriter's open batch, so
	// a separate batchWriter whose txn never follows the batch rotation
	// would make finish write the intent delete through an already-committed
	// (discarded) transaction.
	w := &cw.batchWriter

	versions := make(map[string]MVCCVersion, 1)
	var maxNode, maxEdge uint64
	anyCounters := false
	labelDeltas := make(map[namespaceLabel]int64)
	edgeTypeDeltas := make(map[namespaceEdgeType]int64)
	edgeTypeLabelDeltas := make(map[edgeTypeLabelDelta]int64)
	abort := func(err error) error {
		// Mirror the sync commit's abort: if the generation's writes turned
		// the commit large, beginLargeCommit holds the commit gate
		// exclusively and lc.abort rolls the batches back and releases it.
		// Leaking the gate here would park every later commit (shared and
		// exclusive) forever. The open batch belongs to the writer and is
		// discarded without committing.
		// Entries the replay resolved stay pending (discardTxnCounters).
		b.idDict.discardTxnCounters(cw.txn)
		if abortErr := cw.abort(); abortErr != nil {
			err = fmt.Errorf("%w (rolling back: %v)", err, abortErr)
		}
		cw.discard()
		return err
	}

	for commitIndex := range commits {
		c := &commits[commitIndex]
		// The commit's MVCC version was allocated at ACK time; the flusher
		// only materializes it, so head order matches commit order.
		v := c.version
		if v.IsZero() {
			var err error
			v, err = b.allocateMVCCVersion(txn, c.namespace, time.Now())
			if err != nil {
				return abort(err)
			}
		}
		versions[c.namespace] = v
		if c.hasCounters {
			anyCounters = true
			if c.counterNodeMax > maxNode {
				maxNode = c.counterNodeMax
			}
			if c.counterEdgeMax > maxEdge {
				maxEdge = c.counterEdgeMax
			}
		}
		// Property-key tokens persist out-of-transaction, exactly as the
		// normal commit path does (they never enter the user txn). When
		// the generation has turned into a large commit the flusher holds
		// the commit gate exclusively, so the token batches must ride the
		// large commit: a db-targeted writer would re-enter the gate's
		// RLock and deadlock against this goroutine's own Lock.
		if err := b.persistReplayTxnCounters(cw, c.propKeyDrain); err != nil {
			return abort(err)
		}
		if c.recovered {
			if err := b.restoreBufferedOperationBodies(cw.txn, c); err != nil {
				return abort(err)
			}
		}
		if err := b.materializeMVCCCommitWithHeads(w, versions[c.namespace], c.ops, true); err != nil {
			return abort(err)
		}
		for keyStr := range c.deletes {
			key := []byte(keyStr)
			if err := w.write(func(t *badger.Txn) error { return t.Delete(key) }); err != nil {
				return abort(err)
			}
		}
		for keyStr, value := range c.writes {
			if c.deletes[keyStr] {
				continue
			}
			key := []byte(keyStr)
			val := value
			if err := w.write(func(t *badger.Txn) error { return b.setKeyWithPendingHint(t, key, val) }); err != nil {
				return abort(err)
			}
		}
		// Flush marker rides the same transaction as the data: its presence
		// in Badger means the commit's writes are committed, so recovery
		// replay can treat marker-present as already-applied.
		if c.txID != "" {
			markerKey := bufferedFlushKey(c.txID)
			if err := w.write(func(t *badger.Txn) error { return t.Set(markerKey, nil) }); err != nil {
				return abort(err)
			}
		}
		for key, delta := range c.labelDeltas {
			labelDeltas[key] += delta
		}
		for key, delta := range c.edgeTypeDeltas {
			edgeTypeDeltas[key] += delta
		}
		for key, delta := range c.edgeTypeLabelDeltas {
			edgeTypeLabelDeltas[key] += delta
		}
	}

	// Derived counts change: hold the count locks from the moment the
	// generation's writes can reach Badger until the deltas publish, in the
	// same order the sync path acquires them (label, then edge type), so
	// count readers never observe committed entities without their counts.
	holdLabel := len(labelDeltas) > 0
	holdEdge := len(edgeTypeDeltas) > 0 || len(edgeTypeLabelDeltas) > 0
	if holdLabel {
		b.labelCountWriteMu.Lock()
	}
	if holdEdge {
		b.edgeTypeCountWriteMu.Lock()
	}

	// Total readers also cover cold prefix scans: keep the durable publication
	// and counter-overlay retirement in one read boundary. Lock order is
	// label, edge type, total; callbacks execute after all three unlock.
	b.totalCountWriteMu.Lock()

	// The replay resolves IDs too: entries it found pending are persisted
	// by its commit, as the sync path's (markTxnCommitted).
	b.idDict.flushTxnCounters(cw.txn)
	committing := cw.txn
	if err := cw.finish(); err != nil {
		b.totalCountWriteMu.Unlock()
		b.idDict.discardTxnCounters(committing)
		if holdEdge {
			b.edgeTypeCountWriteMu.Unlock()
		}
		if holdLabel {
			b.labelCountWriteMu.Unlock()
		}
		return abort(err)
	}
	b.idDict.markTxnCommitted(committing)
	for _, c := range commits {
		b.idDict.clearPending(c.idEntries)
	}

	// Commit tail: durability bookkeeping, derived counts, caches, callbacks.
	for ns := range versions {
		b.persistMVCCSequence(ns)
	}
	if anyCounters {
		b.idDict.persistCounters(b.db, maxNode, maxEdge)
	}
	// Apply the generation's merged count deltas in ONE metadata transaction
	// instead of three separate Badger commits while holding the count
	// locks: readers contend with a much shorter locked window. A metadata
	// failure must not surface as a failed user commit, so repair by
	// rebuilding the authoritative counts — the sync path's recovery
	// boundary. The combined transaction is atomic: on failure nothing was
	// applied, so the per-tier retry below stays safe.
	if err := b.applyWriteBehindCountDeltasLocked(labelDeltas, edgeTypeDeltas, edgeTypeLabelDeltas); err != nil {
		if holdLabel {
			if tierErr := b.applyLabelCountDeltasLocked(labelDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeLabelCountsLocked(); repairErr != nil && b.log != nil {
					b.log.Error("write-behind replay: label count delta apply and repair failed",
						"subsystem", "storage", "update_error", tierErr, "repair_error", repairErr)
				} else if b.log != nil {
					b.log.Warn("write-behind replay: repaired label counts after delta apply failed",
						"subsystem", "storage", "error", tierErr)
				}
			}
		}
		if holdEdge {
			if tierErr := b.applyEdgeTypeCountDeltasLocked(edgeTypeDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeEdgeTypeCountsLocked(); repairErr != nil && b.log != nil {
					b.log.Error("write-behind replay: edge-type count delta apply and repair failed",
						"subsystem", "storage", "update_error", tierErr, "repair_error", repairErr)
				} else if b.log != nil {
					b.log.Warn("write-behind replay: repaired edge-type counts after delta apply failed",
						"subsystem", "storage", "error", tierErr)
				}
			}
			if tierErr := b.applyEdgeTypeLabelCountDeltasLocked(edgeTypeLabelDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeEdgeTypeCountsLocked(); repairErr != nil && b.log != nil {
					b.log.Error("write-behind replay: (label, type) count delta apply and repair failed",
						"subsystem", "storage", "update_error", tierErr, "repair_error", repairErr)
				} else if b.log != nil {
					b.log.Warn("write-behind replay: repaired (label, type) counts after delta apply failed",
						"subsystem", "storage", "error", tierErr)
				}
			}
		}
	}
	b.publishBufferedTotalCounts(commits)
	if publishCounts != nil {
		publishCounts()
	}
	b.totalCountWriteMu.Unlock()
	// Close the double-count window before releasing the count locks: the
	// derived-count fast paths hold these same locks, so they observe
	// either the persisted deltas AND the applied flag, or neither —
	// never the persisted deltas while the buffer still sums them.
	if holdEdge {
		b.edgeTypeCountWriteMu.Unlock()
	}
	if holdLabel {
		b.labelCountWriteMu.Unlock()
	}
	// publishCommitOperations below advances the node-cache generation per
	// written node (cacheStoreNode) and invalidates per deleted node, the
	// same protection the sync tail uses; clearing the whole cache here
	// would only destroy warm entries and re-decode work for readers.
	// The replay only queues its notifications: whoever retired the
	// generation delivers them once it holds no lock (drainOne,
	// applyBufferedCommits), so a listener that writes back never waits on a
	// writer that waits for this replay.
	held := b.holdPublication()
	schemas := make(map[string]*SchemaManager, 1)
	for _, c := range commits {
		schema := schemas[c.namespace]
		if schema == nil && c.namespace != "" {
			schema = b.GetSchemaForNamespace(c.namespace)
			schemas[c.namespace] = schema
		}
		b.publishCommitOperations(c.namespace, schema, c.ops, c.recovered)
	}
	b.readyCommitNotification(held)
	return nil
}

// publishCommitOperations mirrors the commit tail's callback and unique-value
// cache updates for one buffered statement.
func (b *BadgerEngine) publishCommitOperations(namespace string, schema *SchemaManager, ops []Operation, publishRevision bool) {
	// Normal buffered writes already advanced their logical revision at ACK.
	// Replaying durability must not invalidate stable query results once per row.
	// Recovery has no prior ACK in this process and still publishes its revision.
	notifyNodeCreated, notifyNodeUpdated := b.publishNodeCreated, b.publishNodeUpdated
	notifyNodeDeleted := b.publishNodeDeleted
	notifyEdgeCreated, notifyEdgeUpdated := b.publishEdgeCreated, b.publishEdgeUpdated
	notifyEdgeDeleted := b.publishEdgeDeleted
	if publishRevision {
		notifyNodeCreated, notifyNodeUpdated, notifyNodeDeleted = b.notifyNodeCreated, b.notifyNodeUpdated, b.notifyNodeDeleted
		notifyEdgeCreated, notifyEdgeUpdated, notifyEdgeDeleted = b.notifyEdgeCreated, b.notifyEdgeUpdated, b.notifyEdgeDeleted
	}

	for _, op := range ops {
		switch op.Type {
		case OpCreateNode:
			b.cacheOnNodeCreatedWithCounts(op.Node, false)
			notifyNodeCreated(op.Node)
		case OpUpdateNode:
			b.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
			notifyNodeUpdated(op.Node)
		case OpDeleteNode:
			if op.OldNode != nil {
				// The replay's own pre-delete snapshot drives the property
				// indexes: buffered nodes never entered the engine node
				// cache, so the cache-based maintenance would leave their
				// committed index entries dangling.
				b.maintainPropertyIndexesOnNodeDeletedWithNode(op.NodeID, op.OldNode)
				b.cacheOnNodeDeletedWithLabelsAndCounts(op.NodeID, op.OldNode.Labels, op.EdgesDeleted, false)
			} else {
				b.cacheOnNodeDeletedWithCounts(op.NodeID, op.EdgesDeleted, false)
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				notifyEdgeDeleted(edgeID)
			}
			notifyNodeDeleted(op.NodeID)
		case OpCreateEdge:
			b.cacheOnEdgeCreatedWithCounts(op.Edge, false)
			notifyEdgeCreated(op.Edge)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.cacheOnEdgeUpdated(oldType, op.Edge)
			notifyEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.cacheOnEdgeDeletedWithCounts(op.EdgeID, oldType, false)
			notifyEdgeDeleted(op.EdgeID)
		}
	}

	if namespace != "" && schema != nil && schema.HasUniqueValueTracking() {
		for _, op := range ops {
			switch op.Type {
			case OpCreateNode:
				if op.Node == nil {
					continue
				}
				for _, label := range op.Node.Labels {
					for propName, propValue := range op.Node.Properties {
						schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
					}
				}
			case OpUpdateNode:
				if op.OldNode != nil {
					for _, label := range op.OldNode.Labels {
						for propName, propValue := range op.OldNode.Properties {
							schema.UnregisterUniqueValue(label, propName, propValue)
						}
					}
				}
				if op.Node != nil {
					for _, label := range op.Node.Labels {
						for propName, propValue := range op.Node.Properties {
							schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
						}
					}
				}
			case OpDeleteNode:
				if op.OldNode == nil {
					continue
				}
				for _, label := range op.OldNode.Labels {
					for propName, propValue := range op.OldNode.Properties {
						schema.UnregisterUniqueValue(label, propName, propValue)
					}
				}
			}
		}
	}
}

// applyWriteBehindCountDeltasLocked persists one generation's merged
// derived-count deltas in a single metadata transaction. The caller must
// hold labelCountWriteMu (for label deltas) and edgeTypeCountWriteMu (for
// the edge-type tiers) across the user-data commit and this metadata update.
func (b *BadgerEngine) applyWriteBehindCountDeltasLocked(label map[namespaceLabel]int64, edgeType map[namespaceEdgeType]int64, edgeTypeLabel map[edgeTypeLabelDelta]int64) error {
	if len(label) == 0 && len(edgeType) == 0 && len(edgeTypeLabel) == 0 {
		return nil
	}
	return b.withUpdate(func(txn *badger.Txn) error {
		for key, delta := range label {
			if err := b.adjustLabelCountInTxn(txn, key.namespace, key.label, delta); err != nil {
				return err
			}
		}
		for key, delta := range edgeType {
			if err := b.adjustEdgeTypeCountInTxn(txn, key.namespace, key.edgeType, delta); err != nil {
				return err
			}
		}
		for key, delta := range edgeTypeLabel {
			if err := b.adjustEdgeTypeLabelCountInTxn(txn, key.sub, key.namespace, key.label, key.edgeType, delta); err != nil {
				return err
			}
		}
		return nil
	})
}

// FlushWriteBehind drains every buffered generation synchronously. It is a
// no-op when write-behind is disabled. It holds the write barrier shared
// while it drains, so Close does not release Badger under it (Close's own
// final drain is admitted), and delivers the drained commits' notifications
// once it released it, so no listener runs while a barrier waiter waits for
// it.
func (b *BadgerEngine) FlushWriteBehind() error {
	if b.writeBehind == nil {
		return nil
	}
	b.writeBarrier.RLock()
	b.mu.RLock()
	closed := b.closed
	b.mu.RUnlock()
	if closed {
		b.writeBarrier.RUnlock()
		if b.writeBehind.PendingOps() > 0 {
			return ErrStorageClosed
		}
		return nil
	}
	gens := b.writeBehind.rotateAndCapture()
	// Every commit acknowledged before the flush is captured; the barrier
	// goes before they are applied. The background worker applies a
	// generation it claimed holding no barrier and then delivers its
	// notifications: a flush waiting for it under the barrier would hold it
	// across that generation's listeners.
	b.writeBarrier.RUnlock()
	err := b.writeBehind.flush(false, false)
	b.drainCommitNotifications()
	if err != nil {
		return err
	}
	// A generation the background worker claimed is applied when flush
	// returns, but the worker may still be delivering its notifications:
	// wait for them, holding no lock, so every listener of a write
	// acknowledged before the flush has run when it returns. A listener
	// that flushes cannot wait on its own generation: it left the queue
	// before its notifications were delivered.
	for _, g := range gens {
		<-g.Settled()
	}
	return nil
}

// writeBehindEnabled reports whether the buffer is active.
func (b *BadgerEngine) writeBehindEnabled() bool {
	return b.writeBehind != nil
}
