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
)

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
	// The exclusive commit gate orders this acknowledgement against every
	// synchronous commit's check of the buffer (commitWriter.admit): a
	// synchronous commit of an entity sees it, or commits before it.
	tx.badgerDB.commitGate.Lock()
	tx.engine.writeBehind.AppendCommit(c)
	tx.badgerDB.commitGate.Unlock()
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

// errBufferedWritePending refuses a synchronous data commit while any
// acknowledged write awaits replay. Dependencies include reads and cascades,
// so the barrier cannot be selected from the entities the commit publishes.
var errBufferedWritePending = errors.New("an acknowledged write-behind commit is not replayed yet")

// orderBehindBufferedWrites runs a synchronous engine write after ALL
// acknowledged writes in this engine, across namespaces. This covers every
// read and cascade dependency without requiring a separate dependency model
// for each writer. An acknowledgement arriving during the write refuses its
// commit (commitWriter.admit), causing a drain and retry of the entire write.
// It drains outside the engine's write
// barrier, which the replay takes, so the write runs from the start again:
// what it reads then includes the replayed state.
func (b *BadgerEngine) orderBehindBufferedWrites(write func() error) error {
	if b.writeBehind == nil {
		return write()
	}
	if b.writeBehind.PendingOps() != 0 {
		if err := b.FlushWriteBehind(); err != nil {
			return err
		}
	}
	for {
		err := write()
		if !errors.Is(err, errBufferedWritePending) {
			return err
		}
		if err := b.FlushWriteBehind(); err != nil {
			return err
		}
	}
}

// bufferableLocked reports whether the commit may be acknowledged through the
// write-behind buffer: an implicit autocommit with no schema,
// knowledge-policy or change-capture writes. Commit also requires that it
// writes no temporal index entries, which it knows only once it has buffered
// them.
func (tx *BadgerTransaction) bufferableLocked() bool {
	return tx.engine.writeBehindEnabled() && tx.implicit && tx.schemaRuntime == nil &&
		!tx.knowledgeSchemaDirty &&
		(tx.namespace == "" || tx.engine.ChangeCapture(tx.namespace) == ChangeCaptureOff)
}

// drainBufferedWritesLocked drains every earlier acknowledgement before a
// synchronous transaction commits. Its physical snapshot does not move: if
// replay changes a dependency it read, Badger's conflict check refuses the
// transaction. An acknowledgement arriving afterward is refused under the
// commit gate with ErrConflict (commitWriter.admit). Bufferable commits keep
// their FIFO place in the buffer and do not drain it.
func (tx *BadgerTransaction) drainBufferedWritesLocked() error {
	wb := tx.engine.writeBehind
	if wb == nil || (tx.bufferableLocked() && !tx.writesTemporalConstrainedNodeLocked()) {
		return nil
	}
	if wb.PendingOps() == 0 {
		return nil
	}
	return tx.engine.FlushWriteBehind()
}

// writesTemporalConstrainedNodeLocked reports whether the transaction writes
// a node carrying a label with a temporal constraint: the writes that may
// give it temporal index entries (bufferTemporalIndexWrites).
func (tx *BadgerTransaction) writesTemporalConstrainedNodeLocked() bool {
	constrained := func(node *Node) bool {
		if node == nil {
			return false
		}
		dbName, _, ok := ParseDatabasePrefix(string(node.ID))
		return ok && len(temporalConstraintsForLabels(tx.engine.GetSchemaForNamespace(dbName), node.Labels)) > 0
	}
	for _, op := range tx.operations {
		if constrained(op.Node) || constrained(op.OldNode) {
			return true
		}
	}
	return false
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
// the generation applied once the replay is published and before its
// listeners run, so the buffer retires its read overlay first: a listener
// reads its own synchronous write, and a flush it starts can reach the
// generations after this one.
func (b *BadgerEngine) replayWriteBehind(buf *CommitBuffer) error {
	commits := buf.Commits()
	if len(commits) == 0 {
		return nil
	}
	return b.applyBufferedCommitsWithCountPublication(commits, func() { buf.countsApplied.Store(true) }, buf.markApplied)
}

// applyBufferedCommits replays buffered commits into Badger: a single Badger
// transaction applies every buffered statement's physical operations and raw
// KV writes, then the commit tail runs (MVCC sequence persistence, ID
// counters, derived counts, caches, callbacks). Before the transaction
// commits it also writes each commit's flush marker key, so the marker is
// durable exactly when the data is — recovery at open skips any commit whose
// marker exists.
func (b *BadgerEngine) applyBufferedCommits(commits []bufferedCommit, beforeNotifications ...func()) error {
	return b.applyBufferedCommitsWithCountPublication(commits, nil, beforeNotifications...)
}

// publishCounts retires the generation count overlay under the same locks that
// publish persisted counters. Entity/cache callbacks run afterward without locks.
// beforeNotifications run once the replay is published, before its listeners.
func (b *BadgerEngine) applyBufferedCommitsWithCountPublication(commits []bufferedCommit, publishCounts func(), beforeNotifications ...func()) error {
	if len(commits) == 0 {
		return nil
	}

	txn := b.db.DB.NewTransactionAt(b.db.oracle.published.Load(), true)
	cw := b.newCommitWriter(b.db, txn)
	cw.replay = true
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
			if err := w.write(func(t *badger.Txn) error { return t.Set(key, val) }); err != nil {
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

	// The caches, indexes and unique-value registry, in commit order
	// (badger_publication_stage.go). publishCommitOperations advances the
	// node-cache generation per written node (cacheStoreNode) and
	// invalidates per deleted node, the same protection the sync tail uses;
	// clearing the whole cache would only destroy warm entries.
	schemas := make(map[string]*SchemaManager, 1)
	for _, c := range commits {
		if _, known := schemas[c.namespace]; !known && c.namespace != "" {
			schemas[c.namespace] = b.GetSchemaForNamespace(c.namespace)
		}
	}
	cw.publish = func() {
		for _, c := range commits {
			b.publishCommitOperations(c.namespace, schemas[c.namespace], c.ops)
		}
	}
	for _, c := range commits {
		cw.publishKeys = appendOperationPublicationKeys(cw.publishKeys, schemas[c.namespace], c.ops)
	}

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
	// The listeners, after the stage published every replayed commit and
	// after the count locks are released, in commit order: a callback never
	// runs inside the stage, so one that commits can't wait on it.
	for _, ready := range beforeNotifications {
		ready()
	}
	for _, c := range commits {
		b.notifyReplayedOperations(c.ops, c.recovered)
	}
	return nil
}

// publishCommitOperations is a replayed commit's publication: the entity
// caches, indexes and unique-value registry, which the ordered stage runs
// (badger_publication_stage.go). Its listeners run afterwards, outside the
// stage (notifyReplayedOperations).
func (b *BadgerEngine) publishCommitOperations(namespace string, schema *SchemaManager, ops []Operation) {
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode:
			b.cacheOnNodeCreatedWithCounts(op.Node, false)
		case OpUpdateNode:
			b.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
		case OpDeleteNode:
			b.unmarkDeletedManualSuppressions(op.NodeID, op.DeletedEdgeIDs)
			if op.OldNode != nil {
				// The replay's own pre-delete snapshot drives the property
				// indexes: buffered nodes never entered the engine node cache.
				b.cacheOnNodeDeletedWithNodeAndCounts(op.NodeID, op.OldNode, op.EdgesDeleted, false)
			} else {
				b.cacheOnNodeDeletedWithCounts(op.NodeID, op.EdgesDeleted, false)
			}
		case OpCreateEdge:
			b.cacheOnEdgeCreatedWithCounts(op.Edge, false)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.cacheOnEdgeUpdated(oldType, op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.unmarkDeletedManualSuppressions("", []EdgeID{op.EdgeID})
			b.cacheOnEdgeDeletedWithCounts(op.EdgeID, oldType, false)
		}
	}
	if namespace != "" {
		b.publishUniqueValuesForOps(schema, ops)
	}
}

// notifyReplayedOperations runs a replayed commit's listeners, in operation
// order. Normal buffered writes already advanced their logical revision at
// ACK, so replaying durability must not invalidate stable query results once
// per row; recovery has no prior ACK in this process and still publishes its
// revision (publishRevision).
func (b *BadgerEngine) notifyReplayedOperations(ops []Operation, publishRevision bool) {
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
			notifyNodeCreated(op.Node)
		case OpUpdateNode:
			notifyNodeUpdated(op.Node)
		case OpDeleteNode:
			for _, edgeID := range op.DeletedEdgeIDs {
				notifyEdgeDeleted(edgeID)
			}
			notifyNodeDeleted(op.NodeID)
		case OpCreateEdge:
			notifyEdgeCreated(op.Edge)
		case OpUpdateEdge:
			notifyEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			notifyEdgeDeleted(op.EdgeID)
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
// no-op when write-behind is disabled.
//
// It is a barrier for the commits acknowledged before it: when it returns,
// each is durable in Badger, its derived-state publication has run (the
// caches and indexes hold it, badger_publication_stage.go), and its listener
// notifications are readied in commit order. It does not wait for listeners
// to complete, as no write does: readied notifications are dispatched by
// whichever goroutine drains the queue, and a drain already running elsewhere
// returns at once (drainCommitNotifications). A listener may therefore flush
// from its callback, as a BEGIN or a snapshot open in it does. Close waits
// for every dispatched listener (notifyWG).
func (b *BadgerEngine) FlushWriteBehind() error {
	if b.writeBehind == nil {
		return nil
	}
	return b.writeBehind.Flush()
}

// writeBehindEnabled reports whether the buffer is active.
func (b *BadgerEngine) writeBehindEnabled() bool {
	return b.writeBehind != nil
}
