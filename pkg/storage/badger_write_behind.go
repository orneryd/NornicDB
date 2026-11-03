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
	"context"
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
		bytes += estimateAffectedFactsBytes(op.DeletedEdgeFacts)
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
	_, err := b.beginHelperTxn()
	if err != nil {
		return
	}
	defer b.work.Leave()
	if b.writeBehind != nil {
		w.bindPropertyDictionary(b.propKeyDict)
	}
	b.bufferedCommitWAL = w
}

// commitBufferedLocked captures the transaction's validated, materialized
// commit state into the engine's write-behind buffer and closes the
// transaction as committed without writing to Badger. The caller has already
// run every validation phase (constraints, snapshot isolation, connected
// deletes) and holds the engine write barrier.
func (tx *BadgerTransaction) commitBufferedLocked(preparationContext context.Context) error {
	// The commit is acknowledged under its entities' write-order stripes, so
	// a write of the same entities that commits to Badger itself
	// (orderAfterBufferedCommits) falls wholly before or after it. They are
	// taken first, as such a write takes them before any commit lock.
	// A node it deletes is held exclusively, so no commit writing a
	// relationship of that node is acknowledged alongside it.
	var stripes, deletions orderStripeSet
	entities, err := operationEntities(preparationContext, tx.operations)
	if err != nil {
		return tx.abortCommitLocked(err)
	}
	stripes.entities(entities)
	for _, id := range entities.deletedNodes {
		deletions.add(string(id))
	}
	if err := tx.engine.writeBehind.order.lockMixedContext(preparationContext, &stripes, &deletions); err != nil {
		return tx.abortCommitLocked(err)
	}
	defer tx.engine.writeBehind.order.unlockMixed(&stripes, &deletions)
	if err := tx.revalidateEndpointsLocked(preparationContext); err != nil {
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
		logicalTxID: tx.commitLogicalID,
		namespace:   tx.namespace,
		txID:        tx.ID,
		ops:         physicalOps,
		writes:      tx.pendingWrites,
		deletes:     tx.pendingDeletes,
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
	// A rejected pre-WAL commit drops entries that no other transaction adopted.
	transferred := false
	defer func() {
		if transferred {
			return
		}
		tx.engine.idDict.abandonPending(c.idEntries)
		if c.idEntries != nil {
			clear(c.idEntries.nodes)
			clear(c.idEntries.edges)
			clear(c.idEntries.claims)
			c.idEntries.nodes = nil
			c.idEntries.edges = nil
			c.idEntries.claims = nil
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

	logicalID := tx.ID
	if tx.commitLogicalID != "" {
		logicalID = tx.commitLogicalID
	}
	var accounting func()
	if registry := tx.engine.storedEntityObservers.Load(); registry != nil {
		if observer := (*registry)[tx.namespace].observer; observer != nil {
			changes, err := tx.engine.captureStoredEntityChanges(preparationContext, storedEntityCapture{staged: tx})
			if err != nil {
				tx.closeLocked(TxStatusRolledBack, true, nil)
				return err
			}
			accounting, err = observer(StoredEntityPublication{Namespace: tx.namespace, LogicalID: logicalID, Changes: changes})
			if err == nil {
				err = preparationContext.Err()
			}
			if err != nil {
				tx.closeLocked(TxStatusRolledBack, true, nil)
				return err
			}
		}
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
	if err := DecidePublication(preparationContext, func() error {
		if tx.engine.bufferedCommitWAL != nil {
			c.walIDs = tx.engine.idDict
			err := tx.engine.bufferedCommitWAL.AppendBufferedCommit(&c, tx.engine.propKeyDict)
			c.walIDs = nil
			if err != nil {
				return err
			}
		}

		tx.releaseSnapshotReaderLocked()
		tx.bufferedStagingTransferred = true
		states := tx.engine.writeBehind.AppendCommit(c)
		// Claims follow logical visibility, not the later materialization of
		// historical images. Entity/value authority remains held until return.
		// A concurrent flusher never republishes these normal ACK-time claims.
		schema := tx.engine.GetSchemaForNamespace(tx.namespace)
		tx.engine.publishUniqueValues(schema, states, ValidatedUniqueClaim)
		if accounting != nil {
			accounting()
		}
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
		return nil
	}); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}
	tx.closeLocked(TxStatusCommitted, true, nil)
	return nil
}

// persistReplayTxnCounters makes one generation's property-key tokens
// durable through the generation's commit writer: through the large commit
// once it has turned large (the flusher holds the commit gate exclusively
// and must not re-enter it), and through ordinary commits otherwise,
// mirroring the sync commit path's finish.
func (b *BadgerEngine) persistReplayTxnCounters(cw *commitWriter, drain propKeyTxnDrain) error {
	var to batchTarget = b.db
	if cw.large != nil {
		to = cw.large
	}
	return b.propKeyDict.persistTxnCounters(cw.ctx, to, drain)
}

// replayWriteBehind replays one drained generation into Badger. It marks
// the generation applied on success so the buffer retires it.
func (b *BadgerEngine) replayWriteBehind(buf *CommitBuffer) error {
	commits := buf.Commits()
	if len(commits) == 0 {
		return nil
	}
	return b.applyBufferedCommitsWithCountPublication(commits, func() { buf.countsApplied.Store(true) }, func() {
		buf.markApplied()
		// A claimed drain owns settlement after dispatch. Remove its overlay
		// before a listener can Flush and wait on that same claim. Unclaimed
		// replay keeps the existing replay/retirement separation: a later
		// drain can retire and settle it itself.
		if buf.draining.Load() {
			b.writeBehind.removeDrained(buf)
		}
	})
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
// publish persisted counters. beforeNotifications runs on success after the
// count locks are released and before listener notifications become ready.
func (b *BadgerEngine) applyBufferedCommitsWithCountPublication(commits []bufferedCommit, publishCounts func(), beforeNotifications ...func()) error {
	if len(commits) == 0 {
		return nil
	}

	txn, readTs, err := b.db.beginTxn(true)
	if err != nil {
		return err
	}
	defer b.db.endRead(readTs)
	// Acknowledged replay owns detached completion, independent of both the
	// original caller and engine-stop cancellation. Select it once for the
	// writer, token persistence and required post-durable count tail.
	accepted := context.Background()
	cw := b.newCommitWriter(accepted, b.db, txn)
	recovered := false
	for _, commit := range commits {
		if commit.recovered {
			recovered = true
			break
		}
	}
	if !recovered {
		cw.configureStoredEntityPublication("", true)
	}
	notification := b.queueCommitOperations(nil, nil, notificationAtCommit)
	cw.notification = notification
	defer func() { b.readyCommitNotification(notification) }()

	// Write through the commit writer's own batch writer, exactly like the
	// sync commit path: cw.finish() commits cw.batchWriter's open batch, so
	// a separate batchWriter whose txn never follows the batch rotation
	// would make finish write the intent delete through an already-committed
	// (discarded) transaction.
	w := &cw.batchWriter

	originalHoldLabel, originalHoldEdge := false, false
	for _, commit := range commits {
		originalHoldLabel = originalHoldLabel || len(commit.labelDeltas) > 0
		originalHoldEdge = originalHoldEdge || len(commit.edgeTypeDeltas) > 0 || len(commit.edgeTypeLabelDeltas) > 0
	}
	commits, err = unappliedBufferedCommits(accepted, txn, commits)
	if err != nil {
		cw.discard()
		return err
	}
	versions := make(map[string]MVCCVersion, 1)
	var maxNode, maxEdge uint64
	anyCounters := false
	labelDeltas := make(map[namespaceLabel]int64)
	edgeTypeDeltas := make(map[namespaceEdgeType]int64)
	edgeTypeLabelDeltas := make(map[edgeTypeLabelDelta]int64)

	// Count locks precede the commit gate even when assembly turns large.
	for _, c := range commits {
		for k, d := range c.labelDeltas {
			labelDeltas[k] += d
		}
		for k, d := range c.edgeTypeDeltas {
			edgeTypeDeltas[k] += d
		}
		for k, d := range c.edgeTypeLabelDeltas {
			edgeTypeLabelDeltas[k] += d
		}
	}
	holdLabel := len(labelDeltas) > 0 || originalHoldLabel
	holdEdge := len(edgeTypeDeltas) > 0 || len(edgeTypeLabelDeltas) > 0 || originalHoldEdge
	countsHeld := false
	acquireCounts := func() {
		if countsHeld {
			return
		}
		if holdLabel {
			b.labelCountWriteMu.Lock()
		}
		if holdEdge {
			b.edgeTypeCountWriteMu.Lock()
		}
		b.totalCountWriteMu.Lock()
		countsHeld = true
	}
	cw.beforeLarge = func(context.Context) error { acquireCounts(); return nil }
	defer func() {
		if countsHeld {
			b.totalCountWriteMu.Unlock()
			if holdEdge {
				b.edgeTypeCountWriteMu.Unlock()
			}
			if holdLabel {
				b.labelCountWriteMu.Unlock()
			}
		}
	}()
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

	if len(commits) == 0 {
		cw.discard()
		acquireCounts()
		if publishCounts != nil {
			publishCounts()
		}
		b.totalCountWriteMu.Unlock()
		if holdEdge {
			b.edgeTypeCountWriteMu.Unlock()
		}
		if holdLabel {
			b.labelCountWriteMu.Unlock()
		}
		countsHeld = false
		for _, before := range beforeNotifications {
			before()
		}
		return nil
	}

	// Capture constraint metadata before any write can promote the physical
	// writer to the exclusive commit gate. Recovered operation images are
	// hydrated below; their late key derivation reads only these snapshots.
	schemas := make(map[string]*SchemaManager, 1)
	constraints := make(map[string]*publicationConstraintView, 1)
	for _, c := range commits {
		if _, exists := schemas[c.namespace]; exists {
			continue
		}
		if c.namespace == "" {
			schemas[c.namespace] = nil // historical frames still publish membership
			continue
		}
		schema := b.GetSchemaForNamespace(c.namespace)
		schemas[c.namespace] = schema
		view, err := capturePublicationConstraintView(accepted, schema)
		if err != nil {
			return abort(err)
		}
		constraints[c.namespace] = &view
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
		if c.hasCounters && !c.recovered {
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
		writes, deletes := c.writes, c.deletes
		if c.recovered {
			var err error
			writes, deletes, err = b.rebindBufferedNumericIDs(cw.txn, c)
			if err != nil {
				return abort(err)
			}
			if err := b.restoreBufferedOperationBodies(cw.txn, c, writes, deletes); err != nil {
				return abort(err)
			}
			if err := validateReboundWALWrites(writes, deletes); err != nil {
				return abort(err)
			}
		}
		if err := b.materializeMVCCCommitWithHeads(accepted, w, versions[c.namespace], c.ops, true); err != nil {
			return abort(err)
		}
		for keyStr := range deletes {
			key := []byte(keyStr)
			cw.rawCache.add(b, key, c.writes[keyStr])
			if err := w.write(accepted, func(t *badger.Txn) error { return t.Delete(key) }); err != nil {
				return abort(err)
			}
		}
		for keyStr, value := range writes {
			if deletes[keyStr] {
				continue
			}
			key := []byte(keyStr)
			cw.rawCache.add(b, key, c.writes[keyStr])
			val := value
			if err := w.write(accepted, func(t *badger.Txn) error { return b.setKeyWithPendingHint(t, key, val) }); err != nil {
				return abort(err)
			}
		}
		// Flush marker rides the same transaction as the data: its presence
		// in Badger means the commit's writes are committed, so recovery
		// replay can treat marker-present as already-applied.
		if c.txID != "" {
			markerKey := bufferedCommitMarkerKey(c.namespace, c.txID, c.logicalTxID)
			if err := w.write(accepted, func(t *badger.Txn) error { return t.Set(markerKey, nil) }); err != nil {
				return abort(err)
			}
		}
	}

	acquireCounts()

	var notificationOps []Operation
	for _, c := range commits {
		var err error
		cw.publishKeys, err = appendOperationPublicationKeys(accepted, cw.publishKeys, constraints[c.namespace], c.ops)
		if err != nil {
			return abort(err)
		}
		notificationOps = append(notificationOps, c.ops...)
	}
	capturePublicationOperations(notification, notificationOps)
	cw.publish = func(images *commitCacheImages) {
		// All frames land at one physical boundary. Publish its endpoints,
		// never historical intermediate operations or acknowledged frames.
		for namespace, schema := range schemas {
			var frames, recoveryFrames [][]Operation
			for _, c := range commits {
				if c.namespace == namespace {
					frames = append(frames, c.ops)
					if c.recovered {
						recoveryFrames = append(recoveryFrames, c.ops)
					}
				}
			}
			// The generation is durable: its frames are selected on that
			// accepted completion lifetime, so the fold cannot cancel.
			states, _ := newCommitStates(accepted, frames...)
			if len(recoveryFrames) > 0 {
				claims := states
				if len(recoveryFrames) != len(frames) {
					// Normal frames already published their claims at ACK.
					claims, _ = newCommitStates(accepted, recoveryFrames...)
				}
				b.publishUniqueValues(schema, claims, RecoveredUniqueClaim)
			}
			b.publishIndexMembership(states)
			images.acceptStates(states)
		}
		for _, c := range commits {
			b.publishCommitEffects(c.ops, false, c.recovered)
		}
	}

	// The replay resolves IDs too: entries it found pending are persisted
	// by its commit, as the sync path's (markTxnCommitted).
	replayNodeMax, replayEdgeMax := b.idDict.flushTxnCounters(cw.txn)
	if replayNodeMax > maxNode {
		maxNode, anyCounters = replayNodeMax, true
	}
	if replayEdgeMax > maxEdge {
		maxEdge, anyCounters = replayEdgeMax, true
	}
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
		countsHeld = false
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
	//
	// The generation was accepted before this tail (cw above carries its
	// explicitly detached completion context): its count writes and any
	// repair run on that same unbounded accepted lifetime, never on a
	// caller's request or the engine stop signal.
	if err := b.applyWriteBehindCountDeltasLocked(accepted, labelDeltas, edgeTypeDeltas, edgeTypeLabelDeltas); err != nil {
		if holdLabel {
			if tierErr := b.applyLabelCountDeltasLocked(accepted, labelDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeLabelCountsLocked(accepted); repairErr != nil && b.log != nil {
					b.log.Error("write-behind replay: label count delta apply and repair failed",
						"subsystem", "storage", "update_error", tierErr, "repair_error", repairErr)
				} else if b.log != nil {
					b.log.Warn("write-behind replay: repaired label counts after delta apply failed",
						"subsystem", "storage", "error", tierErr)
				}
			}
		}
		if holdEdge {
			if tierErr := b.applyEdgeTypeCountDeltasLocked(accepted, edgeTypeDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeEdgeTypeCountsLocked(accepted); repairErr != nil && b.log != nil {
					b.log.Error("write-behind replay: edge-type count delta apply and repair failed",
						"subsystem", "storage", "update_error", tierErr, "repair_error", repairErr)
				} else if b.log != nil {
					b.log.Warn("write-behind replay: repaired edge-type counts after delta apply failed",
						"subsystem", "storage", "error", tierErr)
				}
			}
			if tierErr := b.applyEdgeTypeLabelCountDeltasLocked(accepted, edgeTypeLabelDeltas); tierErr != nil {
				if repairErr := b.rebuildAuthoritativeEdgeTypeCountsLocked(accepted); repairErr != nil && b.log != nil {
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

	countsHeld = false
	for _, before := range beforeNotifications {
		before()
	}
	return nil
}

// publishCommitOperations publishes only derived memory state. The commit
// writer orders it by entity and UNIQUE value; listeners run after retirement.
// A nil schema explicitly excludes UNIQUE claims already published at logical
// buffered ACK. Cache/count/revision publication retains its existing timing.
func (b *BadgerEngine) publishCommitOperations(namespace string, schema *SchemaManager, ops []Operation, publishCounts bool, publishRevision bool, images *commitCacheImages) {
	if namespace == "" {
		schema = nil
	}
	// The commit is accepted: its frame is selected on that completion
	// lifetime, so the fold cannot cancel.
	states, _ := newCommitStates(context.Background(), ops)
	b.publishUniqueValues(schema, states, ValidatedUniqueClaim)
	b.publishIndexMembership(states)
	images.acceptStates(states)
	b.publishCommitEffects(ops, publishCounts, publishRevision)
}

// publishCommitEffects retains operation multiplicity for cardinality,
// observation and notifications. It never edits entity index membership.
func (b *BadgerEngine) publishCommitEffects(ops []Operation, publishCounts, publishRevision bool) {
	for _, op := range ops {
		if publishCounts {
			visitOperationTotalCounts(op, b.publishTotalCountChange)
		}
		observeCommitCacheOperation(op)
	}
	b.prepareCommitOperations(ops, false, publishRevision)
}

func (b *BadgerEngine) applyWriteBehindCountDeltasLocked(ctx context.Context, label map[namespaceLabel]int64, edgeType map[namespaceEdgeType]int64, edgeTypeLabel map[edgeTypeLabelDelta]int64) error {
	if len(label) == 0 && len(edgeType) == 0 && len(edgeTypeLabel) == 0 {
		return nil
	}
	return b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
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
// Cancellation ends this wait; independently accepted replay retains its
// terminal and settlement ownership. A nil context waits non-cancellably:
// this boundary selects a nonnil waiting context once. Accepted replay
// retains its separate completion waitCtx.
func (b *BadgerEngine) FlushWriteBehind(ctx context.Context) error {
	if b.writeBehind == nil {
		return nil
	}
	// Normalize the optional waiting context, without changing replay ownership.
	waitCtx := ctx
	if err := b.writeBarrier.RLockContext(waitCtx); err != nil {
		return err
	}
	b.mu.RLock()
	closed := b.work.Closing()
	b.mu.RUnlock()
	if closed {
		b.writeBarrier.RUnlock()
		if b.writeBehind.PendingOps() > 0 {
			return ErrStorageClosed
		}
		return nil
	}
	gens, rotationErr := b.writeBehind.rotate(waitCtx, true)
	// Every commit acknowledged before the flush is captured; the barrier
	// goes before they are applied. The background worker applies a
	// generation it claimed holding no barrier and then delivers its
	// notifications: a flush waiting for it under the barrier would hold it
	// across that generation's listeners.
	b.writeBarrier.RUnlock()
	if rotationErr != nil {
		return rotationErr
	}
	// Rotation handed acknowledged work to the existing replay owner. Wake it
	// even if cancellation prevents this caller from entering its wait.
	b.writeBehind.signal()
	err := b.writeBehind.flushContext(waitCtx, false, false)
	if ctxErr := waitCtx.Err(); ctxErr != nil {
		// Retain any real apply failure already returned.
		if err != nil {
			return err
		}
		return ctxErr
	}
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
		if waitCtx.Done() == nil {
			// A request that cannot cancel waits on the generation alone: no
			// timer and no allocation per wait.
			<-g.Settled()
			continue
		}
		select {
		case <-g.Settled():
		case <-waitCtx.Done():
			return waitCtx.Err()
		}
	}
	return nil
}

// writeBehindEnabled reports whether the buffer is active.
func (b *BadgerEngine) writeBehindEnabled() bool {
	return b.writeBehind != nil
}
