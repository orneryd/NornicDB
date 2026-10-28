package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// CommitContextWithPublication commits atomically, then invokes afterPublication
// once on success after publication locks are released but before synchronous
// listeners run. Database wrappers finalize accounting and release admission at
// this boundary. The callback must not panic. Failed commits never invoke it.
// Buffered commits invoke it at their acknowledged-publication boundary.
func (tx *BadgerTransaction) CommitContextWithPublication(ctx context.Context, afterPublication func()) error {
	if tx.clientDetached.Load() {
		return ErrTransactionClosed
	}
	return tx.commitContextWithPreparation(ctx, afterPublication, nil)
}

func (tx *BadgerTransaction) commitContextWithPreparation(ctx context.Context, afterPublication func(), prepare func() error) (commitErr error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	tx.mu.Lock()
	tx.commitCtx = ctx
	tx.mu.Unlock()
	defer func() {
		tx.mu.Lock()
		tx.commitCtx = nil
		tx.mu.Unlock()
	}()

	tx.mu.Lock()
	var knowledgePolicyChanged func()
	var notification, publication *commitNotification
	defer func() {
		tx.mu.Unlock()
		if commitErr == nil && afterPublication != nil {
			afterPublication()
		}
		tx.engine.completeCommitNotification(notification)
		tx.engine.completeCommitNotification(publication)
		if knowledgePolicyChanged != nil {
			knowledgePolicyChanged()
		}
	}()
	defer tx.releaseWriteBarrierLocked()

	if err := tx.ensureLifecycleActiveLocked(true); err != nil {
		return err
	}
	// The engine's write barrier is held through publication, before listener
	// delivery (releaseWriteBarrierLocked): exclusively for a schema transaction, so
	// no data write publishes between its snapshot check and its schema
	// publication; shared otherwise, so Close waits for the commit and a
	// commit that arrives after Close finished fails here without touching
	// Badger.
	if tx.commitReleaseWrite != nil && tx.schemaRuntime != nil {
		// A read-only MERGE may precede DDL; release its key admission before
		// acquiring the schema-exclusive lifecycle lease. Schema snapshot validation
		// below still rejects intervening graph changes.
		tx.releaseMergeAdmissionLocked()
		tx.releaseWriteBarrierLocked()
	}
	if tx.commitReleaseWrite == nil {
		lease, writeErr := tx.engine.beginBarrierWrite(tx.schemaRuntime != nil, false)
		if writeErr != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return writeErr
		}
		tx.commitReleaseWrite = lease.releaseCallback()
	}

	if tx.namespace != "" && (len(tx.pendingWrites) > 0 || len(tx.operations) > 0 || tx.schemaRuntime != nil) {
		if err := tx.engine.pendingNamespaceConflict(tx.namespace); err != nil {
			return tx.abortCommitLocked(err)
		}
	}

	if len(tx.operations) > 0 && tx.engine.prefixDroppedSince(tx.lifecycleSeq, tx.namespace) {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("%w: namespace %q was deleted or rewritten by maintenance after the transaction began", ErrConflict, tx.namespace)
	}
	if tx.schemaRuntime != nil && tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence != tx.readTS.CommitSequence {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("schema snapshot changed before commit: %w", ErrConflict)
	}

	// Metadata handlers are external code and may interpose a peer commit.
	// Validate optimistically before invoking them, without retaining entity keys.
	// Metadata-free commits retain the single protected validation phase.
	if len(tx.Metadata) > 0 {
		if err := tx.validateCommitLocked(); err != nil {
			return err
		}
		// Log metadata. D-10a bracket-prefix migrated to a transaction_id slog
		// attribute so log aggregators can group commits by transaction without
		// parsing message text.
		tx.engine.log.Debug("transaction committing with metadata",
			"subsystem", "transaction",
			"transaction_id", tx.ID,
			slog.Any("metadata", tx.Metadata),
		)
	}

	// Optimistic validation above preserves ordinary snapshot conflicts. Only
	// the final authoritative phase owns entity keys: an intervening peer may
	// commit before this acquisition, so revalidate under the existing key order.
	// Entity and endpoint authority precedes unique values for every native commit.
	releaseEntityAuthority, authorityErr := tx.lockPendingEntityAuthorityLocked()
	if authorityErr != nil {
		return tx.abortCommitLocked(authorityErr)
	}
	defer releaseEntityAuthority()

	// Acquire per-(label, property, value) commit locks for every unique
	// constraint value touched by this transaction's pending nodes. Held
	// across validateAllConstraints + badgerTx.Commit + the post-commit
	// RegisterUniqueValue calls below so that a peer transaction touching the
	// same constrained value cannot pass validation against an empty cache
	// while we are still committing. Without this lock, two concurrent
	// transactions that both add a node with the same constrained property
	// value both pass deferred validation (cache empty for both), both reach
	// badgerTx.Commit (Badger writes them under distinct node IDs so the KV
	// layer does not detect the conflict), and both call RegisterUniqueValue
	// — the second overwriting the first — leaving the UNIQUE constraint
	// silently violated in storage. See cross_session_merge_unique_test.go
	// for the reproduction.
	var err error
	releaseCommitLocks := func() {}
	if tx.namespace != "" && (len(tx.pendingNodes) != 0 || len(tx.pendingEdges) != 0) {
		schemaForValues := tx.engine.GetSchemaForNamespace(tx.namespace)
		nodes := make([]*Node, 0, len(tx.pendingNodes))
		for _, node := range tx.pendingNodes {
			nodes = append(nodes, node)
		}
		edges := make([]*Edge, 0, len(tx.pendingEdges))
		for _, edge := range tx.pendingEdges {
			edges = append(edges, edge)
		}
		valueKeys := schemaForValues.appendConstraintValueKeys(nil, nodes, edges)
		releaseCommitLocks, err = schemaForValues.acquireUniqueConstraintCommitLocks(context.Background(), tx.ID, valueKeys)
		if err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return err
		}
	}

	defer releaseCommitLocks()

	if err := tx.validateCommitLocked(); err != nil {
		return err
	}

	temporalTargets, err := tx.bufferTemporalIndexWrites()
	if err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("buffering temporal index writes: %w", err)
	}

	if err := tx.materializeDeferredEdgesLocked(); err != nil {
		return tx.abortCommitLocked(err)
	}

	// A coherent usage cut may already have replayed ACKs this transaction
	// logically observed before begin. Reuse the existing rebase owner before
	// capturing pairs; its logical MVCC snapshot anchor remains unchanged.
	if tx.replayedSinceBeginLocked() && tx.schemaRuntime == nil && !tx.knowledgeSchemaDirty {
		if err := tx.rebaseBadgerAfterDrainLocked(); err != nil {
			return tx.abortCommitLocked(err)
		}
	}
	// MVCC history, change events, the entity caches and the callbacks all
	// take each entity's final body as storage returns it.
	if err := tx.engine.storeWrittenBodies(tx.pendingWrites, tx.operations); err != nil {
		return tx.abortCommitLocked(err)
	}

	// Preparation hands immutable semantic images to the accepted command owner
	// only after the normal validation/order pipeline, before any commit bytes.
	if prepare != nil {
		if err := prepare(); err != nil {
			return tx.abortCommitLocked(err)
		}
	}

	// Buffered replay can run before this function returns, including from
	// the accounting callback itself. Reserve its publication position before
	// either admission path so listeners wait for accounting/admission to
	// finish after every storage lock is released. Failed commits release the
	// same hold without invoking afterPublication.
	publication = tx.engine.holdPublication()

	// Write-behind buffering: an implicit autocommit commit with no schema,
	// knowledge-policy or temporal writes is acknowledged after every
	// validation phase above and replayed into Badger by the background
	// flusher. Everything that must fail the statement synchronously has
	// already run by this point. (Commit holds tx.mu, so read the
	// knowledge-policy flag directly instead of HasKnowledgePolicyChanges.)
	// A database that captures changes commits through Badger: its change
	// events are written in the same commit as the data (#937).
	if tx.engine.writeBehindEnabled() && tx.commitMode == CommitModeImplicit && tx.schemaRuntime == nil &&
		!tx.knowledgeSchemaDirty && len(temporalTargets) == 0 &&
		(tx.namespace == "" || tx.engine.ChangeCapture(tx.namespace) == ChangeCaptureOff) {
		return tx.commitBufferedLocked()
	}

	// A commit the buffer does not take writes Badger itself: it is ordered
	// after the commits acknowledged before it (orderAfterBufferedCommits).
	var order orderHold
	if tx.engine.writeBehindEnabled() {
		drained, err := tx.engine.orderAfterBufferedCommits(&order, operationEntities(tx.operations))
		if err != nil {
			return tx.abortCommitLocked(err)
		}
		defer order.release()
		if (drained || tx.replayedSinceBeginLocked()) && tx.schemaRuntime == nil && !tx.knowledgeSchemaDirty {
			if err := tx.rebaseBadgerAfterDrainLocked(); err != nil {
				tx.closeLocked(TxStatusRolledBack, true, nil)
				return err
			}
		}
		// Commits of its endpoints or of a deleted node's relationships,
		// drained or committed directly, may have landed after this commit
		// staged and validated them.
		if err := tx.revalidateEndpointsLocked(); err != nil {
			return tx.abortCommitLocked(err)
		}
	}

	physicalOperations := tx.physicalOperationsLocked()
	hasWrites := len(physicalOperations) > 0 || len(tx.pendingWrites) > 0 || len(tx.pendingDeletes) > 0
	var version MVCCVersion
	if hasWrites {
		if tx.namespace == "" {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return localizedError(localization.StorageTransactionCommitNamespaceMissing(), nil)
		}
		// A capturing database's commits take its change log in turn, from
		// here until the commit ends (#937).
		if err := tx.beginChangeCaptureLocked(); err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return err
		}
		defer tx.endChangeCaptureLocked(false)
		version, err = tx.engine.allocateMVCCVersion(tx.badgerTx, tx.namespace, time.Now())
		if err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return fmt.Errorf("allocating mvcc commit version: %w", err)
		}
		tx.CommitVersion = version
	}

	// Every commit-phase write goes through the transaction's commit writer.
	// It starts in tx.badgerTx, which carries the transaction's conflict-
	// tracked reads; when a write does not fit there, the commit becomes a
	// large commit whose batches stay invisible until the last one is
	// written (#703). Each new batch becomes tx.badgerTx. Before the commit
	// turns large it takes the count locks publication needs, ahead of the
	// exclusive commit gate.
	// Nothing below reads at the transaction's snapshot, so it stops counting
	// as an active snapshot reader before the commit's writes. A superseded
	// version is then archived for other readers (mustArchiveForHistory), not
	// for this transaction's own registration (#911).
	tx.releaseSnapshotReaderLocked()

	mergeReleases := tx.mergeKeyReleases
	tx.mergeKeyReleases = nil
	defer func() {
		for _, release := range mergeReleases {
			release()
		}
	}()
	acceptedRelease := tx.commitReleaseAcceptedAuthority
	cw := tx.engine.newCommitWriter(tx.badgerDB, tx.badgerTx)
	logicalID := tx.ID
	if tx.commitLogicalID != "" {
		logicalID = tx.commitLogicalID
	}
	cw.configureStoredEntityPublication(logicalID, false)
	// The physical writer owns the same immutable delta for ordinary and large
	// commits. Register it before the timestamp becomes visible; its stage may
	// help an earlier same-key publisher without waiting for that goroutine.
	operations := tx.operations
	engine, namespace := tx.engine, tx.namespace
	if len(operations) > 0 || tx.knowledgeSchemaDirty {
		schema := engine.GetSchemaForNamespace(namespace)
		// The constraint metadata is captured before the physical commit
		// below can turn large and take the commit gate exclusively: late
		// key derivation then reads only this immutable view.
		constraints := capturePublicationConstraintView(schema)
		cw.publishKeys = appendOperationPublicationKeys(nil, &constraints, operations)
		if len(operations) > 0 {
			notification = engine.queueCommitOperations(nil, nil, notificationAtCommit)
			capturePublicationOperations(notification, operations)
			cw.notification = notification
		}
		cw.publish = func() {
			knowledgePolicyChanged = tx.publishKnowledgePolicySchemaLocked()
			engine.publishCommitOperations(namespace, schema, operations, true, true)
			releaseEntityAuthority()
			for _, release := range mergeReleases {
				release()
			}
			if acceptedRelease != nil {
				acceptedRelease()
			}

		}
	}
	cw.beforeLarge = tx.acquireCommitPublicationLocked
	cw.onBatch = func(next *badger.Txn) { tx.badgerTx = next }
	tx.commitW = cw
	if hasWrites {
		if err := tx.engine.materializeMVCCCommit(&cw.batchWriter, version, physicalOperations); err != nil {
			return tx.abortCommitLocked(fmt.Errorf("materializing mvcc commit state: %w", err))
		}
		if err := tx.writeChangeEventsLocked(&cw.batchWriter, physicalOperations, version.CommitTimestamp); err != nil {
			return tx.abortCommitLocked(fmt.Errorf("writing change events: %w", err))
		}
	}
	if err := tx.flushBufferedWrites(&cw.batchWriter); err != nil {
		return tx.abortCommitLocked(fmt.Errorf("flushing buffered writes: %w", err))
	}
	if err := tx.engine.refreshTemporalCurrentPointers(&cw.batchWriter, temporalTargets, temporalPointerState{pending: tx.pendingNodes, deleted: tx.deletedNodes}); err != nil {
		return tx.abortCommitLocked(fmt.Errorf("refreshing temporal current pointers: %w", err))
	}

	// Scalar count deltas ride the commit's own transaction (blind unique
	// keys, no conflict risk) so each statement passes through Badger's
	// commit queue once instead of once for data and once per count tier
	// (PR #1043 retest @39f0f03f, issue C). The positional (label, type)
	// tier stays a read-modify-write after finish below, under its lock.
	if err := tx.writeScalarCountDeltasInCommitLocked(&cw.batchWriter); err != nil {
		return tx.abortCommitLocked(fmt.Errorf("staging scalar count deltas in commit: %w", err))
	}

	// Stage the monotonic ID-counter high-water marks for out-of-txn
	// persistence (persistCounters, below, in a fresh transaction). Writing
	// these metadata keys inside tx.badgerTx would put concurrent commits on
	// shared Badger keys, causing concurrent commits to race on Badger's
	// optimistic conflict check and surface "Transaction Conflict" instead
	// of the genuine commit-time UNIQUE shape; the property-key tokens are
	// persisted outside it for the same reason (commitWriter.finish).
	// Materialization can allocate numIDs, so this follows it.
	idCounterNodeMax, idCounterEdgeMax := tx.engine.idDict.flushTxnCounters(tx.badgerTx)
	committingTxn := tx.badgerTx

	// From here to the end of the tail the derived counts this commit
	// changes stay locked; see acquireCommitPublicationLocked. A large
	// commit acquired them before its first batch.
	tx.acquireCommitPublicationLocked()

	// Commit Badger transaction (atomic!). A large commit writes its last
	// batch and only then becomes visible, all at once.
	if err := cw.finish(); err != nil {
		if !errors.Is(err, errPersistingTokens) && !errors.Is(err, badger.ErrConflict) {
			err = fmt.Errorf("badger commit failed: %w", err)
		}
		return tx.abortCommitLocked(err)
	}
	tx.engine.idDict.markTxnCommitted(committingTxn)
	tx.commitW = nil
	// Later acknowledgements must order after this committed image.
	order.release()
	runCommitTailHook()

	tx.endChangeCaptureLocked(true)

	// Scalar label and edge-type count deltas were written atomically with
	// the entity commit above (writeScalarCountDeltasInCommitLocked); a
	// conflict there already aborted the whole commit, so there is no
	// separate follow-up transaction for them. The positional (label, type)
	// tier is still a read-modify-write on shared absolute keys, applied
	// below under edgeTypeCountWriteMu: release first lets concurrent
	// commits run the positional update without the lock, causing Badger
	// conflicts, a rebuild storm, and ~2x counts (PR #1043 item #2).

	// Persist the positional counters only after the entity and index writes
	// commit, serialized under edgeTypeCountWriteMu so typed-count readers
	// never observe the publication gap. A metadata failure must not turn a
	// successful user commit into an apparent rollback — rebuild from the
	// committed edge-type index immediately; startup verification remains
	// the final recovery boundary.
	if err := tx.engine.applyEdgeTypeLabelCountDeltasLocked(tx.pendingEdgeTypeLabelCountDeltas); err != nil {
		if repairErr := tx.engine.rebuildAuthoritativeEdgeTypeCountsLocked(); repairErr != nil {
			tx.engine.log.Error("failed to repair derived (label, type) counts after transaction commit",
				"subsystem", "transaction",
				"transaction_id", tx.ID,
				slog.Any("update_error", err),
				slog.Any("repair_error", repairErr),
			)
		} else {
			tx.engine.log.Warn("repaired derived (label, type) counts after delta update failed",
				"subsystem", "transaction",
				"transaction_id", tx.ID,
				slog.Any("error", err),
			)
		}
	}
	tx.releaseEdgeTypeCountLockLocked()

	// Persist the namespace's MVCC sequence and the staged ID
	// counters in separate Badger transactions so those high-frequency
	// shared keys do not participate in the user transaction's
	// conflict-detection set. Concurrent commits in the same namespace
	// must not race on these writes — see allocateMVCCVersion's doc.
	if tx.namespace != "" && !tx.CommitVersion.IsZero() {
		tx.engine.persistMVCCSequence(tx.namespace)
	}
	tx.engine.idDict.persistCounters(tx.engine.db, idCounterNodeMax, idCounterEdgeMax)

	// Cache, index, unique-value and logical revision publication completed in
	// the writer's ordered memory-only stage. Durable follow-ups remain above;
	// listeners become ready only in the outer lock/admission release owner.

	// ACID GUARANTEE: Force fsync for EXPLICIT transactions only.
	// Explicit COMMIT must be durable before we return success (user asked
	// for a transaction, they get ACID-D). Implicit transactions (one per
	// Cypher statement under an auto-commit Bolt session) rely on the
	// session-end flush and the async engine's batched syncs — forcing an
	// Msync here once per UNWIND batch amplifies the syscall cost linearly
	// in batch count with no observable durability benefit over the
	// coalesced path. In-memory mode has no disk to sync.
	if !tx.engine.IsInMemory() && tx.commitMode == CommitModeExplicit {
		if err := tx.engine.Sync(); err != nil {
			// Transaction is committed in Badger but fsync failed.
			// Log error but don't rollback - data is in Badger's WAL.
			// D-10a transaction_id slog attribute replaces the prior bracket prefix.
			tx.engine.log.Warn("fsync failed after commit",
				"subsystem", "transaction",
				"transaction_id", tx.ID,
				slog.Any("error", err),
			)
		}
	}

	tx.closeLocked(TxStatusCommitted, false, nil)
	return nil

}

// validateCommitLocked keeps both optimistic and protected validation on the
// same constraint/conflict and consumer error-classification owner.
func (tx *BadgerTransaction) validateCommitLocked() error {
	if err := tx.resolveConnectedDeletesLocked(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}

	if err := tx.validateAllConstraints(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		// Wire contract: prefix "constraint violation:" is matched by downstream Bolt classifiers.
		// See docs/plans/consumer-pinned-error-contract-plan.md §2.1.
		return localizedError(localization.StorageTransactionConstraintViolation(err), err)
	}

	if err := tx.validateSnapshotIsolationConflicts(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		// MERGE-on-same-unique-value race: surface as the consumer-
		// pinned constraint-violation wire shape so retry classifiers
		// see the same prefix as a validate-time UNIQUE failure. The
		// generic SI conflict shape stays unwrapped.
		var cv *ConstraintViolationError
		if errors.As(err, &cv) && cv.Type == ConstraintUnique {
			return localizedError(localization.StorageTransactionConstraintViolation(err), err)
		}
		return err
	}

	return nil
}
