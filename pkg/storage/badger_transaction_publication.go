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
func (tx *BadgerTransaction) CommitContextWithPublication(ctx context.Context, afterPublication func()) (commitErr error) {
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
	var notification *commitNotification
	defer func() {
		tx.mu.Unlock()
		if commitErr == nil && afterPublication != nil {
			afterPublication()
		}
		tx.engine.completeCommitNotification(notification)
		if knowledgePolicyChanged != nil {
			knowledgePolicyChanged()
		}
	}()
	defer tx.releaseWriteBarrierLocked()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}
	// The engine's write barrier is held through publication, before listener
	// delivery (releaseWriteBarrierLocked): exclusively for a schema transaction, so
	// no data write publishes between its snapshot check and its schema
	// publication; shared otherwise, so Close waits for the commit and a
	// commit that arrives after Close finished fails here without touching
	// Badger.
	var releaseWrite func()
	var writeErr error
	if tx.schemaRuntime != nil {
		releaseWrite, writeErr = tx.engine.beginSchemaWrite()
	} else {
		releaseWrite, writeErr = tx.engine.beginWrite()
	}
	if writeErr != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return writeErr
	}
	tx.commitReleaseWrite = releaseWrite
	if tx.schemaRuntime != nil && tx.engine.currentMVCCReadVersion(tx.namespace).CommitSequence != tx.readTS.CommitSequence {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("schema snapshot changed before commit: %w", ErrConflict)
	}

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
	releaseCommitLocks, err := tx.acquireUniqueConstraintCommitLocks()
	if err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}
	defer releaseCommitLocks()

	// Nodes a non-DETACH DELETE left connected: deleted now, or the commit
	// fails (DeleteConnectedNode).
	if err := tx.resolveConnectedDeletesLocked(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return err
	}

	// Final constraint validation before commit
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

	temporalTargets, err := tx.bufferTemporalIndexWrites()
	if err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("buffering temporal index writes: %w", err)
	}

	// Log metadata. D-10a bracket-prefix migrated to a transaction_id slog
	// attribute so log aggregators can group commits by transaction without
	// parsing message text.
	if len(tx.Metadata) > 0 {
		tx.engine.log.Debug("transaction committing with metadata",
			"subsystem", "transaction",
			"transaction_id", tx.ID,
			slog.Any("metadata", tx.Metadata),
		)
	}

	if err := tx.materializeDeferredEdgesLocked(); err != nil {
		return tx.abortCommitLocked(err)
	}

	// Write-behind buffering: an implicit autocommit commit with no schema,
	// knowledge-policy or temporal writes is acknowledged after every
	// validation phase above and replayed into Badger by the background
	// flusher. Everything that must fail the statement synchronously has
	// already run by this point. (Commit holds tx.mu, so read the
	// knowledge-policy flag directly instead of HasKnowledgePolicyChanges.)
	// A database that captures changes commits through Badger: its change
	// events are written in the same commit as the data (#937).
	if tx.engine.writeBehindEnabled() && tx.implicit && tx.schemaRuntime == nil &&
		!tx.knowledgeSchemaDirty && len(temporalTargets) == 0 &&
		(tx.namespace == "" || tx.engine.ChangeCapture(tx.namespace) == ChangeCaptureOff) {
		return tx.commitBufferedLocked()
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

	cw := tx.engine.newCommitWriter(tx.badgerDB, tx.badgerTx)
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
	if err := tx.refreshTemporalCurrentPointers(&cw.batchWriter, temporalTargets); err != nil {
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
	knowledgePolicyChanged = tx.publishKnowledgePolicySchemaLocked()
	if len(tx.operations) > 0 {
		notification = tx.engine.queueCommitOperations(tx.operations, nil)
	}
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

	// Apply cache/count updates and fire callbacks after commit.
	// This keeps cached stats O(1) and ensures external systems (e.g. search indexes)
	// observe transactional writes the same way as non-transactional writes.
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			tx.engine.cacheOnNodeCreated(op.Node)
		case OpUpdateNode:
			tx.engine.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
		case OpDeleteNode:
			if op.OldNode != nil {
				tx.engine.cacheOnNodeDeletedWithLabels(op.NodeID, op.OldNode.Labels, op.EdgesDeleted)
			} else {
				tx.engine.cacheOnNodeDeleted(op.NodeID, op.EdgesDeleted)
			}
		case OpCreateEdge:
			tx.engine.cacheOnEdgeCreatedWithCounts(op.Edge, !op.ReplacesExisting)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeUpdated(oldType, op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeDeleted(op.EdgeID, oldType)
		case OpUpdateEmbedding:
			// Embeddings are regenerable; no-op for cached counts.
		}
	}

	tx.engine.prepareCommitOperations(tx.operations, notification != nil)

	// Update derived unique-constraint caches (in-memory) based on committed operations.
	// This keeps non-transactional CreateNode() uniqueness checks consistent with transactional writes.
	//
	// All committed ops belong to the transaction's pinned namespace, so the
	// schema lookup is hoisted out of the loop.
	//
	// NOTE: We don't persist these caches; they are rebuilt from stored nodes on startup.
	if tx.namespace != "" {
		schema := tx.engine.GetSchemaForNamespace(tx.namespace)
		if schema != nil {
			for _, op := range tx.operations {
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

	// ACID GUARANTEE: Force fsync for EXPLICIT transactions only.
	// Explicit COMMIT must be durable before we return success (user asked
	// for a transaction, they get ACID-D). Implicit transactions (one per
	// Cypher statement under an auto-commit Bolt session) rely on the
	// session-end flush and the async engine's batched syncs — forcing an
	// Msync here once per UNWIND batch amplifies the syscall cost linearly
	// in batch count with no observable durability benefit over the
	// coalesced path. In-memory mode has no disk to sync.
	if !tx.engine.IsInMemory() && !tx.implicit {
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
