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
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

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
		ops:       physicalOps,
		writes:    tx.pendingWrites,
		deletes:   tx.pendingDeletes,
	}
	c.counterNodeMax, c.counterEdgeMax = tx.engine.idDict.flushTxnCounters(tx.badgerTx)
	c.hasCounters = true
	c.propKeyDrain = tx.engine.propKeyDict.flushTxnCounters(tx.badgerTx)
	// Capture the derived-count deltas the sync path applies after commit;
	// replay merges and applies them under the count locks.
	if len(tx.pendingLabelCountDeltas) > 0 {
		c.labelDeltas = cloneInt64Map(tx.pendingLabelCountDeltas)
	}
	if len(tx.pendingEdgeTypeCountDeltas) > 0 {
		c.edgeTypeDeltas = cloneInt64Map(tx.pendingEdgeTypeCountDeltas)
	}
	if len(tx.pendingEdgeTypeLabelCountDeltas) > 0 {
		c.edgeTypeLabelDeltas = cloneInt64Map(tx.pendingEdgeTypeLabelCountDeltas)
	}

	tx.releaseSnapshotReaderLocked()
	tx.bufferedStagingTransferred = true
	tx.engine.writeBehind.AppendCommit(c)
	tx.closeLocked(TxStatusCommitted, true, nil)
	return nil
}

func cloneInt64Map[K comparable](m map[K]int64) map[K]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[K]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// replayWriteBehind replays one drained generation into Badger: a single
// Badger transaction applies every buffered statement's physical operations
// and raw KV writes, then the commit tail runs (MVCC sequence persistence,
// ID counters, derived counts, caches, callbacks).
func (b *BadgerEngine) replayWriteBehind(buf *CommitBuffer) error {
	commits := buf.Commits()
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
		if abortErr := cw.abort(); abortErr != nil {
			err = fmt.Errorf("%w (rolling back: %v)", err, abortErr)
		}
		cw.discard()
		return err
	}

	for _, c := range commits {
		if _, ok := versions[c.namespace]; !ok {
			v, err := b.allocateMVCCVersion(txn, c.namespace, time.Now())
			if err != nil {
				return abort(err)
			}
			versions[c.namespace] = v
		}
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
		// normal commit path does (they never enter the user txn).
		if err := b.propKeyDict.persistTxnCounters(b.db, c.propKeyDrain); err != nil {
			return abort(err)
		}
		if err := b.materializeMVCCCommit(w, versions[c.namespace], c.ops); err != nil {
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

	if err := cw.finish(); err != nil {
		if holdEdge {
			b.edgeTypeCountWriteMu.Unlock()
		}
		if holdLabel {
			b.labelCountWriteMu.Unlock()
		}
		return abort(err)
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
	schemas := make(map[string]*SchemaManager, 1)
	for _, c := range commits {
		schema := schemas[c.namespace]
		if schema == nil && c.namespace != "" {
			schema = b.GetSchemaForNamespace(c.namespace)
			schemas[c.namespace] = schema
		}
		b.publishCommitOperations(c.namespace, schema, c.ops)
	}
	return nil
}

// publishCommitOperations mirrors the commit tail's callback and unique-value
// cache updates for one buffered statement.
func (b *BadgerEngine) publishCommitOperations(namespace string, schema *SchemaManager, ops []Operation) {
	for _, op := range ops {
		switch op.Type {
		case OpCreateNode:
			b.cacheOnNodeCreated(op.Node)
			b.notifyNodeCreated(op.Node)
		case OpUpdateNode:
			b.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
			b.notifyNodeUpdated(op.Node)
		case OpDeleteNode:
			if op.OldNode != nil {
				b.cacheOnNodeDeletedWithLabels(op.NodeID, op.OldNode.Labels, op.EdgesDeleted)
			} else {
				b.cacheOnNodeDeleted(op.NodeID, op.EdgesDeleted)
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				b.notifyEdgeDeleted(edgeID)
			}
			b.notifyNodeDeleted(op.NodeID)
		case OpCreateEdge:
			b.cacheOnEdgeCreated(op.Edge)
			b.notifyEdgeCreated(op.Edge)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.cacheOnEdgeUpdated(oldType, op.Edge)
			b.notifyEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			b.cacheOnEdgeDeleted(op.EdgeID, oldType)
			b.notifyEdgeDeleted(op.EdgeID)
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
// no-op when write-behind is disabled.
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
