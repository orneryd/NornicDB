// Package storage - BadgerDB transaction wrapper with ACID guarantees.
//
// This file implements atomic transactions for BadgerDB with full constraint
// validation and rollback support.
package storage

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
)

// BadgerTransaction wraps Badger's native transaction with constraint validation.
//
// Provides ACID guarantees:
//   - Atomicity: All operations commit together or none do
//   - Consistency: Constraints are validated before commit
//   - Isolation: Changes invisible until commit
//   - Durability: Badger's WAL ensures persistence
type BadgerTransaction struct {
	mu sync.Mutex

	// Transaction identity
	ID        string
	StartTime time.Time
	Status    TransactionStatus
	readTS    MVCCVersion
	// CommitVersion is assigned once for a successful commit that mutates storage.
	CommitVersion MVCCVersion

	// Badger's native transaction
	badgerTx *badger.Txn

	// Parent engine for constraint validation
	engine *BadgerEngine

	// Track operations for constraint validation
	pendingNodes map[NodeID]*Node
	pendingEdges map[EdgeID]*Edge
	deletedNodes map[NodeID]struct{}
	deletedEdges map[EdgeID]struct{}
	operations   []Operation

	// Buffered writes - collected during transaction, flushed at commit
	// This batches all writes together for better performance while maintaining ACID guarantees
	pendingWrites  map[string][]byte // key -> value for Set operations
	pendingDeletes map[string]bool   // key -> true for Delete operations
	// When true, skip per-operation constraint checks and validate at commit only.
	deferConstraintValidation bool
	// When true, skip read-before-write existence checks for CREATE operations.
	skipCreateExistenceCheck bool
	// implicit marks transactions that were auto-opened by the executor for a
	// single Cypher statement outside an explicit BEGIN/COMMIT. The Bolt
	// session coalesces durability at the end of the session via the
	// deferFlush path (pkg/bolt/server.go), so the per-Commit engine.Sync()
	// in Commit() is redundant — dropping it collapses ~N fsyncs/Msyncs to
	// the session-end flush without weakening the durability contract users
	// actually rely on (explicit tx commit and session close).
	implicit bool

	// Transaction metadata (for logging/debugging)
	Metadata           map[string]interface{}
	snapshotReaderInfo SnapshotReaderInfo
	snapshotDeregister func()
	closedErr          error
}

func (b *BadgerEngine) currentMVCCReadVersion() MVCCVersion {
	return MVCCVersion{
		CommitTimestamp: time.Now().UTC(),
		CommitSequence:  b.mvccSeq.Load(),
	}
}

// BeginTransaction starts a new Badger transaction with ACID guarantees.
func (b *BadgerEngine) BeginTransaction() (*BadgerTransaction, error) {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return nil, fmt.Errorf("engine is closed")
	}
	badgerDB := b.db
	b.mu.RUnlock()

	readTS := b.currentMVCCReadVersion()
	txID := generateTxID()
	startTime := time.Now()
	readerInfo := SnapshotReaderInfo{
		ReaderID:        txID,
		SnapshotVersion: readTS,
		StartTime:       startTime,
	}
	var deregister func()
	badgerTx := badgerDB.NewTransaction(true)
	if !readTS.IsZero() {
		var err error
		deregister, err = b.acquireSnapshotReader(readerInfo)
		if err != nil {
			badgerTx.Discard()
			return nil, err
		}
	}

	return &BadgerTransaction{
		ID:                 txID,
		StartTime:          startTime,
		Status:             TxStatusActive,
		readTS:             readTS,
		badgerTx:           badgerTx,
		engine:             b,
		pendingNodes:       make(map[NodeID]*Node),
		pendingEdges:       make(map[EdgeID]*Edge),
		deletedNodes:       make(map[NodeID]struct{}),
		deletedEdges:       make(map[EdgeID]struct{}),
		operations:         make([]Operation, 0),
		pendingWrites:      make(map[string][]byte),
		pendingDeletes:     make(map[string]bool),
		Metadata:           make(map[string]interface{}),
		snapshotReaderInfo: readerInfo,
		snapshotDeregister: deregister,
	}, nil
}

// IsActive returns true if the transaction is still active.
func (tx *BadgerTransaction) IsActive() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.Status == TxStatusActive
}

func (tx *BadgerTransaction) closeLocked(status TransactionStatus, discard bool, closedErr error) {
	if tx.badgerTx != nil && tx.engine != nil && tx.engine.idDict != nil {
		// Drop any staged counter state. Safe whether the txn
		// committed (flushTxnCounters already cleared it) or is being
		// rolled back (we don't persist counters for aborted work).
		tx.engine.idDict.discardTxnCounters(tx.badgerTx)
	}
	if discard && tx.badgerTx != nil {
		tx.badgerTx.Discard()
	}
	tx.pendingWrites = make(map[string][]byte)
	tx.pendingDeletes = make(map[string]bool)
	tx.Status = status
	tx.closedErr = closedErr
	if tx.snapshotDeregister != nil {
		tx.snapshotDeregister()
		tx.snapshotDeregister = nil
	}
}

func (tx *BadgerTransaction) ensureLifecycleActiveLocked() error {
	if tx.Status != TxStatusActive {
		if tx.closedErr != nil {
			return tx.closedErr
		}
		return ErrTransactionClosed
	}
	if tx.snapshotReaderInfo.StartTime.IsZero() || tx.engine == nil {
		return nil
	}
	graceful, hard := tx.engine.evaluateSnapshotReader(tx.snapshotReaderInfo)
	if hard {
		tx.closeLocked(TxStatusRolledBack, true, ErrMVCCSnapshotHardExpired)
		return ErrMVCCSnapshotHardExpired
	}
	if graceful {
		tx.closeLocked(TxStatusRolledBack, true, ErrMVCCSnapshotGracefulCancel)
		return ErrMVCCSnapshotGracefulCancel
	}
	return nil
}

// SetDeferredConstraintValidation controls per-operation constraint checks.
// When enabled, constraints are enforced at commit time only.
func (tx *BadgerTransaction) SetDeferredConstraintValidation(deferValidation bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	tx.deferConstraintValidation = deferValidation
	return nil
}

// SetSkipCreateExistenceCheck controls read-before-write checks for CREATE.
// When enabled, CREATE skips the storage existence read for UUID IDs.
func (tx *BadgerTransaction) SetSkipCreateExistenceCheck(skip bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	tx.skipCreateExistenceCheck = skip
	return nil
}

// SetImplicit marks this transaction as implicit (auto-opened by the executor
// for a single Cypher statement, no user BEGIN). Implicit transactions skip
// the per-Commit engine.Sync() because the Bolt session end and the async
// flush loop coalesce durability for them.
func (tx *BadgerTransaction) SetImplicit(implicit bool) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	tx.implicit = implicit
	return nil
}

// bufferSet buffers a write operation to be applied at commit time.
// If the key was previously marked for deletion, it's removed from deletes.
func (tx *BadgerTransaction) bufferSet(key []byte, value []byte) {
	keyStr := string(key)
	// Remove from deletes if it was marked for deletion
	delete(tx.pendingDeletes, keyStr)
	// Buffer the write (copy value to avoid aliasing)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	tx.pendingWrites[keyStr] = valueCopy
}

// bufferDelete buffers a delete operation to be applied at commit time.
// If the key was previously buffered for write, it's removed from writes.
func (tx *BadgerTransaction) bufferDelete(key []byte) {
	keyStr := string(key)
	// Remove from writes if it was buffered
	delete(tx.pendingWrites, keyStr)
	// Mark for deletion
	tx.pendingDeletes[keyStr] = true
}

// bufferSetEdgeBetweenIndexes stages both exact relationship lookup indexes.
// Allocates numeric IDs for endpoints + edge via the engine's id
// dictionary. Any allocation failure propagates as a returned error.
func (tx *BadgerTransaction) bufferSetEdgeBetweenIndexes(edge *Edge) error {
	startNum, err := tx.engine.idDict.resolveOrAllocateNodeNumIDInTxn(tx.badgerTx, edge.StartNode)
	if err != nil {
		return err
	}
	endNum, err := tx.engine.idDict.resolveOrAllocateNodeNumIDInTxn(tx.badgerTx, edge.EndNode)
	if err != nil {
		return err
	}
	edgeNum, err := tx.engine.idDict.resolveOrAllocateEdgeNumIDInTxn(tx.badgerTx, edge.ID)
	if err != nil {
		return err
	}
	tx.bufferSet(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum), []byte(edge.ID))
	tx.bufferSet(edgeBetweenHeadKey(startNum, endNum, edge.Type), []byte(edge.ID))
	return nil
}

// bufferDeleteEdgeBetweenIndexes stages set removal and conservatively clears
// the head so later reads can self-heal from the set or legacy outgoing index.
// A missing numID means no index entry exists — nothing to delete.
func (tx *BadgerTransaction) bufferDeleteEdgeBetweenIndexes(edge *Edge) {
	startNum, sOK := tx.engine.idDict.lookupNodeNumID(edge.StartNode)
	endNum, eOK := tx.engine.idDict.lookupNodeNumID(edge.EndNode)
	edgeNum, edgeOK := tx.engine.idDict.lookupEdgeNumID(edge.ID)
	if !sOK || !eOK || !edgeOK {
		return
	}
	tx.bufferDelete(edgeBetweenIndexKey(startNum, endNum, edge.Type, edgeNum))
	tx.bufferDelete(edgeBetweenHeadKey(startNum, endNum, edge.Type))
}

// flushBufferedWrites applies all buffered writes and deletes to the Badger transaction.
// This is called at commit time to batch all writes together.
func (tx *BadgerTransaction) flushBufferedWrites() error {
	// Apply deletes first (in case a key is both written and deleted, delete wins)
	for keyStr := range tx.pendingDeletes {
		key := []byte(keyStr)
		if err := tx.badgerTx.Delete(key); err != nil {
			return fmt.Errorf("flushing delete for key %s: %w", keyStr, err)
		}
	}

	// Apply writes (only keys that weren't deleted)
	for keyStr, value := range tx.pendingWrites {
		// Skip if this key was also deleted (delete wins)
		if tx.pendingDeletes[keyStr] {
			continue
		}
		key := []byte(keyStr)
		if err := tx.badgerTx.Set(key, value); err != nil {
			return fmt.Errorf("flushing write for key %s: %w", keyStr, err)
		}
	}

	// Clear buffers after successful flush
	tx.pendingWrites = make(map[string][]byte)
	tx.pendingDeletes = make(map[string]bool)

	return nil
}

// CreateNode adds a node to the transaction with constraint validation.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (tx *BadgerTransaction) CreateNode(node *Node) (NodeID, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return "", err
	}

	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if node != nil && node.ID != "" && !strings.Contains(string(node.ID), ":") {
		return "", fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	// Validate constraints BEFORE writing
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
			return "", err
		}
	}

	// Check for duplicates in pending
	if _, exists := tx.pendingNodes[node.ID]; exists {
		return "", ErrAlreadyExists
	}

	// Check if exists in storage (read from Badger)
	skipExistenceCheck := tx.skipCreateExistenceCheck && shouldSkipCreateExistenceCheck(node.ID)
	if !skipExistenceCheck {
		if _, deleted := tx.deletedNodes[node.ID]; !deleted {
			_, err := tx.getCommittedNodeLocked(node.ID)
			if err == nil {
				return "", ErrAlreadyExists
			}
			if err != ErrNotFound {
				return "", fmt.Errorf("checking node existence: %w", err)
			}
		}
	}

	// PERFORMANCE OPTIMIZATION: Buffer all writes and flush at commit time
	// This batches all writes together for better performance while maintaining ACID guarantees

	// Serialize node (may store embeddings separately if too large)
	data, embeddingsSeparate, err := encodeNode(node)
	if err != nil {
		return "", fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	// Buffer node write
	tx.bufferSet(key, data)

	// If embeddings are stored separately, buffer them
	if embeddingsSeparate {
		for i, emb := range node.ChunkEmbeddings {
			embKey := embeddingKey(node.ID, i)
			embData, err := encodeEmbedding(emb)
			if err != nil {
				return "", fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
			}
			tx.bufferSet(embKey, embData)
		}
	}

	// Buffer all label index writes
	for _, label := range node.Labels {
		indexKey, err := tx.engine.labelIndexKeyString(tx.badgerTx, label, node.ID)
		if err != nil {
			return "", fmt.Errorf("label index: %w", err)
		}
		tx.bufferSet(indexKey, []byte{})
	}

	// Add to pending embeddings index if needed
	if tx.engine.shouldIndexPendingEmbed(node) {
		tx.bufferSet(pendingEmbedKey(node.ID), []byte{})
	}

	// Track for read-your-writes and constraint validation
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy
	delete(tx.deletedNodes, node.ID)

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
		// FreshID propagates the "caller asserts this ID is new and cannot
		// collide with a tombstoned MVCC head" contract into the commit
		// loop so it can skip the head-load round-trip. Gated on the same
		// UUID-shape heuristic that lets us skip the existence-read above.
		FreshID: skipExistenceCheck,
	})

	return node.ID, nil
}

// UpdateNode updates a node in the transaction.
func (tx *BadgerTransaction) UpdateNode(node *Node) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Validate constraints
	if !tx.deferConstraintValidation {
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}

	// Check if node exists
	var oldNode *Node
	createOpIdx := -1
	if pending, exists := tx.pendingNodes[node.ID]; exists {
		oldNode = copyNode(pending)
		createOpIdx = tx.pendingCreateNodeOperationIndexLocked(node.ID)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(node.ID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading node: %w", err)
		}
	}

	// Validate policy constraints on label changes.
	if !tx.deferConstraintValidation && !labelsEqual(node.Labels, oldNode.Labels) {
		if err := tx.validatePolicyOnNodeLabelChange(node, oldNode); err != nil {
			return err
		}
	}

	// Buffer updated node write
	nodeBytes, err := serializeNode(node)
	if err != nil {
		return fmt.Errorf("serializing node: %w", err)
	}

	key := nodeKey(node.ID)
	tx.bufferSet(key, nodeBytes)

	// Update label indexes if changed
	oldLabelSet := make(map[string]bool)
	for _, label := range oldNode.Labels {
		oldLabelSet[label] = true
	}

	newLabelSet := make(map[string]bool)
	for _, label := range node.Labels {
		newLabelSet[label] = true
		if !oldLabelSet[label] {
			// New label - buffer index write
			indexKey, err := tx.engine.labelIndexKeyString(tx.badgerTx, label, node.ID)
			if err != nil {
				return fmt.Errorf("label index: %w", err)
			}
			tx.bufferSet(indexKey, []byte{})
		}
	}

	// Remove old labels (lookup-only — they must have existed at write time)
	for _, label := range oldNode.Labels {
		if !newLabelSet[label] {
			indexKey := tx.engine.labelIndexKeyStringLookup(label, node.ID)
			if indexKey == nil {
				continue
			}
			tx.bufferDelete(indexKey)
		}
	}

	// Track for read-your-writes
	nodeCopy := copyNode(node)
	tx.pendingNodes[node.ID] = nodeCopy
	if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
		tx.bufferDelete(pendingEmbedKey(node.ID))
	} else if tx.engine.shouldIndexPendingEmbed(node) {
		tx.bufferSet(pendingEmbedKey(node.ID), []byte{})
	} else {
		tx.bufferDelete(pendingEmbedKey(node.ID))
	}

	if createOpIdx >= 0 {
		tx.operations[createOpIdx].Node = nodeCopy
		tx.operations[createOpIdx].Timestamp = time.Now()
		return nil
	}

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateNode,
		Timestamp: time.Now(),
		NodeID:    node.ID,
		Node:      nodeCopy,
		OldNode:   oldNode,
	})

	return nil
}

func (tx *BadgerTransaction) pendingCreateNodeOperationIndexLocked(nodeID NodeID) int {
	for i := len(tx.operations) - 1; i >= 0; i-- {
		op := tx.operations[i]
		if op.NodeID != nodeID {
			continue
		}
		switch op.Type {
		case OpDeleteNode:
			return -1
		case OpCreateNode:
			return i
		}
	}
	return -1
}

// deleteNodeBuffered deletes a node and all its edges/embeddings, buffering all writes.
// This is the buffering version of BadgerEngine.deleteNodeInTxn.
func (tx *BadgerTransaction) deleteNodeBuffered(nodeID NodeID, oldNode *Node) (edgesDeleted int64, deletedEdgeIDs []EdgeID, err error) {
	key := nodeKey(nodeID)

	// Buffer deletion of separately stored embeddings
	embPrefix := embeddingPrefix(nodeID)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = embPrefix
	it := tx.badgerTx.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		tx.bufferDelete(it.Item().Key())
	}

	// Get node for label cleanup (if not already provided)
	var deletedNode *Node
	if oldNode != nil {
		deletedNode = oldNode
	} else {
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist, but we've already buffered embedding cleanup.
			// Also buffer pending embeddings index deletion.
			tx.bufferDelete(pendingEmbedKey(nodeID))
			return 0, nil, ErrNotFound
		}
		if err != nil {
			return 0, nil, err
		}

		if err := item.Value(func(val []byte) error {
			var decodeErr error
			// Extract nodeID from key (skip prefix byte)
			nodeIDFromKey := NodeID(key[1:])
			deletedNode, decodeErr = decodeNodeWithEmbeddings(tx.badgerTx, val, nodeIDFromKey)
			return decodeErr
		}); err != nil {
			return 0, nil, err
		}
	}

	// Archive the node body at its current head version BEFORE we
	// buffer the primary-key delete. Gated internally on
	// mustArchiveForHistory — no-op when retention is head-only AND no
	// snapshot reader needs the pre-delete view.
	if head, headErr := tx.engine.loadNodeMVCCHeadInTxn(tx.badgerTx, nodeID); headErr == nil && !head.Tombstoned {
		if err := tx.engine.archiveNodeBodyInTxn(tx.badgerTx, nodeID, deletedNode, head.Version); err != nil {
			return 0, nil, err
		}
	} else if headErr != nil && headErr != ErrNotFound {
		return 0, nil, headErr
	}

	// Buffer label index deletions (lookup-only).
	for _, label := range deletedNode.Labels {
		if lblKey := tx.engine.labelIndexKeyStringLookup(label, nodeID); lblKey != nil {
			tx.bufferDelete(lblKey)
		}
	}

	// Delete outgoing edges (and track count). Lookup-only prefix — a
	// missing numID means no outgoing edges were ever indexed.
	if outPrefix := tx.engine.outgoingIndexPrefixString(nodeID); outPrefix != nil {
		outCount, outIDs, err := tx.deleteEdgesWithPrefixBuffered(outPrefix)
		if err != nil {
			return 0, nil, err
		}
		edgesDeleted += outCount
		deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)
	}

	// Delete incoming edges (and track count).
	if inPrefix := tx.engine.incomingIndexPrefixString(nodeID); inPrefix != nil {
		inCount, inIDs, err := tx.deleteEdgesWithPrefixBuffered(inPrefix)
		if err != nil {
			return 0, nil, err
		}
		edgesDeleted += inCount
		deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)
	}

	// Buffer pending embeddings index deletion
	tx.bufferDelete(pendingEmbedKey(nodeID))

	// Buffer node deletion. Dict cleanup is deferred to the prune
	// pipeline — tombstones + MVCC heads reuse the existing numID.
	tx.bufferDelete(key)

	return edgesDeleted, deletedEdgeIDs, nil
}

// deleteEdgesWithPrefixBuffered deletes all edges with a given prefix, buffering writes.
func (tx *BadgerTransaction) deleteEdgesWithPrefixBuffered(prefix []byte) (int64, []EdgeID, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := tx.badgerTx.NewIterator(opts)
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
		if !ok {
			continue
		}
		edgeID, ok := tx.engine.idDict.lookupEdgeIDByNum(edgeNum)
		if !ok {
			continue
		}
		edgeIDs = append(edgeIDs, edgeID)
	}

	var deletedCount int64
	var deletedIDs []EdgeID
	for _, edgeID := range edgeIDs {
		// Get edge to delete its indexes
		edgeKey := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(edgeKey)
		if err == badger.ErrKeyNotFound {
			continue
		}
		if err != nil {
			return 0, nil, err
		}

		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return 0, nil, err
		}

		edge, err := tx.engine.decodeEdgeBody(edgeBytes)
		if err != nil {
			return 0, nil, err
		}
		edge.ID = edgeID

		// Archive the edge body at its current head version BEFORE we
		// buffer the primary-key delete. Safe to call unconditionally —
		// mustArchiveForHistory gates the actual write.
		if head, headErr := tx.engine.loadEdgeMVCCHeadInTxn(tx.badgerTx, edgeID); headErr == nil && !head.Tombstoned {
			if err := tx.engine.archiveEdgeBodyInTxn(tx.badgerTx, edgeID, edge, head.Version); err != nil {
				return 0, nil, err
			}
		} else if headErr != nil && headErr != ErrNotFound {
			return 0, nil, headErr
		}

		// Buffer edge and index deletions. Lookup-only: these num IDs
		// must have existed at write time.
		tx.bufferDelete(edgeKey)
		if outKey := tx.engine.outgoingIndexKeyStringLookup(edge.StartNode, edgeID); outKey != nil {
			tx.bufferDelete(outKey)
		}
		if inKey := tx.engine.incomingIndexKeyStringLookup(edge.EndNode, edgeID); inKey != nil {
			tx.bufferDelete(inKey)
		}
		if typeKey := tx.engine.edgeTypeIndexKeyStringLookup(edge.Type, edgeID); typeKey != nil {
			tx.bufferDelete(typeKey)
		}
		tx.bufferDeleteEdgeBetweenIndexes(edge)

		deletedCount++
		deletedIDs = append(deletedIDs, edgeID)
	}

	return deletedCount, deletedIDs, nil
}

// DeleteNode deletes a node from the transaction.
func (tx *BadgerTransaction) DeleteNode(nodeID NodeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Capture old node state for constraint bookkeeping (e.g., unique value unregister).
	var oldNode *Node
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		oldNode = copyNode(pending)
	} else {
		var err error
		oldNode, err = tx.getCommittedNodeLocked(nodeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
	}

	// Delete with the same semantics as BadgerEngine.DeleteNode (cascade edges + embedding cleanup),
	// but buffer all writes for batch commit.
	edgesDeleted, deletedEdgeIDs, err := tx.deleteNodeBuffered(nodeID, oldNode)
	if err != nil {
		return err
	}

	// Track deletion
	delete(tx.pendingNodes, nodeID)
	tx.deletedNodes[nodeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:           OpDeleteNode,
		Timestamp:      time.Now(),
		NodeID:         nodeID,
		OldNode:        oldNode,
		EdgesDeleted:   edgesDeleted,
		DeletedEdgeIDs: deletedEdgeIDs,
	})

	return nil
}

// CreateEdge adds an edge to the transaction.
func (tx *BadgerTransaction) CreateEdge(edge *Edge) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Check nodes exist
	if !tx.nodeExists(edge.StartNode) {
		return fmt.Errorf("start node %s does not exist", edge.StartNode)
	}
	if !tx.nodeExists(edge.EndNode) {
		return fmt.Errorf("end node %s does not exist", edge.EndNode)
	}

	// Check for duplicate
	if _, exists := tx.pendingEdges[edge.ID]; exists {
		return ErrAlreadyExists
	}

	// Serialize and buffer write. Compact form allocates endpoint
	// numIDs via the id dictionary — keeps bodies tight.
	edgeBytes, err := tx.engine.encodeEdgeInTxn(tx.badgerTx, edge)
	if err != nil {
		return fmt.Errorf("serializing edge: %w", err)
	}

	key := edgeKey(edge.ID)
	tx.bufferSet(key, edgeBytes)

	// Buffer edge indexes. Keys use 8-byte num IDs from the engine dict.
	outKey, err := tx.engine.outgoingIndexKeyString(tx.badgerTx, edge.StartNode, edge.ID)
	if err != nil {
		return fmt.Errorf("outgoing index: %w", err)
	}
	tx.bufferSet(outKey, []byte{})

	inKey, err := tx.engine.incomingIndexKeyString(tx.badgerTx, edge.EndNode, edge.ID)
	if err != nil {
		return fmt.Errorf("incoming index: %w", err)
	}
	tx.bufferSet(inKey, []byte{})

	// Buffer edge type index for GetEdgesByType().
	// Without this, edges created inside implicit/explicit transactions are invisible
	// to type-based scans and Cypher fast-paths that rely on the edge-type index.
	typeKey, err := tx.engine.edgeTypeIndexKeyString(tx.badgerTx, edge.Type, edge.ID)
	if err != nil {
		return fmt.Errorf("edge type index: %w", err)
	}
	tx.bufferSet(typeKey, []byte{})
	if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
		return fmt.Errorf("edge-between index: %w", err)
	}

	// Track for read-your-writes
	edgeCopy := copyEdge(edge)
	tx.pendingEdges[edge.ID] = edgeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpCreateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
		FreshID:   hasUUIDShape(string(edge.ID)),
	})

	return nil
}

// BulkCreateEdges buffers many edges in one pass, amortizing the lock, the
// lifecycle check, and — most importantly — the committed-node existence
// probes. A batch of N edges that share K distinct endpoint nodes (K ≪ 2N in
// practice for graph fan-in/fan-out) now costs K Badger point-reads instead
// of up to 2N.
func (tx *BadgerTransaction) BulkCreateEdges(edges []*Edge) error {
	if len(edges) == 0 {
		return nil
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Shared existence cache for this batch. Populated lazily from
	// pendingNodes / deletedNodes / committed-storage state. A value of
	// true means "exists and is visible to this tx", false means "not
	// visible" (deleted or absent).
	existence := make(map[NodeID]bool, len(edges)*2)

	nodeVisible := func(id NodeID) bool {
		if cached, ok := existence[id]; ok {
			return cached
		}
		if _, deleted := tx.deletedNodes[id]; deleted {
			existence[id] = false
			return false
		}
		if _, pending := tx.pendingNodes[id]; pending {
			existence[id] = true
			return true
		}
		_, err := tx.getCommittedNodeLocked(id)
		ok := err == nil
		existence[id] = ok
		return ok
	}

	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if edge.ID == "" {
			return ErrInvalidID
		}

		if !nodeVisible(edge.StartNode) {
			return fmt.Errorf("start node %s does not exist", edge.StartNode)
		}
		if !nodeVisible(edge.EndNode) {
			return fmt.Errorf("end node %s does not exist", edge.EndNode)
		}

		if _, exists := tx.pendingEdges[edge.ID]; exists {
			return ErrAlreadyExists
		}

		edgeBytes, err := tx.engine.encodeEdgeInTxn(tx.badgerTx, edge)
		if err != nil {
			return fmt.Errorf("serializing edge: %w", err)
		}

		tx.bufferSet(edgeKey(edge.ID), edgeBytes)
		outKey, err := tx.engine.outgoingIndexKeyString(tx.badgerTx, edge.StartNode, edge.ID)
		if err != nil {
			return fmt.Errorf("outgoing index: %w", err)
		}
		tx.bufferSet(outKey, []byte{})
		inKey, err := tx.engine.incomingIndexKeyString(tx.badgerTx, edge.EndNode, edge.ID)
		if err != nil {
			return fmt.Errorf("incoming index: %w", err)
		}
		tx.bufferSet(inKey, []byte{})
		typeKey, err := tx.engine.edgeTypeIndexKeyString(tx.badgerTx, edge.Type, edge.ID)
		if err != nil {
			return fmt.Errorf("edge type index: %w", err)
		}
		tx.bufferSet(typeKey, []byte{})
		if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
			return fmt.Errorf("edge-between index: %w", err)
		}

		edgeCopy := copyEdge(edge)
		tx.pendingEdges[edge.ID] = edgeCopy
		// Newly created edge — existence cache should reflect that
		// subsequent batches referencing this edge's endpoints still
		// see them.
		existence[edge.StartNode] = true
		existence[edge.EndNode] = true

		tx.operations = append(tx.operations, Operation{
			Type:      OpCreateEdge,
			Timestamp: time.Now(),
			EdgeID:    edge.ID,
			Edge:      edgeCopy,
			FreshID:   hasUUIDShape(string(edge.ID)),
		})
	}

	return nil
}

// UpdateEdge updates an existing edge within the transaction.
//
// This is required so Cypher can do CREATE ... SET r.prop = ... in a single query
// while using implicit/explicit transactions (writes must remain isolated until commit).
func (tx *BadgerTransaction) UpdateEdge(edge *Edge) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}
	if _, deleted := tx.deletedEdges[edge.ID]; deleted {
		return ErrNotFound
	}

	// Load existing edge (pending or committed) for index maintenance.
	var oldEdge *Edge
	if pending, exists := tx.pendingEdges[edge.ID]; exists {
		oldEdge = copyEdge(pending)
	} else {
		var err error
		oldEdge, err = tx.getCommittedEdgeLocked(edge.ID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
	}

	// If endpoints changed, verify they exist and update outgoing/incoming indexes.
	if oldEdge.StartNode != edge.StartNode || oldEdge.EndNode != edge.EndNode {
		if !tx.nodeExists(edge.StartNode) {
			return fmt.Errorf("start node %s does not exist", edge.StartNode)
		}
		if !tx.nodeExists(edge.EndNode) {
			return fmt.Errorf("end node %s does not exist", edge.EndNode)
		}

		if oldOutKey := tx.engine.outgoingIndexKeyStringLookup(oldEdge.StartNode, edge.ID); oldOutKey != nil {
			tx.bufferDelete(oldOutKey)
		}
		if oldInKey := tx.engine.incomingIndexKeyStringLookup(oldEdge.EndNode, edge.ID); oldInKey != nil {
			tx.bufferDelete(oldInKey)
		}
		tx.bufferDeleteEdgeBetweenIndexes(oldEdge)
		newOutKey, err := tx.engine.outgoingIndexKeyString(tx.badgerTx, edge.StartNode, edge.ID)
		if err != nil {
			return fmt.Errorf("outgoing index: %w", err)
		}
		tx.bufferSet(newOutKey, []byte{})
		newInKey, err := tx.engine.incomingIndexKeyString(tx.badgerTx, edge.EndNode, edge.ID)
		if err != nil {
			return fmt.Errorf("incoming index: %w", err)
		}
		tx.bufferSet(newInKey, []byte{})
		if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
			return fmt.Errorf("edge-between index: %w", err)
		}
	}

	// If type changed, update edge type index.
	if oldEdge.Type != edge.Type {
		if oldEdge.Type != "" {
			if oldTypeKey := tx.engine.edgeTypeIndexKeyStringLookup(oldEdge.Type, edge.ID); oldTypeKey != nil {
				tx.bufferDelete(oldTypeKey)
			}
		}
		if oldEdge.StartNode == edge.StartNode && oldEdge.EndNode == edge.EndNode {
			tx.bufferDeleteEdgeBetweenIndexes(oldEdge)
		}
		if edge.Type != "" {
			newTypeKey, err := tx.engine.edgeTypeIndexKeyString(tx.badgerTx, edge.Type, edge.ID)
			if err != nil {
				return fmt.Errorf("edge type index: %w", err)
			}
			tx.bufferSet(newTypeKey, []byte{})
		}
		if oldEdge.StartNode == edge.StartNode && oldEdge.EndNode == edge.EndNode {
			if err := tx.bufferSetEdgeBetweenIndexes(edge); err != nil {
				return fmt.Errorf("edge-between index: %w", err)
			}
		}
	}

	// Serialize and buffer updated edge record.
	edgeBytes, err := tx.engine.encodeEdgeInTxn(tx.badgerTx, edge)
	if err != nil {
		return fmt.Errorf("serializing edge: %w", err)
	}
	tx.bufferSet(edgeKey(edge.ID), edgeBytes)

	// Track for read-your-writes.
	edgeCopy := copyEdge(edge)
	tx.pendingEdges[edge.ID] = edgeCopy

	tx.operations = append(tx.operations, Operation{
		Type:      OpUpdateEdge,
		Timestamp: time.Now(),
		EdgeID:    edge.ID,
		Edge:      edgeCopy,
		OldEdge:   oldEdge,
	})

	return nil
}

// DeleteEdge deletes an edge from the transaction.
func (tx *BadgerTransaction) DeleteEdge(edgeID EdgeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Get edge to delete its indexes
	var edge *Edge
	if pending, exists := tx.pendingEdges[edgeID]; exists {
		edge = pending
	} else {
		var err error
		edge, err = tx.getCommittedEdgeLocked(edgeID)
		if err == ErrNotFound {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("reading edge: %w", err)
		}
	}

	// Buffer edge deletion
	key := edgeKey(edgeID)
	tx.bufferDelete(key)

	// Buffer index deletions (lookup-only — all num IDs were allocated
	// at write time).
	if outKey := tx.engine.outgoingIndexKeyStringLookup(edge.StartNode, edgeID); outKey != nil {
		tx.bufferDelete(outKey)
	}
	if inKey := tx.engine.incomingIndexKeyStringLookup(edge.EndNode, edgeID); inKey != nil {
		tx.bufferDelete(inKey)
	}
	if typeKey := tx.engine.edgeTypeIndexKeyStringLookup(edge.Type, edgeID); typeKey != nil {
		tx.bufferDelete(typeKey)
	}
	tx.bufferDeleteEdgeBetweenIndexes(edge)

	// Track deletion
	delete(tx.pendingEdges, edgeID)
	tx.deletedEdges[edgeID] = struct{}{}

	tx.operations = append(tx.operations, Operation{
		Type:      OpDeleteEdge,
		Timestamp: time.Now(),
		EdgeID:    edgeID,
		OldEdge:   edge,
	})

	return nil
}

// GetNode retrieves a node (read-your-writes).
func (tx *BadgerTransaction) GetNode(nodeID NodeID) (*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	// Check deleted
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil, ErrNotFound
	}

	// Check pending
	if node, exists := tx.pendingNodes[nodeID]; exists {
		return copyNode(node), nil
	}

	return tx.getCommittedNodeLocked(nodeID)
}

// GetEdge retrieves an edge with read-your-writes semantics.
func (tx *BadgerTransaction) GetEdge(edgeID EdgeID) (*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	if _, deleted := tx.deletedEdges[edgeID]; deleted {
		return nil, ErrNotFound
	}

	if edge, exists := tx.pendingEdges[edgeID]; exists {
		return copyEdge(edge), nil
	}

	return tx.getCommittedEdgeLocked(edgeID)
}

// GetOutgoingEdges returns outgoing edges including pending transaction writes.
func (tx *BadgerTransaction) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.engine.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
		return edge.StartNode == nodeID
	}), nil
}

// GetIncomingEdges returns incoming edges including pending transaction writes.
func (tx *BadgerTransaction) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.engine.GetIncomingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
		return edge.EndNode == nodeID
	}), nil
}

// GetEdgesBetween returns edges between two nodes including pending transaction writes.
func (tx *BadgerTransaction) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.engine.GetEdgesBetween(startID, endID)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
		return edge.StartNode == startID && edge.EndNode == endID
	}), nil
}

// GetEdgeBetween returns a matching edge including pending transaction writes.
func (tx *BadgerTransaction) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	edges, err := tx.GetEdgesBetween(startID, endID)
	if err != nil {
		return nil
	}
	for _, edge := range edges {
		if edgeType == "" || edge.Type == edgeType {
			return edge
		}
	}
	return nil
}

// GetEdgesByType returns edges of a given type including pending transaction writes.
func (tx *BadgerTransaction) GetEdgesByType(edgeType string) ([]*Edge, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.engine.GetEdgesByType(edgeType)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingEdgesLocked(committed, func(edge *Edge) bool {
		return edge.Type == edgeType
	}), nil
}

// GetNodesByLabel returns nodes with the given label including pending transaction writes.
func (tx *BadgerTransaction) GetNodesByLabel(label string) ([]*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return nil, err
	}
	return tx.mergePendingNodesLocked(committed, func(node *Node) bool {
		if node == nil {
			return false
		}
		for _, nodeLabel := range node.Labels {
			if nodeLabel == label {
				return true
			}
		}
		return false
	}), nil
}

// GetFirstNodeByLabel returns the first visible node with the given label.
func (tx *BadgerTransaction) GetFirstNodeByLabel(label string) (*Node, error) {
	nodes, err := tx.GetNodesByLabel(label)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	return nodes[0], nil
}

// AllNodes returns all visible nodes including pending transaction writes.
func (tx *BadgerTransaction) AllNodes() ([]*Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return nil, err
	}

	committed, err := tx.engine.AllNodes()
	if err != nil {
		return nil, err
	}
	return tx.mergePendingNodesLocked(committed, func(node *Node) bool {
		return node != nil
	}), nil
}

func (tx *BadgerTransaction) GetAllNodes() []*Node {
	nodes, err := tx.AllNodes()
	if err != nil {
		return nil
	}
	return nodes
}

func (tx *BadgerTransaction) HasPendingNodeMutations() bool {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.pendingNodes) > 0 || len(tx.deletedNodes) > 0
}

func (tx *BadgerTransaction) mergePendingNodesLocked(committed []*Node, includePending func(*Node) bool) []*Node {
	merged := make([]*Node, 0, len(committed)+len(tx.pendingNodes))
	seen := make(map[NodeID]struct{}, len(committed)+len(tx.pendingNodes))

	for _, node := range committed {
		if node == nil {
			continue
		}
		if _, deleted := tx.deletedNodes[node.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingNodes[node.ID]; exists {
			if includePending(pending) {
				merged = append(merged, copyNode(pending))
				seen[node.ID] = struct{}{}
			}
			continue
		}
		if includePending(node) {
			merged = append(merged, copyNode(node))
			seen[node.ID] = struct{}{}
		}
	}

	for id, node := range tx.pendingNodes {
		if _, exists := seen[id]; exists {
			continue
		}
		if includePending(node) {
			merged = append(merged, copyNode(node))
		}
	}

	return merged
}

func (tx *BadgerTransaction) mergePendingEdgesLocked(committed []*Edge, includePending func(*Edge) bool) []*Edge {
	merged := make([]*Edge, 0, len(committed)+len(tx.pendingEdges))
	seen := make(map[EdgeID]struct{}, len(committed)+len(tx.pendingEdges))

	for _, edge := range committed {
		if edge == nil {
			continue
		}
		if _, deleted := tx.deletedEdges[edge.ID]; deleted {
			continue
		}
		if pending, exists := tx.pendingEdges[edge.ID]; exists {
			if includePending(pending) {
				merged = append(merged, copyEdge(pending))
				seen[edge.ID] = struct{}{}
			}
			continue
		}
		if includePending(edge) {
			merged = append(merged, copyEdge(edge))
			seen[edge.ID] = struct{}{}
		}
	}

	for id, edge := range tx.pendingEdges {
		if _, exists := seen[id]; exists {
			continue
		}
		if includePending(edge) {
			merged = append(merged, copyEdge(edge))
		}
	}

	return merged
}

// Commit applies all changes atomically with full constraint validation.
// Explicit transactions get strict ACID durability with immediate fsync.
func (tx *BadgerTransaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
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
	releaseCommitLocks := tx.acquireUniqueConstraintCommitLocks()
	defer releaseCommitLocks()

	// Final constraint validation before commit
	if err := tx.validateAllConstraints(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("constraint violation: %w", err)
	}

	if err := tx.validateSnapshotIsolationConflicts(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
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

	if len(tx.operations) > 0 || len(tx.pendingWrites) > 0 || len(tx.pendingDeletes) > 0 {
		version, err := tx.engine.allocateMVCCVersion(tx.badgerTx, time.Now())
		if err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return fmt.Errorf("allocating mvcc commit version: %w", err)
		}
		tx.CommitVersion = version
		if err := tx.engine.materializeMVCCCommitInTxn(tx.badgerTx, version, tx.operations); err != nil {
			tx.closeLocked(TxStatusRolledBack, true, nil)
			return fmt.Errorf("materializing mvcc commit state: %w", err)
		}
	}

	// Flush all buffered writes before committing
	// This batches all writes together for better performance while maintaining ACID guarantees
	if err := tx.flushBufferedWrites(); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("flushing buffered writes: %w", err)
	}

	// Persist any staged monotonic ID-counter updates. One Badger Set
	// per kind, vs. one per allocation when this was done inline.
	if err := tx.engine.idDict.flushTxnCounters(tx.badgerTx); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("flushing id counters: %w", err)
	}

	if err := tx.refreshTemporalCurrentPointers(temporalTargets); err != nil {
		tx.closeLocked(TxStatusRolledBack, true, nil)
		return fmt.Errorf("refreshing temporal current pointers: %w", err)
	}

	// Commit Badger transaction (atomic!)
	if err := tx.badgerTx.Commit(); err != nil {
		tx.closeLocked(TxStatusRolledBack, false, nil)
		return normalizeTransactionCommitError(err)
	}

	// Apply cache/count updates and fire callbacks after commit.
	// This keeps cached stats O(1) and ensures external systems (e.g. search indexes)
	// observe transactional writes the same way as non-transactional writes.
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			tx.engine.cacheOnNodeCreated(op.Node)
			tx.engine.notifyNodeCreated(op.Node)
		case OpUpdateNode:
			tx.engine.cacheOnNodeUpdatedWithOldNode(op.Node, op.OldNode)
			tx.engine.notifyNodeUpdated(op.Node)
		case OpDeleteNode:
			if op.OldNode != nil {
				tx.engine.cacheOnNodeDeletedWithLabels(op.NodeID, op.OldNode.Labels, op.EdgesDeleted)
			} else {
				tx.engine.cacheOnNodeDeleted(op.NodeID, op.EdgesDeleted)
			}
			for _, edgeID := range op.DeletedEdgeIDs {
				tx.engine.notifyEdgeDeleted(edgeID)
			}
			tx.engine.notifyNodeDeleted(op.NodeID)
		case OpCreateEdge:
			tx.engine.cacheOnEdgeCreated(op.Edge)
			tx.engine.notifyEdgeCreated(op.Edge)
		case OpUpdateEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeUpdated(oldType, op.Edge)
			tx.engine.notifyEdgeUpdated(op.Edge)
		case OpDeleteEdge:
			oldType := ""
			if op.OldEdge != nil {
				oldType = op.OldEdge.Type
			}
			tx.engine.cacheOnEdgeDeleted(op.EdgeID, oldType)
			tx.engine.notifyEdgeDeleted(op.EdgeID)
		case OpUpdateEmbedding:
			// Embeddings are regenerable; no-op for cached counts.
		}
	}

	// Update derived unique-constraint caches (in-memory) based on committed operations.
	// This keeps non-transactional CreateNode() uniqueness checks consistent with transactional writes.
	//
	// NOTE: We don't persist these caches; they are rebuilt from stored nodes on startup.
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if op.Node == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, label := range op.Node.Labels {
				for propName, propValue := range op.Node.Properties {
					schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
				}
			}
		case OpUpdateNode:
			if op.OldNode != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, label := range op.OldNode.Labels {
						for propName, propValue := range op.OldNode.Properties {
							schema.UnregisterUniqueValue(label, propName, propValue)
						}
					}
				}
			}
			if op.Node != nil {
				dbName, _, ok := ParseDatabasePrefix(string(op.Node.ID))
				if ok {
					schema := tx.engine.GetSchemaForNamespace(dbName)
					for _, label := range op.Node.Labels {
						for propName, propValue := range op.Node.Properties {
							schema.RegisterUniqueValue(label, propName, propValue, op.Node.ID)
						}
					}
				}
			}
		case OpDeleteNode:
			if op.OldNode == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(op.OldNode.ID))
			if !ok {
				continue
			}
			schema := tx.engine.GetSchemaForNamespace(dbName)
			for _, label := range op.OldNode.Labels {
				for propName, propValue := range op.OldNode.Properties {
					schema.UnregisterUniqueValue(label, propName, propValue)
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

func normalizeTransactionCommitError(err error) error {
	if errors.Is(err, ErrConflict) || errors.Is(err, badger.ErrConflict) {
		return fmt.Errorf("%w: concurrent transaction modified data before commit: %w", ErrConflict, err)
	}
	return fmt.Errorf("badger commit failed: %w", err)
}

// Rollback discards all changes.
func (tx *BadgerTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Status != TxStatusActive {
		return ErrTransactionClosed
	}

	tx.closeLocked(TxStatusRolledBack, true, nil)
	return nil
}

// SetMetadata sets transaction metadata (same as Transaction).
func (tx *BadgerTransaction) SetMetadata(metadata map[string]interface{}) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.ensureLifecycleActiveLocked(); err != nil {
		return err
	}

	// Validate size
	totalSize := 0
	for k, v := range metadata {
		totalSize += len(k)
		if v != nil {
			totalSize += len(fmt.Sprint(v))
		}
	}

	if totalSize > 2048 {
		return fmt.Errorf("transaction metadata too large: %d chars (max 2048)", totalSize)
	}

	// Merge
	if tx.Metadata == nil {
		tx.Metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		tx.Metadata[k] = v
	}

	return nil
}

// GetMetadata returns transaction metadata copy.
func (tx *BadgerTransaction) GetMetadata() map[string]interface{} {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	result := make(map[string]interface{})
	for k, v := range tx.Metadata {
		result[k] = v
	}
	return result
}

// OperationCount returns the number of buffered operations.
func (tx *BadgerTransaction) OperationCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return len(tx.operations)
}

func (tx *BadgerTransaction) getCommittedNodeLocked(nodeID NodeID) (*Node, error) {
	if tx.readTS.IsZero() {
		return tx.getNodeFromBadgerSnapshotLocked(nodeID)
	}
	node, err := tx.engine.GetNodeVisibleAt(nodeID, tx.readTS)
	if err == ErrNotFound {
		fallbackNode, fallbackErr := tx.getNodeFromBadgerSnapshotLocked(nodeID)
		if fallbackErr == nil {
			return fallbackNode, nil
		}
		if fallbackErr != ErrNotFound {
			return nil, fallbackErr
		}
	}
	return node, err
}

func (tx *BadgerTransaction) getNodeFromBadgerSnapshotLocked(nodeID NodeID) (*Node, error) {
	key := nodeKey(nodeID)
	item, err := tx.badgerTx.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reading node: %w", err)
	}
	var nodeBytes []byte
	if err := item.Value(func(val []byte) error {
		nodeBytes = append([]byte{}, val...)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("reading node value: %w", err)
	}
	return deserializeNode(nodeBytes)
}

func (tx *BadgerTransaction) getCommittedEdgeLocked(edgeID EdgeID) (*Edge, error) {
	if tx.readTS.IsZero() {
		key := edgeKey(edgeID)
		item, err := tx.badgerTx.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil, ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("reading edge: %w", err)
		}
		var edgeBytes []byte
		if err := item.Value(func(val []byte) error {
			edgeBytes = append([]byte{}, val...)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("reading edge value: %w", err)
		}
		return tx.engine.decodeEdgeBody(edgeBytes)
	}
	return tx.engine.GetEdgeVisibleAt(edgeID, tx.readTS)
}

func (tx *BadgerTransaction) getNodesByLabelLocked(label string) ([]*Node, error) {
	if tx.readTS.IsZero() {
		return tx.engine.GetNodesByLabel(label)
	}
	return tx.engine.GetNodesByLabelVisibleAt(label, tx.readTS)
}

// nodeExists checks if a node exists (pending or storage).
func (tx *BadgerTransaction) nodeExists(nodeID NodeID) bool {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return false
	}
	if _, exists := tx.pendingNodes[nodeID]; exists {
		return true
	}

	_, err := tx.getCommittedNodeLocked(nodeID)
	return err == nil
}

func (tx *BadgerTransaction) validateSnapshotIsolationConflicts() error {
	for _, op := range tx.operations {
		switch op.Type {
		case OpCreateNode:
			if err := tx.checkNodeCreateConflict(op.NodeID); err != nil {
				return err
			}
		case OpUpdateNode, OpDeleteNode, OpUpdateEmbedding:
			if err := tx.checkNodeWriteConflict(op.NodeID); err != nil {
				return err
			}
			if op.Type == OpDeleteNode {
				if err := tx.checkNodeAdjacencyConflict(op.NodeID); err != nil {
					return err
				}
			}
		case OpCreateEdge:
			if err := tx.checkEdgeCreateConflict(op.EdgeID); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpUpdateEdge:
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
			if err := tx.checkEdgeEndpointConflicts(op.Edge); err != nil {
				return err
			}
		case OpDeleteEdge:
			if err := tx.checkEdgeWriteConflict(op.EdgeID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeCreateConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: node %s changed after transaction start", ErrConflict, nodeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeWriteConflict(nodeID NodeID) error {
	head, err := tx.engine.GetNodeCurrentHead(nodeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: node %s changed after transaction start", ErrConflict, nodeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeCreateConflict(edgeID EdgeID) error {
	head, err := tx.engine.GetEdgeCurrentHead(edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, edgeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeWriteConflict(edgeID EdgeID) error {
	head, err := tx.engine.GetEdgeCurrentHead(edgeID)
	if err == ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if head.Version.Compare(tx.readTS) > 0 {
		return fmt.Errorf("%w: edge %s changed after transaction start", ErrConflict, edgeID)
	}
	return nil
}

func (tx *BadgerTransaction) checkEdgeEndpointConflicts(edge *Edge) error {
	if edge == nil {
		return nil
	}
	for _, nodeID := range []NodeID{edge.StartNode, edge.EndNode} {
		if pending, exists := tx.pendingNodes[nodeID]; exists {
			if pending != nil {
				continue
			}
		}
		head, err := tx.engine.GetNodeCurrentHead(nodeID)
		if err == ErrNotFound {
			if _, nodeErr := tx.engine.GetNode(nodeID); nodeErr == nil {
				continue
			} else if nodeErr == ErrNotFound {
				return ErrInvalidEdge
			} else {
				return nodeErr
			}
		}
		if err != nil {
			return err
		}
		if head.Tombstoned {
			if head.Version.Compare(tx.readTS) > 0 {
				return fmt.Errorf("%w: endpoint node %s was deleted after transaction start", ErrConflict, nodeID)
			}
			return ErrInvalidEdge
		}
	}
	return nil
}

func (tx *BadgerTransaction) checkNodeAdjacencyConflict(nodeID NodeID) error {
	return tx.engine.withView(func(viewTx *badger.Txn) error {
		var prefixes [][]byte
		if outPrefix := tx.engine.outgoingIndexPrefixString(nodeID); outPrefix != nil {
			prefixes = append(prefixes, outPrefix)
		}
		if inPrefix := tx.engine.incomingIndexPrefixString(nodeID); inPrefix != nil {
			prefixes = append(prefixes, inPrefix)
		}
		for _, prefix := range prefixes {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			opts.PrefetchValues = false
			it := viewTx.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				edgeNum, ok := extractEdgeNumIDFromOutgoingKey(it.Item().KeyCopy(nil))
				if !ok {
					continue
				}
				edgeID, ok := tx.engine.idDict.lookupEdgeIDByNum(edgeNum)
				if !ok {
					continue
				}
				head, err := tx.engine.loadEdgeMVCCHeadInTxn(viewTx, edgeID)
				if err == ErrNotFound {
					continue
				}
				if err != nil {
					it.Close()
					return err
				}
				if head.Version.Compare(tx.readTS) > 0 {
					it.Close()
					return fmt.Errorf("%w: node %s has adjacent edge %s changed after transaction start", ErrConflict, nodeID, edgeID)
				}
			}
			it.Close()
		}
		return nil
	})
}

// shouldSkipCreateExistenceCheck avoids a read-before-write for UUID-based IDs.
// UUID collisions are negligible for generated IDs, so we skip the read to save I/O.
func shouldSkipCreateExistenceCheck(nodeID NodeID) bool {
	return hasUUIDShape(string(nodeID))
}

// hasUUIDShape reports whether an id is a namespace-prefixed UUID. Because
// UUIDv4 collisions are astronomically unlikely, a freshly minted UUID cannot
// refer to a previously deleted entity — so callers that mint IDs this way can
// safely skip both the create existence read AND the MVCC head load-before-write
// during commit without risking incorrect snapshot semantics for a
// tombstoned-then-recreated ID.
func hasUUIDShape(id string) bool {
	_, rawID, ok := ParseDatabasePrefix(id)
	if !ok {
		return false
	}
	_, err := uuid.Parse(rawID)
	return err == nil
}

// validateNodeConstraints checks all constraints for a node.
func (tx *BadgerTransaction) validateNodeConstraints(node *Node) error {
	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
	}

	// Get constraints from the schema for this namespace (per DB).
	schema := tx.engine.GetSchemaForNamespace(dbName)
	constraints := schema.GetConstraintsForLabels(node.Labels)

	for _, constraint := range constraints {
		switch constraint.Type {
		case ConstraintUnique:
			if err := tx.checkUniqueConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintNodeKey:
			if err := tx.checkNodeKeyConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintExists:
			if err := tx.checkExistenceConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := tx.checkTemporalConstraint(node, constraint); err != nil {
				return err
			}
		case ConstraintDomain:
			if len(constraint.Properties) == 1 && len(constraint.AllowedValues) > 0 {
				prop := constraint.Properties[0]
				value := node.Properties[prop]
				if value != nil && !isValueInAllowedList(value, constraint.AllowedValues) {
					return &ConstraintViolationError{
						Type:       ConstraintDomain,
						Label:      constraint.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Property %s value %v is not in allowed values %v", prop, value, constraint.AllowedValues),
					}
				}
			}
		}
	}

	typeConstraints := schema.GetPropertyTypeConstraintsForLabels(node.Labels)
	for _, constraint := range typeConstraints {
		value := node.Properties[constraint.Property]
		if err := ValidatePropertyType(value, constraint.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      constraint.Label,
				Properties: []string{constraint.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", constraint.Property, constraint.ExpectedType, err),
			}
		}
	}

	return nil
}

// checkUniqueConstraint ensures property value is unique across ALL data.
func (tx *BadgerTransaction) checkUniqueConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return nil // NULL doesn't violate uniqueness
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check pending nodes in this transaction (namespace-scoped).
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if hasLabel(n.Labels, c.Label) && n.Properties[prop] == value {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      c.Label,
				Properties: []string{prop},
				Message:    fmt.Sprintf("Node with %s=%v already exists in transaction", prop, value),
			}
		}
	}

	schema := tx.engine.GetSchemaForNamespace(dbName)
	if existingNode, found, authoritative, constrained := schema.lookupUniqueConstraintValueForValidation(c.Label, prop, value); constrained && authoritative {
		if found && existingNode != node.ID {
			if _, deleted := tx.deletedNodes[existingNode]; !deleted {
				return uniqueConstraintViolation(c.Label, prop, value, existingNode)
			}
		}
		return nil
	}

	// If the derived unique-value cache is not known authoritative, fall back to
	// the label scan. Normal schema creation/reload marks the cache authoritative
	// once after rebuilding it from stored nodes, so hot writes avoid this path.
	return tx.scanForUniqueViolation(dbName, c.Label, prop, value, node.ID)
}

func uniqueConstraintViolation(label, property string, value interface{}, nodeID NodeID) *ConstraintViolationError {
	return &ConstraintViolationError{
		Type:       ConstraintUnique,
		Label:      label,
		Properties: []string{property},
		Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, nodeID),
	}
}

// uniqueConstraintScanHook lets storage tests assert that indexed UNIQUE
// validation does not regress to the label-scan fallback.
var uniqueConstraintScanHook func()
var uniqueConstraintScanHookMu sync.RWMutex

func setUniqueConstraintScanHook(hook func()) func() {
	uniqueConstraintScanHookMu.Lock()
	previousHook := uniqueConstraintScanHook
	uniqueConstraintScanHook = hook
	uniqueConstraintScanHookMu.Unlock()

	return func() {
		uniqueConstraintScanHookMu.Lock()
		uniqueConstraintScanHook = previousHook
		uniqueConstraintScanHookMu.Unlock()
	}
}

func getUniqueConstraintScanHook() func() {
	uniqueConstraintScanHookMu.RLock()
	defer uniqueConstraintScanHookMu.RUnlock()
	return uniqueConstraintScanHook
}

// scanForUniqueViolation performs a full database scan to check for UNIQUE violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForUniqueViolation(namespace, label, property string, value interface{}, excludeNodeID NodeID) error {
	if hook := getUniqueConstraintScanHook(); hook != nil {
		hook()
	}

	nodes, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return err
	}
	for _, existingNode := range nodes {
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}
		if existingValue, ok := existingNode.Properties[property]; ok && compareValues(existingValue, value) {
			return &ConstraintViolationError{
				Type:       ConstraintUnique,
				Label:      label,
				Properties: []string{property},
				Message:    fmt.Sprintf("Node with %s=%v already exists (nodeID: %s)", property, value, existingNode.ID),
			}
		}
	}

	return nil
}

// checkNodeKeyConstraint ensures composite key uniqueness across ALL data.
func (tx *BadgerTransaction) checkNodeKeyConstraint(node *Node, c Constraint) error {
	values := make([]interface{}, len(c.Properties))
	for i, prop := range c.Properties {
		values[i] = node.Properties[prop]
		if values[i] == nil {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("NODE KEY property %s cannot be null", prop),
			}
		}
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check pending nodes in this transaction (namespace-scoped).
	for id, n := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !hasLabel(n.Labels, c.Label) {
			continue
		}

		match := true
		for i, prop := range c.Properties {
			if !compareValues(n.Properties[prop], values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      c.Label,
				Properties: c.Properties,
				Message:    fmt.Sprintf("Node with key %v=%v already exists in transaction", c.Properties, values),
			}
		}
	}

	// Full-scan check: scan all existing nodes with this label (namespace-scoped).
	if err := tx.scanForNodeKeyViolation(dbName, c.Label, c.Properties, values, node.ID); err != nil {
		return err
	}

	return nil
}

// scanForNodeKeyViolation performs a full database scan to check for NODE KEY violations
// within a single namespace (database).
func (tx *BadgerTransaction) scanForNodeKeyViolation(namespace, label string, properties []string, values []interface{}, excludeNodeID NodeID) error {
	nodes, err := tx.getNodesByLabelLocked(label)
	if err != nil {
		return err
	}
	for _, existingNode := range nodes {
		if existingNode == nil || existingNode.ID == excludeNodeID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingNode.ID), namespace+":") {
			continue
		}

		match := true
		for i, prop := range properties {
			existingValue, ok := existingNode.Properties[prop]
			if !ok || !compareValues(existingValue, values[i]) {
				match = false
				break
			}
		}

		if match {
			return &ConstraintViolationError{
				Type:       ConstraintNodeKey,
				Label:      label,
				Properties: properties,
				Message:    fmt.Sprintf("Node with composite key %v=%v already exists (nodeID: %s)", properties, values, existingNode.ID),
			}
		}
	}

	return nil
}

// checkExistenceConstraint ensures required property exists.
func (tx *BadgerTransaction) checkExistenceConstraint(node *Node, c Constraint) error {
	prop := c.Properties[0]
	value := node.Properties[prop]

	if value == nil {
		return &ConstraintViolationError{
			Type:       ConstraintExists,
			Label:      c.Label,
			Properties: []string{prop},
			Message:    fmt.Sprintf("Property %s is required but missing", prop),
		}
	}

	return nil
}

// checkTemporalConstraint enforces TEMPORAL NO OVERLAP constraints within a transaction.
//
// It must validate against:
// - other pending nodes in this transaction (read-your-writes), and
// - existing committed nodes in storage (via the label index scan).
func (tx *BadgerTransaction) checkTemporalConstraint(node *Node, c Constraint) error {
	if len(c.Properties) != 3 {
		return fmt.Errorf("TEMPORAL constraint requires 3 properties (key, valid_from, valid_to)")
	}

	keyProp := c.Properties[0]
	startProp := c.Properties[1]
	endProp := c.Properties[2]

	keyVal := node.Properties[keyProp]
	if keyVal == nil {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      c.Label,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", keyProp),
		}
	}

	start, ok := coerceTemporalTime(node.Properties[startProp])
	if !ok {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      c.Label,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
		}
	}
	end, hasEnd := coerceTemporalTime(node.Properties[endProp])

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace, got: %s", node.ID)
	}
	nsPrefix := dbName + ":"

	// Check overlaps against pending nodes in this transaction (namespace + label + key match).
	for id, other := range tx.pendingNodes {
		if id == node.ID {
			continue
		}
		if !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !hasLabel(other.Labels, c.Label) {
			continue
		}

		otherKey := other.Properties[keyProp]
		if otherKey == nil || !compareValues(otherKey, keyVal) {
			continue
		}

		otherStart, ok := coerceTemporalTime(other.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, id),
			}
		}
		otherEnd, otherHasEnd := coerceTemporalTime(other.Properties[endProp])

		if intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					id, keyProp, keyVal),
			}
		}
	}

	// Check overlaps against committed storage using the label index scan.
	visibleNodes, err := tx.getNodesByLabelLocked(c.Label)
	if err != nil {
		return err
	}
	for _, other := range visibleNodes {
		if other == nil || other.ID == node.ID {
			continue
		}
		if !strings.HasPrefix(string(other.ID), nsPrefix) {
			continue
		}
		otherKey := other.Properties[keyProp]
		if otherKey == nil || !compareValues(otherKey, keyVal) {
			continue
		}
		otherStart, ok := coerceTemporalTime(other.Properties[startProp])
		if !ok {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message:    fmt.Sprintf("TEMPORAL constraint requires %s for node %s", startProp, other.ID),
			}
		}
		otherEnd, otherHasEnd := coerceTemporalTime(other.Properties[endProp])
		if intervalsOverlap(
			temporalInterval{start: start, end: end, hasEnd: hasEnd},
			temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd},
		) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      c.Label,
				Properties: []string{keyProp, startProp, endProp},
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with node %s for %s=%v",
					other.ID, keyProp, keyVal),
			}
		}
	}
	return nil
}

// validateAllConstraints performs final validation before commit.
// acquireUniqueConstraintCommitLocks collects the unique constraints touched
// by this transaction's pending nodes and acquires bounded
// per-(label, property, value) mutex stripes on each affected schema. Returns
// a release function that unlocks in reverse order; safe to defer.
//
// Locks are keyed by value, not only by constraint: a transaction adding
// nodes with uids "X" and "Y" hashes those values to bounded lock stripes; a
// peer transaction with uids "X" and "Z" serializes against "X" and commits
// "Z" in parallel unless the bounded stripe hash collides. This was changed
// from coarser per-(label, property) granularity that effectively serialized
// every writer touching a UNIQUE-constrained property — see
// uniqueConstraintLockKey doc.
//
// Locks are partitioned by namespace because each namespace has its own
// SchemaManager and its own constraint registry — a transaction that spans
// multiple namespaces locks each namespace's affected values independently.
// Namespaces are processed in sorted order so that two transactions
// touching overlapping namespaces always acquire locks in the same order,
// eliminating the AB-BA deadlock risk.
//
// Pending nodes with non-comparable property values skip lock acquisition
// for that property. Constraint validation still fires at commit time;
// commit-window serialization is best-effort for non-comparable types
// (which UNIQUE-constrained Eshu/Neo4j workloads do not use in practice).
func (tx *BadgerTransaction) acquireUniqueConstraintCommitLocks() func() {
	if len(tx.pendingNodes) == 0 {
		return func() {}
	}
	perNamespace := make(map[string]map[uniqueConstraintLockKey]struct{})
	for _, node := range tx.pendingNodes {
		if node == nil {
			continue
		}
		dbName, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			continue
		}
		schema := tx.engine.GetSchemaForNamespace(dbName)
		if schema == nil {
			continue
		}
		constraints := schema.GetConstraintsForLabels(node.Labels)
		for _, c := range constraints {
			if c.Type != ConstraintUnique || len(c.Properties) != 1 {
				continue
			}
			prop := c.Properties[0]
			rawValue, has := node.Properties[prop]
			if !has {
				continue
			}
			canonicalValue, ok := uniqueConstraintValueKey(rawValue)
			if !ok {
				// Non-comparable value: skip lock acquisition. Validation
				// still runs at commit; commit-window serialization is
				// best-effort for these (no constraint cache anyway).
				continue
			}
			if perNamespace[dbName] == nil {
				perNamespace[dbName] = make(map[uniqueConstraintLockKey]struct{})
			}
			perNamespace[dbName][uniqueConstraintLockKey{
				label:    c.Label,
				property: prop,
				value:    canonicalValue,
			}] = struct{}{}
		}
	}
	if len(perNamespace) == 0 {
		return func() {}
	}

	namespaces := make([]string, 0, len(perNamespace))
	for ns := range perNamespace {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)

	releases := make([]func(), 0, len(namespaces))
	for _, ns := range namespaces {
		schema := tx.engine.GetSchemaForNamespace(ns)
		if schema == nil {
			continue
		}
		keys := make([]uniqueConstraintLockKey, 0, len(perNamespace[ns]))
		for k := range perNamespace[ns] {
			keys = append(keys, k)
		}
		releases = append(releases, schema.acquireUniqueConstraintCommitLocks(keys))
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
}

func (tx *BadgerTransaction) validateAllConstraints() error {
	for _, node := range tx.pendingNodes {
		if err := tx.validateNodeConstraints(node); err != nil {
			return err
		}
	}
	for _, edge := range tx.pendingEdges {
		if err := tx.validateEdgeConstraints(edge); err != nil {
			return err
		}
	}
	if err := tx.validateConstraintContracts(); err != nil {
		return err
	}
	return nil
}

// validateEdgeConstraints checks relationship constraints for an edge in a transaction.
func (tx *BadgerTransaction) validateEdgeConstraints(edge *Edge) error {
	if edge == nil || edge.Type == "" {
		return nil
	}
	dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}

	constraints := schema.GetConstraintsForLabels([]string{edge.Type})
	for _, c := range constraints {
		if c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		switch c.Type {
		case ConstraintUnique:
			if err := tx.checkEdgeUniqueness(edge, c, dbName); err != nil {
				return err
			}
		case ConstraintExists:
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
		case ConstraintRelationshipKey:
			if err := checkEdgeExistence(edge, c); err != nil {
				return err
			}
			if err := tx.checkEdgeUniqueness(edge, c, dbName); err != nil {
				return err
			}
		case ConstraintTemporal:
			if err := tx.checkEdgeTemporalConstraint(edge, c, dbName); err != nil {
				return err
			}
		case ConstraintDomain:
			if len(c.Properties) == 1 && len(c.AllowedValues) > 0 {
				prop := c.Properties[0]
				value := edge.Properties[prop]
				if value != nil && !isValueInAllowedList(value, c.AllowedValues) {
					return &ConstraintViolationError{
						Type:       ConstraintDomain,
						Label:      edge.Type,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Property %s value %v is not in allowed values %v", prop, value, c.AllowedValues),
					}
				}
			}
		case ConstraintCardinality:
			if err := tx.checkEdgeCardinality(edge, c, dbName); err != nil {
				return err
			}
		}
	}

	// Policy constraints must be evaluated as a set (all ALLOWED policies form a union).
	if err := tx.checkEdgePolicy(edge, schema, dbName); err != nil {
		return err
	}

	ptConstraints := schema.GetPropertyTypeConstraintsForLabels([]string{edge.Type})
	for _, ptc := range ptConstraints {
		if ptc.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		value := edge.Properties[ptc.Property]
		if err := ValidatePropertyType(value, ptc.ExpectedType); err != nil {
			return &ConstraintViolationError{
				Type:       ConstraintPropertyType,
				Label:      edge.Type,
				Properties: []string{ptc.Property},
				Message:    fmt.Sprintf("Property %s must be %s (%v)", ptc.Property, ptc.ExpectedType, err),
			}
		}
	}

	return nil
}

// checkEdgeUniqueness checks uniqueness constraints for an edge against pending and committed edges.
// The namespace parameter filters committed edges to the same database namespace.
func (tx *BadgerTransaction) checkEdgeUniqueness(edge *Edge, c Constraint, namespace string) error {
	// Check against other pending edges in this transaction
	nsPrefix := namespace + ":"
	for id, otherEdge := range tx.pendingEdges {
		if id == edge.ID || otherEdge.Type != edge.Type {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if len(c.Properties) == 1 {
			prop := c.Properties[0]
			newVal := edge.Properties[prop]
			if newVal == nil {
				return nil
			}
			otherVal := otherEdge.Properties[prop]
			if otherVal != nil && compareValues(otherVal, newVal) {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Relationship with %s=%v already exists (edgeID: %s)", prop, newVal, otherEdge.ID),
				}
			}
		} else {
			allMatch := true
			allPresent := true
			for _, prop := range c.Properties {
				newVal := edge.Properties[prop]
				if newVal == nil {
					allPresent = false
					break
				}
				otherVal := otherEdge.Properties[prop]
				if otherVal == nil || !compareValues(otherVal, newVal) {
					allMatch = false
					break
				}
			}
			if !allPresent {
				return nil
			}
			if allMatch {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: c.Properties,
					Message:    fmt.Sprintf("Relationship with duplicate composite key already exists (edgeID: %s)", otherEdge.ID),
				}
			}
		}
	}

	// Check against committed edges via the engine
	existingEdges, err := tx.engine.GetEdgesByType(edge.Type)
	if err != nil {
		return nil // If we can't read edges, skip check rather than block
	}
	for _, existingEdge := range existingEdges {
		if existingEdge.ID == edge.ID {
			continue
		}
		// Filter to same namespace to avoid cross-database false positives
		if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
			continue
		}
		if len(c.Properties) == 1 {
			prop := c.Properties[0]
			newVal := edge.Properties[prop]
			if newVal == nil {
				return nil
			}
			existVal := existingEdge.Properties[prop]
			if existVal != nil && compareValues(existVal, newVal) {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: []string{prop},
					Message:    fmt.Sprintf("Relationship with %s=%v already exists (edgeID: %s)", prop, newVal, existingEdge.ID),
				}
			}
		} else {
			allMatch := true
			allPresent := true
			for _, prop := range c.Properties {
				newVal := edge.Properties[prop]
				if newVal == nil {
					allPresent = false
					break
				}
				existVal := existingEdge.Properties[prop]
				if existVal == nil || !compareValues(existVal, newVal) {
					allMatch = false
					break
				}
			}
			if !allPresent {
				return nil
			}
			if allMatch {
				return &ConstraintViolationError{
					Type:       c.Type,
					Label:      edge.Type,
					Properties: c.Properties,
					Message:    fmt.Sprintf("Relationship with duplicate composite key already exists (edgeID: %s)", existingEdge.ID),
				}
			}
		}
	}

	return nil
}

// checkEdgeTemporalConstraint checks temporal no-overlap constraints for an edge in a transaction.
// The namespace parameter filters committed edges to the same database namespace.
// Supports composite key properties: all properties except the last 2 form the key.
func (tx *BadgerTransaction) checkEdgeTemporalConstraint(edge *Edge, c Constraint, namespace string) error {
	if len(c.Properties) < 3 {
		return fmt.Errorf("TEMPORAL constraint requires at least 3 properties (key..., valid_from, valid_to)")
	}

	keyProps := c.Properties[:len(c.Properties)-2]
	startProp := c.Properties[len(c.Properties)-2]
	endProp := c.Properties[len(c.Properties)-1]

	// Validate all key properties are non-null
	keyVals := make([]interface{}, len(keyProps))
	for i, prop := range keyProps {
		keyVals[i] = edge.Properties[prop]
		if keyVals[i] == nil {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      edge.Type,
				Properties: c.Properties,
				Message:    fmt.Sprintf("TEMPORAL key property %s cannot be null", prop),
			}
		}
	}

	start, ok := coerceTemporalTime(edge.Properties[startProp])
	if !ok {
		return &ConstraintViolationError{
			Type:       ConstraintTemporal,
			Label:      edge.Type,
			Properties: c.Properties,
			Message:    fmt.Sprintf("TEMPORAL start property %s must be a datetime", startProp),
		}
	}
	end, hasEnd := coerceTemporalTime(edge.Properties[endProp])
	newInterval := temporalInterval{start: start, end: end, hasEnd: hasEnd}

	// Check against pending edges in this transaction
	nsPrefix := namespace + ":"
	for id, otherEdge := range tx.pendingEdges {
		if id == edge.ID || otherEdge.Type != edge.Type {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		if !edgeTemporalCompositeKeyMatch(otherEdge, keyProps, keyVals) {
			continue
		}
		otherStart, ok := coerceTemporalTime(otherEdge.Properties[startProp])
		if !ok {
			continue
		}
		otherEnd, otherHasEnd := coerceTemporalTime(otherEdge.Properties[endProp])
		if intervalsOverlap(newInterval, temporalInterval{start: otherStart, end: otherEnd, hasEnd: otherHasEnd}) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      edge.Type,
				Properties: c.Properties,
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with edge %s for key=%v",
					id, keyVals),
			}
		}
	}

	// Check against committed edges (filtered by namespace)
	existingEdges, err := tx.engine.GetEdgesByType(edge.Type)
	if err != nil {
		return nil
	}
	for _, existingEdge := range existingEdges {
		if existingEdge.ID == edge.ID {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
			continue
		}
		if !edgeTemporalCompositeKeyMatch(existingEdge, keyProps, keyVals) {
			continue
		}
		existingStart, ok := coerceTemporalTime(existingEdge.Properties[startProp])
		if !ok {
			continue
		}
		existingEnd, existingHasEnd := coerceTemporalTime(existingEdge.Properties[endProp])
		if intervalsOverlap(newInterval, temporalInterval{start: existingStart, end: existingEnd, hasEnd: existingHasEnd}) {
			return &ConstraintViolationError{
				Type:       ConstraintTemporal,
				Label:      edge.Type,
				Properties: c.Properties,
				Message: fmt.Sprintf("TEMPORAL constraint violation: overlap with edge %s for key=%v",
					existingEdge.ID, keyVals),
			}
		}
	}

	return nil
}

// checkEdgeCardinality enforces cardinality constraints within a transaction.
// It counts pending + committed edges of the given type connected to the anchor node,
// excluding deleted edges, and rejects the new edge if adding it would exceed MaxCount.
func (tx *BadgerTransaction) checkEdgeCardinality(edge *Edge, c Constraint, namespace string) error {
	// Determine anchor node based on direction.
	var anchorNode NodeID
	if c.Direction == "OUTGOING" {
		anchorNode = NodeID(edge.StartNode)
	} else {
		anchorNode = NodeID(edge.EndNode)
	}

	nsPrefix := namespace + ":"
	count := 0

	// Count pending edges in this transaction that match type + anchor + direction.
	for id, pendingEdge := range tx.pendingEdges {
		if id == edge.ID || pendingEdge.Type != c.Label {
			continue
		}
		if namespace != "" && !strings.HasPrefix(string(id), nsPrefix) {
			continue
		}
		var pendingAnchor NodeID
		if c.Direction == "OUTGOING" {
			pendingAnchor = NodeID(pendingEdge.StartNode)
		} else {
			pendingAnchor = NodeID(pendingEdge.EndNode)
		}
		if pendingAnchor == anchorNode {
			count++
		}
	}

	// Count committed edges from the engine.
	var committedEdges []*Edge
	var err error
	if c.Direction == "OUTGOING" {
		committedEdges, err = tx.engine.GetOutgoingEdges(anchorNode)
	} else {
		committedEdges, err = tx.engine.GetIncomingEdges(anchorNode)
	}
	if err == nil {
		for _, existingEdge := range committedEdges {
			if existingEdge.ID == edge.ID || existingEdge.Type != c.Label {
				continue
			}
			if namespace != "" && !strings.HasPrefix(string(existingEdge.ID), nsPrefix) {
				continue
			}
			// Skip edges that are deleted in this transaction.
			if _, deleted := tx.deletedEdges[existingEdge.ID]; deleted {
				continue
			}
			// Skip edges already counted as pending (they may have been updated).
			if _, isPending := tx.pendingEdges[existingEdge.ID]; isPending {
				continue
			}
			count++
		}
	}

	if count >= c.MaxCount {
		dir := strings.ToLower(c.Direction)
		return &ConstraintViolationError{
			Type:  ConstraintCardinality,
			Label: c.Label,
			Message: fmt.Sprintf("Adding this edge would exceed max %s count of %d for relationship type %s on node %s (current: %d)",
				dir, c.MaxCount, c.Label, anchorNode, count),
		}
	}

	return nil
}

// checkEdgePolicy validates DISALLOWED and ALLOWED endpoint policies within a transaction.
// Reads node labels from pending nodes first (read-your-writes), then falls back to committed storage.
func (tx *BadgerTransaction) checkEdgePolicy(edge *Edge, schema *SchemaManager, namespace string) error {
	constraints := schema.GetConstraintsForLabels([]string{edge.Type})

	var allowedPolicies []Constraint
	var disallowedPolicies []Constraint
	for _, c := range constraints {
		if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
			continue
		}
		if c.PolicyMode == "ALLOWED" {
			allowedPolicies = append(allowedPolicies, c)
		} else if c.PolicyMode == "DISALLOWED" {
			disallowedPolicies = append(disallowedPolicies, c)
		}
	}

	if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
		return nil
	}

	// Read source node labels (check pending first for read-your-writes).
	srcLabels := tx.getNodeLabels(NodeID(edge.StartNode))
	if srcLabels == nil {
		return nil // Node not found; other validation catches this
	}

	// Read target node labels.
	tgtLabels := tx.getNodeLabels(NodeID(edge.EndNode))
	if tgtLabels == nil {
		return nil
	}

	// Check DISALLOWED policies first (they take precedence).
	for _, c := range disallowedPolicies {
		if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
			return &ConstraintViolationError{
				Type:  ConstraintPolicy,
				Label: c.Label,
				Message: fmt.Sprintf("DISALLOWED policy violation: (:%s)-[:%s]->(:%s) is forbidden (constraint %q)",
					c.SourceLabel, c.Label, c.TargetLabel, c.Name),
			}
		}
	}

	// Check ALLOWED policies: if any exist, at least one must match.
	if len(allowedPolicies) > 0 {
		matched := false
		for _, c := range allowedPolicies {
			if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
				matched = true
				break
			}
		}
		if !matched {
			return &ConstraintViolationError{
				Type:  ConstraintPolicy,
				Label: edge.Type,
				Message: fmt.Sprintf("ALLOWED policy violation: no ALLOWED policy permits (:%s)-[:%s]->(:%s)",
					strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":")),
			}
		}
	}

	return nil
}

// validatePolicyOnNodeLabelChange checks all policy constraints on edges connected to
// a node whose labels are changing within a transaction.
func (tx *BadgerTransaction) validatePolicyOnNodeLabelChange(node *Node, oldNode *Node) error {
	dbName, _, _ := ParseDatabasePrefix(string(node.ID))
	schema := tx.engine.GetSchemaForNamespace(dbName)
	if schema == nil {
		return nil
	}

	nsPrefix := dbName + ":"

	// Scan both outgoing and incoming edges.
	for _, isOutgoing := range []bool{true, false} {
		var edges []*Edge
		var err error
		if isOutgoing {
			edges, err = tx.engine.GetOutgoingEdges(node.ID)
		} else {
			edges, err = tx.engine.GetIncomingEdges(node.ID)
		}
		if err != nil {
			continue
		}

		// Include pending edges in this transaction.
		for _, pendingEdge := range tx.pendingEdges {
			if isOutgoing && NodeID(pendingEdge.StartNode) == node.ID {
				edges = append(edges, pendingEdge)
			} else if !isOutgoing && NodeID(pendingEdge.EndNode) == node.ID {
				edges = append(edges, pendingEdge)
			}
		}

		for _, edge := range edges {
			if _, deleted := tx.deletedEdges[edge.ID]; deleted {
				continue
			}
			if dbName != "" && !strings.HasPrefix(string(edge.ID), nsPrefix) {
				continue
			}

			// Get the other node's labels.
			var otherNodeID NodeID
			if isOutgoing {
				otherNodeID = NodeID(edge.EndNode)
			} else {
				otherNodeID = NodeID(edge.StartNode)
			}
			otherLabels := tx.getNodeLabels(otherNodeID)
			if otherLabels == nil {
				continue
			}

			var srcLabels, tgtLabels []string
			if isOutgoing {
				srcLabels = node.Labels
				tgtLabels = otherLabels
			} else {
				srcLabels = otherLabels
				tgtLabels = node.Labels
			}

			// Check policy constraints.
			constraints := schema.GetConstraintsForLabels([]string{edge.Type})
			var allowedPolicies []Constraint
			var disallowedPolicies []Constraint
			for _, c := range constraints {
				if c.Type != ConstraintPolicy || c.EffectiveEntityType() != ConstraintEntityRelationship {
					continue
				}
				if c.PolicyMode == "ALLOWED" {
					allowedPolicies = append(allowedPolicies, c)
				} else if c.PolicyMode == "DISALLOWED" {
					disallowedPolicies = append(disallowedPolicies, c)
				}
			}

			if len(allowedPolicies) == 0 && len(disallowedPolicies) == 0 {
				continue
			}

			for _, c := range disallowedPolicies {
				if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
					return &ConstraintViolationError{
						Type:  ConstraintPolicy,
						Label: c.Label,
						Message: fmt.Sprintf("Label change would violate DISALLOWED policy: (:%s)-[:%s]->(:%s) (constraint %q)",
							c.SourceLabel, c.Label, c.TargetLabel, c.Name),
					}
				}
			}

			if len(allowedPolicies) > 0 {
				matched := false
				for _, c := range allowedPolicies {
					if hasLabel(srcLabels, c.SourceLabel) && hasLabel(tgtLabels, c.TargetLabel) {
						matched = true
						break
					}
				}
				if !matched {
					return &ConstraintViolationError{
						Type:  ConstraintPolicy,
						Label: edge.Type,
						Message: fmt.Sprintf("Label change would violate ALLOWED policy: no ALLOWED policy permits (:%s)-[:%s]->(:%s)",
							strings.Join(srcLabels, ":"), edge.Type, strings.Join(tgtLabels, ":")),
					}
				}
			}
		}
	}

	return nil
}

// getNodeLabels returns labels for a node, checking pending nodes first for read-your-writes.
func (tx *BadgerTransaction) getNodeLabels(nodeID NodeID) []string {
	if _, deleted := tx.deletedNodes[nodeID]; deleted {
		return nil
	}
	if pending, exists := tx.pendingNodes[nodeID]; exists {
		return pending.Labels
	}
	node, err := tx.getCommittedNodeLocked(nodeID)
	if err != nil {
		return nil
	}
	return node.Labels
}

// Helper: check if node has label
func hasLabel(labels []string, target string) bool {
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
}

// ConstraintViolationError is returned when a constraint is violated.
type ConstraintViolationError struct {
	Type       ConstraintType
	Label      string
	Properties []string
	Message    string
}

func (e *ConstraintViolationError) Error() string {
	return fmt.Sprintf("Constraint violation (%s on %s.%v): %s",
		e.Type, e.Label, e.Properties, e.Message)
}
