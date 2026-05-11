// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Node Operations
// ============================================================================

// CreateNode creates a new node in persistent storage.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
// This enforces that all nodes are namespaced at the storage layer.
func (b *BadgerEngine) CreateNode(node *Node) (NodeID, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	if node == nil {
		return "", ErrInvalidData
	}
	if node.ID == "" {
		return "", ErrInvalidID
	}
	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if !strings.Contains(string(node.ID), ":") {
		return "", fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	if err := b.ensureOpen(); err != nil {
		return "", err
	}
	if node.CreatedAt.IsZero() {
		node.CreatedAt = time.Now()
	}
	if node.UpdatedAt.IsZero() {
		node.UpdatedAt = node.CreatedAt
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return "", fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
	}
	schema := b.GetSchemaForNamespace(dbName)

	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		key := nodeKey(node.ID)
		_, getErr := txn.Get(key)
		if getErr == nil {
			return ErrAlreadyExists
		}
		if getErr != badger.ErrKeyNotFound {
			return getErr
		}
		if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
			return err
		}
		data, embeddingsSeparate, err := encodeNode(node)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}
		if err := txn.Set(key, data); err != nil {
			return fmt.Errorf("failed to write node: %w", err)
		}
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		}
		for _, label := range node.Labels {
			if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
				return fmt.Errorf("failed to write label index: %w", err)
			}
		}
		if err := putIndexEntryCatalogInTxn(txn, string(node.ID), &IndexEntryCatalog{
			TargetID:    string(node.ID),
			TargetScope: "NODE",
			IndexKeys:   collectNodeIndexKeys(node.ID, node.Labels),
		}); err != nil {
			return fmt.Errorf("failed to write index catalog: %w", err)
		}
		if !isSystemNamespaceID(string(node.ID)) &&
			(len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) &&
			NodeNeedsEmbedding(node) {
			if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
				return fmt.Errorf("failed to write pending embed index: %w", err)
			}
		}
		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, nil, node); err != nil {
			return err
		}
		if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
			return err
		}
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	})
	if err != nil {
		return "", err
	}
	if persistSeparateEmbeddings {
		if err := b.replaceSeparateEmbeddingChunks(node.ID, embeddingsToPersist); err != nil {
			return "", err
		}
	}

	// On successful create, update cache and register unique constraint values
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			schema.RegisterUniqueValue(label, propName, propValue, node.ID)
		}
	}

	b.cacheOnNodeCreated(node)

	// Notify listeners (e.g., search service) to index the new node
	b.notifyNodeCreated(node)

	return node.ID, nil
}

// GetNode retrieves a node by ID.
func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurGet)
	if id == "" {
		return nil, ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return nil, err
	}

	// Check cache first
	b.nodeCacheMu.RLock()
	if cached, ok := b.nodeCache[id]; ok {
		b.nodeCacheMu.RUnlock()
		atomic.AddInt64(&b.cacheHits, 1)
		// Return copy to prevent external mutation of cache
		nodeCopy := copyNode(cached)
		if b.filterNodeByDecay(nodeCopy, DecayScoringTime()) {
			return nil, ErrNotFound
		}
		return nodeCopy, nil
	}
	b.nodeCacheMu.RUnlock()
	atomic.AddInt64(&b.cacheMisses, 1)

	var node *Node
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decodeNodeWithEmbeddings(txn, val, id)
			return decodeErr
		})
	})

	// Cache the result on successful fetch
	if err == nil && node != nil {
		if b.filterNodeByDecay(node, DecayScoringTime()) {
			return nil, ErrNotFound
		}
		b.cacheStoreNode(node)
	}

	return node, err
}

// UpdateNode updates an existing node or creates it if it doesn't exist (upsert).
func (b *BadgerEngine) UpdateNode(node *Node) error {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurPut)
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if !strings.Contains(string(node.ID), ":") {
		return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	dbName, _, ok := ParseDatabasePrefix(string(node.ID))
	if !ok {
		return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
	}
	schema := b.GetSchemaForNamespace(dbName)

	// Track if this is an insert (new node) or update (existing node)
	wasInsert := false
	var existingNode *Node

	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		key := nodeKey(node.ID)

		// Get existing node for label index updates (if exists)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist - do an insert (upsert behavior)
			wasInsert = true
			if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
				return err
			}
			data, embeddingsSeparate, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node: %w", err)
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}

			// Persist separate embeddings in bounded txns after this txn commits.
			if embeddingsSeparate {
				persistSeparateEmbeddings = true
				embeddingsToPersist = node.ChunkEmbeddings
			}
			// Create label indexes
			for _, label := range node.Labels {
				if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
					return err
				}
			}
			// Add to pending embeddings index if needed (same as CreateNode)
			if !isSystemNamespaceID(string(node.ID)) &&
				(len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) &&
				NodeNeedsEmbedding(node) {
				if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
					return err
				}
			}
			if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
				return err
			}
			return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
		}
		if err != nil {
			return err
		}

		// Node exists - update it
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existingNode, decodeErr = decodeNodeWithEmbeddings(txn, val, node.ID)
			return decodeErr
		}); err != nil {
			return err
		}

		if node.CreatedAt.IsZero() {
			node.CreatedAt = existingNode.CreatedAt
		}
		if node.UpdatedAt.IsZero() {
			node.UpdatedAt = time.Now()
		}

		if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
			return err
		}

		// Validate policy constraints on label changes.
		if err := b.validatePolicyOnNodeLabelChangeInTxn(txn, node, existingNode, schema, dbName); err != nil {
			return err
		}

		// Remove old label indexes
		for _, label := range existingNode.Labels {
			if err := txn.Delete(labelIndexKey(label, node.ID)); err != nil {
				return err
			}
		}

		// Serialize and store updated node (may store embeddings separately if too large)
		data, embeddingsSeparate, err := encodeNode(node)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}

		// If embeddings are stored separately, write them after commit in bounded txns.
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = node.ChunkEmbeddings
		} else {
			// Node fits inline - clean up any old separately stored embeddings
			embPrefix := embeddingPrefix(node.ID)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = embPrefix
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				if err := txn.Delete(it.Item().Key()); err != nil {
					return fmt.Errorf("failed to delete old embedding chunk: %w", err)
				}
			}
		}

		// Create new label indexes
		for _, label := range node.Labels {
			if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
				return err
			}
		}
		if err := putIndexEntryCatalogInTxn(txn, string(node.ID), &IndexEntryCatalog{
			TargetID:    string(node.ID),
			TargetScope: "NODE",
			IndexKeys:   collectNodeIndexKeys(node.ID, node.Labels),
		}); err != nil {
			return err
		}

		// Manage pending embeddings index atomically
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			// Node has embedding - remove from pending index
			txn.Delete(pendingEmbedKey(node.ID))
		} else if !isSystemNamespaceID(string(node.ID)) && NodeNeedsEmbedding(node) {
			// Node needs embedding - ensure it's in pending index
			txn.Set(pendingEmbedKey(node.ID), []byte{})
		} else {
			// Never embed system database nodes.
			txn.Delete(pendingEmbedKey(node.ID))
		}

		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, existingNode, node); err != nil {
			return err
		}
		if err := b.writeNodeMVCCVersionInTxn(txn, node, version); err != nil {
			return err
		}
		return b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false)
	})
	if err == nil && persistSeparateEmbeddings {
		err = b.replaceSeparateEmbeddingChunks(node.ID, embeddingsToPersist)
	}

	// Update cache on successful operation
	if err == nil {
		if wasInsert {
			// Register unique constraint values
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					schema.RegisterUniqueValue(label, propName, propValue, node.ID)
				}
			}

			b.cacheOnNodeCreated(node)
			// Notify listeners about the new node
			b.notifyNodeCreated(node)
		} else {
			if existingNode != nil {
				for _, label := range existingNode.Labels {
					for propName, propValue := range existingNode.Properties {
						schema.UnregisterUniqueValue(label, propName, propValue)
					}
				}
			}
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					schema.RegisterUniqueValue(label, propName, propValue, node.ID)
				}
			}

			b.cacheOnNodeUpdatedWithOldNode(node, existingNode)
			// Notify listeners to re-index the updated node
			b.notifyNodeUpdated(node)
		}
	}

	return err
}

// UpdateNodeEmbedding updates only the embedding field of an existing node.
// Returns ErrNotFound if the node doesn't exist (does NOT create the node).
// This is used by the embedding queue to prevent creating orphaned nodes.
// REQUIRES: node.ID must be prefixed with namespace (e.g., "nornic:node-123").
func (b *BadgerEngine) UpdateNodeEmbedding(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	// Enforce namespace prefix at storage layer - all node IDs must be prefixed
	if !strings.Contains(string(node.ID), ":") {
		return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got unprefixed ID: %s", node.ID)
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	var updated *Node
	var persistSeparateEmbeddings bool
	var embeddingsToPersist [][]float32
	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		key := nodeKey(node.ID)

		// Get existing node - MUST exist (no upsert)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound // Node doesn't exist - don't create it
		}
		if err != nil {
			return err
		}

		// Decode existing node
		var existing *Node
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = decodeNodeWithEmbeddings(txn, val, node.ID)
			return decodeErr
		}); err != nil {
			return err
		}

		// Update only the embedding and related metadata (stored in ChunkEmbeddings and EmbedMeta)
		existing.ChunkEmbeddings = node.ChunkEmbeddings
		// Copy embedding metadata from EmbedMeta (not Properties - avoids namespace pollution)
		if node.EmbedMeta != nil {
			existing.EmbedMeta = make(map[string]any, len(node.EmbedMeta))
			for k, v := range node.EmbedMeta {
				existing.EmbedMeta[k] = v
			}
		}
		existing.UpdatedAt = time.Now() // Use time from encoding if available, otherwise current time

		// Serialize and store updated node (may store embeddings separately if too large)
		data, embeddingsSeparate, err := encodeNode(existing)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}

		// If embeddings are stored separately, write them after commit in bounded txns.
		if embeddingsSeparate {
			persistSeparateEmbeddings = true
			embeddingsToPersist = existing.ChunkEmbeddings
		} else {
			// Node fits inline - clean up any old separately stored embeddings
			embPrefix := embeddingPrefix(node.ID)
			opts := badger.DefaultIteratorOptions
			opts.Prefix = embPrefix
			embIt := txn.NewIterator(opts)
			defer embIt.Close()
			for embIt.Rewind(); embIt.Valid(); embIt.Next() {
				if err := txn.Delete(embIt.Item().Key()); err != nil {
					return fmt.Errorf("failed to delete old embedding chunk: %w", err)
				}
			}
		}

		// Remove from pending embeddings index if node now has embeddings
		if len(existing.ChunkEmbeddings) > 0 && len(existing.ChunkEmbeddings[0]) > 0 {
			txn.Delete(pendingEmbedKey(node.ID))
		}

		updated = existing
		if err := b.writeNodeMVCCVersionInTxn(txn, updated, version); err != nil {
			return err
		}
		return b.writeNodeMVCCHeadInTxn(txn, updated.ID, version, false)
	})
	if err == nil && persistSeparateEmbeddings {
		err = b.replaceSeparateEmbeddingChunks(node.ID, embeddingsToPersist)
	}

	// Update cache on successful operation
	if err == nil {
		if updated == nil {
			updated = node
		}
		b.cacheOnNodeUpdated(updated)
		// Notify listeners to re-index the updated node
		b.notifyNodeUpdated(updated)
	}

	return err
}

func (b *BadgerEngine) replaceSeparateEmbeddingChunks(nodeID NodeID, embeddings [][]float32) error {
	if err := b.deleteEmbeddingChunksBatched(nodeID); err != nil {
		return fmt.Errorf("failed to delete old embedding chunks: %w", err)
	}
	if len(embeddings) == 0 {
		return nil
	}
	if err := b.writeEmbeddingChunksBatched(nodeID, embeddings); err != nil {
		return err
	}
	return nil
}

func (b *BadgerEngine) deleteEmbeddingChunksBatched(nodeID NodeID) error {
	const deleteBatchSize = 256
	prefix := embeddingPrefix(nodeID)

	for {
		keys := make([][]byte, 0, deleteBatchSize)
		err := b.withView(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix
			it := txn.NewIterator(opts)
			defer it.Close()
			for it.Rewind(); it.ValidForPrefix(prefix); it.Next() {
				keys = append(keys, append([]byte(nil), it.Item().Key()...))
				if len(keys) >= deleteBatchSize {
					break
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			return nil
		}

		if err := b.withUpdate(func(txn *badger.Txn) error {
			for _, key := range keys {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
}

func (b *BadgerEngine) writeEmbeddingChunksBatched(nodeID NodeID, embeddings [][]float32) error {
	const (
		maxChunksPerTxn = 128
		maxBytesPerTxn  = 1 << 20 // 1 MiB per txn keeps requests small
	)

	for start := 0; start < len(embeddings); {
		next := start
		err := b.withUpdate(func(txn *badger.Txn) error {
			bytesUsed := 0
			chunksWritten := 0
			for i := start; i < len(embeddings); i++ {
				embData, err := encodeEmbedding(embeddings[i])
				if err != nil {
					return fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
				}
				if chunksWritten > 0 &&
					(chunksWritten >= maxChunksPerTxn || bytesUsed+len(embData) > maxBytesPerTxn) {
					break
				}
				if err := txn.Set(embeddingKey(nodeID, i), embData); err != nil {
					return fmt.Errorf("failed to store embedding chunk %d: %w", i, err)
				}
				next = i + 1
				chunksWritten++
				bytesUsed += len(embData)
			}
			if chunksWritten == 0 {
				return fmt.Errorf("failed to store embedding chunk %d: chunk exceeds per-txn write budget", start)
			}
			return nil
		})
		if err != nil {
			return err
		}
		start = next
	}
	return nil
}

// DeleteNode removes a node and all its edges.
func (b *BadgerEngine) DeleteNode(id NodeID) error {
	start := time.Now()
	defer b.observeStorageOp(start, b.opDurDelete)
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Track edge deletions for counter update after transaction
	var totalEdgesDeleted int64
	var deletedEdgeIDs []EdgeID
	var deletedNode *Node

	err := b.withUpdate(func(txn *badger.Txn) error {
		version, allocErr := b.allocateMVCCVersion(txn, time.Now())
		if allocErr != nil {
			return allocErr
		}
		edgesDeleted, edgeIDs, node, err := b.deleteNodeInTxn(txn, id)
		totalEdgesDeleted = edgesDeleted
		deletedEdgeIDs = edgeIDs
		deletedNode = node
		if err != nil {
			return err
		}
		if deletedNode == nil {
			return nil
		}
		dbName, _, ok := ParseDatabasePrefix(string(deletedNode.ID))
		if !ok {
			return nil
		}
		schema := b.GetSchemaForNamespace(dbName)
		if err := b.applyTemporalIndexesForNodeChangeInTxn(txn, dbName, schema, deletedNode, nil); err != nil {
			return err
		}
		if err := b.writeNodeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		if err := b.writeNodeMVCCHeadInTxn(txn, id, version, true); err != nil {
			return err
		}
		for _, edgeID := range deletedEdgeIDs {
			if err := b.writeEdgeMVCCTombstoneInTxn(txn, edgeID, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, edgeID, version, true); err != nil {
				return err
			}
		}
		return nil
	})

	// Invalidate cache on successful delete
	if err == nil {
		if deletedNode != nil {
			dbName, _, ok := ParseDatabasePrefix(string(deletedNode.ID))
			if ok {
				schema := b.GetSchemaForNamespace(dbName)
				for _, label := range deletedNode.Labels {
					for propName, propValue := range deletedNode.Properties {
						schema.UnregisterUniqueValue(label, propName, propValue)
					}
				}
			}
		}

		if deletedNode != nil {
			b.cacheOnNodeDeletedWithLabels(id, deletedNode.Labels, totalEdgesDeleted)
		} else {
			b.cacheOnNodeDeleted(id, totalEdgesDeleted)
		}

		// Notify listeners about deleted edges
		for _, edgeID := range deletedEdgeIDs {
			b.notifyEdgeDeleted(edgeID)
		}

		// Notify listeners (e.g., search service) to remove from indexes
		b.notifyNodeDeleted(id)
	}

	return err
}

// deleteEdgesWithPrefix deletes all edges matching a prefix (helper for DeleteNode).
// deleteEdgesWithPrefix deletes all edges matching the given prefix.
// Returns the count of edges actually deleted for accurate stats tracking.
// IMPORTANT: The returned count MUST be used to decrement edgeCount after txn commits.
func (b *BadgerEngine) deleteEdgesWithPrefix(txn *badger.Txn, prefix []byte) (int64, []EdgeID, error) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var edgeIDs []EdgeID
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		edgeID := extractEdgeIDFromIndexKey(it.Item().Key())
		edgeIDs = append(edgeIDs, edgeID)
	}

	var deletedCount int64
	var deletedIDs []EdgeID
	for _, edgeID := range edgeIDs {
		err := b.deleteEdgeInTxn(txn, edgeID)
		if err == nil {
			deletedCount++
			deletedIDs = append(deletedIDs, edgeID)
		} else if err != ErrNotFound {
			return 0, nil, err
		}
	}

	return deletedCount, deletedIDs, nil
}

// ============================================================================
