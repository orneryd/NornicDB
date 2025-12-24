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

	// Check unique constraints for all labels and properties
	for _, label := range node.Labels {
		for propName, propValue := range node.Properties {
			if err := b.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
				return "", fmt.Errorf("constraint violation: %w", err)
			}
		}
	}

	err := b.withUpdate(func(txn *badger.Txn) error {
		// Check if node already exists
		key := nodeKey(node.ID)
		_, err := txn.Get(key)
		if err == nil {
			return ErrAlreadyExists
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Serialize node (may store embeddings separately if too large)
		data, embeddingsSeparate, err := encodeNode(node)
		if err != nil {
			return fmt.Errorf("failed to encode node: %w", err)
		}

		// Store node
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// If embeddings are stored separately, store them now
		if embeddingsSeparate {
			for i, emb := range node.ChunkEmbeddings {
				embKey := embeddingKey(node.ID, i)
				embData, err := encodeEmbedding(emb)
				if err != nil {
					return fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
				}
				if err := txn.Set(embKey, embData); err != nil {
					return fmt.Errorf("failed to store embedding chunk %d: %w", i, err)
				}
			}
		}

		// Create label indexes
		for _, label := range node.Labels {
			labelKey := labelIndexKey(label, node.ID)
			if err := txn.Set(labelKey, []byte{}); err != nil {
				return err
			}
		}

		// Add to pending embeddings index if it needs embedding (atomic with node creation)
		if (len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) && NodeNeedsEmbedding(node) {
			if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
				return err
			}
		}

		return nil
	})

	// On successful create, update cache and register unique constraint values
	if err == nil {
		// Register unique constraint values
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				b.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
			}
		}

		b.cacheOnNodeCreated(node)

		// Notify listeners (e.g., search service) to index the new node
		b.notifyNodeCreated(node)
	}

	if err != nil {
		return "", err
	}
	return node.ID, nil
}

// GetNode retrieves a node by ID.
func (b *BadgerEngine) GetNode(id NodeID) (*Node, error) {
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
		return copyNode(cached), nil
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
		b.cacheStoreNode(node)
	}

	return node, err
}

// UpdateNode updates an existing node or creates it if it doesn't exist (upsert).
func (b *BadgerEngine) UpdateNode(node *Node) error {
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

	// Track if this is an insert (new node) or update (existing node)
	wasInsert := false

	err := b.withUpdate(func(txn *badger.Txn) error {
		key := nodeKey(node.ID)

		// Get existing node for label index updates (if exists)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// Node doesn't exist - do an insert (upsert behavior)
			wasInsert = true
			data, embeddingsSeparate, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node: %w", err)
			}
			if err := txn.Set(key, data); err != nil {
				return err
			}

			// If embeddings are stored separately, store them now
			if embeddingsSeparate {
				for i, emb := range node.ChunkEmbeddings {
					embKey := embeddingKey(node.ID, i)
					embData, err := encodeEmbedding(emb)
					if err != nil {
						return fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
					}
					if err := txn.Set(embKey, embData); err != nil {
						return fmt.Errorf("failed to store embedding chunk %d: %w", i, err)
					}
				}
			}
			// Create label indexes
			for _, label := range node.Labels {
				if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
					return err
				}
			}
			// Add to pending embeddings index if needed (same as CreateNode)
			if (len(node.ChunkEmbeddings) == 0 || len(node.ChunkEmbeddings[0]) == 0) && NodeNeedsEmbedding(node) {
				if err := txn.Set(pendingEmbedKey(node.ID), []byte{}); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			return err
		}

		// Node exists - update it
		var existing *Node
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = decodeNodeWithEmbeddings(txn, val, node.ID)
			return decodeErr
		}); err != nil {
			return err
		}

		// Remove old label indexes
		for _, label := range existing.Labels {
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

		// If embeddings are stored separately, update them
		if embeddingsSeparate {
			// Delete old embedding chunks (if any)
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

			// Store new embedding chunks
			for i, emb := range node.ChunkEmbeddings {
				embKey := embeddingKey(node.ID, i)
				embData, err := encodeEmbedding(emb)
				if err != nil {
					return fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
				}
				if err := txn.Set(embKey, embData); err != nil {
					return fmt.Errorf("failed to store embedding chunk %d: %w", i, err)
				}
			}
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

		// Manage pending embeddings index atomically
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			// Node has embedding - remove from pending index
			txn.Delete(pendingEmbedKey(node.ID))
		} else if NodeNeedsEmbedding(node) {
			// Node needs embedding - ensure it's in pending index
			txn.Set(pendingEmbedKey(node.ID), []byte{})
		}

		return nil
	})

	// Update cache on successful operation
	if err == nil {
		if wasInsert {
			b.cacheOnNodeCreated(node)
			// Notify listeners about the new node
			b.notifyNodeCreated(node)
		} else {
			b.cacheOnNodeUpdated(node)
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
	err := b.withUpdate(func(txn *badger.Txn) error {
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

		// Update only the embedding and related properties (always stored in ChunkEmbeddings)
		existing.ChunkEmbeddings = node.ChunkEmbeddings
		if node.Properties != nil {
			// Update embedding-related properties
			if val, ok := node.Properties["embedding_model"]; ok {
				if existing.Properties == nil {
					existing.Properties = make(map[string]interface{})
				}
				existing.Properties["embedding_model"] = val
			}
			if val, ok := node.Properties["embedding_dimensions"]; ok {
				if existing.Properties == nil {
					existing.Properties = make(map[string]interface{})
				}
				existing.Properties["embedding_dimensions"] = val
			}
			if val, ok := node.Properties["has_embedding"]; ok {
				if existing.Properties == nil {
					existing.Properties = make(map[string]interface{})
				}
				existing.Properties["has_embedding"] = val
			}
			if val, ok := node.Properties["embedded_at"]; ok {
				if existing.Properties == nil {
					existing.Properties = make(map[string]interface{})
				}
				existing.Properties["embedded_at"] = val
			}
			if val, ok := node.Properties["embedding"]; ok {
				if existing.Properties == nil {
					existing.Properties = make(map[string]interface{})
				}
				existing.Properties["embedding"] = val
			}
			// Also copy other properties that might be set during embedding
			for k, v := range node.Properties {
				if k == "has_chunks" || k == "chunk_count" {
					if existing.Properties == nil {
						existing.Properties = make(map[string]interface{})
					}
					existing.Properties[k] = v
				}
			}
			// Copy chunk embeddings from struct field (not properties - opaque to users)
			existing.ChunkEmbeddings = node.ChunkEmbeddings
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

		// If embeddings are stored separately, update them
		if embeddingsSeparate {
			// Delete old embedding chunks (if any)
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

			// Store new embedding chunks
			for i, emb := range existing.ChunkEmbeddings {
				embKey := embeddingKey(node.ID, i)
				embData, err := encodeEmbedding(emb)
				if err != nil {
					return fmt.Errorf("failed to encode embedding chunk %d: %w", i, err)
				}
				if err := txn.Set(embKey, embData); err != nil {
					return fmt.Errorf("failed to store embedding chunk %d: %w", i, err)
				}
			}
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
		return nil
	})

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

// DeleteNode removes a node and all its edges.
func (b *BadgerEngine) DeleteNode(id NodeID) error {
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Track edge deletions for counter update after transaction
	var totalEdgesDeleted int64
	var deletedEdgeIDs []EdgeID

	err := b.withUpdate(func(txn *badger.Txn) error {
		key := nodeKey(id)

		// Get node for label cleanup
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var node *Node
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			node, decodeErr = decodeNodeWithEmbeddings(txn, val, id)
			return decodeErr
		}); err != nil {
			return err
		}

		// Delete separately stored embeddings (if any)
		embPrefix := embeddingPrefix(id)
		opts := badger.DefaultIteratorOptions
		opts.Prefix = embPrefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := txn.Delete(it.Item().Key()); err != nil {
				return fmt.Errorf("failed to delete embedding chunk: %w", err)
			}
		}

		// Delete label indexes
		for _, label := range node.Labels {
			if err := txn.Delete(labelIndexKey(label, id)); err != nil {
				return err
			}
		}

		// Delete outgoing edges (and track count)
		outPrefix := outgoingIndexPrefix(id)
		outCount, outIDs, err := b.deleteEdgesWithPrefix(txn, outPrefix)
		if err != nil {
			return err
		}
		totalEdgesDeleted += outCount
		deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

		// Delete incoming edges (and track count)
		inPrefix := incomingIndexPrefix(id)
		inCount, inIDs, err := b.deleteEdgesWithPrefix(txn, inPrefix)
		if err != nil {
			return err
		}
		totalEdgesDeleted += inCount
		deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

		// Remove from pending embeddings index (if present)
		txn.Delete(pendingEmbedKey(id))

		// Delete the node
		return txn.Delete(key)
	})

	// Invalidate cache on successful delete
	if err == nil {
		b.cacheOnNodeDeleted(id, totalEdgesDeleted)

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
