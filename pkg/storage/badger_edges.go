// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Edge Operations
// ============================================================================

// CreateEdge creates a new edge between two nodes.
func (b *BadgerEngine) CreateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// PERFORMANCE OPTIMIZATION: Use WriteBatch to batch all writes (edge + indexes)
	// This reduces write amplification from 4 separate writes to 1 batch operation.
	// We still validate existence via a read transaction first.
	var exists bool
	err := b.db.View(func(txn *badger.Txn) error {
		// Check if edge already exists
		key := edgeKey(edge.ID)
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if err != badger.ErrKeyNotFound {
			return err
		}

		// Verify start node exists
		_, err = txn.Get(nodeKey(edge.StartNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		// Verify end node exists
		_, err = txn.Get(nodeKey(edge.EndNode))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}
	if exists {
		return ErrAlreadyExists
	}

	err = b.withUpdate(func(txn *badger.Txn) error {
		// Validate relationship constraints before writing
		dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
		schema := b.GetSchemaForNamespace(dbName)
		if schema != nil {
			if err := b.validateEdgeConstraintsInTxn(txn, edge, schema, dbName, ""); err != nil {
				return err
			}
		}

		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}
		if err := txn.Set(edgeKey(edge.ID), data); err != nil {
			return err
		}
		if err := txn.Set(outgoingIndexKey(edge.StartNode, edge.ID), []byte{}); err != nil {
			return err
		}
		if err := txn.Set(incomingIndexKey(edge.EndNode, edge.ID), []byte{}); err != nil {
			return err
		}
		if err := txn.Set(edgeTypeIndexKey(edge.Type, edge.ID), []byte{}); err != nil {
			return err
		}
		if err := writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(edge.ID), &IndexEntryCatalog{
			TargetID:    string(edge.ID),
			TargetScope: "EDGE",
			IndexKeys:   collectEdgeIndexKeys(edge.ID, edge.StartNode, edge.EndNode, edge.Type),
		}); err != nil {
			return err
		}
		if err := b.writeEdgeMVCCVersionInTxn(txn, edge, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false)
	})

	// Invalidate only this edge type (not entire cache)
	if err == nil {
		b.cacheOnEdgeCreated(edge)

		// Notify listeners (e.g., graph analyzers) about the new edge
		b.notifyEdgeCreated(edge)
	}

	return err
}

// GetEdge retrieves an edge by ID.
func (b *BadgerEngine) GetEdge(id EdgeID) (*Edge, error) {
	if id == "" {
		return nil, ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return nil, err
	}

	var edge *Edge
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(edgeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			var decodeErr error
			edge, decodeErr = decodeEdge(val)
			return decodeErr
		})
	})

	return edge, err
}

// UpdateEdge updates an existing edge.
func (b *BadgerEngine) UpdateEdge(edge *Edge) error {
	if edge == nil {
		return ErrInvalidData
	}
	if edge.ID == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	var oldType string
	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		key := edgeKey(edge.ID)

		// Get existing edge
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		if err != nil {
			return err
		}

		var existing *Edge
		if err := item.Value(func(val []byte) error {
			var decodeErr error
			existing, decodeErr = decodeEdge(val)
			return decodeErr
		}); err != nil {
			return err
		}

		oldType = existing.Type

		// Validate relationship constraints on updated edge
		dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
		schema := b.GetSchemaForNamespace(dbName)
		if schema != nil {
			if err := b.validateEdgeConstraintsInTxn(txn, edge, schema, dbName, edge.ID); err != nil {
				return err
			}
		}

		// If endpoints changed, update indexes
		if existing.StartNode != edge.StartNode || existing.EndNode != edge.EndNode {
			// Verify new endpoints exist
			if _, err := txn.Get(nodeKey(edge.StartNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if _, err := txn.Get(nodeKey(edge.EndNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}

			// Remove old indexes
			if err := txn.Delete(outgoingIndexKey(existing.StartNode, edge.ID)); err != nil {
				return err
			}
			if err := txn.Delete(incomingIndexKey(existing.EndNode, edge.ID)); err != nil {
				return err
			}
			if err := deleteEdgeBetweenIndexesInTxn(txn, existing); err != nil {
				return err
			}

			// Add new indexes
			if err := txn.Set(outgoingIndexKey(edge.StartNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(edge.EndNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
				return err
			}
		}

		// If type changed, update edge type index.
		if existing.Type != edge.Type {
			if existing.Type != "" {
				if err := txn.Delete(edgeTypeIndexKey(existing.Type, edge.ID)); err != nil {
					return err
				}
			}
			if existing.StartNode == edge.StartNode && existing.EndNode == edge.EndNode {
				if err := deleteEdgeBetweenIndexesInTxn(txn, existing); err != nil {
					return err
				}
			}
			if edge.Type != "" {
				if err := txn.Set(edgeTypeIndexKey(edge.Type, edge.ID), []byte{}); err != nil {
					return err
				}
			}
			if existing.StartNode == edge.StartNode && existing.EndNode == edge.EndNode {
				if err := writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
					return err
				}
			}
		}

		// Store updated edge
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}

		if err := txn.Set(key, data); err != nil {
			return err
		}
		if err := putIndexEntryCatalogInTxn(txn, string(edge.ID), &IndexEntryCatalog{
			TargetID:    string(edge.ID),
			TargetScope: "EDGE",
			IndexKeys:   collectEdgeIndexKeys(edge.ID, edge.StartNode, edge.EndNode, edge.Type),
		}); err != nil {
			return err
		}
		if err := b.writeEdgeMVCCVersionInTxn(txn, edge, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false)
	})

	// Notify listeners on successful update
	if err == nil {
		b.cacheOnEdgeUpdated(oldType, edge)
		b.notifyEdgeUpdated(edge)
	}

	return err
}

// DeleteEdge removes an edge.
func (b *BadgerEngine) DeleteEdge(id EdgeID) error {
	if id == "" {
		return ErrInvalidID
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Get edge type before deletion for selective cache invalidation
	edge, _ := b.GetEdge(id)
	var edgeType string
	if edge != nil {
		edgeType = edge.Type
	}

	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		if err := b.deleteEdgeInTxn(txn, id); err != nil {
			return err
		}
		if err := b.writeEdgeMVCCTombstoneInTxn(txn, id, version); err != nil {
			return err
		}
		return b.writeEdgeMVCCHeadInTxn(txn, id, version, true)
	})

	// Invalidate only this edge type (not entire cache)
	if err == nil {
		if edgeType != "" {
			b.cacheOnEdgeDeleted(id, edgeType)
		} else {
			b.cacheOnEdgesDeleted([]EdgeID{id})
		}

		// Notify listeners (e.g., graph analyzers) about the deleted edge
		b.notifyEdgeDeleted(id)
	}

	return err
}

// deleteEdgeInTxn is the internal helper for deleting an edge within a transaction.
func (b *BadgerEngine) deleteEdgeInTxn(txn *badger.Txn, id EdgeID) error {
	key := edgeKey(id)

	// Get edge for index cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	if err != nil {
		return err
	}

	var edge *Edge
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		edge, decodeErr = decodeEdge(val)
		return decodeErr
	}); err != nil {
		return err
	}

	// Delete indexes
	if err := txn.Delete(outgoingIndexKey(edge.StartNode, id)); err != nil {
		return err
	}
	if err := txn.Delete(incomingIndexKey(edge.EndNode, id)); err != nil {
		return err
	}
	if err := txn.Delete(edgeTypeIndexKey(edge.Type, id)); err != nil {
		return err
	}
	if err := deleteEdgeBetweenIndexesInTxn(txn, edge); err != nil {
		return err
	}

	deleteIndexEntryCatalogInTxn(txn, string(id))

	// Delete edge
	return txn.Delete(key)
}

// deleteNodeInTxn is the internal helper for deleting a node within a transaction.
// Returns the count of edges that were deleted along with the node (for stats tracking).
func (b *BadgerEngine) deleteNodeInTxn(txn *badger.Txn, id NodeID) (edgesDeleted int64, deletedEdgeIDs []EdgeID, deletedNode *Node, err error) {
	key := nodeKey(id)

	// CRITICAL: Delete separately stored embeddings FIRST, before checking if node exists.
	// This ensures embeddings are cleaned up even if the node record is missing or corrupted.
	embPrefix := embeddingPrefix(id)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = embPrefix
	it := txn.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		if err := txn.Delete(it.Item().Key()); err != nil {
			return 0, nil, nil, fmt.Errorf("failed to delete embedding chunk: %w", err)
		}
	}

	// Get node for label cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		// Node doesn't exist, but we've already cleaned up embeddings above.
		// Also clean up pending embeddings index.
		txn.Delete(pendingEmbedKey(id))
		return 0, nil, nil, ErrNotFound
	}
	if err != nil {
		return 0, nil, nil, err
	}

	if err := item.Value(func(val []byte) error {
		var decodeErr error
		// Extract nodeID from key (skip prefix byte)
		nodeID := NodeID(key[1:])
		deletedNode, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
		return decodeErr
	}); err != nil {
		return 0, nil, nil, err
	}

	// Delete label indexes
	for _, label := range deletedNode.Labels {
		if err := txn.Delete(labelIndexKey(label, id)); err != nil {
			return 0, nil, nil, err
		}
	}

	// Delete outgoing edges (and track count)
	outPrefix := outgoingIndexPrefix(id)
	outCount, outIDs, err := b.deleteEdgesWithPrefix(txn, outPrefix)
	if err != nil {
		return 0, nil, nil, err
	}
	edgesDeleted += outCount
	deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

	// Delete incoming edges (and track count)
	inPrefix := incomingIndexPrefix(id)
	inCount, inIDs, err := b.deleteEdgesWithPrefix(txn, inPrefix)
	if err != nil {
		return 0, nil, nil, err
	}
	edgesDeleted += inCount
	deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

	// Remove from pending embeddings index (if present)
	txn.Delete(pendingEmbedKey(id))

	// Remove index entry catalog
	deleteIndexEntryCatalogInTxn(txn, string(id))

	// Delete the node
	return edgesDeleted, deletedEdgeIDs, deletedNode, txn.Delete(key)
}

// BulkDeleteNodes removes multiple nodes in a single transaction.
// This is much faster than calling DeleteNode repeatedly.
// IMPORTANT: This also deletes all edges connected to the deleted nodes and updates edge counts.
func (b *BadgerEngine) BulkDeleteNodes(ids []NodeID) error {
	if len(ids) == 0 {
		return nil
	}
	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Track which nodes were actually deleted for accurate counting
	deletedNodeCount := int64(0)
	deletedNodeIDs := make([]NodeID, 0, len(ids))
	deletedNodes := make([]*Node, 0, len(ids))
	// Track edges deleted along with nodes
	totalEdgesDeleted := int64(0)
	deletedEdgeIDs := make([]EdgeID, 0)

	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			edgesDeleted, edgeIDs, deletedNode, err := b.deleteNodeInTxn(txn, id)
			if err == nil {
				deletedNodeCount++                          // Successfully deleted
				deletedNodeIDs = append(deletedNodeIDs, id) // Track for callbacks
				if deletedNode != nil {
					deletedNodes = append(deletedNodes, deletedNode)
				}
				totalEdgesDeleted += edgesDeleted
				deletedEdgeIDs = append(deletedEdgeIDs, edgeIDs...)
			} else if err != ErrNotFound {
				return err // Actual error, abort transaction
			}
			// ErrNotFound is ignored (node didn't exist, no count change)
		}
		for _, id := range deletedNodeIDs {
			if err := b.writeNodeMVCCTombstoneInTxn(txn, id, version); err != nil {
				return err
			}
			if err := b.writeNodeMVCCHeadInTxn(txn, id, version, true); err != nil {
				return err
			}
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

	// Invalidate cache for deleted nodes and update counts
	if err == nil {
		for _, node := range deletedNodes {
			if node == nil {
				continue
			}
			dbName, _, ok := ParseDatabasePrefix(string(node.ID))
			if !ok {
				continue
			}
			schema := b.GetSchemaForNamespace(dbName)
			if schema == nil {
				continue
			}
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					schema.UnregisterUniqueValue(label, propName, propValue)
				}
			}
		}

		b.cacheOnNodesDeletedWithLabels(deletedNodes, deletedNodeCount, totalEdgesDeleted)

		// Notify listeners about deleted edges
		for _, edgeID := range deletedEdgeIDs {
			b.notifyEdgeDeleted(edgeID)
		}

		// Notify listeners (e.g., search service) for each deleted node
		// Use async notifications to avoid blocking bulk deletes (e.g., collection deletion)
		// The search service can handle these notifications in the background
		if len(deletedNodeIDs) > 0 {
			go func(ids []NodeID) {
				for _, id := range ids {
					b.notifyNodeDeleted(id)
				}
			}(deletedNodeIDs)
		}
	}

	return err
}

// BulkDeleteEdges removes multiple edges in a single transaction.
// This is much faster than calling DeleteEdge repeatedly.
func (b *BadgerEngine) BulkDeleteEdges(ids []EdgeID) error {
	if len(ids) == 0 {
		return nil
	}

	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Track which edges were actually deleted for accurate counting
	deletedCount := int64(0)
	deletedIDs := make([]EdgeID, 0, len(ids))
	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			err := b.deleteEdgeInTxn(txn, id)
			if err == nil {
				deletedCount++                      // Successfully deleted
				deletedIDs = append(deletedIDs, id) // Track for callbacks
			} else if err != ErrNotFound {
				return err // Actual error, abort transaction
			}
			// ErrNotFound is ignored (edge didn't exist, no count change)
		}
		for _, id := range deletedIDs {
			if err := b.writeEdgeMVCCTombstoneInTxn(txn, id, version); err != nil {
				return err
			}
			if err := b.writeEdgeMVCCHeadInTxn(txn, id, version, true); err != nil {
				return err
			}
		}
		return nil
	})

	// Invalidate edge type cache on successful bulk delete and update count
	if err == nil && deletedCount > 0 {
		b.cacheOnEdgesDeleted(deletedIDs)

		// Notify listeners (e.g., graph analyzers) for each deleted edge
		for _, id := range deletedIDs {
			b.notifyEdgeDeleted(id)
		}
	}

	return err
}

// ============================================================================
