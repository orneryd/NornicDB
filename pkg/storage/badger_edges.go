// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"

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

	err := b.withUpdate(func(txn *badger.Txn) error {
		// Check if edge already exists
		key := edgeKey(edge.ID)
		_, err := txn.Get(key)
		if err == nil {
			return ErrAlreadyExists
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

		// Serialize edge
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}

		// Store edge
		if err := txn.Set(key, data); err != nil {
			return err
		}

		// Create outgoing index
		outKey := outgoingIndexKey(edge.StartNode, edge.ID)
		if err := txn.Set(outKey, []byte{}); err != nil {
			return err
		}

		// Create incoming index
		inKey := incomingIndexKey(edge.EndNode, edge.ID)
		if err := txn.Set(inKey, []byte{}); err != nil {
			return err
		}

		// Create edge type index
		edgeTypeKey := edgeTypeIndexKey(edge.Type, edge.ID)
		if err := txn.Set(edgeTypeKey, []byte{}); err != nil {
			return err
		}

		return nil
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

			// Add new indexes
			if err := txn.Set(outgoingIndexKey(edge.StartNode, edge.ID), []byte{}); err != nil {
				return err
			}
			if err := txn.Set(incomingIndexKey(edge.EndNode, edge.ID), []byte{}); err != nil {
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
			if edge.Type != "" {
				if err := txn.Set(edgeTypeIndexKey(edge.Type, edge.ID), []byte{}); err != nil {
					return err
				}
			}
		}

		// Store updated edge
		data, err := encodeEdge(edge)
		if err != nil {
			return fmt.Errorf("failed to encode edge: %w", err)
		}

		return txn.Set(key, data)
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
		return b.deleteEdgeInTxn(txn, id)
	})

	// Invalidate only this edge type (not entire cache)
	if err == nil {
		if edgeType != "" {
			b.cacheOnEdgeDeleted(edgeType)
		} else {
			b.cacheOnEdgesDeleted(1)
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

	// Delete edge
	return txn.Delete(key)
}

// deleteNodeInTxn is the internal helper for deleting a node within a transaction.
// Returns the count of edges that were deleted along with the node (for stats tracking).
func (b *BadgerEngine) deleteNodeInTxn(txn *badger.Txn, id NodeID) (edgesDeleted int64, deletedEdgeIDs []EdgeID, err error) {
	key := nodeKey(id)

	// Get node for label cleanup
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil, ErrNotFound
	}
	if err != nil {
		return 0, nil, err
	}

	var node *Node
	if err := item.Value(func(val []byte) error {
		var decodeErr error
		// Extract nodeID from key (skip prefix byte)
		nodeID := NodeID(key[1:])
		node, decodeErr = decodeNodeWithEmbeddings(txn, val, nodeID)
		return decodeErr
	}); err != nil {
		return 0, nil, err
	}

	// Delete label indexes
	for _, label := range node.Labels {
		if err := txn.Delete(labelIndexKey(label, id)); err != nil {
			return 0, nil, err
		}
	}

	// Delete outgoing edges (and track count)
	outPrefix := outgoingIndexPrefix(id)
	outCount, outIDs, err := b.deleteEdgesWithPrefix(txn, outPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += outCount
	deletedEdgeIDs = append(deletedEdgeIDs, outIDs...)

	// Delete incoming edges (and track count)
	inPrefix := incomingIndexPrefix(id)
	inCount, inIDs, err := b.deleteEdgesWithPrefix(txn, inPrefix)
	if err != nil {
		return 0, nil, err
	}
	edgesDeleted += inCount
	deletedEdgeIDs = append(deletedEdgeIDs, inIDs...)

	// Delete the node
	return edgesDeleted, deletedEdgeIDs, txn.Delete(key)
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
	// Track edges deleted along with nodes
	totalEdgesDeleted := int64(0)
	deletedEdgeIDs := make([]EdgeID, 0)

	err := b.withUpdate(func(txn *badger.Txn) error {
		for _, id := range ids {
			if id == "" {
				continue // Skip invalid IDs
			}
			edgesDeleted, edgeIDs, err := b.deleteNodeInTxn(txn, id)
			if err == nil {
				deletedNodeCount++                          // Successfully deleted
				deletedNodeIDs = append(deletedNodeIDs, id) // Track for callbacks
				totalEdgesDeleted += edgesDeleted
				deletedEdgeIDs = append(deletedEdgeIDs, edgeIDs...)
			} else if err != ErrNotFound {
				return err // Actual error, abort transaction
			}
			// ErrNotFound is ignored (node didn't exist, no count change)
		}
		return nil
	})

	// Invalidate cache for deleted nodes and update counts
	if err == nil {
		b.cacheOnNodesDeleted(deletedNodeIDs, deletedNodeCount, totalEdgesDeleted)

		// Notify listeners about deleted edges
		for _, edgeID := range deletedEdgeIDs {
			b.notifyEdgeDeleted(edgeID)
		}

		// Notify listeners (e.g., search service) for each deleted node
		for _, id := range deletedNodeIDs {
			b.notifyNodeDeleted(id)
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
		return nil
	})

	// Invalidate edge type cache on successful bulk delete and update count
	if err == nil && deletedCount > 0 {
		b.cacheOnEdgesDeleted(deletedCount)

		// Notify listeners (e.g., graph analyzers) for each deleted edge
		for _, id := range deletedIDs {
			b.notifyEdgeDeleted(id)
		}
	}

	return err
}

// ============================================================================
