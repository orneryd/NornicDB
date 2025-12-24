// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// Bulk Operations
// ============================================================================

// BulkCreateNodes creates multiple nodes in a single transaction.
func (b *BadgerEngine) BulkCreateNodes(nodes []*Node) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Validate all nodes first
	for _, node := range nodes {
		if node == nil {
			return ErrInvalidData
		}
		if node.ID == "" {
			return ErrInvalidID
		}
	}

	// Check unique constraints for all nodes BEFORE inserting any
	for _, node := range nodes {
		for _, label := range node.Labels {
			for propName, propValue := range node.Properties {
				if err := b.schema.CheckUniqueConstraint(label, propName, propValue, ""); err != nil {
					return fmt.Errorf("constraint violation: %w", err)
				}
			}
		}
	}

	err := b.withUpdate(func(txn *badger.Txn) error {
		// Check for duplicates
		for _, node := range nodes {
			_, err := txn.Get(nodeKey(node.ID))
			if err == nil {
				return ErrAlreadyExists
			}
			if err != badger.ErrKeyNotFound {
				return err
			}
		}

		// Insert all nodes
		for _, node := range nodes {
			data, embeddingsSeparate, err := encodeNode(node)
			if err != nil {
				return fmt.Errorf("failed to encode node: %w", err)
			}

			if err := txn.Set(nodeKey(node.ID), data); err != nil {
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

			for _, label := range node.Labels {
				if err := txn.Set(labelIndexKey(label, node.ID), []byte{}); err != nil {
					return err
				}
			}
		}

		return nil
	})

	// Register unique constraint values after successful bulk insert
	if err == nil {
		for _, node := range nodes {
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					b.schema.RegisterUniqueValue(label, propName, propValue, node.ID)
				}
			}
		}

		b.cacheOnNodesCreated(nodes)

		// Notify listeners (e.g., search service) to index all new nodes
		for _, node := range nodes {
			b.notifyNodeCreated(node)
		}
	}

	return err
}

// BulkCreateEdges creates multiple edges in a single transaction.
func (b *BadgerEngine) BulkCreateEdges(edges []*Edge) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}

	// Validate all edges first
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if edge.ID == "" {
			return ErrInvalidID
		}
	}

	err := b.withUpdate(func(txn *badger.Txn) error {
		// Validate all edges
		for _, edge := range edges {
			// Check edge doesn't exist
			_, err := txn.Get(edgeKey(edge.ID))
			if err == nil {
				return ErrAlreadyExists
			}
			if err != badger.ErrKeyNotFound {
				return err
			}

			// Verify nodes exist
			if _, err := txn.Get(nodeKey(edge.StartNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
			if _, err := txn.Get(nodeKey(edge.EndNode)); err == badger.ErrKeyNotFound {
				return ErrNotFound
			}
		}

		// Insert all edges
		for _, edge := range edges {
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
		}

		return nil
	})

	// Invalidate edge type cache on successful bulk create
	if err == nil && len(edges) > 0 {
		b.cacheOnEdgesCreated(edges)

		// Notify listeners (e.g., graph analyzers) for all new edges
		for _, edge := range edges {
			b.notifyEdgeCreated(edge)
		}
	}

	return err
}

// ============================================================================
// Degree Functions
// ============================================================================

// GetInDegree returns the number of incoming edges to a node.
func (b *BadgerEngine) GetInDegree(nodeID NodeID) int {
	if nodeID == "" {
		return 0
	}
	if b.ensureOpen() != nil {
		return 0
	}

	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
		prefix := incomingIndexPrefix(nodeID)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// GetOutDegree returns the number of outgoing edges from a node.
func (b *BadgerEngine) GetOutDegree(nodeID NodeID) int {
	if nodeID == "" {
		return 0
	}
	if b.ensureOpen() != nil {
		return 0
	}

	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
		prefix := outgoingIndexPrefix(nodeID)
		it := txn.NewIterator(badgerIterOptsKeyOnly(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// ============================================================================
