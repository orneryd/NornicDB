// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"
	"time"

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

	if err := b.validateBulkNodeConstraints(nodes); err != nil {
		return err
	}

	err := b.withUpdate(func(txn *badger.Txn) error {
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
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
			dbName, _, ok := ParseDatabasePrefix(string(node.ID))
			if !ok {
				return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
			}
			schema := b.GetSchemaForNamespace(dbName)
			if err := b.validateNodeConstraintsInTxn(txn, node, schema, dbName, node.ID); err != nil {
				return err
			}

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
				lblKey, err := b.labelIndexKeyString(txn, label, node.ID)
				if err != nil {
					return err
				}
				if err := txn.Set(lblKey, []byte{}); err != nil {
					return err
				}
			}
			// Create-only path: the primary key (nodeKey) IS the current
			// head body. No version-record write — that halves write
			// amplification on the hot path.
			if err := b.writeNodeMVCCHeadInTxn(txn, node.ID, version, false); err != nil {
				return err
			}
		}

		return nil
	})

	// Register unique constraint values after successful bulk insert
	if err == nil {
		for _, node := range nodes {
			dbName, _, ok := ParseDatabasePrefix(string(node.ID))
			if !ok {
				continue
			}
			schema := b.GetSchemaForNamespace(dbName)
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					schema.RegisterUniqueValue(label, propName, propValue, node.ID)
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

func (b *BadgerEngine) validateBulkNodeConstraints(nodes []*Node) error {
	seen := make(map[string]struct{})

	for _, node := range nodes {
		dbName, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			return fmt.Errorf("node ID must be prefixed with namespace (e.g., 'nornic:node-123'), got: %s", node.ID)
		}
		schema := b.GetSchemaForNamespace(dbName)
		if schema == nil {
			continue
		}

		constraints := schema.GetConstraintsForLabels(node.Labels)
		for _, c := range constraints {
			switch c.Type {
			case ConstraintUnique:
				if len(c.Properties) != 1 {
					continue
				}
				prop := c.Properties[0]
				value := node.Properties[prop]
				if value == nil {
					continue
				}
				key := fmt.Sprintf("%s:%s:%s", dbName, c.Name, constraintValueKey(value))
				if _, exists := seen[key]; exists {
					return &ConstraintViolationError{
						Type:       ConstraintUnique,
						Label:      c.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Node with %s=%v already exists in batch", prop, value),
					}
				}
				seen[key] = struct{}{}
			case ConstraintNodeKey:
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
				key := fmt.Sprintf("%s:%s:%s", dbName, c.Name, constraintCompositeKey(values))
				if _, exists := seen[key]; exists {
					return &ConstraintViolationError{
						Type:       ConstraintNodeKey,
						Label:      c.Label,
						Properties: c.Properties,
						Message:    fmt.Sprintf("Node with key %v=%v already exists in batch", c.Properties, values),
					}
				}
				seen[key] = struct{}{}
			case ConstraintExists:
				if len(c.Properties) != 1 {
					continue
				}
				prop := c.Properties[0]
				if node.Properties == nil {
					return &ConstraintViolationError{
						Type:       ConstraintExists,
						Label:      c.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Required property %s is missing", prop),
					}
				}
				if val, ok := node.Properties[prop]; !ok || val == nil {
					return &ConstraintViolationError{
						Type:       ConstraintExists,
						Label:      c.Label,
						Properties: []string{prop},
						Message:    fmt.Sprintf("Required property %s is missing", prop),
					}
				}
			}
		}
	}

	return nil
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
		version, err := b.allocateMVCCVersion(txn, time.Now())
		if err != nil {
			return err
		}
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

			// Validate relationship constraints
			dbName, _, _ := ParseDatabasePrefix(string(edge.ID))
			schema := b.GetSchemaForNamespace(dbName)
			if schema != nil {
				if err := b.validateEdgeConstraintsInTxn(txn, edge, schema, dbName, ""); err != nil {
					return err
				}
			}
		}

		// Insert all edges
		for _, edge := range edges {
			data, err := b.encodeEdgeInTxn(txn, edge)
			if err != nil {
				return fmt.Errorf("failed to encode edge: %w", err)
			}

			if err := txn.Set(edgeKey(edge.ID), data); err != nil {
				return err
			}

			outKey, err := b.outgoingIndexKeyString(txn, edge.StartNode, edge.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(outKey, []byte{}); err != nil {
				return err
			}
			inKey, err := b.incomingIndexKeyString(txn, edge.EndNode, edge.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(inKey, []byte{}); err != nil {
				return err
			}
			typeKey, err := b.edgeTypeIndexKeyString(txn, edge.Type, edge.ID)
			if err != nil {
				return err
			}
			if err := txn.Set(typeKey, []byte{}); err != nil {
				return err
			}
			if err := b.writeEdgeBetweenIndexesInTxn(txn, edge); err != nil {
				return err
			}
			// Create-only path: primary key IS the current head body.
			if err := b.writeEdgeMVCCHeadInTxn(txn, edge.ID, version, false); err != nil {
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

	prefix := b.incomingIndexPrefixString(nodeID)
	if prefix == nil {
		return 0
	}
	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
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

	prefix := b.outgoingIndexPrefixString(nodeID)
	if prefix == nil {
		return 0
	}
	count := 0
	_ = b.withView(func(txn *badger.Txn) error {
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
