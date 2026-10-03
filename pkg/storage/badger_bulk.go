// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// Bulk Operations
// ============================================================================

// BulkCreateNodes creates nodes as one transaction (BadgerTransaction): all
// of them or none, with the transaction's existence and constraint checks,
// and of any size (#703). A node whose ID exists fails the call with
// ErrAlreadyExists and nothing is written. Nodes of one call share a
// namespace.
func (b *BadgerEngine) BulkCreateNodes(nodes []*Node) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
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
	return b.inBulkTransaction(namespaceForNodeID(nodes[0].ID), func(tx *BadgerTransaction) error {
		for _, node := range nodes {
			if _, err := tx.CreateNode(node); err != nil {
				return err
			}
		}
		return nil
	})
}

// inBulkTransaction runs a bulk operation on namespace as one transaction,
// which commits all of its writes at once whatever their size. Like the
// Cypher executor it loads the namespace's MVCC state before beginning, so
// the snapshot includes what was committed before the engine was reopened. It
// is implicit: like the engine's other writes it leaves durability to
// Badger's write options instead of forcing a sync per call.
func (b *BadgerEngine) inBulkTransaction(namespace string, apply func(tx *BadgerTransaction) error) error {
	if err := b.EnsureNamespaceMVCC(namespace); err != nil {
		return err
	}
	tx, err := b.BeginTransaction()
	if err != nil {
		return err
	}
	err = tx.SetImplicit(true)
	if err == nil {
		err = apply(tx)
	}
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (b *BadgerEngine) validateBulkNodeConstraints(nodes []*Node) error {
	seen := make(map[string]struct{})

	for _, node := range nodes {
		dbName, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			return localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
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

// BulkCreateEdges creates edges as one transaction, all of them or none, of
// any size (#703). An existing edge ID fails the call with ErrAlreadyExists,
// a missing endpoint with ErrNotFound; nothing is written.
func (b *BadgerEngine) BulkCreateEdges(edges []*Edge) error {
	if err := b.ensureOpen(); err != nil {
		return err
	}
	if len(edges) == 0 {
		return nil
	}
	for _, edge := range edges {
		if edge == nil {
			return ErrInvalidData
		}
		if edge.ID == "" {
			return ErrInvalidID
		}
	}
	return b.inBulkTransaction(namespaceForEdgeID(edges[0].ID), func(tx *BadgerTransaction) error {
		return tx.BulkCreateEdges(edges)
	})
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
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
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
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count
}

// ============================================================================
