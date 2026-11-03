// Package storage provides storage engine implementations for NornicDB.
package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Bulk Operations
// ============================================================================

// BulkCreateNodes creates nodes as one transaction (BadgerTransaction): all
// of them or none, with the transaction's existence and constraint checks,
// and of any size (#703). A node whose ID exists fails the call with
// ErrAlreadyExists and nothing is written. Nodes of one call share a
// namespace.
func (b *BadgerEngine) BulkCreateNodes(ctx context.Context, nodes []*Node) error {
	return createBulkEntities(ctx, b, nodes, func(node *Node) string { return string(node.ID) }, func() error { return b.validateBulkNodeConstraints(ctx, nodes) }, func(tx *BadgerTransaction) error {
		for _, node := range nodes {
			if _, err := tx.CreateNode(ctx, node); err != nil {
				return err
			}
		}
		return nil
	}, nil)
}

// inBulkTransaction runs a bulk operation on namespace as one transaction,
// which commits all of its writes at once whatever their size. Like the
// Cypher executor it loads the namespace's MVCC state before beginning, so
// the snapshot includes what was committed before the engine was reopened. It
// is implicit: like the engine's other writes it leaves durability to
// Badger's write options instead of forcing a sync per call.
func (b *BadgerEngine) inBulkTransaction(ctx context.Context, namespace string, apply func(tx *BadgerTransaction) error) error {
	if err := b.EnsureNamespaceMVCC(namespace); err != nil {
		return err
	}
	tx, err := b.BeginTransaction(ctx)
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
	return tx.CommitContext(ctx)
}

func (b *BadgerEngine) validateBulkNodeConstraints(ctx context.Context, nodes []*Node) error {
	type batchConstraint struct{ namespace, name string }
	seen := make(map[batchConstraint]constraintCreationValues)

	for _, node := range nodes {
		dbName, _, ok := ParseDatabasePrefix(string(node.ID))
		if !ok {
			return localizedError(localization.StorageClientNodeIDNamespaceRequired(string(node.ID)), nil)
		}
		schema := b.GetSchemaForNamespace(dbName)
		if schema == nil {
			continue
		}

		// Bulk preflight is part of an accepted write, so its constraint
		// discovery has the write's lifetime and no request to cancel.
		constraints, err := schema.GetConstraintsForLabels(ctx, node.Labels)
		if err != nil {
			return err
		}
		for _, c := range constraints {
			if c.EffectiveEntityType() != ConstraintEntityNode {
				continue
			}
			var values []interface{}
			switch c.Type {
			case ConstraintUnique:
				if len(c.Properties) != 1 {
					continue
				}
				value := node.Properties[c.Properties[0]]
				if value == nil {
					continue
				}
				values = []interface{}{value}
			case ConstraintNodeKey:
				values = make([]interface{}, len(c.Properties))
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
			if values == nil {
				continue
			}
			key := batchConstraint{namespace: dbName, name: c.Name}
			bucket := seen[key]
			if bucket == nil {
				bucket = make(constraintCreationValues)
				seen[key] = bucket
			}
			// Native BulkCreateNodes has no caller context; this preflight runs
			// before inBulkTransaction. Reuse constraint creation's candidate
			// buckets and actual equality: text/hash identity alone would make
			// NaN or differently shaped lists into false duplicates.
			_, duplicate, err := bucket.add(ctx, string(node.ID), values)
			if err != nil {
				return err
			}
			if duplicate {
				message := fmt.Sprintf("Node with key %v=%v already exists in batch", c.Properties, values)
				properties := c.Properties
				if c.Type == ConstraintUnique {
					message = fmt.Sprintf("Node with %s=%v already exists in batch", c.Properties[0], values[0])
					properties = []string{c.Properties[0]}
				}
				return &ConstraintViolationError{Type: c.Type, Label: c.Label, Properties: properties, Message: message}
			}
		}
	}
	return nil
}

// BulkCreateEdges creates edges as one transaction, all of them or none, of
// any size (#703). An existing edge ID fails the call with ErrAlreadyExists,
// a missing endpoint with ErrNotFound; nothing is written.
func (b *BadgerEngine) BulkCreateEdges(ctx context.Context, edges []*Edge) error {
	return createBulkEntities(ctx, b, edges, func(edge *Edge) string { return string(edge.ID) }, nil, func(tx *BadgerTransaction) error { return tx.BulkCreateEdges(ctx, edges) }, func(tx *BadgerTransaction) error {
		for _, edge := range edges {
			if err := tx.CreateEdge(ctx, edge); err != nil {
				return err
			}
		}
		return nil
	})
}

// createBulkEntities owns capture routing, validation admission and the atomic
// transaction boundary; each entity kind supplies only its constraints/writes.
func createBulkEntities[T any](ctx context.Context, b *BadgerEngine, entities []*T, id func(*T) string, validate func() error, apply func(*BadgerTransaction) error, capture func(*BadgerTransaction) error) error {
	var firstID string
	if len(entities) > 0 && entities[0] != nil {
		firstID = id(entities[0])
		captureWrite := capture
		if captureWrite == nil {
			captureWrite = apply
		}
		if handled, err := b.inMutationTransaction(ctx, firstID, false, captureWrite); handled {
			return err
		}
	}
	if err := b.ensureOpen(false); err != nil {
		return err
	}
	if len(entities) == 0 {
		return nil
	}
	for _, entity := range entities {
		if entity == nil {
			return ErrInvalidData
		}
		if id(entity) == "" {
			return ErrInvalidID
		}
	}
	if validate != nil {
		if err := validate(); err != nil {
			return err
		}
	}
	namespace, _, _ := ParseDatabasePrefix(firstID)
	return b.inBulkTransaction(ctx, namespace, apply)
}

// deleteBulkEntities owns bulk deletion routing, filtering and NotFound
// handling. Capture retains its existing ordered input validation; ordinary
// bulk calls omit empty and repeated IDs before invoking the same command.
func deleteBulkEntities[T ~string](ctx context.Context, b *BadgerEngine, ids []T, remove func(*BadgerTransaction, T) error) error {
	if len(ids) == 0 {
		return nil
	}
	apply := func(tx *BadgerTransaction, filter bool) error {
		var seen map[T]struct{}
		if filter {
			seen = make(map[T]struct{}, len(ids))
		}
		for _, id := range ids {
			if filter {
				if _, repeated := seen[id]; repeated || id == "" {
					continue
				}
				seen[id] = struct{}{}
			}
			if err := remove(tx, id); err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
		}
		return nil
	}
	if handled, err := b.inMutationTransaction(ctx, string(ids[0]), false, func(tx *BadgerTransaction) error { return apply(tx, false) }); handled {
		return err
	}
	return b.inBulkTransaction(ctx, firstIDNamespace(ids), func(tx *BadgerTransaction) error { return apply(tx, true) })
}

// ============================================================================
// Degree Functions
// ============================================================================

// GetInDegree returns the number of incoming edges to a node.
func (b *BadgerEngine) GetInDegree(ctx context.Context, nodeID NodeID) (int, error) {
	count, err := b.countByPrefix(ctx, "", 0, nil, adjacencyCountRequest{nodeID: nodeID, incoming: true})
	return int(count), err
}

// GetOutDegree returns the number of outgoing edges from a node.
func (b *BadgerEngine) GetOutDegree(ctx context.Context, nodeID NodeID) (int, error) {
	count, err := b.countByPrefix(ctx, "", 0, nil, adjacencyCountRequest{nodeID: nodeID, incoming: false})
	return int(count), err
}

// ============================================================================
