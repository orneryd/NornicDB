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
func (b *BadgerEngine) BulkCreateNodes(nodes []*Node) error {
	return b.BulkCreateNodesWithContext(context.Background(), nodes)
}

// BulkCreateNodesWithContext carries the caller through mutation validation.
func (b *BadgerEngine) BulkCreateNodesWithContext(ctx context.Context, nodes []*Node) error {
	return createBulkEntitiesWithContext(ctx, b, nodes, func(node *Node) string { return string(node.ID) }, func() error { return b.validateBulkNodeConstraintsWithContext(ctx, nodes) }, func(tx *BadgerTransaction) error {
		for _, node := range nodes {
			if _, err := CreateNodeWithContext(ctx, tx, node); err != nil {
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
func (b *BadgerEngine) inBulkTransaction(namespace string, apply func(tx *BadgerTransaction) error) error {
	return b.inBulkTransactionWithContext(context.Background(), namespace, apply)
}

func (b *BadgerEngine) inBulkTransactionWithContext(ctx context.Context, namespace string, apply func(tx *BadgerTransaction) error) error {
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
	return tx.CommitContext(ctx)
}

func (b *BadgerEngine) validateBulkNodeConstraints(nodes []*Node) error {
	return b.validateBulkNodeConstraintsWithContext(context.Background(), nodes)
}

func (b *BadgerEngine) validateBulkNodeConstraintsWithContext(ctx context.Context, nodes []*Node) error {
	seen := make(map[string]struct{})

	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
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
			if err := ctx.Err(); err != nil {
				return err
			}
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
					if err := ctx.Err(); err != nil {
						return err
					}
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
	return b.BulkCreateEdgesWithContext(context.Background(), edges)
}

// BulkCreateEdgesWithContext uses the existing bulk transaction/publication owner.
func (b *BadgerEngine) BulkCreateEdgesWithContext(ctx context.Context, edges []*Edge) error {
	return createBulkEntitiesWithContext(ctx, b, edges, func(edge *Edge) string { return string(edge.ID) }, nil,
		func(tx *BadgerTransaction) error { return tx.BulkCreateEdgesWithContext(ctx, edges) },
		func(tx *BadgerTransaction) error { return tx.BulkCreateEdgesWithContext(ctx, edges) })
}

// CreateEdgeWithContext consumes the same native transaction owner as a batch;
// physical publication, hidden-ID decisions and accounting remain there.
func (b *BadgerEngine) CreateEdgeWithContext(ctx context.Context, edge *Edge) error {
	return b.BulkCreateEdgesWithContext(ctx, []*Edge{edge})
}

// createBulkEntities owns capture routing, validation admission and the atomic
// transaction boundary; each entity kind supplies only its constraints/writes.
func createBulkEntities[T any](b *BadgerEngine, entities []*T, id func(*T) string, validate func() error, apply func(*BadgerTransaction) error, capture func(*BadgerTransaction) error) error {
	return createBulkEntitiesWithContext(context.Background(), b, entities, id, validate, apply, capture)
}

func createBulkEntitiesWithContext[T any](ctx context.Context, b *BadgerEngine, entities []*T, id func(*T) string, validate func() error, apply func(*BadgerTransaction) error, capture func(*BadgerTransaction) error) error {
	var firstID string
	if len(entities) > 0 && entities[0] != nil {
		firstID = id(entities[0])
		captureWrite := capture
		if captureWrite == nil {
			captureWrite = apply
		}
		if handled, err := b.inCaptureTransaction(firstID, captureWrite); handled {
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
		if err := ctx.Err(); err != nil {
			return err
		}
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
	return b.inBulkTransactionWithContext(ctx, namespace, apply)
}

// deleteBulkEntities owns bulk deletion routing, filtering and NotFound
// handling. Capture retains its existing ordered input validation; ordinary
// bulk calls omit empty and repeated IDs before invoking the same command.
func deleteBulkEntities[T ~string](b *BadgerEngine, ids []T, remove func(*BadgerTransaction, T) error) error {
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
	if handled, err := b.inCaptureTransaction(string(ids[0]), func(tx *BadgerTransaction) error { return apply(tx, false) }); handled {
		return err
	}
	return b.inBulkTransaction(firstIDNamespace(ids), func(tx *BadgerTransaction) error { return apply(tx, true) })
}

// ============================================================================
// Degree Functions
// ============================================================================

// GetInDegree returns the number of incoming edges to a node.
func (b *BadgerEngine) GetInDegree(nodeID NodeID) int {
	count, _ := b.countByPrefix("", 0, nil, adjacencyCountRequest{nodeID: nodeID, incoming: true})
	return int(count)
}

// GetOutDegree returns the number of outgoing edges from a node.
func (b *BadgerEngine) GetOutDegree(nodeID NodeID) int {
	count, _ := b.countByPrefix("", 0, nil, adjacencyCountRequest{nodeID: nodeID, incoming: false})
	return int(count)
}

// ============================================================================
