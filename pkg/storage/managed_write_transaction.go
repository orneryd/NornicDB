package storage

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
)

// ManagedWriteTransaction stages a direct Engine write using the transaction
// owner. Its namespace, stored after-image, atomic commit and publication are
// therefore the same inputs used by transactional quota admission. Direct API
// semantics (strict creates, node upsert, timestamps and WAL) remain here at the
// storage boundary, rather than being reimplemented by quota callers.
type ManagedWriteTransaction struct {
	*BadgerTransaction
	engine    Engine
	namespace string
	journal   *WALEngine
	op        Operation
	batch     []Operation
}

// NewManagedWrite defers a direct write's snapshot and staging until the commit
// owner holds admission. Direct calls retain their serialized write semantics;
// no stale snapshot is taken while waiting for another writer to publish.
func NewManagedWrite(engine Engine, namespace string, op Operation) *ManagedWriteTransaction {
	return &ManagedWriteTransaction{engine: engine, namespace: namespace, op: op}
}

// NewManagedCreateBatch stages an all-or-nothing direct bulk create at the
// same owner. Operations must all be node creates or all relationship creates.
func NewManagedCreateBatch(engine Engine, namespace string, operations []Operation) *ManagedWriteTransaction {
	return &ManagedWriteTransaction{engine: engine, namespace: namespace, batch: operations}
}

// PendingChanges supplies an admission summary before staging, then forwards
// the actual final entities. The summary never substitutes for stored images.
func (m *ManagedWriteTransaction) PendingChanges(includeEntities bool) TransactionChanges {
	if m.BadgerTransaction == nil {
		operations := 1
		if m.batch != nil {
			operations = len(m.batch)
		}
		return TransactionChanges{Namespace: m.namespace, Operations: operations}
	}
	return m.BadgerTransaction.PendingChanges(includeEntities)
}

// Rollback also covers a direct operation rejected before its snapshot starts.
func (m *ManagedWriteTransaction) Rollback() error {
	if m.BadgerTransaction == nil {
		return nil
	}
	return m.BadgerTransaction.Rollback()
}

// PrepareChanges is called by the accounting owner under exclusive admission,
// after database validation and before reading any prior entity.
func (m *ManagedWriteTransaction) PrepareChanges() error {
	engine := m.engine
	var journal *WALEngine
	for engine != nil {
		if wal, ok := engine.(*WALEngine); ok {
			journal = wal
		}
		if beginner, ok := engine.(interface {
			BeginTransaction() (*BadgerTransaction, error)
		}); ok {
			tx, err := beginner.BeginTransaction()
			if err != nil {
				return err
			}
			m.BadgerTransaction = tx
			m.journal = journal
			if err := tx.SetNamespace(m.namespace); err != nil {
				_ = tx.Rollback()
				return err
			}
			if m.batch == nil {
				return m.stage(m.op)
			}
			// Engine bulk calls already use an implicit transaction, including
			// its relationship replacement and batched-edge validation.
			if err := tx.SetImplicit(true); err != nil {
				return err
			}
			if len(m.batch) > 0 && m.batch[0].Type == OpCreateEdge {
				ns := NewNamespacedEngine(tx.engine, tx.namespace)
				edges := make([]*Edge, len(m.batch))
				for i, op := range m.batch {
					op = namespacedManagedOperation(ns, op)
					if op.Edge == nil {
						return ErrInvalidData
					}
					if err := ns.GetSchema().RegisterTokens(nil, []string{op.Edge.Type}); err != nil {
						return err
					}
					m.batch[i], edges[i], m.op = op, op.Edge, op
				}
				return tx.BulkCreateEdges(edges)
			}
			for i, op := range m.batch {
				if err := m.stage(op); err != nil {
					return err
				}
				m.batch[i] = m.op
			}
			return nil
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			break
		}
		engine = wrapper.GetInnerEngine()
	}
	return ErrNotImplemented
}

// stage preserves direct CRUD acceptance rules while delegating every physical
// write, including node-delete cascades, to the transaction's staging owner.
// The caller owns this transaction until commit or rollback.
func (m *ManagedWriteTransaction) stage(op Operation) error {
	tx := m.BadgerTransaction
	ns := NewNamespacedEngine(tx.engine, tx.namespace)
	op = namespacedManagedOperation(ns, op)
	m.op = op
	switch op.Type {
	case OpCreateNode:
		if op.Node == nil {
			return ErrInvalidData
		}
		if err := ns.GetSchema().RegisterTokens(op.Node.Labels, nil); err != nil {
			return err
		}
		tx.mu.Lock()
		defer tx.mu.Unlock()
		_, err := tx.createNodeLocked(op.Node, true)
		return err
	case OpUpdateNode:
		if op.Node == nil {
			return ErrInvalidData
		}
		if err := ns.GetSchema().RegisterTokens(op.Node.Labels, nil); err != nil {
			return err
		}
		prior, err := tx.GetStoredNode(op.Node.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if op.Node.UpdatedAt.IsZero() && (prior != nil || m.journal != nil) {
			op.Node.UpdatedAt = time.Now()
		}
		if prior == nil {
			// Direct UpdateNode inserts a missing body without applying
			// CreateNode's timestamp defaults.
			tx.mu.Lock()
			defer tx.mu.Unlock()
			_, err := tx.createNodeLocked(op.Node, false)
			return err
		}
		if op.Node.CreatedAt.IsZero() {
			op.Node.CreatedAt = prior.CreatedAt
		}
		return tx.UpdateNode(op.Node)
	case OpDeleteNode:
		m.op.NodeID = ns.prefixNodeID(op.NodeID)
		return tx.DeleteNode(m.op.NodeID)
	case OpCreateEdge:
		if op.Edge == nil {
			return ErrInvalidData
		}
		if err := ns.GetSchema().RegisterTokens(nil, []string{op.Edge.Type}); err != nil {
			return err
		}
		tx.mu.Lock()
		defer tx.mu.Unlock()
		if err := tx.ensureDataWriteAllowedLocked(); err != nil {
			return err
		}
		if _, err := tx.getCommittedEdgeLocked(op.Edge.ID); err == nil {
			return ErrAlreadyExists
		} else if !errors.Is(err, ErrNotFound) {
			return err
		}
		// Direct creation accepts stored endpoints, even when query reads
		// hide them. Transactional Cypher keeps its visible-endpoint rule.
		endpoints := func(id NodeID) ([]string, bool) {
			node, err := tx.getCommittedNodeLocked(id)
			if err != nil {
				return nil, false
			}
			return node.Labels, true
		}
		start, end, prior, err := tx.validateNewEdgeLocked(op.Edge, endpoints, nil)
		if err != nil {
			return err
		}
		return tx.bufferNewEdgeLocked(op.Edge, start, end, prior)
	case OpUpdateEdge:
		if op.Edge == nil {
			return ErrInvalidData
		}
		if err := ns.GetSchema().RegisterTokens(nil, []string{op.Edge.Type}); err != nil {
			return err
		}
		return tx.UpdateEdge(op.Edge)
	case OpDeleteEdge:
		m.op.EdgeID = ns.prefixEdgeID(op.EdgeID)
		return tx.DeleteEdge(m.op.EdgeID)
	default:
		return ErrNotImplemented
	}
}

func namespacedManagedOperation(ns *NamespacedEngine, op Operation) Operation {
	if op.Node != nil {
		op.Node = copyNode(op.Node)
		op.Node.ID = ns.prefixNodeID(op.Node.ID)
		op.NodeID = op.Node.ID
	}
	if op.Edge != nil {
		op.Edge = copyEdge(op.Edge)
		op.Edge.ID = ns.prefixEdgeID(op.Edge.ID)
		op.Edge.StartNode = ns.prefixNodeID(op.Edge.StartNode)
		op.Edge.EndNode = ns.prefixNodeID(op.Edge.EndNode)
		op.EdgeID = op.Edge.ID
	}
	return op
}

// PrepareCommit preserves the direct API's WAL wrapper after quota admission
// and before attempting a physical commit. Its release covers publication;
// an error here is still a known pre-commit failure at the accounting owner.
func (m *ManagedWriteTransaction) PrepareCommit(ctx context.Context) (func(), error) {
	if m.journal == nil {
		return func() {}, nil
	}
	w := m.journal
	id := string(m.op.NodeID)
	if id == "" {
		id = string(m.op.EdgeID)
	}
	release, err := w.admitJournalIDs(id)
	if err != nil {
		return nil, err
	}
	w.mutationMu.RLock()
	cleanup := func() {
		w.mutationMu.RUnlock()
		release()
	}
	if config.IsWALEnabled() {
		var data any
		operation := m.op.Type
		switch m.op.Type {
		case OpCreateNode, OpUpdateNode:
			data = WALNodeData{Node: cloneNodeForWAL(m.namespace, m.op.Node)}
		case OpCreateEdge, OpUpdateEdge:
			data = WALEdgeData{Edge: cloneEdgeForWAL(m.namespace, m.op.Edge)}
		case OpDeleteNode, OpDeleteEdge:
			data = WALDeleteData{ID: StripDatabasePrefix(m.namespace, id)}
		}
		if m.batch != nil {
			switch m.op.Type {
			case OpCreateNode:
				nodes := make([]*Node, len(m.batch))
				for i, op := range m.batch {
					nodes[i] = cloneNodeForWAL(m.namespace, op.Node)
				}
				operation, data = OpBulkNodes, WALBulkNodesData{Nodes: nodes}
			case OpCreateEdge:
				edges := make([]*Edge, len(m.batch))
				for i, op := range m.batch {
					edges[i] = cloneEdgeForWAL(m.namespace, op.Edge)
				}
				operation, data = OpBulkEdges, WALBulkEdgesData{Edges: edges}
			}
		}
		if err := w.wal.AppendWithDatabase(operation, data, m.namespace); err != nil {
			cleanup()
			return nil, err
		}
	}
	return cleanup, nil
}
