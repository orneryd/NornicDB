package storage

import (
	"context"
	"errors"
	"time"
)

// ManagedWriteTransaction stages a direct Engine write using the transaction
// owner. Its namespace, stored after-image, atomic commit and publication are
// therefore the same inputs used by transactional quota admission. Direct API
// semantics (strict creates, node upsert, timestamps and WAL) remain here at the
// storage boundary, rather than being reimplemented by quota callers.
type ManagedWriteTransaction struct {
	*BadgerTransaction
	engine                  Engine
	namespace               string
	journal                 *WALEngine
	op                      Operation
	batch                   []Operation
	router                  ManagedWriteRouter
	journalAttempt          *commandJournal
	transaction             bool
	skipRouting             bool
	lifetime                ManagedCommandLifetime
	commitPending           bool
	replaySchema            *SchemaDefinition
	replaySchemaTransaction bool
	replayImplicit          bool
	replayConditions        *ReplayConditions
}

// ManagedWriteRouter preserves non-transparent write wrappers. The callback is
// the already-admitted local transaction, invoked only at local command apply;
// a forwarded command instead uses the remote engine's accounting owner.
type ManagedWriteRouter interface {
	CommitManagedWrite(context.Context, []Operation, bool, func(context.Context, func()) error) error
	CommitManagedTransaction(context.Context, ManagedReplay, func(context.Context, func()) error) error
}

// A replication wait can end before its admitted command is applied. The
// accounting owner must preserve that outcome instead of marking a timeout as
// a known rollback of the logical write.
func (m *ManagedWriteTransaction) CommitMayBePending() bool { return m.commitPending }

func (m *ManagedWriteTransaction) SetCommandLifetime(lifetime ManagedCommandLifetime) {
	m.lifetime = lifetime
}

func (m *ManagedWriteTransaction) CanCommitNative() bool {
	return m.BadgerTransaction == nil && m.batch == nil && !m.transaction && m.replayConditions == nil && SupportsNativeCommand(m.engine)
}

func (m *ManagedWriteTransaction) CommitNative(ctx context.Context) error {
	return ExecuteNativeCommand(ctx, m.engine, m.op, CommandPublicationFromContext(ctx))
}

func (m *ManagedWriteTransaction) RoutesManagedCommand() bool { return m.router != nil }

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
		return TransactionChanges{Namespace: m.namespace, Operations: operations, HasSchemaChanges: m.replaySchema != nil}
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
func (m *ManagedWriteTransaction) PrepareChanges() (result error) {
	defer func() {
		if result == nil && m.BadgerTransaction != nil {
			if m.router != nil && !m.skipRouting && m.replayConditions == nil {
				result = m.captureReplayConditions()
			} else {
				m.BadgerTransaction.replayConditions = m.replayConditions
			}
		}
	}()
	engine := m.engine
	var journal *WALEngine
	for engine != nil {
		if router, ok := engine.(ManagedWriteRouter); ok && m.router == nil && !m.skipRouting {
			m.router = router
		}
		if wal, ok := engine.(*WALEngine); ok {
			journal = wal
		}
		if beginner, ok := engine.(interface {
			BeginTransaction() (*BadgerTransaction, error)
		}); ok {
			if m.BadgerTransaction != nil {
				m.journal = journal
				return nil
			}
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
			// Direct operations have the same coalesced durability contract as
			// native direct writes, rather than an explicit user COMMIT's Sync.
			if err := tx.SetImplicit(true); err != nil {
				return err
			}
			if m.transaction || (m.skipRouting && m.batch != nil) {
				if err := tx.SetImplicit(m.replayImplicit); err != nil {
					return err
				}
				for _, op := range m.batch {
					if err := m.stageReplay(op); err != nil {
						return err
					}
				}
				if m.replaySchema != nil {
					return tx.stageReplaySchema(m.replaySchema, m.replaySchemaTransaction)
				}
				return nil
			}
			if m.batch == nil {
				tx.managedDirect = true
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
	if m.BadgerTransaction != nil {
		// A supplied raw transaction already owns its snapshot. Its read
		// decorator need not also expose BeginTransaction; retain that seam
		// for admission and use the supplied transaction for publication.
		m.journal = journal
		return nil
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
		tx.mu.Lock()
		defer tx.mu.Unlock()
		return tx.updateNodeLocked(op.Node, prior, true)
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
		tx.mu.Lock()
		defer tx.mu.Unlock()
		return tx.updateEdgeLocked(op.Edge, true)
	case OpDeleteEdge:
		m.op.EdgeID = ns.prefixEdgeID(op.EdgeID)
		return tx.DeleteEdge(m.op.EdgeID)
	default:
		return ErrNotImplemented
	}
}

func namespacedManagedOperation(ns *NamespacedEngine, op Operation) Operation {
	if op.NodeID != "" || op.Type == OpDeleteNode {
		op.NodeID = ns.prefixNodeID(op.NodeID)
	}
	if op.EdgeID != "" || op.Type == OpDeleteEdge {
		op.EdgeID = ns.prefixEdgeID(op.EdgeID)
	}
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
	if m.router != nil && !IsManagedLocalApply(ctx) {
		// Native WAL belongs to local apply, after wrapper admission, rather
		// than a proposal that replication might reject or forward elsewhere.
		return func() {}, nil
	}
	return m.prepareJournal(ctx)
}

func (m *ManagedWriteTransaction) CommitContextWithPublication(ctx context.Context, published func()) error {
	if m.router == nil || IsManagedLocalApply(ctx) {
		return m.journalAttempt.result(m.BadgerTransaction.CommitContextWithPublication(ctx, published))
	}
	ctx = WithManagedCommandLifetime(ctx, m.lifetime)
	local := func(applyContext context.Context, replicationPublication func()) error {
		applyContext = context.WithoutCancel(applyContext)
		release, err := m.prepareJournal(applyContext)
		if err != nil {
			return err
		}
		defer release()
		applyContext = WithCommandPublication(applyContext, replicationPublication)
		return m.journalAttempt.result(m.BadgerTransaction.CommitContextWithPublication(applyContext, published))
	}
	err := m.router.CommitManagedTransaction(ctx, m.ReplayCommand(), local)
	m.commitPending = MutationPending(err)
	return err
}

func (m *ManagedWriteTransaction) prepareJournal(ctx context.Context) (func(), error) {
	if m.journal == nil {
		return func() {}, nil
	}
	// Journal the same final physical operations that replication carries,
	// including hidden-record replacements accepted by a bulk or MERGE.
	ops := []Operation{m.op}
	if m.transaction || m.batch != nil {
		m.mu.Lock()
		ops = append([]Operation(nil), m.physicalOperationsLocked()...)
		if m.knowledgeSchemaDirty {
			ops = append(ops, Operation{Type: OpSchemaDefinition, Schema: m.knowledgeSchemaDefinition, SchemaTransaction: m.schemaRuntime != nil})
		}
		m.mu.Unlock()
	}
	journal, release, err := m.journal.prepareCommandJournal(m.namespace, ops, m.batch != nil && !m.transaction, CommandVersionFromContext(ctx))
	if err != nil {
		return nil, err
	}
	m.journalAttempt = journal
	m.BadgerTransaction.setCommandPublication([]nativePublication{{Publish: journal.publish, Token: CommandVersionFromContext(ctx)}})
	return release, nil
}
