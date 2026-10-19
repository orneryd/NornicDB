package storage

import (
	"context"
	"errors"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
)

// ManagedCommandLifetime outlives an ACK waiter. Replay owns a fresh admitted
// execution, not a retired snapshot; Resolved retires its capacity exactly once.
type ManagedCommandLifetime struct {
	LocalApply func(context.Context, func()) error
	Replay     func(context.Context, func()) error
	Resolved   func(error)
	Pending    func()
}

type managedCommandKey struct{}
type commandPublicationKey struct{}
type managedLocalApplyKey struct{}
type commandAfterPublicationKey struct{}

func WithManagedLocalApply(ctx context.Context) context.Context {
	return context.WithValue(ctx, managedLocalApplyKey{}, true)
}
func IsManagedLocalApply(ctx context.Context) bool {
	local, _ := ctx.Value(managedLocalApplyKey{}).(bool)
	return local
}
func WithCommandAfterPublication(ctx context.Context, publish func()) context.Context {
	return context.WithValue(ctx, commandAfterPublicationKey{}, publish)
}
func CommandAfterPublicationFromContext(ctx context.Context) func() {
	publish, _ := ctx.Value(commandAfterPublicationKey{}).(func())
	return publish
}

func WithCommandPublication(ctx context.Context, publish func()) context.Context {
	return context.WithValue(ctx, commandPublicationKey{}, publish)
}
func CommandPublicationFromContext(ctx context.Context) func() {
	publish, _ := ctx.Value(commandPublicationKey{}).(func())
	return publish
}

func WithManagedCommandLifetime(ctx context.Context, lifetime ManagedCommandLifetime) context.Context {
	return context.WithValue(ctx, managedCommandKey{}, lifetime)
}

func ManagedCommandLifetimeFromContext(ctx context.Context) ManagedCommandLifetime {
	lifetime, _ := ctx.Value(managedCommandKey{}).(ManagedCommandLifetime)
	return lifetime
}

func HasManagedWriteRouter(engine Engine) bool {
	for engine != nil {
		if _, ok := engine.(ManagedWriteRouter); ok {
			return true
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return false
		}
		engine = wrapper.GetInnerEngine()
	}
	return false
}

// NativeReplay fixes auto-generated fields once, before either replication's
// wire encoding or local apply. It reads presence only when UpdateNode's
// missing-body timestamp contract requires it, without a user transaction.
func (m *ManagedWriteTransaction) NativeReplay() (ManagedReplay, error) {
	replay := m.ReplayCommand()
	op := replay.Operations[0]
	switch op.Type {
	case OpCreateNode, OpUpdateNode:
		if op.Node == nil {
			return replay, ErrInvalidData
		}
		if op.Type == OpCreateNode {
			initializeCreationTimes(&op.Node.CreatedAt, &op.Node.UpdatedAt)
		} else if op.Node.UpdatedAt.IsZero() {
			present, err := StoredNodeExists(m.engine, op.Node.ID)
			if err != nil {
				return replay, err
			}
			journal := false
			for engine := m.engine; engine != nil; {
				if _, ok := engine.(*WALEngine); ok {
					journal = true
					break
				}
				wrapper, ok := engine.(EngineUnwrapper)
				if !ok {
					break
				}
				engine = wrapper.GetInnerEngine()
			}
			if present || journal {
				op.Node.UpdatedAt = time.Now()
			}
		}
	case OpCreateEdge, OpUpdateEdge:
		if op.Edge == nil {
			return replay, ErrInvalidData
		}
		if op.Type == OpCreateEdge {
			initializeCreationTimes(&op.Edge.CreatedAt, &op.Edge.UpdatedAt)
		} else if op.Edge.UpdatedAt.IsZero() {
			op.Edge.UpdatedAt = time.Now()
		}
	}
	m.op = op
	replay.Operations[0] = op
	return replay, nil
}

type PendingMutationError struct{ Cause error }

func (e *PendingMutationError) Error() string { return e.Cause.Error() }
func (e *PendingMutationError) Unwrap() error { return e.Cause }
func MutationPending(err error) bool {
	var pending *PendingMutationError
	return errors.As(err, &pending)
}

// ManagedReplay is a detached, immutable command body. Transaction commands
// remain one atomic transaction at every peer and recovery boundary.
type ManagedReplay struct {
	Namespace         string
	Operations        []Operation
	Bulk              bool
	Transaction       bool
	Schema            *SchemaDefinition
	SchemaTransaction bool
	Implicit          bool
	Conditions        *ReplayConditions
}

func NewManagedTransaction(engine Engine, tx *BadgerTransaction) *ManagedWriteTransaction {
	return &ManagedWriteTransaction{BadgerTransaction: tx, engine: engine, namespace: tx.PendingChanges(false).Namespace, transaction: true}
}

// CommitWrappedTransaction preserves command wrappers when no database
// admission owner is installed. It uses the same atomic command and journal
// protocol as managed transactions, without introducing a second accounting
// owner. A late apply owns detached replay, never the caller's retired snapshot.
func CommitWrappedTransaction(ctx context.Context, engine Engine, tx *BadgerTransaction) error {
	changes := tx.PendingChanges(false)
	if !HasManagedWriteRouter(engine) || (changes.Operations == 0 && !changes.HasSchemaChanges) {
		return tx.CommitContext(ctx)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m := NewManagedTransaction(engine, tx)
	defer m.Rollback()
	if err := ctx.Err(); err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	if err := m.PrepareChanges(); err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	replay := m.ReplayCommand()
	m.SetCommandLifetime(ManagedCommandLifetime{
		Replay: func(applyContext context.Context, published func()) error {
			return ApplyManagedReplay(WithCommandPublication(applyContext, published), engine, replay)
		},
	})
	release, err := m.PrepareCommit(ctx)
	if err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	defer release()
	return m.CommitContextWithPublication(ctx, nil)
}

func NewManagedReplay(engine Engine, replay ManagedReplay) *ManagedWriteTransaction {
	m := &ManagedWriteTransaction{engine: engine, namespace: replay.Namespace, transaction: replay.Transaction, skipRouting: true}
	m.replaySchema, m.replaySchemaTransaction = replay.Schema, replay.SchemaTransaction
	m.replayImplicit = replay.Implicit
	m.replayConditions = replay.Conditions
	if replay.Transaction || replay.Bulk {
		m.batch = replay.Operations
	} else if len(replay.Operations) > 0 {
		m.op = replay.Operations[0]
	}
	return m
}

func (m *ManagedWriteTransaction) ReplayCommand() ManagedReplay {
	ops := m.batch
	schema, ordinary := m.replaySchema, m.replaySchemaTransaction
	implicit := m.replayImplicit
	if m.BadgerTransaction != nil && (m.transaction || m.batch != nil) {
		m.mu.Lock()
		ops = append([]Operation(nil), m.physicalOperationsLocked()...)
		if m.knowledgeSchemaDirty {
			schema, ordinary = m.knowledgeSchemaDefinition, m.schemaRuntime != nil
		}
		implicit = m.implicit
		m.mu.Unlock()
	} else if ops == nil {
		ops = []Operation{m.op}
	}
	copyOps := make([]Operation, len(ops))
	for i, op := range ops {
		op.Node, op.Edge = copyNode(op.Node), copyEdge(op.Edge)
		op.OldNode, op.OldEdge = nil, nil
		copyOps[i] = op
	}
	return ManagedReplay{Namespace: m.namespace, Operations: copyOps, Bulk: m.batch != nil && !m.transaction, Transaction: m.transaction, Schema: schema, SchemaTransaction: ordinary, Implicit: implicit, Conditions: m.replayConditions}
}

// ApplyManagedReplay is the physical atomic owner used on peers and recovery.
// Local uncertain commands instead enter the registered manager's fresh
// admission callback before reaching this same storage execution boundary.
func ApplyManagedReplay(ctx context.Context, engine Engine, replay ManagedReplay) error {
	m := NewManagedReplay(engine, replay)
	if err := m.PrepareChanges(); err != nil {
		_ = m.Rollback()
		return err
	}
	defer m.Rollback()
	release, err := m.PrepareCommit(ctx)
	if err != nil {
		return err
	}
	defer release()
	return m.CommitContextWithPublication(ctx, nil)
}
