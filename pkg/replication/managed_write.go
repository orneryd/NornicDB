package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var _ storage.ManagedWriteRouter = (*ReplicatedEngine)(nil)

// A consensus timeout can precede local apply. Retire the staged callback before
// its caller rolls back; later committed replay then uses the ordinary command
// path and registered accounting owner. An apply already running owns the
// callback through physical commit/publication, so retirement waits for it.
type managedCommandWrite struct {
	mu       sync.Mutex
	local    func(context.Context, func()) error
	replay   func(context.Context, func()) error
	resolved func(error)
	pending  func()
	retired  bool
	admitted atomic.Bool
	applied  bool
	err      error
}

func (m *managedCommandWrite) apply(ctx context.Context, published func()) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.applied {
		return true, m.err
	}
	if m.local == nil {
		if m.replay == nil {
			return false, nil
		}
		m.local, m.replay = m.replay, nil
	}
	m.applied = true
	m.err = m.local(ctx, published)
	m.local = nil
	m.replay = nil
	m.resolve(m.err)
	return true, m.err
}

func (m *managedCommandWrite) retire(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.local = nil
	if m.applied {
		return m.result(err)
	}
	if knownManagedRejection(err) && !m.admitted.Load() {
		m.replay = nil
		m.resolve(err)
		return nornicerrors.MarkCommitRolledBack(err)
	}
	// The proposal can still apply. Keep its reservation and detached replay,
	// but never retain or execute the caller's rolled-back staging snapshot.
	m.retired = true
	if m.pending != nil {
		m.pending()
	}
	if err != nil {
		return &storage.PendingMutationError{Cause: err}
	}
	return nil
}

func (m *managedCommandWrite) result(err error) error {
	if m.err != nil {
		return m.err
	}
	if err != nil && m.applied && m.err == nil && !storage.MutationApplied(err) {
		return &storage.AppliedMutationError{Cause: err}
	}
	return err
}

func (m *managedCommandWrite) resolve(err error) {
	if m.resolved != nil {
		complete := m.resolved
		m.resolved = nil
		complete(err)
	}
}

func (m *managedCommandWrite) reject(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.applied {
		return
	}
	m.local, m.replay = nil, nil
	m.applied, m.err = true, nornicerrors.MarkCommitRolledBack(err)
	m.resolve(err)
}

func rejectManagedCommand(cmd *Command, err error) {
	if cmd != nil && cmd.managedWrite != nil {
		cmd.managedWrite.reject(err)
	}
}

func knownManagedRejection(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNoLeader) || errors.Is(err, ErrNotReady) || errors.Is(err, ErrStandbyMode) || errors.Is(err, ErrClosed)
}

type managedCommandIdentity struct {
	engine  storage.Engine
	request string
}

var managedCommands sync.Map

func commandIdentity(engine storage.Engine, request string) managedCommandIdentity {
	for {
		wrapper, ok := engine.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		next := wrapper.GetInnerEngine()
		if next == nil || next == engine {
			break
		}
		engine = next
	}
	return managedCommandIdentity{engine, request}
}

func (e *ReplicatedEngine) dispatchManaged(ctx context.Context, cmd *Command, local func(context.Context, func()) error) error {
	lifetime := storage.ManagedCommandLifetimeFromContext(ctx)
	apply := lifetime.LocalApply
	if apply == nil {
		apply = local
	}
	publication := &managedCommandWrite{local: apply, replay: lifetime.Replay, pending: lifetime.Pending}
	cmd.Timestamp, cmd.RequestID = time.Now(), uuid.NewString()
	key := commandIdentity(e.Engine, cmd.RequestID)
	publication.resolved = func(err error) {
		managedCommands.Delete(key)
		if lifetime.Resolved != nil {
			lifetime.Resolved(err)
		}
	}
	cmd.managedWrite = publication
	managedCommands.Store(key, publication)
	return publication.retire(e.replicator.Apply(cmd, e.timeout))
}

// CommitManagedWrite sends the ordinary direct-write command through the
// replicator. Local apply commits the transaction already admitted by the one
// accounting owner. Wire/WAL replay uses the ordinary engine write and the same
// registered stored-pair accounting owner, without serializing local closures.
func (e *ReplicatedEngine) CommitManagedWrite(ctx context.Context, ops []storage.Operation, bulk bool, local func(context.Context, func()) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return storage.ErrInvalidData
	}
	cmd := &Command{}
	op := ops[0]
	var err error
	if bulk {
		payload := make([][]byte, len(ops))
		for i, item := range ops {
			if item.Type != op.Type {
				return storage.ErrInvalidData
			}
			switch item.Type {
			case storage.OpCreateNode:
				payload[i], err = encodeNodePayload(item.Node)
			case storage.OpCreateEdge:
				payload[i], err = encodeEdgePayload(item.Edge)
			default:
				return storage.ErrNotImplemented
			}
			if err != nil {
				return err
			}
		}
		cmd.Type = CmdBulkCreateNodes
		if op.Type == storage.OpCreateEdge {
			cmd.Type = CmdBulkCreateEdges
		}
		cmd.Data, err = encodeGob(payload)
	} else {
		switch op.Type {
		case storage.OpCreateNode:
			cmd.Type = CmdCreateNode
			cmd.Data, err = encodeNodePayload(op.Node)
		case storage.OpUpdateNode:
			cmd.Type = CmdUpdateNode
			cmd.Data, err = encodeNodePayload(op.Node)
		case storage.OpDeleteNode:
			cmd.Type, cmd.Data = CmdDeleteNode, []byte(op.NodeID)
		case storage.OpCreateEdge:
			cmd.Type = CmdCreateEdge
			cmd.Data, err = encodeEdgePayload(op.Edge)
		case storage.OpUpdateEdge:
			cmd.Type = CmdUpdateEdge
			cmd.Data, err = encodeEdgePayload(op.Edge)
		case storage.OpDeleteEdge:
			cmd.Type = CmdDeleteEdge
			cmd.Data, err = encodeGob(struct{ EdgeID string }{string(op.EdgeID)})
		default:
			return storage.ErrNotImplemented
		}
	}
	if err != nil {
		return nornicerrors.MarkCommitRolledBack(err)
	}
	// Dispatch through Apply so its leader rejection/forwarding and command
	// publication remain authoritative, exactly as ordinary direct CRUD.
	return e.dispatchManaged(ctx, cmd, local)
}

func (e *ReplicatedEngine) SupportsNativeCommand() bool {
	return storage.SupportsNativeCommand(e.Engine)
}

func (e *ReplicatedEngine) NativeCommand(ctx context.Context, op storage.Operation, published func()) error {
	if storage.IsManagedLocalApply(ctx) {
		return storage.ExecuteNativeCommand(ctx, e.Engine, op, published)
	}
	if op.Type == storage.OpCreateNode && op.Node != nil {
		if op.Node.CreatedAt.IsZero() {
			op.Node.CreatedAt = time.Now()
		}
		if op.Node.UpdatedAt.IsZero() {
			op.Node.UpdatedAt = op.Node.CreatedAt
		}
	}
	if op.Type == storage.OpCreateEdge && op.Edge != nil {
		if op.Edge.CreatedAt.IsZero() {
			op.Edge.CreatedAt = time.Now()
		}
		if op.Edge.UpdatedAt.IsZero() {
			op.Edge.UpdatedAt = op.Edge.CreatedAt
		}
	}
	lifetime := storage.ManagedCommandLifetimeFromContext(ctx)
	if lifetime.Replay == nil {
		lifetime.Replay = func(ctx context.Context, replicated func()) error {
			return storage.ExecuteNativeCommand(ctx, e.Engine, op, func() {
				if published != nil {
					published()
				}
				if replicated != nil {
					replicated()
				}
			})
		}
	}
	ctx = storage.WithManagedCommandLifetime(ctx, lifetime)
	return e.CommitManagedWrite(ctx, []storage.Operation{op}, false, func(applyContext context.Context, replicated func()) error {
		if after := storage.CommandAfterPublicationFromContext(ctx); after != nil {
			applyContext = storage.WithCommandAfterPublication(applyContext, after)
		}
		return storage.ExecuteNativeCommand(context.WithoutCancel(applyContext), e.Engine, op, func() {
			if published != nil {
				published()
			}
			if replicated != nil {
				replicated()
			}
		})
	})
}
