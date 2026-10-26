package replication

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"sync"
	"sync/atomic"
	"time"
)

// AcceptedCommandSubmitter returns after definite queue/log ownership transfer.
// Once accepted, caller wait cancellation cannot discard the command.
type AcceptedCommandSubmitter interface {
	SubmitAcceptedCommand(context.Context, *Command, func(storage.CommitOutcome)) (*storage.CommitReceipt, error)
}

// acceptedEntityCommand is local ownership, absent from serialized commands.
// Its physical and full native boundaries use the same storage receipt owner.
type acceptedEntityCommand struct {
	owner         storage.Engine
	prepared      *storage.AcceptedEntityCommitHandle
	inner         storage.EntityCommitSubmitter
	once          sync.Once
	started       atomic.Bool
	physical      storage.CommitOutcome
	native        *storage.CommitReceipt
	nativeDone    chan struct{}
	nativeOutcome storage.CommitOutcome
}

func (c *acceptedEntityCommand) apply() error {
	c.once.Do(func() {
		c.started.Store(true)
		native, err := c.prepared.Submit(context.Background(), c.inner)
		if err != nil {
			c.prepared.CloseNative()
			c.physical = storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: err}
			c.nativeOutcome = c.physical
			close(c.nativeDone)
			return
		}
		c.native = native
		c.physical = native.WaitPhysicalOutcome(context.Background())
		if c.physical.State == storage.CommitRecoveryPending && c.physical.Err != nil {
			c.prepared.MarkRecoveryPending()
		}
		if c.physical.State == storage.CommitDiscarded {
			c.physical.State = storage.CommitRecoveryPending
		}
		go func() {
			c.nativeOutcome = native.WaitOutcome(context.Background())
			if c.nativeOutcome.State == storage.CommitDiscarded {
				c.nativeOutcome.State = storage.CommitRecoveryPending
			}
			close(c.nativeDone)
		}()
	})
	return c.physical.Err
}

// acceptedCommandOutcome joins transport acknowledgement and native listeners.
// A recorded command with a failed inner apply always remains recovery-pending.
func acceptedCommandOutcome(cmd *Command, transportErr error) storage.CommitOutcome {
	local := cmd.accepted
	if local == nil {
		return storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: transportErr}
	}
	if !local.started.Load() {
		return storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: transportErr}
	}
	<-local.nativeDone
	outcome := local.nativeOutcome
	if outcome.State == storage.CommitApplied {
		outcome.Err = transportErr
	}
	return outcome
}
func (e *ReplicatedEngine) CheckEntityCommitAdmission() error {
	if !e.IsLeader() {
		return ErrNotLeader
	}
	return nil
}
func (e *ReplicatedEngine) SubmitEntityCommit(ctx context.Context, prepared *storage.PreparedEntityCommit, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if err := e.CheckEntityCommitAdmission(); err != nil {
		return nil, err
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	submitter, ok := e.replicator.(AcceptedCommandSubmitter)
	if !ok {
		return nil, ErrNotReady
	}
	inner, found := storage.EntityCommitSubmitterFor(e.Engine)
	if !found {
		return nil, storage.ErrInvalidData
	}
	// The prepared commit owns an immutable private snapshot; encoding reads it
	// rather than duplicating a detached public Operations copy.
	payload, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Prepared: prepared})
	if err != nil {
		return nil, err
	}
	releaseAuthority, err := prepared.RetainAcceptedAuthorityWithContext(ctx)
	if err != nil {
		return nil, err
	}

	cmd := &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: time.Now(), RequestID: prepared.LogicalID()}
	cmd.accepted = &acceptedEntityCommand{owner: e.Engine, prepared: storage.NewAcceptedEntityCommitHandle(prepared), inner: inner.Submitter, nativeDone: make(chan struct{})}
	if ctx == nil {
		ctx = context.Background()
	}
	queueCtx, cancelQueue := context.WithTimeout(ctx, e.timeout)
	defer cancelQueue()
	var receipt *storage.CommitReceipt
	err = storage.DecidePublication(ctx, func() error {
		var submitErr error
		receipt, submitErr = submitter.SubmitAcceptedCommand(queueCtx, cmd, func(outcome storage.CommitOutcome) {
			if outcome.State != storage.CommitRecoveryPending {
				cmd.accepted.prepared.Retire()
				releaseAuthority()
			}
			if settle != nil {
				settle(outcome)
			}
		})
		return submitErr
	})
	if err != nil {
		releaseAuthority()
	}
	return receipt, err
}

func submitAcceptedByApply(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome), validate func() error, apply func(*Command, time.Duration) error, timeout time.Duration) (*storage.CommitReceipt, error) {
	if err := validate(); err != nil {
		return nil, err
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	receipt, resolve := storage.NewCommitReceipt(settle)
	go func() {
		err := apply(cmd, timeout)
		if cmd.accepted != nil && !cmd.accepted.started.Load() && (errors.Is(err, ErrClosed) || errors.Is(err, ErrNotReady) || errors.Is(err, ErrNotLeader) || errors.Is(err, ErrStandbyMode)) {
			cmd.accepted.prepared.AbortNative()
			_ = resolve.Resolve(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
			return
		}
		_ = resolve.Resolve(acceptedCommandOutcome(cmd, err))
	}()
	return receipt, nil
}
func (r *StandaloneReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	return submitAcceptedByApply(ctx, cmd, settle, func() error {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return checkApplyReadiness(r.closed, r.started)
	}, r.Apply, 0)
}
func (r *HAStandbyReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	return submitAcceptedByApply(ctx, cmd, settle, func() error {
		if err := checkApplyReadiness(r.closed.Load(), r.started.Load()); err != nil {
			return err
		}
		if !r.isPrimary.Load() {
			return ErrNotLeader
		}
		return nil
	}, r.Apply, 0)
}

func (r *MultiRegionReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if err := checkApplyReadiness(r.closed.Load(), r.started.Load()); err != nil {
		return nil, err
	}
	if !r.IsLeader() {
		return nil, ErrNotLeader
	}
	// Preserve the existing mode owner: local Raft accepts and owns this entry.
	submitter, ok := any(r.localRaft).(AcceptedCommandSubmitter)
	if !ok {
		return nil, ErrNotReady
	}
	return submitter.SubmitAcceptedCommand(ctx, cmd, settle)
}

// ForwardEntityChanges preserves the existing direct leaf and homogeneous bulk
// APIs. Followers forward unprepared input; the leader owns admission.
func (e *ReplicatedEngine) ForwardEntityChanges(ctx context.Context, ops []storage.Operation, mode storage.EntityCommitMode) error {
	if len(ops) == 0 {
		return nil
	}
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	forward := *e
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return context.DeadlineExceeded
			}
			if remaining < forward.timeout {
				forward.timeout = remaining
			}
		}
	}
	if mode == storage.CommitModeDirect {
		if len(ops) != 1 {
			return storage.ErrInvalidData
		}
		op := ops[0]
		switch op.Type {
		case storage.OpCreateNode:
			_, err := forward.CreateNode(op.Node)
			return err
		case storage.OpUpdateNode:
			return forward.UpdateNode(op.NodeUpdate())
		case storage.OpDeleteNode:
			return forward.DeleteNode(op.NodeID)
		case storage.OpCreateEdge:
			return forward.CreateEdge(op.Edge)
		case storage.OpUpdateEdge:
			return forward.UpdateEdge(op.EdgeUpdate())
		case storage.OpDeleteEdge:
			return forward.DeleteEdge(op.EdgeID)
		}
	}
	if mode != storage.CommitModeImplicit {
		return storage.ErrInvalidData
	}
	kind := ops[0].Type
	for _, op := range ops {
		if op.Type != kind {
			return storage.ErrInvalidData
		}
	}
	switch kind {
	case storage.OpCreateNode:
		nodes := make([]*storage.Node, len(ops))
		for i := range ops {
			nodes[i] = ops[i].Node
		}
		return forward.BulkCreateNodes(nodes)
	case storage.OpCreateEdge:
		edges := make([]*storage.Edge, len(ops))
		for i := range ops {
			edges[i] = ops[i].Edge
		}
		return forward.BulkCreateEdges(edges)
	case storage.OpDeleteNode:
		ids := make([]storage.NodeID, len(ops))
		for i := range ops {
			ids[i] = ops[i].NodeID
		}
		return forward.BulkDeleteNodes(ids)
	case storage.OpDeleteEdge:
		ids := make([]storage.EdgeID, len(ops))
		for i := range ops {
			ids[i] = ops[i].EdgeID
		}
		return forward.BulkDeleteEdges(ids)
	}
	return storage.ErrInvalidData
}
