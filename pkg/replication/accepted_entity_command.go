package replication

import (
	"context"
	"errors"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// AcceptedCommandSubmitter returns after definite queue/log ownership transfer.
// Once accepted, caller wait cancellation cannot discard the command.
type AcceptedCommandSubmitter interface {
	SubmitAcceptedCommand(context.Context, *Command, func(storage.CommitOutcome)) (*storage.CommitReceipt, error)
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
	// The storage accepted owner encodes the prepared snapshot once and
	// retains its authority; every transport settles through that owner.
	accepted, payload, err := storage.AcceptEntityCommit(e.Engine, prepared)
	if err != nil {
		return nil, err
	}
	cmd := &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: time.Now(), RequestID: prepared.LogicalID(), accepted: accepted}
	if ctx == nil {
		ctx = context.Background()
	}
	queueCtx, cancelQueue := context.WithTimeout(ctx, e.timeout)
	defer cancelQueue()
	receipt, err := submitter.SubmitAcceptedCommand(queueCtx, cmd, accepted.Terminal(settle))
	if err != nil {
		accepted.Discard(nil, err)
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
		// An admission error before the command was recorded is a definite
		// rejection; the accepted owner still settles a started command.
		if errors.Is(err, ErrClosed) || errors.Is(err, ErrNotReady) || errors.Is(err, ErrNotLeader) || errors.Is(err, ErrStandbyMode) {
			cmd.accepted.Reject(resolve, err)
			return
		}
		cmd.accepted.Join(resolve, err)
	}()
	return receipt, nil
}
func (r *StandaloneReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	return submitAcceptedByApply(ctx, cmd, settle, func() error {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return checkApplyReadiness(r.work.Closing(), r.started)
	}, r.Apply, 0)
}
func (r *HAStandbyReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	return submitAcceptedByApply(ctx, cmd, settle, func() error {
		if err := checkApplyReadiness(r.work.Closing(), r.started.Load()); err != nil {
			return err
		}
		if !r.isPrimary.Load() {
			return ErrNotLeader
		}
		return nil
	}, r.Apply, 0)
}

func (r *MultiRegionReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if err := checkApplyReadiness(r.work.Closing(), r.started.Load()); err != nil {
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
			return forward.UpdateNode(op.Node)
		case storage.OpDeleteNode:
			return forward.DeleteNode(op.NodeID)
		case storage.OpCreateEdge:
			return forward.CreateEdge(op.Edge)
		case storage.OpUpdateEdge:
			return forward.UpdateEdge(op.Edge)
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
