package replication

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"time"
)

type leaderEntityCommandPreparer interface {
	PrepareEntityCommand(context.Context, *Command) (*Command, func(storage.CommitOutcome), bool, error)
}

// PrepareEntityCommand normalizes client leaf/bulk input only at its leader.
// Managed namespace admission comes from the existing stored-image registry.
func (a *StorageAdapter) PrepareEntityCommand(ctx context.Context, cmd *Command) (*Command, func(storage.CommitOutcome), bool, error) {
	ops, handled, err := decodeEntityCommandOperations(cmd.Type, cmd.Data)
	if !handled || err != nil {
		return nil, nil, handled, err
	}
	if len(ops) == 0 {
		return nil, nil, true, nil
	}
	namespace, err := entityOperationsNamespace(ops)
	if err != nil {
		return nil, nil, true, err
	}
	mode := storage.CommitModeDirect
	switch cmd.Type {
	case CmdBulkCreateNodes, CmdBulkCreateEdges, CmdBulkDeleteNodes, CmdBulkDeleteEdges:
		mode = storage.CommitModeImplicit
	}
	prepared, settle, managed, err := storage.PrepareManagedEntityChanges(ctx, a.engine, namespace, ops, mode)
	if !managed && err == nil {
		tx, beginErr := storage.BeginEntityTransaction(a.engine, mode)
		if beginErr != nil {
			return nil, nil, true, beginErr
		}
		defer tx.Rollback()
		if err = tx.SetNamespace(namespace); err == nil {
			err = tx.SetDeferredConstraintValidation(true)
		}
		if err == nil {
			for _, op := range ops {
				if err = storage.ApplyEntityChange(tx, op, func(node *storage.Node) error { return storage.ApplyNodeUpdate(tx, node) }, func(id storage.NodeID) error { return tx.DeleteGraphNode(id, storage.CascadeEdges) }); err != nil {
					break
				}
			}
		}
		if err != nil {
			return nil, nil, true, err
		}
		if tx.OperationCount() == 0 {
			return nil, nil, true, tx.CommitContext(ctx)
		}
		prepared, err = tx.PrepareEntityCommit(ctx, tx.ID)
	}
	if err != nil {
		return nil, nil, true, err
	}
	if prepared == nil {
		return nil, nil, true, nil
	}
	payload, err := storage.EncodeEntityTransaction(prepared.Namespace(), prepared.LogicalID(), prepared.Operations(), prepared.Mode())
	if err != nil {
		prepared.Abort()
		if settle != nil {
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
		}
		return nil, nil, true, err
	}
	inner, found := storage.EntityCommitSubmitterFor(a.engine)
	if !found {
		prepared.Abort()
		if settle != nil {
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: storage.ErrInvalidData})
		}
		return nil, nil, true, storage.ErrInvalidData
	}
	releaseAuthority, err := prepared.RetainAcceptedAuthority()
	if err != nil {
		prepared.Abort()
		if settle != nil {
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
		}
		return nil, nil, true, err
	}
	normalized := &Command{Type: CmdCommitTransaction, Data: payload, Timestamp: cmd.Timestamp, RequestID: prepared.LogicalID()}
	normalized.accepted = &acceptedEntityCommand{owner: a.engine, prepared: storage.NewAcceptedEntityCommitHandle(prepared), inner: inner.Submitter, nativeDone: make(chan struct{})}
	complete := func(outcome storage.CommitOutcome) {
		if outcome.State != storage.CommitRecoveryPending {
			normalized.accepted.prepared.Retire()
			releaseAuthority()
		}
		if settle != nil {
			settle(outcome)
		}
	}
	return normalized, complete, true, nil
}
func entityOperationsNamespace(ops []storage.Operation) (string, error) {
	if len(ops) == 0 {
		return "", storage.ErrInvalidData
	}
	op := ops[0]
	var id string
	switch op.Type {
	case storage.OpCreateNode, storage.OpUpdateNode:
		if op.Node == nil {
			return "", storage.ErrInvalidData
		}
		id = string(op.Node.ID)
	case storage.OpDeleteNode:
		id = string(op.NodeID)
	case storage.OpCreateEdge, storage.OpUpdateEdge:
		if op.Edge == nil {
			return "", storage.ErrInvalidData
		}
		id = string(op.Edge.ID)
	case storage.OpDeleteEdge:
		id = string(op.EdgeID)
	default:
		return "", storage.ErrInvalidData
	}
	namespace, _, ok := storage.ParseDatabasePrefix(id)
	if !ok {
		return "", storage.ErrInvalidID
	}
	return namespace, nil
}

// routeLeaderEntityCommand is called only after the mode's leader check and
// before any enqueue/log record. Cmd19 and non-entity commands stay at their
// existing owners; one shared accepted submit path handles normalized work.
func routeLeaderEntityCommand(owner Storage, replicator Replicator, cmd *Command, timeout time.Duration) (bool, error) {
	if err := retiredCommandError(cmd.Type); err != nil {
		return true, err
	}
	if cmd.Type == CmdCommitTransaction {
		if cmd.accepted == nil {
			return true, storage.ErrInvalidData
		}
		return false, nil
	}
	preparer, ok := owner.(leaderEntityCommandPreparer)
	if !ok {
		return false, nil
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	normalized, settle, handled, err := preparer.PrepareEntityCommand(ctx, cmd)
	if !handled || err != nil {
		return handled, err
	}
	if normalized == nil {
		return true, nil
	}
	submitter, ok := replicator.(AcceptedCommandSubmitter)
	if !ok {
		normalized.accepted.prepared.AbortNative()
		if settle != nil {
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: ErrNotReady})
		}
		return true, ErrNotReady
	}
	receipt, err := submitter.SubmitAcceptedCommand(ctx, normalized, settle)
	if err != nil {
		normalized.accepted.prepared.AbortNative()
		if settle != nil {
			settle(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
		}
		return true, err
	}
	return true, receipt.WaitOutcome(ctx).Err
}
