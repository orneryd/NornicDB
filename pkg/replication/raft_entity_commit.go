package replication

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// raftAcceptedOutcome belongs to a queued command, rather than to its waiter.
// Term binds prepared authority to the leader that admitted the command.
type raftAcceptedOutcome struct {
	receipt *storage.CommitReceipt
	owner   storage.CommitReceiptOwner
	term    uint64
}

func (r *RaftReplicator) SubmitAcceptedCommand(ctx context.Context, cmd *Command, settle func(storage.CommitOutcome)) (*storage.CommitReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cmd == nil || cmd.accepted == nil {
		return nil, storage.ErrInvalidData
	}
	r.mu.RLock()
	err := r.acceptedAdmissionErrorLocked(0)
	term := r.currentTerm
	if err == nil {
		// Shutdown closes admission under mu before waiting for all queue
		// producers and consumers. No accepted producer may enqueue after drain.
		r.wg.Add(1)
	}
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	defer r.wg.Done()
	receipt, owner := storage.NewCommitReceipt(settle)
	future := &applyFuture{cmd: cmd, errCh: make(chan error, 1), accepted: &raftAcceptedOutcome{receipt: receipt, owner: owner, term: term}}
	if err := r.processApply(ctx, future, raftApplyQueue{}); err != nil {
		return nil, err
	}
	return receipt, nil
}

// acceptedAdmissionErrorLocked is checked before queue transfer and again
// while append retains the same role/term read lease. Prepared work is never
// forwarded to another authority after a leadership change.
func (r *RaftReplicator) acceptedAdmissionErrorLocked(term uint64) error {
	if err := checkApplyReadiness(r.closed.Load(), r.started.Load()); err != nil {
		return err
	}
	if r.state != StateLeader || (term != 0 && term != r.currentTerm) {
		return ErrNotLeader
	}
	return nil
}

func (f *applyFuture) discardBeforeCommit(err error) {
	if f.accepted == nil {
		select {
		case f.errCh <- err:
		default:
		}
		return
	}
	if f.cmd.accepted != nil && f.cmd.accepted.prepared != nil {
		f.cmd.accepted.prepared.AbortNative()
	}
	_ = f.accepted.owner.Resolve(storage.CommitOutcome{State: storage.CommitDiscarded, Err: err})
}

// reportRecorded preserves pending recovery when a consensus decision exists.
// Physical publication can release storage authority before native listeners
// finish. The final join runs outside Raft's application/log locks.
func (f *applyFuture) reportRecorded(err error) {
	if f.accepted == nil {
		select {
		case f.errCh <- err:
		default:
		}
		return
	}
	if err != nil {
		if f.cmd.accepted != nil && f.cmd.accepted.started.Load() {
			f.cmd.accepted.prepared.MarkRecoveryPending()
		}
		_ = f.accepted.owner.Resolve(storage.CommitOutcome{State: storage.CommitRecoveryPending, Err: err})
		// Snapshot this attempt's ownership before scheduling a join. An apply
		// rejected before native execution must not later observe another
		// successful attempt and attach its stale error to that completion.
		if f.cmd.accepted == nil || !f.cmd.accepted.started.Load() {
			return
		}
	} else {
		f.accepted.owner.PublishPhysical(storage.CommitOutcome{State: storage.CommitApplied})
	}
	go func() {
		outcome := acceptedCommandOutcome(f.cmd, err)
		if outcome.State != storage.CommitRecoveryPending || outcome.Err != nil {
			_ = f.accepted.owner.Resolve(outcome)
		}
	}()
}
