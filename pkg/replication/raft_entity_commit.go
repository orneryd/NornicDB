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
		ctx = r.work.Context() // a caller without a context gets this replicator's lifetime
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
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	// The producer is accepted work: Shutdown's retirement drains the queue
	// only after every accepted producer and consumer has returned.
	release, accepted := r.enter()
	if !accepted {
		return nil, ErrClosed
	}
	defer release()
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
	if err := checkApplyReadiness(r.work.Closing(), r.started.Load()); err != nil {
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
	f.cmd.accepted.Reject(f.accepted.owner, err)
}

// reportRecorded hands every recorded application result, initial or retry,
// before or after native submission, to the one accepted owner. Its join runs
// outside Raft's application/log locks.
func (f *applyFuture) reportRecorded(err error) {
	if f.accepted == nil {
		select {
		case f.errCh <- err:
		default:
		}
		return
	}
	f.cmd.accepted.Settle(f.accepted.owner, err)
}
