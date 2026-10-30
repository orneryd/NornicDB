package storage

import (
	"context"
	"sync"
)

// OpEntityCommit journals one atomic accepted semantic entity transaction.
const OpEntityCommit OperationType = "entity_commit"

type walEntityCommitData struct {
	Payload []byte `json:"payload"`
}

type walAcceptedEntityCommit struct {
	prepared        *AcceptedEntityCommitHandle
	receipt         *CommitReceipt
	resolve         CommitReceiptOwner
	releaseMutation func()
}

// SubmitEntityCommit journals the same strict semantic record peers apply.
// Definite rejection happens before journaling; once journal I/O begins, an
// uncertain error retains accepted ownership for recovery rather than rollback.
func (w *WALEngine) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if prepared == nil {
		return nil, ErrInvalidData
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// Preparation owns an immutable private operation snapshot. Encoding only
	// reads it; the exported Operations method still gives callers detached
	// images, but making that public copy here would duplicate every image.
	ops := prepared.operations
	// A durable duplicate receipt has no new mutation to journal.
	if len(ops) == 0 {
		return prepared.engine.SubmitEntityCommit(ctx, prepared, settle)
	}
	inner, found, err := EntityCommitSubmitterForWithContext(ctx, w.engine)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrInvalidData
	}
	payload, err := EncodeEntityTransactionRequestWithContext(ctx, EntityTransactionRequest{Prepared: prepared})
	if err != nil {
		return nil, err
	}
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	releaseAuthority, err := prepared.RetainAcceptedAuthorityWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		releaseAuthority()
		return nil, err
	}
	w.mutationMu.RLock()
	var mutationOnce sync.Once
	releaseMutation := func() { mutationOnce.Do(w.mutationMu.RUnlock) }
	logicalID := prepared.LogicalID()
	handle := NewAcceptedEntityCommitHandle(prepared)
	// The existing WAL mutex joins registration to its closed admission cut.
	w.wal.mu.Lock()
	if w.wal.closed.Load() {
		w.wal.mu.Unlock()
		releaseMutation()
		releaseAuthority()
		return nil, ErrWALClosed
	}
	receipt, resolve := NewCommitReceipt(func(outcome CommitOutcome) {
		releaseAuthority()
		handle.Retire()
		w.pendingEntityCommits.Delete(logicalID)
		releaseMutation()
		if settle != nil {
			settle(outcome)
		}
	})
	accepted := &walAcceptedEntityCommit{prepared: handle, receipt: receipt, resolve: resolve, releaseMutation: releaseMutation}
	existing, loaded := w.pendingEntityCommits.LoadOrStore(logicalID, accepted)
	w.wal.mu.Unlock()
	if loaded {
		releaseAuthority()
		releaseMutation()
		prepared.Abort()
		return existing.(*walAcceptedEntityCommit).receipt, nil
	}
	if err = w.wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, prepared.Namespace()); err != nil {
		handle.MarkRecoveryPending()
		prepared.Abort()
		_ = resolve.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: err})
		return receipt, nil
	}
	go func() {
		applied, err := handle.Submit(context.Background(), inner.Submitter)
		if err != nil {
			_ = resolve.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: err})
			return
		}
		physical := applied.WaitPhysicalOutcome(w.entityWaitCtx)
		if physical.State == CommitDiscarded {
			physical.State = CommitRecoveryPending
		}
		if physical.State == CommitRecoveryPending && physical.Err != nil {
			handle.MarkRecoveryPending()
		}
		resolve.PublishPhysical(physical)
		outcome := applied.WaitOutcome(w.entityWaitCtx)
		if outcome.State == CommitRecoveryPending {
			_ = resolve.Resolve(outcome)
			select {
			case <-applied.Done():
			case <-w.entityWaitCtx.Done():
				return
			}
			outcome = applied.Outcome()
		}
		if outcome.State == CommitDiscarded {
			outcome.State = CommitRecoveryPending
		}
		_ = resolve.Resolve(outcome)
	}()

	return receipt, nil
}
