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
	handle          *AcceptedEntityCommitHandle
	receipt         *CommitReceipt
	resolve         CommitReceiptOwner
	releaseMutation func()
}

// SubmitEntityCommit journals the same strict semantic record peers apply.
// Definite rejection happens before journaling; once journal I/O begins, an
// uncertain error retains accepted ownership for recovery rather than rollback.
func (w *WALEngine) SubmitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome)) (*CommitReceipt, error) {
	return w.submitEntityCommit(ctx, prepared, settle, nil)
}

// submitEntityCommit takes ownership of an optional existing mutation lease.
// This joins observed preparation to journal acceptance without recursively
// acquiring an RWMutex read lock while a restore writer may be queued.
func (w *WALEngine) submitEntityCommit(ctx context.Context, prepared *PreparedEntityCommit, settle func(CommitOutcome), releaseMutation func()) (*CommitReceipt, error) {
	defer func() {
		if releaseMutation != nil {
			releaseMutation()
		}
	}()
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
	if ctx != nil && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	// The accepted owner encodes the prepared snapshot once and retains its
	// authority; every outcome below settles through that owner.
	handle, payload, err := AcceptEntityCommit(ctx, w.engine, prepared)
	if err != nil {
		return nil, err
	}
	if releaseMutation == nil {
		if err := w.mutationMu.RLockContext(ctx); err != nil {
			handle.Discard(nil, err)
			return nil, err
		}
		var mutationOnce sync.Once
		releaseMutation = func() { mutationOnce.Do(w.mutationMu.RUnlock) }
	}
	releaseAcceptedMutation := releaseMutation
	logicalID := prepared.LogicalID()
	// The existing WAL mutex joins registration to its closed admission cut.
	if err := w.wal.mu.LockContext(ctx); err != nil {
		releaseMutation()
		handle.Discard(nil, err)
		return nil, err
	}
	if w.wal.work.Closing() {
		w.wal.mu.Unlock()
		releaseMutation()
		handle.Discard(nil, ErrWALClosed)
		return nil, ErrWALClosed
	}
	receipt, resolve := NewCommitReceipt(func(outcome CommitOutcome) {
		handle.Settled()
		w.pendingEntityCommits.Delete(logicalID)
		releaseAcceptedMutation()
		if settle != nil {
			settle(outcome)
		}
	})
	accepted := &walAcceptedEntityCommit{handle: handle, receipt: receipt, resolve: resolve, releaseMutation: releaseAcceptedMutation}
	existing, loaded := w.pendingEntityCommits.LoadOrStore(logicalID, accepted)
	w.wal.mu.Unlock()
	if loaded {
		releaseMutation()
		handle.Discard(nil, nil)
		return existing.(*walAcceptedEntityCommit).receipt, nil
	}
	// The release owner decides publication; the accepted handle settles what
	// it decided. Journal entry transfers the mutation lease to the receipt.
	journalStarted := false
	err = DecidePublication(ctx, func() error {
		releaseMutation = nil
		journalStarted = true
		return w.wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, prepared.Namespace())
	})
	if err != nil {
		if !journalStarted {
			w.pendingEntityCommits.Delete(logicalID)
			handle.Discard(nil, err)
			releaseAcceptedMutation()
			return nil, err
		}
		handle.Settle(resolve, err)
		return receipt, nil
	}
	// The native application and its join are accepted work of the engine's
	// lifecycle owner, entered before launch and left when the goroutine
	// returns; a closing engine leaves the record to recovery.
	work := &prepared.engine.work
	if !work.Enter() {
		handle.Settle(resolve, ErrStorageClosed)
		return receipt, nil
	}
	go func() {
		defer work.Leave()
		ctx := work.Context()
		handle.JoinContext(ctx, resolve, handle.ApplyContext(ctx, payload))
	}()
	return receipt, nil
}
