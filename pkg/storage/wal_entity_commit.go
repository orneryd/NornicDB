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
	handle, payload, err := AcceptEntityCommit(w.engine, prepared)
	if err != nil {
		return nil, err
	}
	w.mutationMu.RLock()
	var mutationOnce sync.Once
	releaseMutation := func() { mutationOnce.Do(w.mutationMu.RUnlock) }
	logicalID := prepared.LogicalID()
	// The existing WAL mutex joins registration to its closed admission cut.
	w.wal.mu.Lock()
	if w.wal.closed.Load() {
		w.wal.mu.Unlock()
		releaseMutation()
		handle.Discard(nil, ErrWALClosed)
		return nil, ErrWALClosed
	}
	receipt, resolve := NewCommitReceipt(func(outcome CommitOutcome) {
		handle.Settled()
		w.pendingEntityCommits.Delete(logicalID)
		releaseMutation()
		if settle != nil {
			settle(outcome)
		}
	})
	accepted := &walAcceptedEntityCommit{handle: handle, receipt: receipt, resolve: resolve, releaseMutation: releaseMutation}
	existing, loaded := w.pendingEntityCommits.LoadOrStore(logicalID, accepted)
	w.wal.mu.Unlock()
	if loaded {
		releaseMutation()
		handle.Discard(nil, nil)
		return existing.(*walAcceptedEntityCommit).receipt, nil
	}
	if err = w.wal.AppendWithDatabase(OpEntityCommit, walEntityCommitData{Payload: payload}, prepared.Namespace()); err != nil {
		handle.Settle(resolve, err)
		return receipt, nil
	}
	// The native application and its join are accepted work of the engine's
	// lifecycle owner; a closing engine leaves the record to recovery.
	if !prepared.engine.RunAcceptedContinuation(func(ctx context.Context) { handle.joinContext(ctx, resolve, handle.Apply(payload)) }) {
		handle.Settle(resolve, ErrStorageClosed)
	}
	return receipt, nil
}
