package storage

import (
	"context"
	"sync/atomic"
)

// AcceptedEntityCommitHandle owns the native frame of accepted logical work.
// Retirement drops this owner's heavy reference; a concurrent operation's local
// snapshot remains alive and uses the prepared frame's existing once guard.
// Immutable journal/replication payload and logical receipts live independently.
type AcceptedEntityCommitHandle struct {
	prepared atomic.Pointer[PreparedEntityCommit]
}

// NewAcceptedEntityCommitHandle transfers native-frame reference ownership.
func NewAcceptedEntityCommitHandle(prepared *PreparedEntityCommit) *AcceptedEntityCommitHandle {
	h := &AcceptedEntityCommitHandle{}
	h.prepared.Store(prepared)
	return h
}

// Submit snapshots the native reference without retaining a lock across callbacks.
func (h *AcceptedEntityCommitHandle) Submit(ctx context.Context, submitter EntityCommitSubmitter) (*CommitReceipt, error) {
	p := h.prepared.Load()
	if p == nil {
		return nil, ErrStorageClosed
	}
	return submitter.SubmitEntityCommit(ctx, p, nil)
}

// AbortNative discards only an unstarted native frame using its existing guard.
func (h *AcceptedEntityCommitHandle) AbortNative() {
	if p := h.prepared.Load(); p != nil {
		p.Abort()
	}
}

// MarkRecoveryPending wakes conflicting writers while retaining accepted authority.
func (h *AcceptedEntityCommitHandle) MarkRecoveryPending() {
	if p := h.prepared.Load(); p != nil {
		p.MarkRecoveryPending()
	}
}

// Retire drops the heavy frame after terminal settlement without invalidating readers.
func (h *AcceptedEntityCommitHandle) Retire() { h.prepared.Store(nil) }

// CloseNative releases staging without declaring accepted logical work discarded.
func (h *AcceptedEntityCommitHandle) CloseNative() {
	p := h.prepared.Swap(nil)
	if p != nil {
		p.MarkRecoveryPending()
		p.Abort()
	}
}
