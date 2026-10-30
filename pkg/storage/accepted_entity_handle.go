package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// AcceptedEntityCommitHandle is the one local owner of an accepted entity
// command between its outer log (WAL, Standalone, HA or Raft) and native
// application. Transports reach staging and authority only through it:
// Apply runs the initial native submission or an ordered replay, Settle and
// Join join a recorded or uncertain transport outcome (parking an unstarted
// frame for recovery), Reject discards a command no log recorded, and Settled
// releases the frame and authority when the outer receipt settles.
// Author: NOR-288 lane (Claude), 2026-10-29.
type AcceptedEntityCommitHandle struct {
	prepared  atomic.Pointer[PreparedEntityCommit]
	engine    Engine
	work      *BadgerEngine // the prepared frame's engine: its lifecycle owner runs the join
	inner     EntityCommitSubmitter
	logicalID string
	release   func()

	once         sync.Once
	initialMu    sync.Mutex
	initialReady bool
	initialWait  *sync.Cond
	started      atomic.Bool
	parkCause    error
	physical     CommitOutcome
	native       *CommitReceipt
	// Pending reports to the outer receipt are ordered by when their cause
	// arose, not by when an asynchronous join finishes: a later cause is
	// never replaced by an earlier one.
	pendingSeq atomic.Uint64
	pendingMu  sync.Mutex
	pendingTop uint64
}

// AcceptEntityCommit encodes the prepared snapshot once and retains its
// accepted authority for the returned owner. On error nothing is retained and
// the caller still owns the unaccepted prepared frame.
func AcceptEntityCommit(ctx context.Context, engine Engine, prepared *PreparedEntityCommit) (*AcceptedEntityCommitHandle, []byte, error) {
	inner, found := EntityCommitSubmitterFor(engine)
	if !found {
		return nil, nil, ErrInvalidData
	}
	// Preparation owns an immutable private operation snapshot; encoding only
	// reads it rather than duplicating a detached public Operations copy.
	payload, err := EncodeEntityTransaction(EntityTransactionRequest{Prepared: prepared})
	if err != nil {
		return nil, nil, err
	}
	release, err := prepared.RetainAcceptedAuthority(ctx)
	if err != nil {
		return nil, nil, err
	}
	h := &AcceptedEntityCommitHandle{engine: engine, work: prepared.engine, inner: inner.Submitter, logicalID: prepared.LogicalID(), release: release}
	h.prepared.Store(prepared)
	return h, payload, nil
}

// AppliesTo reports whether this owner applies its command on engine. A
// command from another process or engine has no local owner.
func (h *AcceptedEntityCommitHandle) AppliesTo(engine Engine) bool {
	return h != nil && h.engine == engine
}

// Settled is the outer receipt's terminal settlement: it retires the native
// frame and releases accepted authority. Transports call it from their own
// settle callback before observers run.
func (h *AcceptedEntityCommitHandle) Settled() {
	h.prepared.Store(nil)
	h.release()
}

// Terminal wraps settle with Settled for transports without their own callback.
func (h *AcceptedEntityCommitHandle) Terminal(settle func(CommitOutcome)) func(CommitOutcome) {
	return func(outcome CommitOutcome) {
		h.Settled()
		if settle != nil {
			settle(outcome)
		}
	}
}

// Discard drops a command before any outer receipt exists: the unstarted frame
// is aborted, authority released, and settle (if any) sees the discard.
func (h *AcceptedEntityCommitHandle) Discard(settle func(CommitOutcome), err error) {
	if p := h.prepared.Swap(nil); p != nil {
		p.Abort()
	}
	h.release()
	if settle != nil {
		settle(CommitOutcome{State: CommitDiscarded, Err: err})
	}
}

// begin selects the initial native action. Submission, parking and receipt
// observation may call foreign code and run after this guard exits.
func (h *AcceptedEntityCommitHandle) begin() (initial bool) {
	h.once.Do(func() {
		initial = true
		h.started.Store(true)
	})
	return initial
}

func (h *AcceptedEntityCommitHandle) finishInitial() {
	h.initialMu.Lock()
	h.initialReady = true
	if h.initialWait != nil {
		h.initialWait.Broadcast()
	}
	h.initialMu.Unlock()
}

// waitInitial joins only publication of the first native receipt. Ordinary
// commits allocate no waiter; a competing application creates the condition.
func (h *AcceptedEntityCommitHandle) waitInitial() {
	h.initialMu.Lock()
	for !h.initialReady {
		if h.initialWait == nil {
			h.initialWait = sync.NewCond(&h.initialMu)
		}
		h.initialWait.Wait()
	}
	h.initialMu.Unlock()
}

// park retains the accepted native authority when the outer log cannot submit
// it. Only recovery of an actual accepted record starts application; uncertain
// log I/O is not permission to apply. The caller won begin.
func (h *AcceptedEntityCommitHandle) park(cause error) {
	h.parkCause = cause
	// The registered application now owns the parked frame and its authority.
	if p := h.prepared.Swap(nil); p != nil {
		h.native = p.engine.parkAcceptedEntityApplication(p, cause)
	} else {
		r, owner := NewCommitReceipt(nil)
		_ = owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: AcceptedOutcome(CommitRecoveryPending, h.logicalID, cause)})
		h.native = r
	}
	h.finishInitial()
}

// ApplyContext runs this command's native application for an outer log entry: the
// initial submission, or an ordered replay of the same entry. A replay of a
// still-pending entry resumes the registered native owner from data.
//
// ctx is the caller's work context, so its owner's Close cancels an initial
// native submission it would otherwise wait on. A cancelled submission parks
// the frame for recovery. The waits on the native receipt outlive the caller's
// request and take the frame engine's own lifetime (h.work.work); its Close
// ends them as recovery-pending, never as applied or discarded.
func (h *AcceptedEntityCommitHandle) ApplyContext(ctx context.Context, data []byte) error {
	initial := h.begin()
	if initial {
		var native *CommitReceipt
		err := ErrStorageClosed
		if p := h.prepared.Load(); p != nil {
			native, err = h.inner.SubmitEntityCommit(ctx, p, nil)
		}
		if err != nil {
			h.park(err)
			return h.native.WaitPhysicalOutcome(h.work.work.Context()).Err
		}
		h.native = native
		h.physical = native.WaitPhysicalOutcome(h.work.work.Context())
		if h.physical.State == CommitRecoveryPending && h.physical.Err != nil {
			h.markRecoveryPending()
		}
		// Native accepted application owns bounded conflict completion for
		// every transport; the join reads its receipt directly.
		h.finishInitial()
	} else {
		h.waitInitial()
	}
	if h.native == nil {
		return h.physical.Err // rejected before any record
	}
	// Ordered replay may revisit a formerly pending entry after recovery
	// settled it. The native receipt, not the first error, is authoritative.
	if !initial && h.native.Outcome().State == CommitRecoveryPending {
		envelope, operations, err := DecodeEntityTransaction(data)
		if err != nil {
			return err
		}
		recovery, err := SubmitAcceptedEntityTransaction(ctx, h.engine, envelope, operations, nil)
		if err != nil {
			return err
		}
		return recovery.WaitPhysicalOutcome(h.work.work.Context()).Err
	}
	return h.native.WaitPhysicalOutcome(h.work.work.Context()).Err
}

// Settle joins a recorded or uncertain transport outcome to the native owner.
// A transport error before native submission parks the frame and publishes
// recovery-pending at once, so recovery of the record resumes this owner and
// settles the original receipt. A started attempt reports its own native
// outcome through the join. Settle never waits: the join runs as accepted
// work of the engine's lifecycle owner, outside the caller's locks.
func (h *AcceptedEntityCommitHandle) Settle(outer CommitReceiptOwner, transportErr error) {
	h.record(outer, transportErr)
	h.launchJoin(outer, transportErr)
}

// JoinContext is Settle for a caller already on its own goroutine and holding
// no locks: the same recording, with the native join run inline and bounded by
// ctx (the caller's work context). When ctx ends first, the outer receipt is
// left pending and settles from the native owner's terminal callback.
func (h *AcceptedEntityCommitHandle) JoinContext(ctx context.Context, outer CommitReceiptOwner, transportErr error) {
	h.record(outer, transportErr)
	h.join(ctx, outer, transportErr, h.pendingSeq.Add(1))
}

// launchJoin runs the join as accepted work of the engine's lifecycle owner
// (RunAcceptedContinuation): engine Close cancels and joins it. A closing
// engine admits no work; the record then stays pending for recovery, which
// settles it at the next open. Author: NOR-288 lane (Claude), 2026-10-30.
func (h *AcceptedEntityCommitHandle) launchJoin(outer CommitReceiptOwner, transportErr error) {
	seq := h.pendingSeq.Add(1)
	if h.work.RunAcceptedContinuation(func(ctx context.Context) { h.join(ctx, outer, transportErr, seq) }) {
		return
	}
	if transportErr == nil {
		transportErr = ErrStorageClosed
	}
	h.ObservePending(outer, transportErr)
}

func (h *AcceptedEntityCommitHandle) record(outer CommitReceiptOwner, transportErr error) {
	if transportErr != nil {
		if h.begin() {
			h.park(transportErr)
			h.ObservePending(outer, transportErr)
		}
	} else {
		outer.PublishPhysical(CommitOutcome{State: CommitApplied})
	}
}

// Close is a closing transport's cut. Native staging is released without
// settling the accepted record, which stays pending for recovery: an unstarted
// frame is parked, and an attempt not yet staged natively fails and parks.
func (h *AcceptedEntityCommitHandle) Close(outer CommitReceiptOwner, err error) {
	if h.begin() {
		h.park(err)
	} else if p := h.prepared.Swap(nil); p != nil {
		p.MarkRecoveryPending()
		p.Abort()
	}
	h.ObservePending(outer, err)
	h.launchJoin(outer, err)
}

// NativeFrameRetained reports whether this owner still references its native
// frame. Terminal settlement and parking drop it; diagnostics use this.
func (h *AcceptedEntityCommitHandle) NativeFrameRetained() bool {
	return h.prepared.Load() != nil
}

// Reject discards a command its outer log never recorded. A command already
// applying or parked is accepted work and is settled instead.
func (h *AcceptedEntityCommitHandle) Reject(outer CommitReceiptOwner, err error) {
	if !h.begin() {
		h.Settle(outer, err)
		return
	}
	if p := h.prepared.Load(); p != nil {
		p.Abort()
	}
	h.physical = CommitOutcome{State: CommitDiscarded, Err: err}
	h.finishInitial()
	_ = outer.Resolve(h.physical)
}

// ObservePending reports accepted uncertainty to the outer receipt without
// settling it: a recorded entry's acknowledgement cannot certify rejection.
func (h *AcceptedEntityCommitHandle) ObservePending(outer CommitReceiptOwner, err error) {
	h.publishPending(outer, AcceptedOutcome(CommitRecoveryPending, h.logicalID, err), h.pendingSeq.Add(1))
}

// publishPending reports a pending cause taken at seq unless a later cause
// has already been reported.
func (h *AcceptedEntityCommitHandle) publishPending(outer CommitReceiptOwner, err error, seq uint64) {
	h.pendingMu.Lock()
	defer h.pendingMu.Unlock()
	if seq < h.pendingTop {
		return
	}
	h.pendingTop = seq
	_ = outer.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: err})
}

// join resolves the outer receipt from the native terminal, including a later
// in-process recovery of a pending command.
func (h *AcceptedEntityCommitHandle) join(ctx context.Context, outer CommitReceiptOwner, transportErr error, seq uint64) {
	if !h.started.Load() {
		// A successful acknowledgement without a local application parks too;
		// only recovery of the record may apply it.
		if h.begin() {
			h.park(transportErr)
		}
	}
	h.waitInitial()
	if h.native == nil {
		_ = outer.Resolve(h.physical) // rejected before any record
		return
	}
	outcome := h.native.WaitOutcome(ctx)
	select {
	case <-h.native.Done():
		outcome = h.native.Outcome()
	default:
	}
	if outcome.State == CommitApplied {
		// A recovered native attempt supersedes its own first error, and an
		// error that parked the frame preceded the application it reports.
		// Independent replication acknowledgement errors remain errors.
		if (h.physical.State == CommitRecoveryPending && errors.Is(transportErr, h.physical.Err)) || (h.parkCause != nil && transportErr == h.parkCause) {
			transportErr = nil
		}
		outcome.Err = transportErr
	}
	if outcome.State == CommitRecoveryPending {
		// The caller's own loss of certainty (a closing transport, a failed
		// record) stays the reported cause; the native cause reports otherwise.
		if transportErr != nil {
			outcome.Err = AcceptedOutcome(CommitRecoveryPending, h.logicalID, transportErr)
		}
		h.publishPending(outer, outcome.Err, seq)
		h.native.OnSettled(func(final CommitOutcome) { _ = outer.Resolve(final) })
		return
	}
	_ = outer.Resolve(outcome)
}

func (h *AcceptedEntityCommitHandle) markRecoveryPending() {
	if p := h.prepared.Load(); p != nil {
		p.MarkRecoveryPending()
	}
}
