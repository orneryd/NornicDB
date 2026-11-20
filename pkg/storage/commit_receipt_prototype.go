package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// CommitOutcomeState separates a physical result from caller wait cancellation.
type CommitOutcomeState uint8

const (
	CommitRecoveryPending CommitOutcomeState = iota
	CommitApplied
	CommitDiscarded
)

// CommitOutcome carries acknowledgement errors without disguising an applied write.
type CommitOutcome struct {
	State CommitOutcomeState
	Err   error
}

// AcceptedCommitError reports an error after a command was accepted. It must
// not be treated as a definite rejection or used to roll back an admission.
// State preserves an applied acknowledgement failure versus recovery-pending
// work. A native cause must never classify this outcome as a rejection.
//
// Identify an accepted outcome with errors.As, then read Err for the native
// diagnostic. Err retains that diagnostic while public text describes only the
// accepted outcome, without republishing a rejection phrase that text-based
// clients would retry. AcceptedCommitError has no Unwrap: errors.Is, errors.As
// and errors.Unwrap stop at this durable acceptance boundary, so the enclosed
// diagnostic never confers a retryable rejection identity on the outcome.
type AcceptedCommitError struct {
	State     CommitOutcomeState
	LogicalID string
	Err       error
}

// Author: NOR-288 lane (Codex), 2026-10-26. Preserve accepted uncertainty on
// the wire instead of disguising it as a new command's retryable rejection.
func (e *AcceptedCommitError) Error() string {
	command := "accepted command"
	if e.LogicalID != "" {
		command += " " + e.LogicalID
	}
	state := "committed-pending or unknown"
	if e.State == CommitApplied {
		state = "committed; acknowledgement failed"
	}
	return fmt.Sprintf("%s is %s", command, state)
}

// AcceptedOutcome is the one constructor of the accepted-outcome barrier. An
// error that already is this barrier, in the same state and for the same (or
// an unnamed) command, is returned unchanged. Anything else is enclosed whole,
// including an accepted error nested beside a raw sibling in errors.Join, so
// no rejection escapes the barrier. An unnamed caller keeps the logical ID of
// an enclosed accepted error. Author: NOR-288 lane (Claude), 2026-10-30.
func AcceptedOutcome(state CommitOutcomeState, logicalID string, err error) *AcceptedCommitError {
	if accepted, ok := err.(*AcceptedCommitError); ok && accepted.State == state && (logicalID == "" || accepted.LogicalID == logicalID) {
		return accepted
	}
	if logicalID == "" {
		var enclosed *AcceptedCommitError
		if errors.As(err, &enclosed) {
			logicalID = enclosed.LogicalID
		}
	}
	return &AcceptedCommitError{State: state, LogicalID: logicalID, Err: err}
}

// CommitReceipt owns accepted work until physical application or definite discard.
// Wait cancellation never discards accepted work or releases its admission lease.
type CommitReceipt struct {
	physicalReady   chan struct{}
	physicalOutcome CommitOutcome
	physicalSet     bool
	mu              sync.Mutex
	done            chan struct{}
	changed         chan struct{}
	outcome         CommitOutcome
	settle          func(CommitOutcome)
}

// NewCommitReceipt returns the waiter view and a terminal-owner resolver.
// Only the accepted command coordinator retains the resolver.
type CommitReceiptOwner struct{ receipt *CommitReceipt }

func NewCommitReceipt(settle func(CommitOutcome)) (*CommitReceipt, CommitReceiptOwner) {
	receipt := &CommitReceipt{physicalReady: make(chan struct{}), done: make(chan struct{}), changed: make(chan struct{}), outcome: CommitOutcome{State: CommitRecoveryPending}, settle: settle}
	return receipt, CommitReceiptOwner{receipt: receipt}
}

func (r *CommitReceipt) Done() <-chan struct{} { return r.done }

// Decision returns the outcome recorded for this receipt together with the
// channel that closes at its next recorded state change. Reading both under the
// receipt's mutex is the snapshot an observer needs, because every recording
// closes exactly the channel a prior snapshot returned, so an observer that
// re-reads after waking cannot miss a change. A recovery-pending outcome is an
// observation, not a decision: an observer leaves only on a terminal state.
// This is the signal for work the transport's own Shutdown joins, which must
// leave on the decision and cannot wait for the foreign settlement observer
// that Done covers. External callers keep waiting on Done.
// For example, a Raft transport waiting for an accepted command snapshots
// Decision on each iteration and waits on the returned channel while pending.
// It retains one timeout across those observations and leaves on a terminal
// outcome, so a settlement observer can call Shutdown without joining itself.
// Author: NOR-288 lane (Claude), 2026-10-30.
func (r *CommitReceipt) Decision() (CommitOutcome, <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.outcome, r.changed
}

func (r *CommitReceipt) Outcome() CommitOutcome { r.mu.Lock(); defer r.mu.Unlock(); return r.outcome }
func (r *CommitReceipt) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return acceptedOutcomeView(r.Outcome()).Err
	default:
	}
	select {
	case <-r.done:
		return acceptedOutcomeView(r.Outcome()).Err
	case <-ctx.Done():
		return acceptedOutcomeView(CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()}).Err
	}
}

// claim records this receipt's one outcome. Everything that decides the receipt
// is synchronous with the caller and under the receipt's mutex: validation, the
// physical decision, the recorded outcome, the detached settlement callback and
// the state-change signal. claim returns the settlement callback this call took
// ownership of and whether it recorded a terminal outcome; a recovery-pending
// recording decides nothing and reports decided false. A caller that delivers in
// its own frame must not hold the receipt's mutex or locks that frame's join
// covers; no receipt mutex is held here when delivery runs. The common path
// returns values rather than a delivery closure, so an ordinary commit
// allocates nothing for the decision.
func (owner CommitReceiptOwner) claim(outcome CommitOutcome) (func(CommitOutcome), bool, error) {
	r := owner.receipt
	if outcome.State != CommitApplied && outcome.State != CommitDiscarded && outcome.State != CommitRecoveryPending {
		return nil, false, errors.New("invalid commit outcome")
	}
	r.mu.Lock()
	if r.outcome.State != CommitRecoveryPending {
		r.mu.Unlock()
		return nil, false, errors.New("commit outcome already terminal")
	}
	if outcome.State != CommitRecoveryPending || outcome.Err != nil {
		r.markPhysicalLocked(outcome)
	}
	r.outcome = outcome
	// The recording is the receipt's one state change. Its signal closes before
	// any settlement callback runs, so an observer that snapped the state under
	// this mutex wakes on the decision itself. A terminal recording keeps the
	// closed channel: no later observation can follow it.
	close(r.changed)
	if outcome.State == CommitRecoveryPending {
		// A pending observation is not a decision: reopen for the next one.
		r.changed = make(chan struct{})
		r.mu.Unlock()
		return nil, false, nil
	}
	settle := r.settle
	r.settle = nil
	r.mu.Unlock()
	return settle, true, nil
}

// deliverTerminal is the receipt's one terminal delivery: the caller's
// settlement callback, then the terminal signal. Settlement must precede that
// signal; waiters may immediately read the accounting published by the callback.
func (owner CommitReceiptOwner) deliverTerminal(settle func(CommitOutcome), outcome CommitOutcome) {
	defer close(owner.receipt.done)
	if settle != nil {
		settle(outcome)
	}
}

// resolve is called only by the accepted command owner, never by a waiter.
func (owner CommitReceiptOwner) Resolve(outcome CommitOutcome) error {
	settle, decided, err := owner.claim(outcome)
	if decided {
		owner.deliverTerminal(settle, outcome)
	}
	return err
}

// WaitOutcome is for an accepted outer owner composing this physical owner.
// It returns a recovery-pending error without falsely settling the receipt;
// ordinary callers use Wait, which only completes on a terminal outcome.
func (r *CommitReceipt) WaitOutcome(ctx context.Context) CommitOutcome {
	for {
		r.mu.Lock()
		outcome, changed := r.outcome, r.changed
		r.mu.Unlock()
		if outcome.State != CommitRecoveryPending {
			select {
			case <-r.done:
				return acceptedOutcomeView(outcome)
			case <-ctx.Done():
				return acceptedOutcomeView(CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()})
			}
		}
		if outcome.Err != nil {
			return acceptedOutcomeView(outcome)
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return acceptedOutcomeView(CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()})
		}
	}
}

func (r *CommitReceipt) markPhysicalLocked(outcome CommitOutcome) {
	if r.physicalSet {
		if r.physicalOutcome.State == CommitRecoveryPending && outcome.State != CommitRecoveryPending {
			r.physicalOutcome = outcome
		}
		return
	}
	r.physicalSet = true
	r.physicalOutcome = outcome
	close(r.physicalReady)
}
func (owner CommitReceiptOwner) PublishPhysical(outcome CommitOutcome) {
	r := owner.receipt
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markPhysicalLocked(outcome)
}

// WaitPhysicalOutcome is the state-machine boundary before listener delivery.
// Receipt Wait still waits for the accepted owner's full terminal completion.
func (r *CommitReceipt) WaitPhysicalOutcome(ctx context.Context) CommitOutcome {
	for {
		r.mu.Lock()
		if r.physicalSet {
			outcome := r.physicalOutcome
			r.mu.Unlock()
			return acceptedOutcomeView(outcome)
		}
		ready := r.physicalReady
		r.mu.Unlock()
		select {
		case <-ready:
		case <-ctx.Done():
			return acceptedOutcomeView(CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()})
		}
	}
}

// beginRecovery starts a new bounded observation of the same accepted owner.
// A preceding pending physical error is not a permanent physical decision.
// Author: NOR-288 lane (Codex), 2026-10-27.
func (owner CommitReceiptOwner) beginRecovery() {
	r := owner.receipt
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcome.State != CommitRecoveryPending {
		return
	}
	if r.outcome.Err == nil && !r.physicalSet {
		return // First application has no preceding observation to invalidate.
	}
	r.outcome.Err = nil
	if r.physicalSet && r.physicalOutcome.State == CommitRecoveryPending {
		r.physicalSet = false
		r.physicalOutcome = CommitOutcome{State: CommitRecoveryPending}
		r.physicalReady = make(chan struct{})
	}
	close(r.changed)
	r.changed = make(chan struct{})
}

// Author: NOR-288 lane (Codex), 2026-10-27. Caller cancellation only stops
// observation. It cannot turn an accepted command into a retryable rejection;
// acknowledgement failure likewise cannot conceal that application succeeded.
func acceptedOutcomeView(outcome CommitOutcome) CommitOutcome {
	if outcome.Err == nil || outcome.State == CommitDiscarded {
		return outcome
	}
	outcome.Err = AcceptedOutcome(outcome.State, "", outcome.Err)
	return outcome
}
