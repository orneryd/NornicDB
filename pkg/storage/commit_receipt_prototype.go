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
// Cause retains native diagnostics; public text describes the accepted outcome
// without republishing a rejection phrase that text-based clients would retry.
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

// Unwrap keeps the existing method available without exposing a retry sentinel.
// Author: NOR-288 lane (Codex), 2026-10-27. Inspect Cause after identifying this
// accepted outcome; errors.Is/As must not erase its durable acceptance boundary.
func (e *AcceptedCommitError) Unwrap() error { return nil }

// Cause exposes diagnostics after the caller has identified an accepted outcome.
// It is intentionally separate from errors.Is/As, including wait cancellation.
func (e *AcceptedCommitError) Cause() error { return e.Err }

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

func (r *CommitReceipt) Done() <-chan struct{}  { return r.done }
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

// resolve is called only by the accepted command owner, never by a waiter.
func (owner CommitReceiptOwner) Resolve(outcome CommitOutcome) error {
	r := owner.receipt
	if outcome.State != CommitApplied && outcome.State != CommitDiscarded && outcome.State != CommitRecoveryPending {
		return errors.New("invalid commit outcome")
	}
	r.mu.Lock()
	if r.outcome.State != CommitRecoveryPending {
		r.mu.Unlock()
		return errors.New("commit outcome already terminal")
	}
	if outcome.State != CommitRecoveryPending || outcome.Err != nil {
		r.markPhysicalLocked(outcome)
	}
	r.outcome = outcome
	if outcome.State == CommitRecoveryPending {
		close(r.changed)
		r.changed = make(chan struct{})
		r.mu.Unlock()
		return nil
	}
	settle := r.settle
	r.settle = nil
	// Settlement must precede the terminal signal; waiters may immediately read
	// the accounting published by that callback. No receipt mutex in callback.
	r.mu.Unlock()
	defer func() { close(r.done); r.mu.Lock(); close(r.changed); r.mu.Unlock() }()
	if settle != nil {
		settle(outcome)
	}
	return nil
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
