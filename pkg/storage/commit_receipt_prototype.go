package storage

import (
	"context"
	"errors"
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
// work. Unwrap preserves the original timeout or transport error for errors.Is.
type AcceptedCommitError struct {
	State     CommitOutcomeState
	LogicalID string
	Err       error
}

func (e *AcceptedCommitError) Error() string { return e.Err.Error() }
func (e *AcceptedCommitError) Unwrap() error { return e.Err }

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
		return r.Outcome().Err
	default:
	}
	select {
	case <-r.done:
		return r.Outcome().Err
	case <-ctx.Done():
		return ctx.Err()
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
				return outcome
			case <-ctx.Done():
				return CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()}
			}
		}
		if outcome.Err != nil {
			return outcome
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()}
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
	select {
	case <-r.physicalReady:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.physicalOutcome
	case <-ctx.Done():
		return CommitOutcome{State: CommitRecoveryPending, Err: ctx.Err()}
	}
}
