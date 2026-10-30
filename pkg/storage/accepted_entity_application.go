// Author: NOR-288 implementation lane (Codex), 2026-10-26.
package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// A retry is another native application of one accepted command, never a new
// user validation or log entry. Backoff releases the native frame and yields
// to metadata writers; retained accepted entity authority remains with this ID.
const acceptedApplicationAttempts = 8

type acceptedApplicationKey struct{ namespace, logicalID string }

type acceptedEntityApplication struct {
	prepared *PreparedEntityCommit
	receipt  *CommitReceipt
	owner    CommitReceiptOwner
	mu       sync.Mutex
	running  bool
	terminal bool
	used     bool
	stop     <-chan struct{}
	release  func()
}

func (b *BadgerEngine) submitAcceptedEntityApplication(prepared *PreparedEntityCommit, settle func(CommitOutcome)) *CommitReceipt {
	// The engine's lifecycle owner decides admission; a sealed owner admits
	// no new accepted work.
	if b.work.Closing() {
		prepared.MarkRecoveryPending()
		prepared.Abort()
		receipt, owner := NewCommitReceipt(settle)
		_ = owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: AcceptedOutcome(CommitRecoveryPending, prepared.logicalID, ErrStorageClosed)})
		return receipt
	}
	application := b.registerAcceptedEntityApplication(prepared, settle)
	application.start()
	return application.receipt
}

func (b *BadgerEngine) registerAcceptedEntityApplication(prepared *PreparedEntityCommit, settle func(CommitOutcome)) *acceptedEntityApplication {
	key := acceptedApplicationKey{prepared.namespace, prepared.logicalID}
	b.mu.Lock()
	application := b.acceptedApplications[key]
	if application != nil {
		b.mu.Unlock()
		if prepared != application.prepared {
			prepared.Abort()
			// Concurrent replay may have prepared a second frame before the
			// first owner registered. Its retained holds also belong to this
			// accepted terminal outcome, even though this frame never applies.
			application.receipt.OnSettled(func(outcome CommitOutcome) {
				drainReleaseCallbacks(&prepared.hooksMu, &prepared.appliedHooks)
				if settle != nil {
					settle(outcome)
				}
			})
		} else {
			application.receipt.OnSettled(settle)
		}
		return application
	}
	// The already allocated prepared frame owns this state. The typed registry
	// and one engine stop channel avoid per-command boxing/entry/lifetime objects.
	application = &prepared.application
	application.prepared = prepared
	if b.acceptedApplications == nil {
		b.acceptedApplications = make(map[acceptedApplicationKey]*acceptedEntityApplication)
	}
	application.receipt, application.owner = NewCommitReceipt(settle)
	b.acceptedApplications[key] = application
	b.mu.Unlock()
	return application
}

func (a *acceptedEntityApplication) retireRegistry() {
	p := a.prepared
	p.engine.mu.Lock()
	key := acceptedApplicationKey{p.namespace, p.logicalID}
	if p.engine.acceptedApplications[key] == a {
		delete(p.engine.acceptedApplications, key)
	}
	p.engine.mu.Unlock()
}

func (a *acceptedEntityApplication) publish() {
	drainReleaseCallbacks(&a.prepared.hooksMu, &a.prepared.appliedHooks)
	a.owner.PublishPhysical(CommitOutcome{State: CommitApplied})
}

// parkAcceptedEntityApplication retains the same native owner when an outer
// accepted log cannot submit it. Only recovery of an actual accepted record
// starts application; uncertain log I/O is not permission to apply new bytes.
func (b *BadgerEngine) parkAcceptedEntityApplication(prepared *PreparedEntityCommit, cause error) *CommitReceipt {
	a := b.registerAcceptedEntityApplication(prepared, nil)
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.running && !a.terminal {
		prepared.MarkRecoveryPending()
		prepared.Abort()
		a.used = true
		_ = a.owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: AcceptedOutcome(CommitRecoveryPending, prepared.logicalID, cause)})
	}
	return a.receipt
}

func (a *acceptedEntityApplication) start() {
	b := a.prepared.engine
	b.mu.RLock()
	a.mu.Lock()
	if a.running || a.terminal {
		a.mu.Unlock()
		b.mu.RUnlock()
		return
	}
	// Author: NOR-288 lane (Claude), 2026-10-30. Each application is accepted
	// work of the engine's lifecycle owner: enrolled before launch, stopped by
	// its cancellation and joined by Close (lifecycle.WorkLifetime).
	ctx, release, admitted := b.work.Acquire(b.work.Context())
	if !admitted {
		a.mu.Unlock()
		b.mu.RUnlock()
		a.prepared.MarkRecoveryPending()
		a.prepared.Abort()
		_ = a.owner.Resolve(CommitOutcome{State: CommitRecoveryPending, Err: AcceptedOutcome(CommitRecoveryPending, a.prepared.logicalID, ErrStorageClosed)})
		return
	}
	a.stop, a.release = ctx.Done(), release
	a.running = true
	retry := a.used
	// A recovery invocation waits for this bounded application, rather than
	// immediately observing the preceding attempt's already-reported error.
	a.owner.beginRecovery()
	a.mu.Unlock()
	b.mu.RUnlock()
	// Starting a prepared frame only sends its existing decision; it does not
	// wait for native publication or call listeners. Match the original submit
	// order without a scheduling handoff before the first native attempt. The
	// worker still owns completion, bounded retries and lifetime retirement.
	// Author: NOR-288 lane (Codex),2026-10-29; contract-based cost correction.
	var current *PreparedEntityCommit
	var err error
	if !retry {
		if a.stopped() {
			err = ErrStorageClosed
		} else {
			current = a.prepared
			err = a.beginAttempt(current)
		}
	}
	go a.apply(current, err)
}

func (a *acceptedEntityApplication) beginAttempt(current *PreparedEntityCommit) error {
	a.used = true
	return current.startCommit(a.publish)
}

func (a *acceptedEntityApplication) apply(current *PreparedEntityCommit, startErr error) {
	p := a.prepared
	retired := false
	release := a.release
	defer func() {
		if !retired {
			release()
		}
	}()
	var outcome CommitOutcome
	if startErr != nil {
		outcome = CommitOutcome{State: CommitRecoveryPending, Err: startErr}
		goto complete
	}
	for attempt := 0; attempt < acceptedApplicationAttempts; attempt++ {
		if current == nil {
			if a.stopped() {
				outcome = CommitOutcome{State: CommitRecoveryPending, Err: ErrStorageClosed}
				goto complete
			}
			delay := time.NewTimer(time.Millisecond << attempt)
			select {
			case <-delay.C:
			case <-a.stop:
				delay.Stop()
			}
			if a.stopped() {
				outcome = CommitOutcome{State: CommitRecoveryPending, Err: ErrStorageClosed}
				goto complete
			}
			var err error
			current, _, err = prepareAcceptedEntityApplication(context.Background(), p.engine,
				EntityTransactionEnvelope{Namespace: p.namespace, LogicalTxID: p.logicalID, Mode: p.mode}, p.Operations(), p.authorityOwner)
			if err != nil {
				current = nil
				outcome = CommitOutcome{State: CommitRecoveryPending, Err: err}
				if errors.Is(err, ErrConflict) {
					continue
				}
				break
			}
			if err := a.beginAttempt(current); err != nil {
				outcome = CommitOutcome{State: CommitRecoveryPending, Err: err}
				break
			}
		}
		// Once an attempt starts, Close must join its native completion. It may
		// stop a later retry, but cannot abandon this frame or its publication.
		<-current.finished
		outcome = current.terminal
		if outcome.State == CommitApplied && current != p && len(current.operations) == 0 {
			// A durable idempotency marker is already published. Its empty
			// frame has no native continuation to invoke the original callback.
			a.publish()
		}
		if outcome.State == CommitApplied || !errors.Is(outcome.Err, ErrConflict) {
			break
		}
		current = nil
	}
complete:
	if outcome.State != CommitApplied {
		if !a.used {
			// Close may win before the first continuation starts. Retire its
			// staging frame so the engine's write barrier can drain.
			p.Abort()
			a.used = true
		}
		if outcome.Err == nil {
			outcome.Err = errors.New("native application did not reach a terminal outcome")
		}
		p.MarkRecoveryPending()
		outcome = CommitOutcome{State: CommitRecoveryPending, Err: AcceptedOutcome(CommitRecoveryPending, p.logicalID,
			fmt.Errorf("native application could not complete within %d attempts: %w", acceptedApplicationAttempts, outcome.Err))}
	}
	a.mu.Lock()
	a.running = false
	a.terminal = outcome.State == CommitApplied
	if !a.terminal {
		// Pending resolution invokes no settlement callback. Serialize it with
		// recovery start so an older error cannot overwrite a newer attempt.
		_ = a.owner.Resolve(outcome)
		a.mu.Unlock()
		return
	}
	a.mu.Unlock()
	// Every native frame and engine lifetime lease is finished. External
	// settlement may re-enter Close; it must not wait on this worker itself.
	retired = true
	release()
	a.retireRegistry()
	_ = a.owner.Resolve(outcome)
}

// RunAcceptedContinuation runs fn as accepted work of this engine's lifecycle
// owner (lifecycle.WorkLifetime), the same owner as accepted applications:
// enrolled before launch, cancelled and joined by Close. It reports false,
// without running fn, once the owner's admission is sealed. fn receives the owner's
// work context. Author: NOR-288 lane (Claude), 2026-10-30.
func (b *BadgerEngine) RunAcceptedContinuation(fn func(context.Context)) bool {
	ctx, release, admitted := b.work.Acquire(b.work.Context())
	if !admitted {
		return false
	}
	go func() {
		defer release()
		fn(ctx)
	}()
	return true
}

// RunAcceptedContinuation finds the accepted-command owner beneath engine's
// wrappers and runs fn as its accepted work. It reports false when there is no
// such owner or it is closing; the caller then leaves the work to recovery.
func RunAcceptedContinuation(engine Engine, fn func(context.Context)) bool {
	type owner interface {
		RunAcceptedContinuation(func(context.Context)) bool
	}
	for engine != nil {
		if o, ok := engine.(owner); ok {
			return o.RunAcceptedContinuation(fn)
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return false
		}
		next := wrapper.GetInnerEngine()
		if next == engine {
			return false
		}
		engine = next
	}
	return false
}

// stopped reports that this application must yield, from the engine's
// lifecycle owner alone: its admission is sealed, or it cancelled the work.
func (a *acceptedEntityApplication) stopped() bool {
	select {
	case <-a.stop:
		return true
	default:
	}
	return a.prepared.engine.work.Closing()
}

// OnSettled attaches a consumer to the existing terminal owner. Pending errors
// do not invoke it. Callbacks complete before Done closes; callers must not
// wait on this receipt from their callback. This joins recovery without parking
// a goroutine for the lifetime of an unresolved accepted command.
func (r *CommitReceipt) OnSettled(settle func(CommitOutcome)) {
	if settle == nil {
		return
	}
	r.mu.Lock()
	if r.outcome.State != CommitRecoveryPending {
		outcome := r.outcome
		r.mu.Unlock()
		<-r.done
		settle(outcome)
		return
	}
	prior := r.settle
	r.settle = func(outcome CommitOutcome) {
		if prior != nil {
			prior(outcome)
		}
		settle(outcome)
	}
	r.mu.Unlock()
}
