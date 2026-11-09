package cypher

import (
	"context"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// transactionEnd ends an explicit transaction from outside its owner (the
// client connection that runs its statements), as Neo4j does when it marks a
// transaction for termination: TERMINATE TRANSACTIONS, or the server's
// transaction timeout reached while the transaction is idle (NOR-284). The
// end goes through the transaction's one failure owner (TransactionContext's
// fail): its staged storage is rolled back at once, which releases its key
// locks, its write admission and its memory, and the transaction stays failed
// until its client ends it: later statements and COMMIT are refused with
// the termination's status, except a statement of the other transaction
// type, which admission refuses first with ForbiddenDueToTransactionType.
//
// When a statement of the transaction is running, the end belongs to that
// statement: TERMINATE cancels it and the statement's failure ends the
// transaction through the same owner; a termination that arrives once the
// statement is past its last check is applied when the owner's operation
// returns. Only an idle transaction is ended by the caller itself.
//
// The server timeout is an idle timeout here: it counts from the end of the
// transaction's last operation, so a transaction that keeps working is never
// ended by it. One timer per transaction, reset when an operation ends;
// nothing runs until it fires. A client that sets its own transaction
// timeout (Bolt tx_timeout, WithClientTransactionTimeout), zero included,
// replaces the server's, as in Neo4j: the client's protocol enforces it.
//
// This owner is bound to the client's explicit transaction handle (its
// TransactionContext) for the handle's whole life, and is the only one that
// decides the handle's end:
//   - Every client BEGIN registers it (beginExplicit), native or composite;
//     the timeout is the configured one of the handle's engine
//     (storage.ServerTransactionTimeoutOf), not of a storage transaction type.
//   - Administration routing replaces the handle's storage snapshot, never the
//     handle, so the registration, timer and failure state carry over.
//   - A refused statement keeps the failed handle; only the client's terminal
//     operation (COMMIT, ROLLBACK, its transport closing) retires it.
//   - A participant is never an end owner. A local participant (the part of
//     a composite transaction on a local constituent, withLocalParticipant)
//     is listed with its owner: the composite transaction's running entry.
//     An end request on the participant's listed id goes to that owner
//     (runningTransactionRegistry.terminate), whose end reaches the
//     participant through the failure rule they share.
//     A remote participant (a Fabric coordinator's transaction on this
//     server, whose BEGIN carries the coordinator mark) gets no idle timer
//     here: its coordinator's transaction decides its end, as a client's own
//     timeout replaces the server's, and rolls it back through the failure
//     rule. Its lifetime here is a lease its coordinator holds: the
//     coordinator renews the transport session holding it while its
//     composite transaction lives, and a session that expires unrenewed
//     means the coordinator is gone, so the session ends the part
//     (TransactionOutlivesSession); over Bolt the lease is the connection.
//     TERMINATE here still ends it at once; the coordinator receives that as
//     the participant's failure at its next statement or COMMIT.
//   - A transport session that holds the handle (pkg/txsession) decides no
//     other end: it retires a session only once TransactionEnded.
//
// Its state is guarded by the running transaction's mu.
type transactionEnd struct {
	tc *TransactionContext

	// active counts the owner's operations in progress (Execute calls on the
	// transaction, nested ones included).
	active int
	// ending is non-nil while an end runs outside the owner; closed when it
	// has finished. The owner's next operation waits for it.
	ending chan struct{}
	// ended records that the end has run.
	ended bool

	idleTimeout  time.Duration
	idleDeadline time.Time // zero while an operation runs
	idleTimer    *time.Timer
	// lifetime is the context the idle timer's end runs under, since a timer
	// has no caller: the BEGIN context without its cancellation (its values
	// and trace survive), taken once at BEGIN; cancelLifetime cancels it when
	// the transaction is retired (stopIdleTimeout). Only a transaction with an
	// idle timer has one.
	lifetime       context.Context
	cancelLifetime context.CancelFunc
}

type clientTransactionTimeoutKey struct{}

type localParticipantKey struct{}

// localParticipant is the value of localParticipantKey: the participant's
// owner, the running entry of the client transaction it is part of (nil for
// a participant of a single auto-commit statement, which ends with it).
type localParticipant struct{ owner *runningTransaction }

// withLocalParticipant marks ctx's BEGIN as that of a local participant's
// sub-transaction, which a composite transaction or statement opens and ends
// (ensureLocalShardTxExecutor), owned by owner: it gets no end owner of its
// own, and end requests on it go to owner.
func withLocalParticipant(ctx context.Context, owner *runningTransaction) context.Context {
	return context.WithValue(ctx, localParticipantKey{}, localParticipant{owner: owner})
}

// WithClientTransactionTimeout marks ctx's BEGIN as that of a transaction
// whose client set its own timeout (Bolt tx_timeout): it replaces the
// server's transaction timeout for the transaction.
func WithClientTransactionTimeout(ctx context.Context) context.Context {
	return context.WithValue(ctx, clientTransactionTimeoutKey{}, true)
}

// serverTransactionTimeoutError is Neo4j's error for a transaction the
// server's transaction timeout ended: Neo.ClientError.Transaction.TransactionTimedOut.
func serverTransactionTimeoutError() error {
	return nornicerrors.MarkStatementTimeout(nornicerrors.ErrServerTransactionTimeout, nornicerrors.ErrServerTransactionTimeout)
}

// beginExplicit lists tc, the explicit transaction handle a client's BEGIN
// (run with ctx) opened, as tc.running, binds its end owner and arms its idle
// timeout at the configured timeout of the handle's engine. tc.running is set
// before the timer is armed, so the end sees the whole handle. A local
// participant is listed with its owner and no end of its own. No timer is
// armed when the server's timeout doesn't govern the transaction: its client
// set its own, or it is a Fabric coordinator's participant
// (tc.coordinatorOwned), whose coordinator decides its end.
func (r *runningTransactionRegistry) beginExplicit(ctx context.Context, database string, tc *TransactionContext) {
	if participant, ok := ctx.Value(localParticipantKey{}).(localParticipant); ok {
		tx := &runningTransaction{}
		tc.running = tx
		r.setOwner(tx, participant.owner)
		r.register(ctx, tx, database, time.Now())
		return
	}
	tx := &runningTransaction{}
	tc.running = tx
	end := &tc.end
	end.tc = tc
	if ctx.Value(clientTransactionTimeoutKey{}) == nil && !tc.coordinatorOwned {
		end.idleTimeout = storage.ServerTransactionTimeoutOf(tc.engine)
	}
	tx.end = end
	started := time.Now()
	if end.idleTimeout > 0 {
		end.lifetime, end.cancelLifetime = context.WithCancel(context.WithoutCancel(ctx))
		end.idleDeadline = started.Add(end.idleTimeout)
		end.idleTimer = time.AfterFunc(end.idleTimeout, tx.idleTimeoutReached)
	}
	r.register(ctx, tx, database, started)
}

// terminationError is the error of a terminated transaction's statements
// and COMMIT: Terminated, or TransactionTimedOut when the server's
// transaction timeout ended it.
func (tx *runningTransaction) terminationError() error {
	if tx.timedOut.Load() {
		return serverTransactionTimeoutError()
	}
	return transactionTerminatedError()
}

// markTerminatedLocked marks tx terminated, by the server's transaction
// timeout when timedOut, else by TERMINATE TRANSACTIONS; the first
// termination stands. For an idle explicit transaction it claims the end,
// which the caller runs (endIdle) once it holds no lock. tx.mu is held.
func (tx *runningTransaction) markTerminatedLocked(timedOut bool) (claimed bool) {
	if !tx.terminated.Load() {
		tx.timedOut.Store(timedOut)
		tx.terminated.Store(true)
	}
	return tx.end != nil && tx.end.claimLocked()
}

// claimLocked claims the end of an idle, still open transaction.
func (end *transactionEnd) claimLocked() bool {
	if end.active > 0 || end.ending != nil || end.ended || !end.tc.active {
		return false
	}
	end.ending = make(chan struct{})
	return true
}

// endIdle ends tx, whose end the caller claimed, through the transaction's
// failure owner, and lets the owner's next operation proceed. ctx is the
// caller's context without its cancellation (TERMINATE's statement, the
// owner's operation), or the transaction's lifetime for the idle timer,
// which has no caller: the end must finish once claimed.
func (tx *runningTransaction) endIdle(ctx context.Context) {
	end := tx.end
	end.tc.fail(ctx, tx.terminationError())
	tx.mu.Lock()
	end.ended = true
	close(end.ending)
	end.ending = nil
	end.idleDeadline = time.Time{}
	tx.mu.Unlock()
}

// idleTimeoutReached is the idle timer's callback: it ends the transaction
// when it is still idle at its deadline. A callback that an operation, or a
// later deadline, overtook does nothing.
func (tx *runningTransaction) idleTimeoutReached() {
	end := tx.end
	tx.mu.Lock()
	claimed := false
	if !end.idleDeadline.IsZero() && !time.Now().Before(end.idleDeadline) {
		end.idleDeadline = time.Time{}
		claimed = tx.markTerminatedLocked(true)
	}
	tx.mu.Unlock()
	if claimed {
		tx.endIdle(end.lifetime)
	}
}

// enterOwner starts an operation of the transaction's owner. It waits for
// an end in progress, and pauses the idle timeout.
func (tx *runningTransaction) enterOwner() {
	end := tx.end
	tx.mu.Lock()
	for end.ending != nil {
		ending := end.ending
		tx.mu.Unlock()
		<-ending
		tx.mu.Lock()
	}
	end.active++
	if end.active == 1 && end.idleTimer != nil {
		end.idleDeadline = time.Time{}
		end.idleTimer.Stop()
	}
	tx.mu.Unlock()
}

// leaveOwner ends an operation of the transaction's owner, which ran with
// ctx. The last one applies a termination that arrived while it ran and no
// statement ended the transaction for, or else restarts the idle timeout of
// a transaction still open.
func (tx *runningTransaction) leaveOwner(ctx context.Context) {
	end := tx.end
	tx.mu.Lock()
	end.active--
	if end.active > 0 {
		tx.mu.Unlock()
		return
	}
	claimed := false
	if tx.terminated.Load() {
		claimed = end.claimLocked()
	} else if end.idleTimer != nil && end.tc.active {
		end.idleDeadline = time.Now().Add(end.idleTimeout)
		end.idleTimer.Reset(end.idleTimeout)
	}
	tx.mu.Unlock()
	if claimed {
		tx.endIdle(context.WithoutCancel(ctx))
	}
}

// TransactionEnded reports whether this executor's explicit transaction can
// do no more work: none is active, or its end owner ended it (TERMINATE
// TRANSACTIONS, the server's transaction timeout). Its client's statements
// see the termination's status until it ends the transaction (one of the
// other transaction type sees ForbiddenDueToTransactionType first). A
// transport session holding the handle retires only once this holds
// (pkg/txsession).
func (e *StorageExecutor) TransactionEnded() bool {
	if e == nil || e.txContext == nil || !e.txContext.active {
		return true
	}
	return e.txContext.running != nil && e.txContext.running.terminated.Load()
}

// TransactionOutlivesSession reports whether this executor's explicit
// transaction stays open when the transport session holding it expires
// unrenewed (pkg/txsession). An open client transaction does: its end owner
// decides its end (NOR-284). A Fabric coordinator's participant doesn't:
// its coordinator renews the session while its composite transaction lives,
// so the session's expiry is the lapse of the coordinator's lease, and the
// session ends the part, as it ends a transaction that has ended.
func (e *StorageExecutor) TransactionOutlivesSession() bool {
	return !e.TransactionEnded() && !e.txContext.coordinatorOwned
}

// stopIdleTimeout disarms the idle timeout of a transaction that ended, and
// cancels its lifetime.
func (tx *runningTransaction) stopIdleTimeout() {
	if tx.end == nil || tx.end.idleTimer == nil {
		return
	}
	tx.mu.Lock()
	tx.end.idleDeadline = time.Time{}
	tx.end.idleTimer.Stop()
	tx.mu.Unlock()
	tx.end.cancelLifetime()
}
