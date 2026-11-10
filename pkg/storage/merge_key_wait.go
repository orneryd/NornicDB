package storage

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// LockWaitOptions bound and report waits for transaction-held UNIQUE, NODE KEY
// and entity write locks. Competing writers wait until the holding transaction
// ends (PrepareMergeNode and EntityWriteLocker).
type LockWaitOptions struct {
	// Timeout is the server's transaction timeout. A writer that has waited
	// this long for a key fails with Neo4j's TransactionTimedOut status
	// (ErrLockWaitTimeout): its transaction has run at least that long. Zero
	// or negative leaves the wait unbounded. The statement's own deadline
	// and cancellation end the wait either way.
	Timeout time.Duration
	// LogAfter is the slow-query threshold. A writer still waiting after this
	// long is logged once, at WARN, with the transaction holding the key; a
	// wait the Timeout ends first is not. Zero or negative logs nothing.
	LogAfter time.Duration
}

// ErrLockWaitTimeout is the cause of a MERGE that waited its engine's
// LockWaitOptions.Timeout for a key another transaction holds.
var ErrLockWaitTimeout = errors.New("lock wait timed out")

// lockWaitTimeoutError reports ErrLockWaitTimeout with Neo4j's status for a
// transaction stopped by the server's transaction timeout. Like that status,
// it is a client error, which drivers don't retry.
type lockWaitTimeoutError struct{ cause error }

func (err *lockWaitTimeoutError) Error() string { return err.cause.Error() }
func (err *lockWaitTimeoutError) Unwrap() error { return err.cause }
func (err *lockWaitTimeoutError) BoltErrorCode() string {
	return "Neo.ClientError.Transaction.TransactionTimedOut"
}

// uniqueKeyLockWait is how an owner waits for a key lock another owner holds
// (waitForUniqueConstraintLock). A nil wait, every commit's, ends only when
// the key is released, when it would close a deadlock, or when ctx ends.
// MERGE admission's (mergeKeyWait) also ends at its Timeout and logs a wait
// that outlasts its LogAfter.
type uniqueKeyLockWait struct {
	LockWaitOptions
	log      *slog.Logger
	database string
}

// uniqueKeyLockClock is one wait for one key across its wakeups: when it
// started blocking, and whether it has been logged.
type uniqueKeyLockClock struct {
	since    time.Time
	reported bool
}

// lockWaitTimers reuses the timers of bounded key waits, so a MERGE that
// blocks allocates none. A stopped timer delivers no stale value (Go 1.23
// timer semantics), so a reused one fires only for its new deadline.
var lockWaitTimers = sync.Pool{New: func() any {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return timer
}}

// mergeKeyWait is how the transaction's MERGE admission in namespace waits
// for a key another transaction holds: as the engine's LockWaitOptions say.
func (tx *BadgerTransaction) mergeKeyWait(namespace string) uniqueKeyLockWait {
	return uniqueKeyLockWait{LockWaitOptions: tx.engine.lockWait, log: tx.engine.log, database: namespace}
}

// ServerTransactionTimeout is the server's transaction timeout
// (EngineOptions.LockWait.Timeout); zero or negative means none. An explicit
// transaction idle this long is ended by its owner (pkg/cypher, NOR-284).
func (b *BadgerEngine) ServerTransactionTimeout() time.Duration {
	if b == nil {
		return 0
	}
	return b.lockWait.Timeout
}

// ServerTransactionTimeoutOf is the server's transaction timeout for an
// explicit transaction opened on engine, whatever its kind: the first engine
// that states it (the physical engine, or a composite, which carries the
// value its database manager configured), looking through wrappers
// (EngineUnwrapper). Zero when none states it.
func ServerTransactionTimeoutOf(engine Engine) time.Duration {
	seen := 0
	for engine != nil && seen < 64 {
		if owner, ok := engine.(interface{ ServerTransactionTimeout() time.Duration }); ok {
			return owner.ServerTransactionTimeout()
		}
		unwrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return 0
		}
		inner := unwrapper.GetInnerEngine()
		if inner == engine {
			return 0
		}
		engine = inner
		seen++
	}
	return 0
}

// awaitUniqueConstraintLock blocks owner, which waits for request's key, until
// released closes (the key's eligibility changed) or the wait ends: ctx ends,
// or the wait has lasted wait's Timeout. A wait still blocked after wait's
// LogAfter (when that comes before the timeout) is logged once. clock carries
// the wait's start across wakeups, so the timeout and the log count from when
// owner first blocked on the key. One timer serves both: it fires for the
// log while that is due, then for the timeout.
func (sm *SchemaManager) awaitUniqueConstraintLock(ctx context.Context, owner string, request uniqueConstraintLockRequest, released <-chan struct{}, wait *uniqueKeyLockWait, clock *uniqueKeyLockClock) error {
	if wait == nil || (wait.Timeout <= 0 && wait.LogAfter <= 0) {
		select {
		case <-released:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if clock.since.IsZero() {
		clock.since = time.Now()
	}
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
			lockWaitTimers.Put(timer)
		}
	}()
	for {
		report := wait.LogAfter > 0 && !clock.reported && (wait.Timeout <= 0 || wait.LogAfter < wait.Timeout)
		var fired <-chan time.Time
		if report || wait.Timeout > 0 {
			due := wait.Timeout
			if report {
				due = wait.LogAfter
			}
			due -= time.Since(clock.since)
			if timer == nil {
				timer = lockWaitTimers.Get().(*time.Timer)
			}
			timer.Reset(due)
			fired = timer.C
		}
		select {
		case <-released:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-fired:
			label, property := request.key.names()
			if !report {
				message := localization.StorageTransactionLockWaitTimedOut(label, property, wait.Timeout.String())
				return &lockWaitTimeoutError{cause: localizedError(message, ErrLockWaitTimeout)}
			}
			clock.reported = true
			sm.uniqueConstraintCommitLocksMu.Lock()
			holder := request.lock.owner
			sm.uniqueConstraintCommitLocksMu.Unlock()
			event := localization.StorageLockWaitProlongedEvent(wait.database, owner, holder, label, property, time.Since(clock.since), max(wait.Timeout, 0))
			logStorageEvent(ctx, wait.log, slog.LevelWarn, event)
		}
	}
}
