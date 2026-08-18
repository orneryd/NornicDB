package bolt

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	transactionCleanupRequestTimeout = 5 * time.Second
	transactionTimedOutCode          = "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration"
	transactionTimedOutMsg           = "The transaction has timed out according to the timeout configured by the client."
)

var (
	errTransactionNotActive = errors.New("transaction is not active")
	errTransactionTimedOut  = errors.New("transaction timed out")
)

type transactionTerminalReason string

const (
	transactionTerminalCommit                  transactionTerminalReason = "commit"
	transactionTerminalRollback                transactionTerminalReason = "rollback"
	transactionTerminalReset                   transactionTerminalReason = "reset"
	transactionTerminalGoodbye                 transactionTerminalReason = "goodbye"
	transactionTerminalDisconnect              transactionTerminalReason = "disconnect"
	transactionTerminalTimeoutCleanupRequested transactionTerminalReason = "timeout_cleanup_requested"
	transactionTerminalTimeout                 transactionTerminalReason = "timeout"
)

type transactionLifecycleState uint8

const (
	transactionStateIdle transactionLifecycleState = iota
	transactionStateBeginning
	transactionStateActive
	transactionStateCommitting
	transactionStateRollingBack
	transactionStateTimingOut
	transactionStateTimedOut
	transactionStateTerminal
	transactionStateDefunct
)

// transactionLifecycle arbitrates the terminal owner of one Bolt explicit
// transaction. Its mutex is session-local: transactions on other connections
// never contend on it.
type transactionLifecycle struct {
	mu sync.Mutex

	state                   transactionLifecycleState
	ctx                     context.Context
	cancel                  context.CancelFunc
	cleanupBase             context.Context
	timer                   *time.Timer
	executor                TransactionalExecutor
	startedAt               time.Time
	database                string
	observe                 func(transactionTerminalReason, string, time.Duration, error)
	operationActive         bool
	cleanupPending          bool
	cleanupDone             chan struct{}
	cleanupErr              error
	afterFunc               func(time.Duration, func()) *time.Timer
	generation              uint64
	onTimeoutCleanupFailure func(error)
}

// begin claims lifecycle ownership before backend allocation and publishes an
// accepted transaction as Active or already TimingOut without an expired gap.
func (l *transactionLifecycle) begin(
	parent context.Context,
	timeout time.Duration,
	database string,
	executor TransactionalExecutor,
	metadata map[string]any,
	observe func(transactionTerminalReason, string, time.Duration, error),
) error {
	if parent == nil {
		parent = context.Background()
	}

	l.mu.Lock()
	if l.state != transactionStateIdle && l.state != transactionStateTerminal {
		l.mu.Unlock()
		return errors.New("an explicit transaction is already active")
	}
	l.state = transactionStateBeginning
	l.generation++
	generation := l.generation
	l.mu.Unlock()

	startedAt := time.Now()
	txCtx, cancel := context.WithCancel(parent)
	var beginErr error
	var beginPanic any
	if executor != nil {
		beginPanic, beginErr = invokeTransactionBegin(txCtx, executor, metadata)
	}
	if beginErr == nil && beginPanic == nil {
		beginErr = txCtx.Err()
	}
	if beginErr != nil || beginPanic != nil {
		cancel()
		cleanupErr := rollbackWithCleanupDeadline(parent, executor)
		l.mu.Lock()
		if cleanupErr != nil {
			l.state = transactionStateDefunct
			l.cleanupErr = cleanupErr
		} else {
			l.state = transactionStateIdle
		}
		l.mu.Unlock()
		observeTransactionLifecycle(
			observe,
			transactionTerminalRollback,
			database,
			time.Since(startedAt),
			cleanupErr,
		)
		if beginPanic != nil {
			panic(beginPanic)
		}
		if cleanupErr != nil {
			return fmt.Errorf("BEGIN failed: %w; rollback cleanup failed: %v", beginErr, cleanupErr)
		}
		return beginErr
	}

	remaining := timeout - time.Since(startedAt)
	expired := timeout > 0 && remaining <= 0

	l.mu.Lock()
	if expired {
		l.state = transactionStateTimingOut
		l.cleanupDone = make(chan struct{})
		l.cleanupErr = nil
	} else {
		l.state = transactionStateActive
	}
	l.ctx = txCtx
	l.cancel = cancel
	// Preserve the parent only as the source of cleanup values. The rollback
	// request strips connection cancellation when (and only when) cleanup is
	// needed, avoiding an allocation on the successful COMMIT path.
	l.cleanupBase = parent
	l.executor = executor
	l.startedAt = startedAt
	l.database = database
	l.observe = observe
	if timeout > 0 && !expired {
		afterFunc := time.AfterFunc
		if l.afterFunc != nil {
			afterFunc = l.afterFunc
		}
		l.timer = afterFunc(remaining, func() { l.expireGeneration(generation) })
	} else {
		l.timer = nil
	}
	l.mu.Unlock()
	if expired {
		cancel()
		err := rollbackWithCleanupDeadline(parent, executor)
		l.completeTimeoutCleanup(startedAt, database, observe, err, false)
		if err != nil {
			_ = l.finishTimedOut(transactionTerminalTimeout)
			return fmt.Errorf("expired BEGIN cleanup failed: %w", err)
		}
	}
	return nil
}

// claimOperation atomically admits one executor operation to the active
// transaction. Once admitted, timeout cleanup cannot overlap the executor: the
// operation defer becomes the cleanup owner if the deadline fires before it
// returns.
func (l *transactionLifecycle) claimOperation() (context.Context, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case transactionStateTimingOut, transactionStateTimedOut:
		return nil, errTransactionTimedOut
	case transactionStateActive:
		if l.ctx == nil || l.operationActive {
			return nil, errTransactionNotActive
		}
		l.operationActive = true
		return l.ctx, nil
	default:
		return nil, errTransactionNotActive
	}
}

// claimRun preserves the RUN-specific call site while sharing the generic
// executor-operation ownership used by deferred Flush.
func (l *transactionLifecycle) claimRun() (context.Context, error) {
	return l.claimOperation()
}

// finishOperation releases executor operation ownership. If the transaction
// expired while the executor was active, this defer performs the single
// synchronous rollback before allowing the operation (or its panic) to leave
// the session.
func (l *transactionLifecycle) finishOperation() error {
	l.mu.Lock()
	if !l.operationActive {
		l.mu.Unlock()
		return nil
	}
	l.operationActive = false
	if l.state != transactionStateTimingOut || !l.cleanupPending {
		l.mu.Unlock()
		return nil
	}
	l.cleanupPending = false
	executor := l.executor
	cleanupBase := l.cleanupBase
	startedAt := l.startedAt
	database := l.database
	observe := l.observe
	l.mu.Unlock()

	err := rollbackWithCleanupDeadline(cleanupBase, executor)
	l.completeTimeoutCleanup(startedAt, database, observe, err, true)
	if err != nil {
		return err
	}
	return errTransactionTimedOut
}

// finishRun preserves the RUN-specific call site.
func (l *transactionLifecycle) finishRun() error {
	return l.finishOperation()
}

// isTimedOut reports whether timeout owns cleanup or has completed it.
func (l *transactionLifecycle) isTimedOut() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state == transactionStateTimingOut || l.state == transactionStateTimedOut
}

// isDefunct reports that cleanup failed and executor reuse is unsafe.
func (l *transactionLifecycle) isDefunct() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state == transactionStateDefunct
}

// claimCommit atomically makes COMMIT terminal owner only while Active and
// stops the deadline before storage commit begins.
func (l *transactionLifecycle) claimCommit() (TransactionalExecutor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case transactionStateTimingOut, transactionStateTimedOut:
		return nil, errTransactionTimedOut
	case transactionStateActive:
		l.state = transactionStateCommitting
		l.stopTimerLocked()
		return l.executor, nil
	default:
		return nil, errTransactionNotActive
	}
}

// finishCommit publishes the known commit outcome and releases lifecycle-owned
// context only for the matching commit claim.
func (l *transactionLifecycle) finishCommit(commitErr error) {
	l.mu.Lock()
	if l.state != transactionStateCommitting {
		l.mu.Unlock()
		return
	}
	l.state = transactionStateTerminal
	cancel := l.cancel
	startedAt := l.startedAt
	database := l.database
	observe := l.observe
	l.clearOwnedLocked()
	l.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	observeTransactionLifecycle(observe, transactionTerminalCommit, database, time.Since(startedAt), commitErr)
}

// abortCommitPanic reclaims transaction ownership when code invoked after a
// successful commit claim panics. It preserves the original panic at the
// Session boundary while ensuring storage cleanup and lifecycle terminalization
// still happen exactly once.
func (l *transactionLifecycle) abortCommitPanic() error {
	l.mu.Lock()
	if l.state != transactionStateCommitting {
		l.mu.Unlock()
		return nil
	}
	l.state = transactionStateRollingBack
	executor := l.executor
	cancel := l.cancel
	cleanupBase := l.cleanupBase
	startedAt := l.startedAt
	database := l.database
	observe := l.observe
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	err := rollbackWithCleanupDeadline(cleanupBase, executor)

	l.mu.Lock()
	if l.state == transactionStateRollingBack {
		l.state = transactionStateTerminal
		l.clearOwnedLocked()
	}
	l.mu.Unlock()
	observeTransactionLifecycle(observe, transactionTerminalRollback, database, time.Since(startedAt), err)
	return err
}

// rollback either claims synchronous cleanup or joins timeout-owned cleanup;
// cleanup failure remains sticky for every later waiter.
func (l *transactionLifecycle) rollback(reason transactionTerminalReason) error {
	l.mu.Lock()
	switch l.state {
	case transactionStateDefunct:
		err := l.cleanupErr
		l.mu.Unlock()
		if err == nil {
			return errors.New("transaction cleanup failed; session is defunct")
		}
		return err
	case transactionStateTimingOut:
		done := l.cleanupDone
		l.mu.Unlock()
		if done != nil {
			<-done
		}
		return l.finishTimedOut(reason)
	case transactionStateTimedOut:
		l.mu.Unlock()
		return l.finishTimedOut(reason)
	case transactionStateActive:
		l.state = transactionStateRollingBack
		l.stopTimerLocked()
	default:
		l.mu.Unlock()
		return nil
	}
	executor := l.executor
	cancel := l.cancel
	cleanupBase := l.cleanupBase
	startedAt := l.startedAt
	database := l.database
	observe := l.observe
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	err := rollbackWithCleanupDeadline(cleanupBase, executor)

	l.mu.Lock()
	if l.state == transactionStateRollingBack {
		l.state = transactionStateTerminal
		l.clearOwnedLocked()
	}
	l.mu.Unlock()
	observeTransactionLifecycle(observe, reason, database, time.Since(startedAt), err)
	return err
}

// expire atomically transfers terminal ownership to timeout; an admitted
// executor operation inherits the single pending cleanup attempt.
func (l *transactionLifecycle) expire() {
	l.mu.Lock()
	generation := l.generation
	l.mu.Unlock()
	l.expireGeneration(generation)
}

func (l *transactionLifecycle) expireGeneration(generation uint64) {
	l.mu.Lock()
	if l.state != transactionStateActive || l.generation != generation {
		l.mu.Unlock()
		return
	}
	l.state = transactionStateTimingOut
	executor := l.executor
	cancel := l.cancel
	cleanupBase := l.cleanupBase
	startedAt := l.startedAt
	database := l.database
	observe := l.observe
	done := make(chan struct{})
	l.cleanupDone = done
	l.cleanupErr = nil
	if l.operationActive {
		l.cleanupPending = true
	}
	l.timer = nil
	operationActive := l.operationActive
	l.mu.Unlock()

	// Cancellation happens before rollback. If an executor operation still owns
	// this transaction, its defer observes the pending request and performs the
	// single durable rollback before releasing storage.
	if cancel != nil {
		cancel()
	}
	if operationActive {
		observeTransactionLifecycle(
			observe, transactionTerminalTimeoutCleanupRequested, database, time.Since(startedAt), nil)
		return
	}
	err := rollbackWithCleanupDeadline(cleanupBase, executor)
	l.completeTimeoutCleanup(startedAt, database, observe, err, true)
}

// completeTimeoutCleanup publishes one timeout cleanup result and releases all
// waiters before invoking untrusted observability callbacks.
func (l *transactionLifecycle) completeTimeoutCleanup(
	startedAt time.Time,
	database string,
	observe func(transactionTerminalReason, string, time.Duration, error),
	cleanupErr error,
	notifyFailure bool,
) {
	l.mu.Lock()
	if l.state != transactionStateTimingOut {
		l.mu.Unlock()
		return
	}
	l.state = transactionStateTimedOut
	l.cleanupErr = cleanupErr
	l.executor = nil
	l.ctx = nil
	l.cancel = nil
	l.cleanupBase = nil
	l.observe = nil
	done := l.cleanupDone
	onFailure := l.onTimeoutCleanupFailure
	if done != nil {
		close(done)
	}
	l.mu.Unlock()

	if cleanupErr != nil && notifyFailure {
		notifyTransactionCleanupFailure(onFailure, cleanupErr)
	}
	observeTransactionLifecycle(observe, transactionTerminalTimeout, database, time.Since(startedAt), cleanupErr)
}

func (l *transactionLifecycle) setTimeoutCleanupFailureHandler(handler func(error)) {
	l.mu.Lock()
	l.onTimeoutCleanupFailure = handler
	l.mu.Unlock()
}

// finishTimedOut terminalizes successful timeout cleanup or makes its failure
// sticky by moving the lifecycle to Defunct.
func (l *transactionLifecycle) finishTimedOut(reason transactionTerminalReason) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == transactionStateDefunct {
		return l.cleanupErr
	}
	if l.state != transactionStateTimedOut {
		return nil
	}
	err := l.cleanupErr
	if err != nil {
		l.state = transactionStateDefunct
		return err
	}
	l.state = transactionStateTerminal
	l.clearOwnedLocked()
	return nil
}

// stopTimerLocked disarms the deadline while lifecycle ownership is held.
func (l *transactionLifecycle) stopTimerLocked() {
	if l.timer != nil {
		l.timer.Stop()
		l.timer = nil
	}
}

// clearOwnedLocked releases terminal resources without clearing the test-only
// timer factory used to prove deadline arbitration.
func (l *transactionLifecycle) clearOwnedLocked() {
	l.stopTimerLocked()
	l.ctx = nil
	l.cancel = nil
	l.cleanupBase = nil
	l.executor = nil
	l.startedAt = time.Time{}
	l.database = ""
	l.observe = nil
	l.operationActive = false
	l.cleanupPending = false
	l.cleanupDone = nil
	l.cleanupErr = nil
}
