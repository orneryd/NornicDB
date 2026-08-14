package bolt

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type observerBlockingRollbackExecutor struct {
	rollbackStarted chan struct{}
	once            sync.Once
}

func (e *observerBlockingRollbackExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (e *observerBlockingRollbackExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}

func (e *observerBlockingRollbackExecutor) CommitTransaction(context.Context) error { return nil }

func (e *observerBlockingRollbackExecutor) RollbackTransaction(context.Context) error {
	e.once.Do(func() { close(e.rollbackStarted) })
	return nil
}

type partialBeginLifecycleExecutor struct {
	beginErr      error
	beginPanic    any
	rollbackErr   error
	active        bool
	rollbackCalls int
}

func TestHandleCommitPanicPreservesCleanupFailure(t *testing.T) {
	executor := &commitPanicLifecycleExecutor{
		panicPending: true,
		rollbackErr:  errors.New("commit panic rollback failed"),
	}
	session := newTestSession(&mockConn{}, executor)
	session.inTransaction = true
	require.NoError(t, session.txLifecycle.begin(
		context.Background(), 0, "neo4j", executor, nil, nil))

	require.PanicsWithValue(t, "pending-write reporter panic", func() { _ = session.handleCommit(nil) })
	require.True(t, session.transactionCleanupFailed,
		"rollback failure during pre-commit panic must poison connection teardown")
	require.True(t, session.inTransaction,
		"unknown backend ownership must not be cleared after rollback failure")
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func (e *partialBeginLifecycleExecutor) Execute(context.Context, string, map[string]any) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (e *partialBeginLifecycleExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.active = true
	if e.beginPanic != nil {
		panic(e.beginPanic)
	}
	return e.beginErr
}

func (e *partialBeginLifecycleExecutor) CommitTransaction(context.Context) error { return nil }

func (e *partialBeginLifecycleExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls++
	if e.rollbackErr == nil {
		e.active = false
	}
	return e.rollbackErr
}

func TestTransactionLifecycleBeginFailureCleansPartialAllocation(t *testing.T) {
	tests := []struct {
		name       string
		beginErr   error
		beginPanic any
	}{
		{name: "error", beginErr: errors.New("partial begin error")},
		{name: "panic", beginPanic: "partial begin panic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &partialBeginLifecycleExecutor{beginErr: tt.beginErr, beginPanic: tt.beginPanic}
			lifecycle := &transactionLifecycle{}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				err := lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
				if tt.beginErr != nil {
					require.ErrorIs(t, err, tt.beginErr)
				}
			}()
			require.Equal(t, tt.beginPanic, recovered)
			require.Equal(t, 1, executor.rollbackCalls)
			require.False(t, executor.active, "failed BEGIN retained backend transaction ownership")
			require.Equal(t, transactionStateIdle, lifecycle.state)
		})
	}
}

func TestTransactionLifecycleBeginCleanupFailurePoisonsLifecycle(t *testing.T) {
	beginErr := errors.New("partial begin error")
	rollbackErr := errors.New("partial begin rollback error")
	executor := &partialBeginLifecycleExecutor{beginErr: beginErr, rollbackErr: rollbackErr}
	lifecycle := &transactionLifecycle{}

	err := lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
	require.ErrorIs(t, err, beginErr)
	require.ErrorContains(t, err, rollbackErr.Error())
	require.Equal(t, 1, executor.rollbackCalls)
	require.True(t, executor.active)
	require.Error(t, lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil),
		"unknown backend ownership must prevent lifecycle reuse")
	require.ErrorIs(t, lifecycle.rollback(transactionTerminalReset), rollbackErr,
		"RESET must surface poisoned BEGIN cleanup instead of restoring reuse")
}

func TestTransactionLifecycleTimeoutCleanupFailureIsStickyForAllWaiters(t *testing.T) {
	cleanupErr := errors.New("timeout cleanup failed")
	lifecycle := &transactionLifecycle{state: transactionStateTimedOut, cleanupErr: cleanupErr}
	const waiterCount = 4
	start := make(chan struct{})
	results := make(chan error, waiterCount)
	for range waiterCount {
		go func() {
			<-start
			results <- lifecycle.rollback(transactionTerminalReset)
		}()
	}
	close(start)
	for range waiterCount {
		require.ErrorIs(t, <-results, cleanupErr)
	}
}

func TestTransactionLifecycleIdleExpiryCleanupDoesNotDependOnObserver(t *testing.T) {
	executor := &observerBlockingRollbackExecutor{rollbackStarted: make(chan struct{})}
	observerEntered := make(chan transactionTerminalReason, 1)
	releaseObserver := make(chan struct{})
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "neo4j", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, _ error) {
			observerEntered <- reason
			<-releaseObserver
		}))
	expireReturned := make(chan struct{})
	go func() {
		lifecycle.expire()
		close(expireReturned)
	}()

	reason := <-observerEntered
	select {
	case <-executor.rollbackStarted:
	case <-time.After(50 * time.Millisecond):
		close(releaseObserver)
		<-expireReturned
		t.Fatalf("observer %q blocked durable timeout rollback", reason)
	}
	waiter := make(chan error, 1)
	go func() { waiter <- lifecycle.rollback(transactionTerminalReset) }()
	select {
	case err := <-waiter:
		require.NoError(t, err)
	case <-time.After(50 * time.Millisecond):
		close(releaseObserver)
		<-expireReturned
		t.Fatal("observer blocked timeout cleanup waiter")
	}
	close(releaseObserver)
	<-expireReturned
	require.Equal(t, transactionTerminalTimeout, reason,
		"idle cleanup emits only its honest completion event")
}
