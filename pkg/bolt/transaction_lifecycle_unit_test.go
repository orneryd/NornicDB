package bolt

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type lifecycleContextExecutor struct {
	mockExecutor
	beginErr     error
	rollbackErr  error
	rollbackCtx  context.Context
	ctxErrAtCall error
}

type blockingLifecycleExecutor struct {
	mockExecutor
	entered chan struct{}
	release <-chan struct{}
	calls   int
}

type commitPanicLifecycleExecutor struct {
	mockExecutor
	panicPending  bool
	panicCommit   bool
	rollbackErr   error
	rollbackCalls atomic.Int64
}

func (e *commitPanicLifecycleExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}

func (e *commitPanicLifecycleExecutor) CommitTransaction(context.Context) error {
	if e.panicCommit {
		panic("commit panic")
	}
	return nil
}

func (e *commitPanicLifecycleExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls.Add(1)
	return e.rollbackErr
}

func (e *commitPanicLifecycleExecutor) HasPendingTransactionWrites() bool {
	if e.panicPending {
		panic("pending-write reporter panic")
	}
	return false
}

type concurrentBeginLifecycleExecutor struct {
	beginEntered chan struct{}
	beginRelease <-chan struct{}
	beginCalls   atomic.Int64
	beginErr     error
}

func (e *concurrentBeginLifecycleExecutor) Execute(context.Context, string, map[string]any) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (e *concurrentBeginLifecycleExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.beginCalls.Add(1)
	select {
	case e.beginEntered <- struct{}{}:
	default:
	}
	<-e.beginRelease
	return e.beginErr
}

func (e *concurrentBeginLifecycleExecutor) CommitTransaction(context.Context) error { return nil }
func (e *concurrentBeginLifecycleExecutor) RollbackTransaction(context.Context) error {
	return nil
}

func (e *blockingLifecycleExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}

func (e *blockingLifecycleExecutor) CommitTransaction(context.Context) error {
	return nil
}

func (e *blockingLifecycleExecutor) RollbackTransaction(context.Context) error {
	e.calls++
	close(e.entered)
	<-e.release
	return nil
}

func (e *lifecycleContextExecutor) BeginTransaction(context.Context, map[string]any) error {
	return e.beginErr
}

func (e *lifecycleContextExecutor) CommitTransaction(context.Context) error {
	return nil
}

func (e *lifecycleContextExecutor) RollbackTransaction(ctx context.Context) error {
	e.rollbackCtx = ctx
	e.ctxErrAtCall = ctx.Err()
	return e.rollbackErr
}

func TestValidateTransactionTimeoutBoundaries(t *testing.T) {
	maxMilliseconds := int64((1<<63 - 1) / int64(time.Millisecond))
	tests := []struct {
		name     string
		metadata map[string]any
		want     time.Duration
		wantErr  string
	}{
		{name: "absent", metadata: nil},
		{name: "null", metadata: map[string]any{"tx_timeout": nil}},
		{name: "zero", metadata: map[string]any{"tx_timeout": int64(0)}},
		{name: "positive", metadata: map[string]any{"tx_timeout": int64(17)}, want: 17 * time.Millisecond},
		{name: "wrong type", metadata: map[string]any{"tx_timeout": 17}, wantErr: "Expected long"},
		{name: "negative", metadata: map[string]any{"tx_timeout": int64(-1)}},
		{name: "overflow", metadata: map[string]any{"tx_timeout": maxMilliseconds + 1}, want: time.Duration(1<<63 - 1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := validateTransactionTimeout(tt.metadata)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTransactionLifecycleBeginFailureLeavesIdle(t *testing.T) {
	wantErr := errors.New("begin failed")
	executor := &lifecycleContextExecutor{beginErr: wantErr}
	lifecycle := &transactionLifecycle{}

	err := lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
	require.ErrorIs(t, err, wantErr)
	_, err = lifecycle.claimCommit()
	require.ErrorIs(t, err, errTransactionNotActive)
}

func TestHandleBeginRejectsDuplicateWithoutReplacingActiveExecutor(t *testing.T) {
	original := &mockTransactionalExecutor{}
	session := newTestSession(&mockConn{}, original)
	session.baseExec = &mockExecutor{}
	require.NoError(t, session.handleBegin(nil))
	require.Same(t, original, session.executor)

	require.NoError(t, session.handleBegin(nil))
	require.True(t, session.inTransaction)
	require.Same(t, original, session.executor,
		"rejected duplicate BEGIN must not detach the active storage transaction")
	require.NoError(t, session.rollbackExplicitTransaction(transactionTerminalRollback))
}

func TestTransactionLifecycleRejectsOverlappingBegin(t *testing.T) {
	executor := &lifecycleContextExecutor{}
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil))

	err := lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
	require.ErrorContains(t, err, "already active")
	require.NoError(t, lifecycle.rollback(transactionTerminalRollback))
}

func TestTransactionLifecycleConcurrentBeginAllocatesOnce(t *testing.T) {
	release := make(chan struct{})
	executor := &concurrentBeginLifecycleExecutor{
		beginEntered: make(chan struct{}, 2),
		beginRelease: release,
	}
	lifecycle := &transactionLifecycle{}
	first := make(chan error, 1)
	go func() {
		first <- lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
	}()
	<-executor.beginEntered

	second := make(chan error, 1)
	go func() {
		second <- lifecycle.begin(context.Background(), 0, "neo4j", executor, nil, nil)
	}()
	select {
	case err := <-second:
		require.ErrorContains(t, err, "already active")
	case <-time.After(100 * time.Millisecond):
		close(release)
		<-first
		<-second
		t.Fatal("second BEGIN reached backend allocation while the first BEGIN was in flight")
	}
	close(release)
	require.NoError(t, <-first)
	require.Equal(t, int64(1), executor.beginCalls.Load())
	require.NoError(t, lifecycle.rollback(transactionTerminalRollback))
}

func TestHandleCommitPanicRollsBackBeforePreservingPanic(t *testing.T) {
	for _, test := range []struct {
		name              string
		panicValue        string
		panicPending      bool
		panicCommit       bool
		wantRollbackCalls int64
		wantInTransaction bool
		wantPoisoned      bool
	}{
		{name: "pending write reporter", panicValue: "pending-write reporter panic", panicPending: true,
			wantRollbackCalls: 1},
		{name: "commit executor", panicValue: "commit panic", panicCommit: true,
			wantInTransaction: true, wantPoisoned: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &commitPanicLifecycleExecutor{
				panicPending: test.panicPending,
				panicCommit:  test.panicCommit,
			}
			session := newTestSession(&mockConn{}, executor)
			session.inTransaction = true
			require.NoError(t, session.txLifecycle.begin(
				context.Background(), 0, "neo4j", executor, nil, nil))

			require.PanicsWithValue(t, test.panicValue, func() { _ = session.handleCommit(nil) })
			require.Equal(t, test.wantInTransaction, session.inTransaction)
			require.Equal(t, test.wantPoisoned, session.transactionCleanupFailed)
			require.Equal(t, test.wantRollbackCalls, executor.rollbackCalls.Load())
			require.NoError(t, session.txLifecycle.rollback(transactionTerminalDisconnect))
			require.Equal(t, test.wantRollbackCalls, executor.rollbackCalls.Load(),
				"panic terminal ownership must be exactly once")
		})
	}
}

func TestTransactionLifecycleCommitWithoutExecutor(t *testing.T) {
	lifecycle := &transactionLifecycle{}
	observed := 0
	require.NoError(t, lifecycle.begin(nil, 0, "neo4j", nil, nil, //nolint:staticcheck // Exercises the nil-parent fallback.
		func(reason transactionTerminalReason, database string, _ time.Duration, err error) {
			observed++
			require.Equal(t, transactionTerminalCommit, reason)
			require.Equal(t, "neo4j", database)
			require.NoError(t, err)
		}))

	claimed, err := lifecycle.claimRun()
	require.NoError(t, err)
	require.NotNil(t, claimed)
	_ = lifecycle.finishRun()
	executor, err := lifecycle.claimCommit()
	require.NoError(t, err)
	require.Nil(t, executor)
	lifecycle.finishCommit(nil)
	lifecycle.finishCommit(errors.New("ignored after terminal"))
	require.Equal(t, 1, observed)
	_, err = lifecycle.claimRun()
	require.ErrorIs(t, err, errTransactionNotActive)
}

func TestTransactionLifecycleTimeoutStateRejectsCommitAndRollback(t *testing.T) {
	lifecycle := &transactionLifecycle{state: transactionStateTimedOut}
	executor, err := lifecycle.claimCommit()
	require.Nil(t, executor)
	require.ErrorIs(t, err, errTransactionTimedOut)
	require.NoError(t, lifecycle.rollback(transactionTerminalRollback))

	idle := &transactionLifecycle{}
	require.NoError(t, idle.rollback(transactionTerminalReset))
	idle.expire()
}

func TestTransactionLifecycleRollbackUsesUncancelledCleanupContext(t *testing.T) {
	wantErr := errors.New("rollback failed")
	executor := &lifecycleContextExecutor{rollbackErr: wantErr}
	parent, cancel := context.WithCancel(context.Background())
	lifecycle := &transactionLifecycle{}
	var observedErr error
	require.NoError(t, lifecycle.begin(parent, 0, "neo4j", executor, nil,
		func(_ transactionTerminalReason, _ string, _ time.Duration, err error) {
			observedErr = err
		}))
	cancel()

	err := lifecycle.rollback(transactionTerminalDisconnect)
	require.ErrorIs(t, err, wantErr)
	require.ErrorIs(t, observedErr, wantErr)
	require.NotNil(t, executor.rollbackCtx)
	require.NoError(t, executor.ctxErrAtCall, "cleanup must not inherit connection cancellation")
	require.NoError(t, lifecycle.rollback(transactionTerminalDisconnect), "cleanup ownership is exactly once")
}

func TestRollbackWithCleanupDeadlineHandlesMissingInputs(t *testing.T) {
	require.NoError(t, rollbackWithCleanupDeadline(nil, nil)) //nolint:staticcheck // Exercises the nil-base fallback.
	executor := &lifecycleContextExecutor{}
	require.NoError(t, rollbackWithCleanupDeadline(nil, executor)) //nolint:staticcheck // Exercises the nil-base fallback.
	require.NotNil(t, executor.rollbackCtx)
	require.NoError(t, executor.ctxErrAtCall)
}

func TestRollbackWithCleanupDeadlineDoesNotAbandonUncooperativeExecutor(t *testing.T) {
	release := make(chan struct{})
	executor := &blockingLifecycleExecutor{entered: make(chan struct{}), release: release}
	done := make(chan error, 1)
	const cleanupDeadline = 25 * time.Millisecond
	go func() {
		done <- rollbackWithCleanupTimeout(context.Background(), executor, cleanupDeadline)
	}()
	select {
	case <-executor.entered:
	case <-time.After(time.Second):
		t.Fatal("rollback did not reach the admitted storage implementation")
	}

	returnedWithinBound := false
	select {
	case <-done:
		returnedWithinBound = true
	case <-time.After(cleanupDeadline + 100*time.Millisecond):
	}
	close(release)
	if !returnedWithinBound {
		require.NoError(t, <-done)
	}
	require.False(t, returnedWithinBound,
		"an admitted non-cooperative rollback must retain synchronous ownership instead of abandoning a goroutine")
}

func TestTransactionLifecycleDisjointRollbacksDoNotSerialize(t *testing.T) {
	release := make(chan struct{})
	executors := []*blockingLifecycleExecutor{
		{entered: make(chan struct{}), release: release},
		{entered: make(chan struct{}), release: release},
	}
	lifecycles := []*transactionLifecycle{{}, {}}
	for index := range lifecycles {
		require.NoError(t, lifecycles[index].begin(
			context.Background(), 0, "neo4j", executors[index], nil, nil))
	}

	done := make(chan error, len(lifecycles))
	for index := range lifecycles {
		go func(index int) {
			done <- lifecycles[index].rollback(transactionTerminalRollback)
		}(index)
	}
	for _, executor := range executors {
		select {
		case <-executor.entered:
		case <-time.After(time.Second):
			t.Fatal("disjoint rollback was serialized behind another session")
		}
	}
	close(release)
	for range lifecycles {
		require.NoError(t, <-done)
	}
}

func TestTransactionLifecycleIdleTimeoutReportsCompletionAfterCleanup(t *testing.T) {
	release := make(chan struct{})
	executor := &blockingLifecycleExecutor{entered: make(chan struct{}), release: release}
	events := make(chan transactionTerminalReason, 2)
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 10*time.Millisecond, "neo4j", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, _ error) {
			events <- reason
		}))
	select {
	case <-executor.entered:
	case <-time.After(time.Second):
		t.Fatal("timeout cleanup did not reach the admitted executor")
	}

	select {
	case event := <-events:
		t.Fatalf("idle timeout reported %s before cleanup completed", event)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	require.Equal(t, transactionTerminalTimeout, <-events)
	select {
	case duplicate := <-events:
		t.Fatalf("unexpected duplicate timeout lifecycle event: %s", duplicate)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestTransactionLifecycleCompletionObserverPanicCannotBlockWaiters(t *testing.T) {
	release := make(chan struct{})
	executor := &blockingLifecycleExecutor{entered: make(chan struct{}), release: release}
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "neo4j", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, _ error) {
			if reason == transactionTerminalTimeout {
				panic("completion observer panic")
			}
		}))

	expireResult := make(chan any, 1)
	go func() {
		var recovered any
		defer func() { expireResult <- recovered }()
		defer func() { recovered = recover() }()
		lifecycle.expire()
	}()
	select {
	case <-executor.entered:
	case <-time.After(time.Second):
		t.Fatal("timeout cleanup did not reach the executor")
	}
	waiter := make(chan error, 1)
	waiterStarted := make(chan struct{})
	go func() {
		close(waiterStarted)
		waiter <- lifecycle.rollback(transactionTerminalReset)
	}()
	<-waiterStarted
	select {
	case err := <-waiter:
		t.Fatalf("RESET waiter returned before cleanup: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	lifecycle.mu.Lock()
	cleanupDone := lifecycle.cleanupDone
	lifecycle.mu.Unlock()

	close(release)
	recovered := <-expireResult
	if recovered != nil {
		// Keep the RED test from leaking its intentionally blocked waiter on
		// production bytes that have not yet made observer callbacks safe.
		close(cleanupDone)
	}
	require.NoError(t, <-waiter)
	require.Nil(t, recovered, "observer panic escaped lifecycle cleanup")
	require.Equal(t, 1, executor.calls)
}

func TestObserveTransactionTerminalStructuredDiagnostics(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))
	session := &Session{server: &Server{log: logger}}

	session.observeTransactionTerminal(transactionTerminalCommit, "neo4j", time.Millisecond, nil)
	session.observeTransactionTerminal(transactionTerminalCommit, "neo4j", time.Millisecond, errors.New("commit boom"))
	session.observeTransactionTerminal(transactionTerminalTimeoutCleanupRequested, "neo4j", 2*time.Millisecond, nil)
	session.observeTransactionTerminal(transactionTerminalTimeout, "neo4j", 2*time.Millisecond, nil)
	session.observeTransactionTerminal(transactionTerminalRollback, "neo4j", 3*time.Millisecond, errors.New("boom"))
	text := output.String()
	require.Contains(t, text, "explicit transaction terminated")
	require.Equal(t, 1, strings.Count(text, "explicit transaction commit failed"))
	require.Contains(t, text, "commit_error=\"commit boom\"")
	require.Equal(t, 1, strings.Count(text, "explicit transaction timeout cleanup requested"))
	require.Equal(t, 1, strings.Count(text, "explicit transaction timeout cleanup completed"))
	require.Contains(t, text, "explicit transaction cleanup failed")
	require.Contains(t, text, "cleanup_error=boom")
	require.NotContains(t, text, "cleanup_error=\"commit boom\"")

	var disabled bytes.Buffer
	disabledSession := &Session{server: &Server{log: slog.New(slog.NewTextHandler(&disabled, nil))}}
	disabledSession.observeTransactionTerminal(transactionTerminalCommit, "neo4j", time.Millisecond, nil)
	require.Empty(t, disabled.String(), "disabled debug diagnostics must stay on the allocation-free path")

	(&Session{}).observeTransactionTerminal(transactionTerminalCommit, "neo4j", 0, nil)
}
