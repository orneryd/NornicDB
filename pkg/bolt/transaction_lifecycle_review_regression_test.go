package bolt

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const transactionPreAdmissionBarrierQuery = "CREATE (:TxAdmissionProbe {id: 'must-rollback'})"

// preAdmissionBarrierExecutor blocks before entering the production adapter,
// making expiry between dispatch and executor admission deterministic.
type preAdmissionBarrierExecutor struct {
	adapter           *transactionalBoltQueryExecutorAdapter
	runEntered        chan struct{}
	runCanceled       chan struct{}
	runRelease        <-chan struct{}
	runOnce           sync.Once
	rollbackCompleted chan struct{}
	rollbackOnce      sync.Once
}

func newPreAdmissionBarrierExecutor(store storage.Engine, release <-chan struct{}) *preAdmissionBarrierExecutor {
	return &preAdmissionBarrierExecutor{
		adapter: &transactionalBoltQueryExecutorAdapter{
			boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(store)},
		},
		runEntered:        make(chan struct{}),
		runCanceled:       make(chan struct{}),
		runRelease:        release,
		rollbackCompleted: make(chan struct{}),
	}
}

func (e *preAdmissionBarrierExecutor) Execute(
	ctx context.Context,
	query string,
	params map[string]any,
) (*QueryResult, error) {
	if query == transactionPreAdmissionBarrierQuery {
		e.runOnce.Do(func() {
			close(e.runEntered)
			<-ctx.Done()
			close(e.runCanceled)
			<-e.runRelease
		})
		ctx = context.WithoutCancel(ctx)
	}
	return e.adapter.Execute(ctx, query, params)
}

func (e *preAdmissionBarrierExecutor) HasPendingTransactionWrites() bool {
	return e.adapter.HasPendingTransactionWrites()
}

func (e *preAdmissionBarrierExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	return e.adapter.BeginTransaction(ctx, metadata)
}

func (e *preAdmissionBarrierExecutor) CommitTransaction(ctx context.Context) error {
	return e.adapter.CommitTransaction(ctx)
}

func (e *preAdmissionBarrierExecutor) RollbackTransaction(ctx context.Context) error {
	err := e.adapter.RollbackTransaction(ctx)
	e.rollbackOnce.Do(func() { close(e.rollbackCompleted) })
	return err
}

type overlapDetectingTransactionExecutor struct {
	runEntered     chan struct{}
	runCanceled    chan struct{}
	runRelease     <-chan struct{}
	running        atomic.Bool
	overlap        atomic.Bool
	rollbackCalls  atomic.Int64
	rollbackCalled chan struct{}
	rollbackOnce   sync.Once
}

func (e *overlapDetectingTransactionExecutor) Execute(
	ctx context.Context,
	_ string,
	_ map[string]any,
) (*QueryResult, error) {
	e.running.Store(true)
	close(e.runEntered)
	<-ctx.Done()
	close(e.runCanceled)
	<-e.runRelease
	e.running.Store(false)
	return nil, ctx.Err()
}

func (e *overlapDetectingTransactionExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}

func (e *overlapDetectingTransactionExecutor) CommitTransaction(context.Context) error { return nil }

func (e *overlapDetectingTransactionExecutor) RollbackTransaction(context.Context) error {
	if e.running.Load() {
		e.overlap.Store(true)
	}
	e.rollbackCalls.Add(1)
	e.rollbackOnce.Do(func() { close(e.rollbackCalled) })
	return nil
}

type blockingRollbackExecutor struct {
	mockExecutor
	rollbackEntered chan struct{}
	rollbackRelease <-chan struct{}
	rollbackOnce    sync.Once
	rollbackCalls   atomic.Int64
}

func (e *blockingRollbackExecutor) BeginTransaction(context.Context, map[string]any) error {
	return nil
}
func (e *blockingRollbackExecutor) CommitTransaction(context.Context) error { return nil }
func (e *blockingRollbackExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls.Add(1)
	e.rollbackOnce.Do(func() { close(e.rollbackEntered) })
	<-e.rollbackRelease
	return nil
}

type panicRollbackExecutor struct {
	mockExecutor
	rollbackCalls atomic.Int64
}

func (e *panicRollbackExecutor) BeginTransaction(context.Context, map[string]any) error { return nil }
func (e *panicRollbackExecutor) CommitTransaction(context.Context) error                { return nil }
func (e *panicRollbackExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls.Add(1)
	panic("rollback panic")
}

func TestBoltTimeoutCannotAutocommitRunWaitingBeforeAdapterAdmission(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	executor := newPreAdmissionBarrierExecutor(storage.NewNamespacedEngine(base, "nornic"), release)
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, transactionPreAdmissionBarrierQuery, nil, nil))
	select {
	case <-executor.runEntered:
	case <-time.After(time.Second):
		t.Fatal("RUN did not reach the pre-admission barrier")
	}
	select {
	case <-executor.runCanceled:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		t.Fatal("timeout did not cancel the pre-admission RUN")
	}
	select {
	case <-executor.rollbackCompleted:
		t.Fatal("timeout rollback overlapped the admitted RUN")
	default:
	}
	close(release)
	released = true
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
	select {
	case <-executor.rollbackCompleted:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		t.Fatal("RUN owner did not complete timeout rollback")
	}

	fresh := openBoltTestConn(t, port)
	require.Equal(t, [][]any{{int64(0)}}, runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (n:TxAdmissionProbe {id: 'must-rollback'}) RETURN count(n)"),
		"a RUN admitted after timeout rollback must never auto-commit")
}

func TestBoltTimeoutDoesNotOverlapCustomExecutorRunAndRollback(t *testing.T) {
	release := make(chan struct{})
	executor := &overlapDetectingTransactionExecutor{
		runEntered:     make(chan struct{}),
		runCanceled:    make(chan struct{}),
		runRelease:     release,
		rollbackCalled: make(chan struct{}),
	}
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-executor.runEntered:
	case <-time.After(time.Second):
		t.Fatal("custom RUN did not start")
	}
	select {
	case <-executor.runCanceled:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		t.Fatal("timeout did not cancel custom RUN")
	}

	rollbackBeforeRunRelease := false
	select {
	case <-executor.rollbackCalled:
		rollbackBeforeRunRelease = true
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
	require.False(t, rollbackBeforeRunRelease,
		"timeout rollback overlapped a non-threadsafe custom executor RUN")
	require.False(t, executor.overlap.Load())
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestBoltResetWaitsForAdmittedTimeoutCleanup(t *testing.T) {
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: release,
	}
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	select {
	case <-executor.rollbackEntered:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		t.Fatal("timeout rollback did not enter")
	}
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err := ReadMessage(conn)
	var netErr net.Error
	require.True(t, errors.As(err, &netErr) && netErr.Timeout(),
		"RESET reported SUCCESS before timeout cleanup completed: %v", err)

	close(release)
	released = true
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	require.NoError(t, ReadSuccess(t, conn))
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestTransactionLifecycleRecoversCleanupPanic(t *testing.T) {
	executor := &panicRollbackExecutor{}
	events := make(chan struct {
		reason transactionTerminalReason
		err    error
	}, 2)
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "nornic", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, err error) {
			events <- struct {
				reason transactionTerminalReason
				err    error
			}{reason: reason, err: err}
		}))
	require.NotPanics(t, lifecycle.expire)
	completed := <-events
	require.Equal(t, transactionTerminalTimeout, completed.reason)
	require.ErrorContains(t, completed.err, "rollback panic")
	select {
	case duplicate := <-events:
		t.Fatalf("idle cleanup emitted misleading extra event: %s", duplicate.reason)
	default:
	}
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestBoltDisconnectWaitsForAdmittedTimeoutCleanup(t *testing.T) {
	release := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(release)
		}
	})
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: release,
	}
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	select {
	case <-executor.rollbackEntered:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		t.Fatal("timeout rollback did not enter")
	}
	require.NoError(t, conn.Close())
	require.Never(t, func() bool {
		return server.activeConnections.Load() == 0
	}, 100*time.Millisecond, 10*time.Millisecond,
		"raw EOF returned connection ownership before timeout cleanup completed")

	close(release)
	released = true
	require.Eventually(t, func() bool {
		return server.activeConnections.Load() == 0
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestTransactionLifecycleActiveRunPanicStillCompletesOwnedCleanup(t *testing.T) {
	executor := &panicRollbackExecutor{}
	events := make(chan struct {
		reason transactionTerminalReason
		err    error
	}, 2)
	lifecycle := &transactionLifecycle{}
	require.NoError(t, lifecycle.begin(context.Background(), 0, "nornic", executor, nil,
		func(reason transactionTerminalReason, _ string, _ time.Duration, err error) {
			events <- struct {
				reason transactionTerminalReason
				err    error
			}{reason: reason, err: err}
		}))
	runCtx, err := lifecycle.claimRun()
	require.NoError(t, err)

	go lifecycle.expire()
	select {
	case <-runCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("expiry did not cancel active RUN")
	}
	requested := <-events
	require.Equal(t, transactionTerminalTimeoutCleanupRequested, requested.reason)
	require.NoError(t, requested.err)

	waiter := make(chan error, 1)
	go func() { waiter <- lifecycle.rollback(transactionTerminalReset) }()
	select {
	case err := <-waiter:
		t.Fatalf("RESET waiter returned before RUN-owned cleanup: %v", err)
	default:
	}

	require.PanicsWithValue(t, "run panic", func() {
		defer func() { _ = lifecycle.finishRun() }()
		panic("run panic")
	})
	require.ErrorContains(t, <-waiter, "transaction rollback panicked: rollback panic")
	completed := <-events
	require.Equal(t, transactionTerminalTimeout, completed.reason)
	require.ErrorContains(t, completed.err, "transaction rollback panicked: rollback panic")
	select {
	case duplicate := <-events:
		t.Fatalf("duplicate timeout terminal telemetry: %+v", duplicate)
	default:
	}
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestTransactionLifecycleContendedTimeoutStressIsTransactionLocal(t *testing.T) {
	const transactionCount = 24
	release := make(chan struct{})
	executors := make([]*overlapDetectingTransactionExecutor, 0, transactionCount)
	runDone := make(chan error, transactionCount)

	for range transactionCount {
		executor := &overlapDetectingTransactionExecutor{
			runEntered:     make(chan struct{}),
			runCanceled:    make(chan struct{}),
			runRelease:     release,
			rollbackCalled: make(chan struct{}),
		}
		lifecycle := &transactionLifecycle{}
		require.NoError(t, lifecycle.begin(
			context.Background(), transactionLifecycleShortTimeout, "nornic", executor, nil, nil))
		runCtx, err := lifecycle.claimRun()
		require.NoError(t, err)
		executors = append(executors, executor)
		go func() {
			_, executeErr := executor.Execute(runCtx, "RETURN 1", nil)
			_ = lifecycle.finishRun()
			runDone <- executeErr
		}()
	}

	for _, executor := range executors {
		select {
		case <-executor.runCanceled:
		case <-time.After(transactionLifecycleShortCleanupDeadline):
			t.Fatal("contended RUN did not observe its transaction-local timeout")
		}
		require.Zero(t, executor.rollbackCalls.Load(),
			"timeout cleanup overlapped an active transaction operation")
	}
	close(release)
	for range transactionCount {
		require.ErrorIs(t, <-runDone, context.Canceled)
	}
	for _, executor := range executors {
		require.False(t, executor.overlap.Load())
		require.Equal(t, int64(1), executor.rollbackCalls.Load())
	}
}
