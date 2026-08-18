package bolt

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type failingRollbackTransactionalExecutor struct {
	rollbackErr     error
	rollbackPanic   any
	rollbackStarted chan struct{}
	rollbackOnce    sync.Once
	beginCalls      atomic.Int64
	executeCalls    atomic.Int64
	flushCalls      atomic.Int64
	deferEnabled    atomic.Bool
	deferDisabled   atomic.Bool
}

type sharedSingleTransactionExecutor struct {
	mu            sync.Mutex
	active        bool
	rollbackCalls int
	executeCalls  atomic.Int64
}

type mismatchedTransactionSessionFactory struct {
	*sharedSingleTransactionExecutor
}

type uncertainCommitTransactionalExecutor struct {
	mu            sync.Mutex
	active        bool
	beginCalls    int
	rollbackCalls int
	flushCalls    int
	executeCalls  int
	panicCommit   bool
}

func (e *uncertainCommitTransactionalExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	e.mu.Lock()
	e.executeCalls++
	e.mu.Unlock()
	return &QueryResult{}, nil
}

func (e *uncertainCommitTransactionalExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.beginCalls++
	e.active = true
	return nil
}

func (e *uncertainCommitTransactionalExecutor) CommitTransaction(context.Context) error {
	if e.panicCommit {
		panic("uncertain commit panic")
	}
	return errors.New("commit outcome unknown")
}

func (e *uncertainCommitTransactionalExecutor) RollbackTransaction(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollbackCalls++
	e.active = false
	return nil
}

func (e *uncertainCommitTransactionalExecutor) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushCalls++
	return nil
}

func (e *uncertainCommitTransactionalExecutor) SetDeferFlush(bool) {}

func (f mismatchedTransactionSessionFactory) NewSessionExecutor() QueryExecutor {
	return &mockExecutor{}
}

func (e *sharedSingleTransactionExecutor) Execute(
	context.Context,
	string,
	map[string]any,
) (*QueryResult, error) {
	e.executeCalls.Add(1)
	return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
}

func (e *sharedSingleTransactionExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active {
		return errors.New("shared executor already has an active transaction")
	}
	e.active = true
	return nil
}

func (e *sharedSingleTransactionExecutor) CommitTransaction(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.active {
		return errors.New("shared executor transaction is not active")
	}
	e.active = false
	return nil
}

func (e *sharedSingleTransactionExecutor) RollbackTransaction(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollbackCalls++
	e.active = false
	return nil
}

func (e *failingRollbackTransactionalExecutor) Execute(
	context.Context,
	string,
	map[string]any,
) (*QueryResult, error) {
	e.executeCalls.Add(1)
	return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
}

func (e *failingRollbackTransactionalExecutor) BeginTransaction(
	context.Context,
	map[string]any,
) error {
	e.beginCalls.Add(1)
	return nil
}

func (e *failingRollbackTransactionalExecutor) CommitTransaction(context.Context) error {
	return nil
}

func (e *failingRollbackTransactionalExecutor) RollbackTransaction(context.Context) error {
	e.rollbackOnce.Do(func() { close(e.rollbackStarted) })
	if e.rollbackPanic != nil {
		panic(e.rollbackPanic)
	}
	return e.rollbackErr
}

func (e *failingRollbackTransactionalExecutor) Flush() error {
	e.flushCalls.Add(1)
	return nil
}

func (e *failingRollbackTransactionalExecutor) SetDeferFlush(enabled bool) {
	if enabled {
		e.deferEnabled.Store(true)
	} else {
		e.deferDisabled.Store(true)
	}
}

func requireBoltConnectionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	msgType, err := AssertMessageType(t, conn, MsgSuccess)
	if err == nil {
		t.Fatalf("unsafe session reuse: received Bolt message %#x after cleanup failed", msgType)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("connection remained open after cleanup failure: %v", err)
	}
}

func TestBoltTimeoutCleanupFailureClosesConnectionWithoutReset(t *testing.T) {
	tests := []struct {
		name          string
		rollbackErr   error
		rollbackPanic any
	}{
		{name: "error", rollbackErr: errors.New("forced rollback failure")},
		{name: "panic", rollbackPanic: "forced rollback panic"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &failingRollbackTransactionalExecutor{
				rollbackErr:     tt.rollbackErr,
				rollbackPanic:   tt.rollbackPanic,
				rollbackStarted: make(chan struct{}),
			}
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, map[string]any{
				"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
			})
			select {
			case <-executor.rollbackStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout cleanup did not attempt rollback")
			}

			requireBoltConnectionClosed(t, conn)
		})
	}
}

func TestBoltRollbackFailureMakesConnectionDefunct(t *testing.T) {
	executor := &failingRollbackTransactionalExecutor{
		rollbackErr:     errors.New("forced rollback failure"),
		rollbackStarted: make(chan struct{}),
	}
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)

	require.NoError(t, SendRollback(t, conn))
	_, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	requireBoltConnectionClosed(t, conn)
	require.Eventually(t, executor.deferDisabled.Load, time.Second, time.Millisecond)
	require.True(t, executor.deferEnabled.Load())
	require.Zero(t, executor.flushCalls.Load(), "failed transaction must not be flushed during teardown")
}

func TestBoltRejectsSharedTransactionalExecutorBeforeCrossSessionBegin(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	connA := openBoltTestConn(t, port)
	connB := openBoltTestConn(t, port)

	require.NoError(t, SendBegin(t, connA, nil))
	_, _, err := AssertFailure(t, connA)
	require.NoError(t, err, "unsafe shared TransactionalExecutor must be rejected before allocation")
	require.NoError(t, SendBegin(t, connB, nil))
	_, _, err = AssertFailure(t, connB)
	require.NoError(t, err)

	executor.mu.Lock()
	defer executor.mu.Unlock()
	require.False(t, executor.active)
	require.Zero(t, executor.rollbackCalls,
		"a rejected session must never compensate against another session's transaction")
}

func TestBoltAllowsRawTransactionalExecutorForSingleConnectionServer(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)

	beginExplicitTransaction(t, conn, nil)
	require.NoError(t, SendRollback(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
}

func TestBoltQuarantinesSingleConnectionRawExecutorAfterCleanupFailure(t *testing.T) {
	executor := &failingRollbackTransactionalExecutor{
		rollbackErr:     errors.New("forced rollback failure"),
		rollbackStarted: make(chan struct{}),
	}
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	connA := openBoltTestConn(t, port)
	beginExplicitTransaction(t, connA, nil)
	require.NoError(t, SendRollback(t, connA))
	_, _, err := AssertFailure(t, connA)
	require.NoError(t, err)
	requireBoltConnectionClosed(t, connA)
	require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 },
		time.Second, time.Millisecond)

	connB := openBoltTestConn(t, port)
	require.NoError(t, SendRun(t, connB, "RETURN 1", nil, nil))
	_, _, err = AssertFailure(t, connB)
	require.NoError(t, err)
	require.Equal(t, int64(1), executor.beginCalls.Load(),
		"quarantined executor must not receive another BEGIN")
	require.Zero(t, executor.executeCalls.Load(),
		"quarantined executor must not receive autocommit RUN")
	requireBoltConnectionClosed(t, connB)
}

func TestBoltRejectsFactoryReturningNonTransactionalSessionExecutor(t *testing.T) {
	factory := mismatchedTransactionSessionFactory{
		sharedSingleTransactionExecutor: &sharedSingleTransactionExecutor{},
	}
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, factory)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)

	require.NoError(t, SendBegin(t, conn, nil))
	_, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	factory.mu.Lock()
	defer factory.mu.Unlock()
	require.False(t, factory.active)
}

func TestBoltBeginCleanupFailureClosesAndQuarantinesRawExecutor(t *testing.T) {
	executor := &partialBeginLifecycleExecutor{
		beginErr:    errors.New("partial BEGIN failed"),
		rollbackErr: errors.New("partial BEGIN rollback failed"),
	}
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)

	require.NoError(t, SendBegin(t, conn, nil))
	_, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	requireBoltConnectionClosed(t, conn)
	require.Equal(t, 1, executor.rollbackCalls)
	require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 },
		time.Second, time.Millisecond)

	fresh := openBoltTestConn(t, port)
	require.NoError(t, SendBegin(t, fresh, nil))
	_, _, err = AssertFailure(t, fresh)
	require.NoError(t, err)
	require.Equal(t, 1, executor.rollbackCalls, "quarantine must reject before executor reuse")
}

func TestBoltCommitErrorClosesAndQuarantinesUnknownTransactionOutcome(t *testing.T) {
	executor := &uncertainCommitTransactionalExecutor{}
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)

	require.NoError(t, SendCommit(t, conn))
	_, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	requireBoltConnectionClosed(t, conn)
	executor.mu.Lock()
	require.True(t, executor.active)
	require.Zero(t, executor.rollbackCalls, "unknown commit outcome must not be rolled back as if commit failed")
	require.Zero(t, executor.flushCalls, "unknown commit outcome must not be flushed")
	executor.mu.Unlock()
	require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 },
		time.Second, time.Millisecond)

	fresh := openBoltTestConn(t, port)
	require.NoError(t, SendBegin(t, fresh, nil))
	_, _, err = AssertFailure(t, fresh)
	require.NoError(t, err)
	executor.mu.Lock()
	defer executor.mu.Unlock()
	require.Equal(t, 1, executor.beginCalls, "quarantine must reject before executor reuse")
}

func TestBoltCommitPanicClosesAndQuarantinesWithoutRollbackOrFlush(t *testing.T) {
	executor := &uncertainCommitTransactionalExecutor{panicCommit: true}
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)

	require.NoError(t, SendCommit(t, conn))
	requireBoltConnectionClosed(t, conn)
	executor.mu.Lock()
	require.True(t, executor.active)
	require.Zero(t, executor.rollbackCalls)
	require.Zero(t, executor.flushCalls)
	executor.mu.Unlock()
	require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 },
		time.Second, time.Millisecond)

	fresh := openBoltTestConn(t, port)
	require.NoError(t, SendRun(t, fresh, "RETURN 1", nil, nil))
	_, _, err := AssertFailure(t, fresh)
	require.NoError(t, err)
	requireBoltConnectionClosed(t, fresh)
	executor.mu.Lock()
	defer executor.mu.Unlock()
	require.Zero(t, executor.executeCalls, "quarantine must reject RUN before executor reuse")
}
