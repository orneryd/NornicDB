package bolt

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type slowBeginTransactionExecutor struct {
	beginEntered  chan struct{}
	beginRelease  <-chan struct{}
	cooperative   bool
	beginCanceled atomic.Bool
	rollbackCalls atomic.Int64
	rollbackErr   error
	rollbackPanic any
	flushCalls    atomic.Int64
	executeCalls  atomic.Int64
}

func (e *slowBeginTransactionExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	e.executeCalls.Add(1)
	return &QueryResult{}, nil
}

func (e *slowBeginTransactionExecutor) BeginTransaction(ctx context.Context, _ map[string]any) error {
	close(e.beginEntered)
	if e.cooperative {
		select {
		case <-ctx.Done():
			e.beginCanceled.Store(true)
			return ctx.Err()
		case <-e.beginRelease:
			return nil
		}
	}
	<-e.beginRelease
	return nil
}

func (e *slowBeginTransactionExecutor) CommitTransaction(context.Context) error { return nil }

func (e *slowBeginTransactionExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls.Add(1)
	if e.rollbackPanic != nil {
		panic(e.rollbackPanic)
	}
	return e.rollbackErr
}

func (e *slowBeginTransactionExecutor) Flush() error {
	e.flushCalls.Add(1)
	return nil
}

func (e *slowBeginTransactionExecutor) SetDeferFlush(bool) {}

func TestBoltBeginTimeoutStartsBeforeCooperativeBackendAllocationCompletes(t *testing.T) {
	release := make(chan struct{})
	executor := &slowBeginTransactionExecutor{
		beginEntered: make(chan struct{}),
		beginRelease: release,
		cooperative:  true,
	}
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": int64(100)}))
	select {
	case <-executor.beginEntered:
	case <-time.After(time.Second):
		t.Fatal("BEGIN did not enter backend allocation")
	}
	time.Sleep(200 * time.Millisecond)
	require.False(t, executor.beginCanceled.Load(),
		"tx_timeout must not cancel backend allocation before BEGIN succeeds")
	close(release)
	require.NoError(t, ReadSuccess(t, conn))
	assertCommitTimeoutFailure(t, conn)
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestBoltBeginTimeoutRollsBackBackendSuccessAfterDeadline(t *testing.T) {
	release := make(chan struct{})
	executor := &slowBeginTransactionExecutor{
		beginEntered: make(chan struct{}),
		beginRelease: release,
	}
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": int64(100)}))
	select {
	case <-executor.beginEntered:
	case <-time.After(time.Second):
		t.Fatal("BEGIN did not enter backend allocation")
	}
	time.Sleep(200 * time.Millisecond)
	close(release)
	require.NoError(t, ReadSuccess(t, conn))
	assertCommitTimeoutFailure(t, conn)
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestTransactionBeginCannotPublishExpiredTransactionAsActive(t *testing.T) {
	release := make(chan struct{})
	time.AfterFunc(50*time.Millisecond, func() { close(release) })
	executor := &slowBeginTransactionExecutor{
		beginEntered: make(chan struct{}),
		beginRelease: release,
	}
	lifecycle := &transactionLifecycle{
		afterFunc: func(time.Duration, func()) *time.Timer {
			// Freeze the asynchronous zero-delay callback to expose any Active
			// window left after backend allocation consumed the deadline.
			return nil
		},
	}
	require.NoError(t, lifecycle.begin(
		context.Background(), 10*time.Millisecond, "nornic", executor, nil, nil))
	_, err := lifecycle.claimCommit()
	require.ErrorIs(t, err, errTransactionTimedOut)
	require.Equal(t, int64(1), executor.rollbackCalls.Load())
}

func TestBoltExpiredBeginCleanupFailureClosesAndQuarantines(t *testing.T) {
	for _, tt := range []struct {
		name          string
		rollbackErr   error
		rollbackPanic any
	}{
		{name: "error", rollbackErr: errors.New("forced expired BEGIN cleanup failure")},
		{name: "panic", rollbackPanic: "forced expired BEGIN cleanup panic"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			executor := &slowBeginTransactionExecutor{
				beginEntered:  make(chan struct{}),
				beginRelease:  release,
				rollbackErr:   tt.rollbackErr,
				rollbackPanic: tt.rollbackPanic,
			}
			server := New(&Config{
				Port:            0,
				MaxConnections:  1,
				ReadBufferSize:  8192,
				WriteBufferSize: 8192,
			}, executor)
			port := startBoltTestServer(t, server)
			conn := openBoltTestConn(t, port)
			require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": int64(50)}))
			select {
			case <-executor.beginEntered:
			case <-time.After(time.Second):
				t.Fatal("BEGIN did not enter backend allocation")
			}
			time.Sleep(100 * time.Millisecond)
			close(release)
			code, _, err := AssertFailure(t, conn)
			require.NoError(t, err, "failed cleanup must not return BEGIN SUCCESS")
			require.Equal(t, "Neo.ClientError.Transaction.TransactionStartFailed", code)
			requireBoltConnectionClosed(t, conn)
			require.Eventually(t, func() bool { return server.activeConnections.Load() == 0 },
				time.Second, time.Millisecond)
			require.True(t, server.rawTransactionExecutorPoisoned.Load())
			require.Equal(t, int64(1), executor.rollbackCalls.Load())
			require.Zero(t, executor.flushCalls.Load())

			fresh := openBoltTestConn(t, port)
			require.NoError(t, SendRun(t, fresh, "RETURN 1", nil, nil))
			_, _, err = AssertFailure(t, fresh)
			require.NoError(t, err)
			requireBoltConnectionClosed(t, fresh)
			require.Zero(t, executor.executeCalls.Load())
		})
	}
}

func TestBoltTransactionTimeoutMetadataMatchesNeo4j526Edges(t *testing.T) {
	for _, edge := range []struct {
		name  string
		value any
	}{
		{name: "null", value: nil},
		{name: "negative", value: int64(-1)},
		{name: "maximum long", value: int64(1<<63 - 1)},
	} {
		t.Run(edge.name, func(t *testing.T) {
			executor := &sharedSingleTransactionExecutor{}
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": edge.value}))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, SendCommit(t, conn))
			require.NoError(t, ReadSuccess(t, conn))
		})
	}
}

func assertCommitTimeoutFailure(t *testing.T, conn net.Conn) {
	t.Helper()
	require.NoError(t, SendCommit(t, conn))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
}
