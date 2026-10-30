package bolt

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type disconnectBeginExecutor struct {
	entered        chan struct{}
	release        chan struct{}
	returnOnCancel bool
	enterOnce      sync.Once
	active         atomic.Bool
	rollbackCalls  atomic.Int64
}

type resetReusableExecutor struct {
	started  chan struct{}
	canceled chan struct{}
	calls    atomic.Int64
}

func (e *resetReusableExecutor) Execute(
	ctx context.Context, _ string, _ map[string]any,
) (*QueryResult, error) {
	if e.calls.Add(1) == 1 {
		close(e.started)
		<-ctx.Done()
		close(e.canceled)
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &QueryResult{Columns: []string{"value"}, Rows: [][]any{{int64(1)}}}, nil
}

func (e *disconnectBeginExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	return &QueryResult{}, nil
}

func (e *disconnectBeginExecutor) BeginTransaction(ctx context.Context, _ map[string]any) error {
	e.active.Store(true)
	e.enterOnce.Do(func() { close(e.entered) })
	if e.returnOnCancel {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.release:
			return errors.New("test released blocked BEGIN")
		}
	}
	<-e.release
	return nil
}

func (e *disconnectBeginExecutor) CommitTransaction(context.Context) error { return nil }

func (e *disconnectBeginExecutor) RollbackTransaction(context.Context) error {
	e.rollbackCalls.Add(1)
	e.active.Store(false)
	return nil
}

func TestBoltDisconnectCancelsAndCleansInFlightBegin(t *testing.T) {
	for _, tt := range []struct {
		name           string
		returnOnCancel bool
	}{
		{name: "cooperative begin", returnOnCancel: true},
		{name: "success after cancellation edge"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor := &disconnectBeginExecutor{
				entered:        make(chan struct{}),
				release:        make(chan struct{}),
				returnOnCancel: tt.returnOnCancel,
			}
			t.Cleanup(func() {
				select {
				case <-executor.release:
				default:
					close(executor.release)
				}
			})
			server := New(&Config{
				Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192,
			}, fixedSessionExecutorFactory{QueryExecutor: executor})
			port := startBoltTestServer(t, server)
			conn := openBoltTestConn(t, port)
			require.NoError(t, SendBegin(t, conn, nil))
			select {
			case <-executor.entered:
			case <-time.After(time.Second):
				t.Fatal("BEGIN did not reach executor")
			}
			require.NoError(t, conn.Close())
			if !tt.returnOnCancel {
				close(executor.release)
			}
			require.Eventually(t, func() bool {
				return executor.rollbackCalls.Load() == 1 && !executor.active.Load() &&
					server.activeConnections.Load() == 0
			}, time.Second, time.Millisecond,
				"disconnect did not cancel BEGIN and synchronously release ownership")
		})
	}
}

func TestBoltDisconnectAfterSuccessfulBeginRollsBackExactlyOnce(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	server := New(&Config{
		Port: 0, MaxConnections: 8, ReadBufferSize: 8192, WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)
	require.NoError(t, conn.Close())

	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return !executor.active && executor.rollbackCalls == 1 &&
			server.activeConnections.Load() == 0
	}, time.Second, time.Millisecond)
}

func TestBoltResetCancellationKeepsConnectionContextReusable(t *testing.T) {
	executor := &resetReusableExecutor{started: make(chan struct{}), canceled: make(chan struct{})}
	server := New(&Config{Port: 0, MaxConnections: 8}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)

	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("first RUN did not reach executor")
	}
	require.NoError(t, SendReset(t, conn))
	select {
	case <-executor.canceled:
	case <-time.After(time.Second):
		t.Fatal("RESET did not cancel first RUN")
	}
	require.NoError(t, ReadSuccess(t, conn))

	require.NoError(t, SendRun(t, conn, "RETURN 2", nil, nil))
	require.NoError(t, ReadSuccess(t, conn))
	require.Equal(t, int64(2), executor.calls.Load())
}
