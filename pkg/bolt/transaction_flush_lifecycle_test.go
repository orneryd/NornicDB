package bolt

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockingTransactionalFlushExecutor struct {
	mu sync.Mutex

	flushEntered    chan struct{}
	flushRelease    chan struct{}
	flushOnce       sync.Once
	releaseOnce     sync.Once
	rollbackEntered chan struct{}
	rollbackRelease chan struct{}
	active          bool
	stagedWrite     bool
	flushActive     bool
	overlapped      bool
	rollbackCalls   int
}

func (e *blockingTransactionalFlushExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	e.mu.Lock()
	e.stagedWrite = true
	e.mu.Unlock()
	return &QueryResult{Stats: &QueryStats{NodesCreated: 1}}, nil
}

func (e *blockingTransactionalFlushExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.mu.Lock()
	e.active = true
	e.mu.Unlock()
	return nil
}

func (e *blockingTransactionalFlushExecutor) CommitTransaction(context.Context) error { return nil }

func (e *blockingTransactionalFlushExecutor) RollbackTransaction(context.Context) error {
	e.mu.Lock()
	if e.flushActive {
		e.overlapped = true
	}
	e.rollbackCalls++
	e.mu.Unlock()
	if e.rollbackEntered != nil {
		close(e.rollbackEntered)
		<-e.rollbackRelease
	}
	e.mu.Lock()
	e.active = false
	e.stagedWrite = false
	e.mu.Unlock()
	return nil
}

func (e *blockingTransactionalFlushExecutor) Flush() error {
	blocked := false
	e.flushOnce.Do(func() { blocked = true })
	if !blocked {
		return nil
	}
	e.mu.Lock()
	e.flushActive = true
	e.mu.Unlock()
	close(e.flushEntered)
	<-e.flushRelease
	e.mu.Lock()
	e.flushActive = false
	e.mu.Unlock()
	return nil
}

func (e *blockingTransactionalFlushExecutor) SetDeferFlush(bool) {}

func (e *blockingTransactionalFlushExecutor) releaseFlush() {
	e.releaseOnce.Do(func() { close(e.flushRelease) })
}

func TestBoltTimeoutNeverOverlapsExplicitTransactionFlush(t *testing.T) {
	for _, tt := range []struct {
		name string
		send func(*testing.T, net.Conn) error
	}{
		{name: "PULL", send: func(t *testing.T, conn net.Conn) error { return SendPull(t, conn, nil) }},
		{name: "DISCARD", send: func(t *testing.T, conn net.Conn) error {
			return SendMessage(conn, []byte{0xB1, MsgDiscard, 0xA0})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor := &blockingTransactionalFlushExecutor{
				flushEntered: make(chan struct{}),
				flushRelease: make(chan struct{}),
			}
			t.Cleanup(executor.releaseFlush)
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, map[string]any{
				"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
			})
			require.NoError(t, SendRun(t, conn, "CREATE (:FlushTimeoutProbe)", nil, nil))
			require.NoError(t, ReadSuccess(t, conn))
			require.NoError(t, tt.send(t, conn))
			select {
			case <-executor.flushEntered:
			case <-time.After(time.Second):
				t.Fatal("PULL/DISCARD did not enter deferred executor flush")
			}
			time.Sleep(2 * transactionLifecycleShortTimeout)
			executor.mu.Lock()
			require.Zero(t, executor.rollbackCalls,
				"timeout rollback overlapped an admitted executor Flush")
			executor.mu.Unlock()
			executor.releaseFlush()

			code, _, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, transactionTimedOutCode, code)
			require.NoError(t, SendCommit(t, conn))
			_, err = AssertMessageType(t, conn, MsgIgnored)
			require.NoError(t, err)
			require.NoError(t, SendReset(t, conn))
			require.NoError(t, ReadSuccess(t, conn))

			executor.mu.Lock()
			defer executor.mu.Unlock()
			require.False(t, executor.overlapped)
			require.False(t, executor.stagedWrite)
			require.Equal(t, 1, executor.rollbackCalls)
		})
	}
}

func TestPendingFlushJoinsTimeoutCleanupThatWonAdmission(t *testing.T) {
	executor := &blockingTransactionalFlushExecutor{
		flushEntered:    make(chan struct{}),
		flushRelease:    make(chan struct{}),
		rollbackEntered: make(chan struct{}),
		rollbackRelease: make(chan struct{}),
	}
	session := &Session{
		executor:      executor,
		inTransaction: true,
		pendingFlush:  true,
	}
	require.NoError(t, session.txLifecycle.begin(
		context.Background(), transactionLifecycleShortTimeout, "nornic", executor, nil, nil))

	select {
	case <-executor.rollbackEntered:
	case <-time.After(time.Second):
		t.Fatal("timeout cleanup did not enter rollback")
	}
	flushResult := make(chan error, 1)
	go func() { flushResult <- session.flushPendingExecutorWrites() }()
	select {
	case err := <-flushResult:
		t.Fatalf("pending Flush returned before timeout cleanup completed: %v", err)
	case <-time.After(3 * transactionLifecycleShortTimeout / 4):
	}
	close(executor.rollbackRelease)
	require.ErrorIs(t, <-flushResult, errTransactionTimedOut)
	select {
	case <-executor.flushEntered:
		t.Fatal("executor Flush ran after timeout won operation admission")
	default:
	}
}
