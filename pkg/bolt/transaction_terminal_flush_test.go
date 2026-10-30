package bolt

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type terminalFlushTransactionExecutor struct {
	mu sync.Mutex

	active        bool
	staged        bool
	persisted     bool
	flushCalls    int
	rollbackCalls int
	commitCalls   int
	flushErr      error
}

func (e *terminalFlushTransactionExecutor) Execute(
	context.Context, string, map[string]any,
) (*QueryResult, error) {
	e.mu.Lock()
	e.staged = true
	e.mu.Unlock()
	return &QueryResult{Stats: &QueryStats{NodesCreated: 1}}, nil
}

func (e *terminalFlushTransactionExecutor) BeginTransaction(context.Context, map[string]any) error {
	e.mu.Lock()
	e.active = true
	e.mu.Unlock()
	return nil
}

func (e *terminalFlushTransactionExecutor) CommitTransaction(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.commitCalls++
	e.persisted = e.staged
	e.staged = false
	e.active = false
	return nil
}

func (e *terminalFlushTransactionExecutor) RollbackTransaction(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rollbackCalls++
	e.staged = false
	e.active = false
	return nil
}

func (e *terminalFlushTransactionExecutor) Flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushCalls++
	if e.flushErr != nil {
		return e.flushErr
	}
	if e.staged {
		e.persisted = true
		e.staged = false
	}
	return nil
}

func TestBoltExplicitFlushFailureMarksTransactionFailedUntilReset(t *testing.T) {
	for _, message := range []string{"PULL", "DISCARD"} {
		t.Run(message, func(t *testing.T) {
			executor := &terminalFlushTransactionExecutor{
				flushErr: errors.New("forced deferred flush failure"),
			}
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, nil)
			require.NoError(t, SendRun(t, conn, "CREATE (:FailedDeferredFlush)", nil, nil))
			require.NoError(t, ReadSuccess(t, conn))
			if message == "PULL" {
				require.NoError(t, SendPull(t, conn, nil))
			} else {
				require.NoError(t, SendMessage(conn, []byte{0xB1, MsgDiscard, 0xA0}))
			}
			code, _, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, "Neo.DatabaseError.General.UnknownError", code)

			require.NoError(t, SendCommit(t, conn))
			_, err = AssertMessageType(t, conn, MsgIgnored)
			require.NoError(t, err)
			require.NoError(t, SendReset(t, conn))
			require.NoError(t, ReadSuccess(t, conn))

			executor.mu.Lock()
			defer executor.mu.Unlock()
			require.Equal(t, 1, executor.flushCalls)
			require.Equal(t, 1, executor.rollbackCalls)
			require.Zero(t, executor.commitCalls)
			require.False(t, executor.persisted)
		})
	}
}

func (e *terminalFlushTransactionExecutor) SetDeferFlush(bool) {}

func TestBoltExplicitTerminalNeverFlushesPendingResult(t *testing.T) {
	for _, terminal := range []string{"RESET", "ROLLBACK", "GOODBYE", "EOF", "COMMIT"} {
		t.Run(terminal, func(t *testing.T) {
			executor := &terminalFlushTransactionExecutor{}
			server := New(&Config{
				Port:            0,
				MaxConnections:  8,
				ReadBufferSize:  8192,
				WriteBufferSize: 8192,
			}, fixedSessionExecutorFactory{QueryExecutor: executor})
			port := startBoltTestServer(t, server)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, nil)
			require.NoError(t, SendRun(t, conn, "CREATE (:PendingTerminalFlush)", nil, nil))
			require.NoError(t, ReadSuccess(t, conn))

			switch terminal {
			case "RESET":
				require.NoError(t, SendReset(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			case "ROLLBACK":
				require.NoError(t, SendRollback(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			case "GOODBYE":
				require.NoError(t, SendGoodbye(t, conn))
			case "EOF":
				require.NoError(t, conn.Close())
			case "COMMIT":
				require.NoError(t, SendCommit(t, conn))
				require.NoError(t, ReadSuccess(t, conn))
			}

			if terminal == "RESET" || terminal == "ROLLBACK" || terminal == "COMMIT" {
				require.NoError(t, SendPull(t, conn, nil))
				require.NoError(t, ReadSuccess(t, conn),
					"terminal path must discard the prior transaction result")
				require.NoError(t, conn.Close())
			}
			require.Eventually(t, func() bool {
				return server.activeConnections.Load() == 0
			}, time.Second, time.Millisecond)

			executor.mu.Lock()
			defer executor.mu.Unlock()
			require.Zero(t, executor.flushCalls, "terminal path must not Flush transaction writes")
			require.False(t, executor.staged)
			if terminal == "COMMIT" {
				require.True(t, executor.persisted, "CommitTransaction owns durability")
				require.Equal(t, 1, executor.commitCalls)
			} else {
				require.False(t, executor.persisted)
				require.Equal(t, 1, executor.rollbackCalls)
			}
		})
	}
}
