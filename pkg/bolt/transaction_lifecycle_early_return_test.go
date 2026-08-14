package bolt

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const forcedTransactionalRunFailure = "RETURN 1 /* forced transactional failure */"

type failingRunTransactionalExecutor struct {
	adapter *transactionalBoltQueryExecutorAdapter
}

func newFailingRunTransactionalExecutor(store storage.Engine) *failingRunTransactionalExecutor {
	return &failingRunTransactionalExecutor{adapter: &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: cypher.NewStorageExecutor(store)},
	}}
}

func (e *failingRunTransactionalExecutor) Execute(
	ctx context.Context,
	query string,
	params map[string]any,
) (*QueryResult, error) {
	if query == forcedTransactionalRunFailure {
		return nil, errors.New("forced transactional RUN failure")
	}
	return e.adapter.Execute(ctx, query, params)
}

func (e *failingRunTransactionalExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	return e.adapter.BeginTransaction(ctx, metadata)
}

func (e *failingRunTransactionalExecutor) CommitTransaction(ctx context.Context) error {
	return e.adapter.CommitTransaction(ctx)
}

func (e *failingRunTransactionalExecutor) RollbackTransaction(ctx context.Context) error {
	return e.adapter.RollbackTransaction(ctx)
}

func TestBoltTimeoutCleanupIsNotOwnedByPreExecutionFailurePath(t *testing.T) {
	rollbackRelease := make(chan struct{})
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: rollbackRelease,
	}
	resolverEntered := make(chan struct{})
	resolverRelease := make(chan struct{})
	var resolverOnce sync.Once
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	server.databaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		resolverOnce.Do(func() { close(resolverEntered) })
		<-resolverRelease
		return auth.DenyAllDatabaseAccessMode
	}
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-resolverEntered:
	case <-time.After(time.Second):
		t.Fatal("RUN did not reach the pre-execution access check")
	}

	cleanupStartedBeforeFailure := false
	select {
	case <-executor.rollbackEntered:
		cleanupStartedBeforeFailure = true
	case <-time.After(2 * transactionLifecycleShortTimeout):
	}
	if cleanupStartedBeforeFailure {
		close(rollbackRelease)
	}
	close(resolverRelease)
	if !cleanupStartedBeforeFailure {
		select {
		case <-executor.rollbackEntered:
			close(rollbackRelease)
		case <-time.After(time.Second):
			t.Fatal("timeout cleanup never started")
		}
	}
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
	require.True(t, cleanupStartedBeforeFailure,
		"pre-execution failure path retained RUN ownership until after its Bolt response")
}

func TestBoltExpiryBeforeRunClaimNeverExecutesAndResetRecovers(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	resolverEntered := make(chan struct{})
	resolverRelease := make(chan struct{})
	var resolverOnce sync.Once
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	server.databaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		resolverOnce.Do(func() { close(resolverEntered) })
		<-resolverRelease
		return auth.FullDatabaseAccessMode
	}
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-resolverEntered:
	case <-time.After(time.Second):
		t.Fatal("RUN did not reach the pre-claim resolver barrier")
	}
	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.rollbackCalls == 1
	}, time.Second, time.Millisecond, "timeout cleanup did not finish before resolver release")
	close(resolverRelease)
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
	require.Zero(t, executor.executeCalls.Load(), "expired RUN must not reach executor.Execute")

	require.NoError(t, SendRun(t, conn, "RETURN 2", nil, nil))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	beginExplicitTransaction(t, conn, nil)
	runExplicitStatement(t, conn, "RETURN 3", nil)
	require.Equal(t, int64(1), executor.executeCalls.Load())
}

func TestBoltExpiryBeforeRunClaimJoinsCleanupBeforeFailure(t *testing.T) {
	rollbackRelease := make(chan struct{})
	var releaseOnce sync.Once
	releaseRollback := func() { releaseOnce.Do(func() { close(rollbackRelease) }) }
	t.Cleanup(releaseRollback)
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: rollbackRelease,
	}
	resolverEntered := make(chan struct{})
	resolverRelease := make(chan struct{})
	var resolverOnce sync.Once
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	server.databaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		resolverOnce.Do(func() { close(resolverEntered) })
		<-resolverRelease
		return auth.FullDatabaseAccessMode
	}
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-resolverEntered:
	case <-time.After(time.Second):
		t.Fatal("RUN did not reach pre-claim resolver barrier")
	}
	select {
	case <-executor.rollbackEntered:
	case <-time.After(time.Second):
		t.Fatal("idle timeout cleanup did not enter rollback")
	}
	close(resolverRelease)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err := ReadMessage(conn)
	require.Error(t, err, "RUN failure returned before pre-claim timeout cleanup completed")
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	releaseRollback()
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
}

func TestBoltFactoryTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	var logs bytes.Buffer
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{"db": "alpha"})

	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, map[string]any{"db": "beta"}))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.InvalidBookmark", code)
	require.Zero(t, executor.executeCalls.Load())
	require.NoError(t, SendCommit(t, conn))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))

	require.Contains(t, logs.String(), "database=alpha")
	require.True(t, strings.Contains(logs.String(), "reason=reset"))
}

func TestBoltRawTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun(t *testing.T) {
	executor := &sharedSingleTransactionExecutor{}
	var logs bytes.Buffer
	server := New(&Config{
		Port:            0,
		MaxConnections:  1,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
		Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	}, executor)
	port := startBoltTestServer(t, server)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{"db": "alpha"})

	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, map[string]any{"db": "beta"}))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.InvalidBookmark", code)
	require.Zero(t, executor.executeCalls.Load())
	require.NoError(t, SendCommit(t, conn))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))

	require.Contains(t, logs.String(), "database=alpha")
	require.True(t, strings.Contains(logs.String(), "reason=reset"))
}

func TestBoltFailedRunIgnoresCommitUntilResetAndRollsBack(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	executor := newFailingRunTransactionalExecutor(storage.NewNamespacedEngine(base, "nornic"))
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, nil)
	runExplicitStatement(t, conn,
		"CREATE (n:TxFailedRunProbe {id: 'must-rollback'})", nil)

	require.NoError(t, SendRun(t, conn, forcedTransactionalRunFailure, nil, nil))
	_, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.NoError(t, SendCommit(t, conn))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err, "failed transactional RUN must ignore COMMIT until RESET")
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))

	fresh := openBoltTestConn(t, port)
	require.Equal(t, [][]any{{int64(0)}}, runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (n:TxFailedRunProbe {id: 'must-rollback'}) RETURN count(n)"))
}
