package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
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

// startResolverBarrierTransaction owns the pre-claim access-check barrier shared
// by the transaction ownership tests: the same 8-connection server, fresh
// resolver channels, the caller's executor and resolved auth mode, and the
// shared serve/open/BEGIN-timeout/RUN owners. It returns once RUN is parked
// inside the resolver so the caller can drive timeout and cleanup ordering.
//
// The returned release unblocks the resolver. It is idempotent and registered
// for cleanup before the server teardown, so a failed assertion cannot leave a
// blocked access check (and its Bolt handler) behind.
func startResolverBarrierTransaction(
	t *testing.T,
	executor QueryExecutor,
	mode auth.DatabaseAccessMode,
	releaseRollback func(),
) (net.Conn, func()) {
	t.Helper()
	resolverEntered := make(chan struct{})
	resolverRelease := make(chan struct{})
	var resolverOnce sync.Once
	var resolverReleaseOnce sync.Once
	release := func() { resolverReleaseOnce.Do(func() { close(resolverRelease) }) }
	server := New(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, fixedSessionExecutorFactory{QueryExecutor: executor})
	server.databaseAccessModeResolver = func([]string) auth.DatabaseAccessMode {
		resolverOnce.Do(func() { close(resolverEntered) })
		<-resolverRelease
		return mode
	}
	port := serveBoltIntegrationServer(t, server)
	t.Cleanup(func() {
		release()
		// Register before any assertion in this helper can abort. Registering
		// in its caller after return is too late if BEGIN/RUN itself fails.
		if releaseRollback != nil {
			releaseRollback()
		}
	})
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	select {
	case <-resolverEntered:
	case <-time.After(time.Second):
		t.Fatal("RUN did not reach the resolver barrier")
	}
	return conn, release
}

func TestBoltTimeoutCleanupIsNotOwnedByPreExecutionFailurePath(t *testing.T) {
	rollbackRelease := make(chan struct{})
	var rollbackReleaseOnce sync.Once
	releaseRollback := func() { rollbackReleaseOnce.Do(func() { close(rollbackRelease) }) }
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: rollbackRelease,
	}
	conn, releaseResolver := startResolverBarrierTransaction(t, executor, auth.DenyAllDatabaseAccessMode, releaseRollback)

	cleanupStartedBeforeFailure := false
	select {
	case <-executor.rollbackEntered:
		cleanupStartedBeforeFailure = true
	case <-time.After(2 * transactionLifecycleShortTimeout):
	}
	if cleanupStartedBeforeFailure {
		releaseRollback()
	}
	releaseResolver()
	if !cleanupStartedBeforeFailure {
		select {
		case <-executor.rollbackEntered:
			releaseRollback()
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
	conn, releaseResolver := startResolverBarrierTransaction(t, executor, auth.FullDatabaseAccessMode, nil)

	require.Eventually(t, func() bool {
		executor.mu.Lock()
		defer executor.mu.Unlock()
		return executor.rollbackCalls == 1
	}, time.Second, time.Millisecond, "timeout cleanup did not finish before resolver release")
	releaseResolver()
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
	executor := &blockingRollbackExecutor{
		rollbackEntered: make(chan struct{}),
		rollbackRelease: rollbackRelease,
	}
	conn, releaseResolver := startResolverBarrierTransaction(t, executor, auth.FullDatabaseAccessMode, releaseRollback)

	select {
	case <-executor.rollbackEntered:
	case <-time.After(time.Second):
		t.Fatal("idle timeout cleanup did not enter rollback")
	}
	releaseResolver()

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	_, _, err := ReadMessage(conn)
	require.Error(t, err, "RUN failure returned before pre-claim timeout cleanup completed")
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
	releaseRollback()
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)
}

func TestBoltTransactionPinsBeginDatabaseAndRejectsCrossDatabaseRun(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		maxConnections int
		factory        bool
	}{
		{
			name:           "factory",
			maxConnections: 8,
			factory:        true,
		},
		{
			name:           "raw",
			maxConnections: 1,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			executor := &sharedSingleTransactionExecutor{}
			var logs bytes.Buffer
			var boundExecutor QueryExecutor = executor
			if testCase.factory {
				boundExecutor = fixedSessionExecutorFactory{QueryExecutor: executor}
			}
			server := New(&Config{
				Port:            0,
				MaxConnections:  testCase.maxConnections,
				ReadBufferSize:  8192,
				WriteBufferSize: 8192,
				Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
					Level: slog.LevelDebug,
				})),
			}, boundExecutor)
			port := serveBoltIntegrationServer(t, server)
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
		})
	}
}

func TestBoltFailedRunIgnoresCommitUntilResetAndRollsBack(t *testing.T) {
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
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
	require.Equal(t, [][]any{{int64(0)}}, runBoltQueryWithParams(t, fresh,
		"MATCH (n:TxFailedRunProbe {id: 'must-rollback'}) RETURN count(n)", nil))
}

func TestBoltReturnRuntimeFailureKeepsConnectionAfterReset(t *testing.T) {
	for _, testCase := range []struct {
		query string
		code  string
	}{
		{"RETURN 1 / 0", "Neo.ClientError.Statement.ArithmeticError"},
		{"RETURN 1 % 0", "Neo.ClientError.Statement.ArithmeticError"},
		{"RETURN substring('abc', -1)", "Neo.DatabaseError.Statement.ExecutionFailed"},
	} {
		t.Run(testCase.query, func(t *testing.T) {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			executor := newFailingRunTransactionalExecutor(storage.NewNamespacedEngine(base, "nornic"))
			port := startControlledTransactionServer(t, executor)
			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, nil)
			require.NoError(t, SendRun(t, conn, testCase.query, nil, nil))
			code, _, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, testCase.code, code)
			require.NoError(t, SendReset(t, conn))
			require.NoError(t, ReadSuccess(t, conn))
			require.Equal(t, [][]any{{int64(1)}}, runBoltQueryWithParams(t, conn, "RETURN 1", nil))
		})
	}
}
