package bolt

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBoltExplicitTransactionTimeoutCancelsActiveRunBeforeRollback(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nornic")
	executor := newControlledTransactionExecutor(store)
	port := startControlledTransactionServer(t, executor)

	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE (r:Repository {repo_id: 'active-run-seed'})")
	require.NoError(t, setup.Close())
	baselineReaders := base.ActiveReaders()

	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	runExplicitStatement(t, conn,
		"MATCH (seed:Repository {repo_id: 'active-run-seed'}) CREATE (r:Repository {repo_id: 'active-run-abandoned'})",
		nil)
	require.Greater(t, base.ActiveReaders(), baselineReaders)
	require.NoError(t, SendRun(t, conn, transactionLifecycleControlledQuery, nil, nil))

	select {
	case <-executor.runStarted:
	case <-time.After(time.Second):
		require.NoError(t, conn.Close())
		t.Fatal("controlled RUN did not reach the production transaction adapter")
	}

	var cancelErr error
	select {
	case cancelErr = <-executor.runCanceled:
	case <-time.After(transactionLifecycleShortCleanupDeadline):
		require.NoError(t, conn.Close())
		t.Fatal("BEGIN tx_timeout did not promptly cancel the active RUN context")
	}
	require.True(t,
		errors.Is(cancelErr, context.Canceled) || errors.Is(cancelErr, context.DeadlineExceeded),
		"active RUN cancellation error = %v", cancelErr)

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", code)
	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleShortCleanupDeadline, 10*time.Millisecond,
		"deadline cancellation must unblock RUN, roll back, and release the snapshot")

	fresh := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'active-run-abandoned'}) RETURN count(r)")
	require.Equal(t, [][]any{{int64(0)}}, records,
		"a deadline-canceled active RUN must not persist the transaction's buffered write")
}

func TestBoltExplicitTransactionRunAfterTimeoutFailsUntilReset(t *testing.T) {
	base, port := startTransactionLifecycleServer(t)
	baselineReaders := base.ActiveReaders()
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleShortCleanupDeadline, 10*time.Millisecond)

	require.NoError(t, SendRun(t, conn,
		"CREATE (r:Repository {repo_id: 'run-after-timeout'})", nil, nil))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code)

	require.NoError(t, SendRun(t, conn,
		"CREATE (r:Repository {repo_id: 'ignored-after-timeout'})", nil, nil))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)

	fresh := openBoltTestConn(t, port)
	require.Equal(t, [][]any{{int64(0)}}, runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository) WHERE r.repo_id IN ['run-after-timeout', 'ignored-after-timeout'] RETURN count(r)"))

	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	runBoltQueryAndCollectRecords(t, conn,
		"CREATE (r:Repository {repo_id: 'run-after-reset'})")
	require.Equal(t, [][]any{{int64(1)}}, runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'run-after-reset'}) RETURN count(r)"))
}

func TestBoltExplicitTransactionTimeoutRejectsExecutorSuccessAfterCancel(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	executor := newControlledTransactionExecutor(storage.NewNamespacedEngine(base, "nornic"))
	executor.succeedAfterCancel = true
	port := startControlledTransactionServer(t, executor)
	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})

	require.NoError(t, SendRun(t, conn, transactionLifecycleControlledQuery, nil, nil))
	select {
	case <-executor.runStarted:
	case <-time.After(time.Second):
		t.Fatal("controlled RUN did not start")
	}
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, transactionTimedOutCode, code,
		"executor success after cancellation must not escape timeout arbitration")

	require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err)
	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	require.Equal(t, [][]any{{int64(1)}}, runBoltQueryAndCollectRecords(t, conn, "RETURN 1"))
}

func TestTransactionLifecycleDefersCompletionTelemetryUntilRunOwnedRollback(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	executor := newControlledTransactionExecutor(storage.NewNamespacedEngine(base, "nornic"))
	release := make(chan struct{})
	executor.runRelease = release
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
	runDone := make(chan error, 1)
	go func() {
		defer func() { _ = lifecycle.finishRun() }()
		_, err := executor.Execute(runCtx, transactionLifecycleControlledQuery, nil)
		runDone <- err
	}()
	select {
	case <-executor.runStarted:
	case <-time.After(time.Second):
		t.Fatal("controlled RUN did not start")
	}
	go lifecycle.expire()

	requested := <-events
	require.Equal(t, transactionTerminalTimeoutCleanupRequested, requested.reason)
	require.NoError(t, requested.err)
	select {
	case event := <-events:
		t.Fatalf("cleanup completion reported before RUN-owned rollback: %+v", event)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	require.ErrorIs(t, <-runDone, context.Canceled)
	completed := <-events
	require.Equal(t, transactionTerminalTimeout, completed.reason)
	require.NoError(t, completed.err)
}
