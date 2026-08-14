package bolt

import (
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	transactionLifecycleShortTimeout         = 300 * time.Millisecond
	transactionLifecycleShortCleanupDeadline = 900 * time.Millisecond
	transactionLifecycleLongTimeout          = 2500 * time.Millisecond
	transactionLifecycleLongControlWait      = 1200 * time.Millisecond
)

func startTransactionLifecycleServer(t *testing.T) (*storage.MemoryEngine, int) {
	t.Helper()

	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nornic")
	return base, startTransactionLifecycleServerWithStore(t, store)
}

func runExplicitStatement(t *testing.T, conn net.Conn, query string, params map[string]any) {
	t.Helper()

	require.NoError(t, SendRun(t, conn, query, params, nil))
	require.NoError(t, ReadSuccess(t, conn))
	require.NoError(t, SendPull(t, conn, nil))
	for {
		messageType, _, err := ReadMessage(conn)
		require.NoError(t, err)
		if messageType == MsgSuccess {
			return
		}
		require.Equal(t, byte(MsgRecord), messageType)
	}
}

func beginExplicitTransaction(t *testing.T, conn net.Conn, metadata map[string]any) {
	t.Helper()
	require.NoError(t, SendBegin(t, conn, metadata))
	require.NoError(t, ReadSuccess(t, conn))
}

func TestBoltExplicitTransactionTimeoutTerminatesBeforeCommit(t *testing.T) {
	base, port := startTransactionLifecycleServer(t)
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE CONSTRAINT repository_id IF NOT EXISTS FOR (r:Repository) REQUIRE r.repo_id IS UNIQUE")
	runBoltQueryAndCollectRecords(t, setup,
		"MERGE (r:Repository {repo_id: 'same'}) SET r.owner = 'seed'")
	require.NoError(t, setup.Close())
	baselineReaders := base.ActiveReaders()

	timed := openBoltTestConn(t, port)
	beginExplicitTransaction(t, timed, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	runExplicitStatement(t, timed,
		"MATCH (r:Repository {repo_id: $repo_id}) SET r.owner = $owner",
		map[string]any{"repo_id": "same", "owner": "timed"})
	require.Greater(t, base.ActiveReaders(), baselineReaders,
		"explicit transaction must pin an MVCC snapshot reader before its timeout")

	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleShortCleanupDeadline, 10*time.Millisecond,
		"timeout monitor must release the explicit transaction snapshot after the deadline")

	require.NoError(t, SendCommit(t, timed))
	code, _, err := AssertFailure(t, timed)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", code)

	fresh := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'same', owner: 'timed'}) RETURN count(r)")
	require.Equal(t, [][]any{{int64(0)}}, records,
		"a transaction terminated by tx_timeout must not commit its buffered write")
}

func TestBoltExplicitTransactionTimeoutStartsAtBegin(t *testing.T) {
	base, port := startTransactionLifecycleServer(t)
	baselineReaders := base.ActiveReaders()
	conn := openBoltTestConn(t, port)

	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleShortTimeout / time.Millisecond),
	})
	require.Greater(t, base.ActiveReaders(), baselineReaders,
		"BEGIN must pin the MVCC snapshot before any RUN message")

	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleShortCleanupDeadline, 10*time.Millisecond,
		"an idle explicit transaction must expire from its BEGIN deadline")

	require.NoError(t, SendCommit(t, conn))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", code)
}

func TestBoltExplicitTransactionLongTimeoutDoesNotExpireEarly(t *testing.T) {
	base, port := startTransactionLifecycleServer(t)
	setup := openBoltTestConn(t, port)
	runBoltQueryAndCollectRecords(t, setup,
		"CREATE (r:Repository {repo_id: 'long-timeout-seed'})")
	require.NoError(t, setup.Close())
	baselineReaders := base.ActiveReaders()

	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleLongTimeout / time.Millisecond),
	})
	runExplicitStatement(t, conn,
		"MATCH (seed:Repository {repo_id: 'long-timeout-seed'}) CREATE (r:Repository {repo_id: 'long-timeout-result'})",
		nil)

	time.Sleep(transactionLifecycleLongControlWait)
	require.Greater(t, base.ActiveReaders(), baselineReaders,
		"a 2500ms transaction must remain active after 1200ms")
	require.NoError(t, SendCommit(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, time.Second, 10*time.Millisecond,
		"successful COMMIT must release its snapshot reader")

	fresh := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'long-timeout-result'}) RETURN count(r)")
	require.Equal(t, [][]any{{int64(1)}}, records)
}

func TestBoltExplicitTransactionWithoutPositiveTimeoutCommits(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]any
	}{
		{name: "absent", metadata: nil},
		{name: "zero", metadata: map[string]any{"tx_timeout": int64(0)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base, port := startTransactionLifecycleServer(t)
			setup := openBoltTestConn(t, port)
			runBoltQueryAndCollectRecords(t, setup,
				"CREATE (r:Repository {repo_id: 'timeout-control-seed'})")
			require.NoError(t, setup.Close())
			baselineReaders := base.ActiveReaders()

			conn := openBoltTestConn(t, port)
			beginExplicitTransaction(t, conn, tt.metadata)
			runExplicitStatement(t, conn,
				"MATCH (seed:Repository {repo_id: 'timeout-control-seed'}) CREATE (r:Repository {repo_id: 'timeout-control-result'})",
				nil)
			time.Sleep(transactionLifecycleLongControlWait)
			require.Greater(t, base.ActiveReaders(), baselineReaders,
				"absent or zero tx_timeout must not schedule transaction termination")

			require.NoError(t, SendCommit(t, conn))
			require.NoError(t, ReadSuccess(t, conn))
			require.Eventually(t, func() bool {
				return base.ActiveReaders() == baselineReaders
			}, time.Second, 10*time.Millisecond,
				"successful COMMIT must release its snapshot reader")

			fresh := openBoltTestConn(t, port)
			records := runBoltQueryAndCollectRecords(t, fresh,
				"MATCH (r:Repository {repo_id: 'timeout-control-result'}) RETURN count(r)")
			require.Equal(t, [][]any{{int64(1)}}, records)
		})
	}
}

func TestBoltExplicitTransactionRejectsInvalidTimeoutBeforeStorageAllocation(t *testing.T) {
	tests := []struct {
		name        string
		value       any
		wantMessage string
	}{
		{name: "wrong wire type", value: "500", wantMessage: "Expected long"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, base.Close()) })
			counting := &countingTransactionEngine{Engine: base, inner: base}
			store := storage.NewNamespacedEngine(counting, "nornic")
			port := startTransactionLifecycleServerWithStore(t, store)
			baselineReaders := base.ActiveReaders()
			conn := openBoltTestConn(t, port)

			require.NoError(t, SendBegin(t, conn, map[string]any{"tx_timeout": tt.value}))
			code, message, err := AssertFailure(t, conn)
			require.NoError(t, err)
			require.Equal(t, "Neo.ClientError.Request.Invalid", code)
			require.Contains(t, message, "tx_timeout")
			require.Contains(t, message, tt.wantMessage)
			require.Zero(t, counting.begins.Load(),
				"invalid BEGIN metadata must be rejected before storage transaction allocation")
			require.Equal(t, baselineReaders, base.ActiveReaders(),
				"invalid BEGIN metadata must not allocate an MVCC snapshot reader")
			require.NoError(t, SendRun(t, conn, "RETURN 1", nil, nil))
			_, err = AssertMessageType(t, conn, MsgIgnored)
			require.NoError(t, err, "failed BEGIN must ignore RUN until RESET")
			require.NoError(t, SendReset(t, conn))
			require.NoError(t, ReadSuccess(t, conn))
			require.Equal(t, [][]any{{int64(1)}}, runBoltQueryAndCollectRecords(t, conn, "RETURN 1"))
		})
	}
}

func TestBoltDuplicateBeginPreservesOriginalTransactionExecutor(t *testing.T) {
	originalBase := storage.NewMemoryEngine()
	otherBase := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, originalBase.Close())
		require.NoError(t, otherBase.Close())
	})
	original := &countingTransactionEngine{Engine: originalBase, inner: originalBase}
	other := &countingTransactionEngine{Engine: otherBase, inner: otherBase}
	mgr := &mockDBManager{
		stores: map[string]storage.Engine{
			"nornic": storage.NewNamespacedEngine(original, "nornic"),
			"other":  storage.NewNamespacedEngine(other, "other"),
		},
		defaultDB: "nornic",
	}
	server := NewWithDatabaseManager(&Config{
		Port:            0,
		MaxConnections:  8,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, &mockExecutor{}, mgr)
	port := startBoltTestServer(t, server)
	originalReaders := originalBase.ActiveReaders()
	otherReaders := otherBase.ActiveReaders()
	conn := openBoltTestConn(t, port)

	beginExplicitTransaction(t, conn, map[string]any{"db": "nornic"})
	runExplicitStatement(t, conn,
		"CREATE (r:Repository {repo_id: 'duplicate-begin-original'})", nil)
	require.Equal(t, int64(1), original.begins.Load())
	require.Greater(t, originalBase.ActiveReaders(), originalReaders)

	require.NoError(t, SendBegin(t, conn, map[string]any{"db": "other"}))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionStartFailed", code)
	require.Equal(t, int64(1), original.begins.Load(),
		"duplicate BEGIN must not replace or reallocate the original executor")
	require.Zero(t, other.begins.Load(),
		"duplicate BEGIN must fail before resolving and allocating another database executor")
	require.Equal(t, otherReaders, otherBase.ActiveReaders())
	require.NoError(t, SendCommit(t, conn))
	_, err = AssertMessageType(t, conn, MsgIgnored)
	require.NoError(t, err, "duplicate BEGIN must ignore COMMIT until RESET")

	require.NoError(t, SendReset(t, conn))
	require.NoError(t, ReadSuccess(t, conn))
	require.Eventually(t, func() bool {
		return originalBase.ActiveReaders() == originalReaders
	}, time.Second, 10*time.Millisecond)
	fresh := openBoltTestConn(t, port)
	require.Equal(t, [][]any{{int64(0)}}, runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'duplicate-begin-original'}) RETURN count(r)"))
}
