package bolt

import (
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	transactionLifecycleRaceTimeout = 500 * time.Millisecond
	transactionLifecycleRaceCleanup = 1500 * time.Millisecond
	transactionLifecycleRaceRepeats = 5
)

func TestBoltExplicitTransactionCommitTimeoutArbitration(t *testing.T) {
	t.Run("commit_wins_before_deadline", func(t *testing.T) {
		for iteration := 0; iteration < transactionLifecycleRaceRepeats; iteration++ {
			t.Run(fmt.Sprintf("iteration_%02d", iteration), func(t *testing.T) {
				testBoltCommitWinsBeforeTransactionTimeout(t)
			})
		}
	})

	t.Run("timeout_wins_before_commit", func(t *testing.T) {
		for iteration := 0; iteration < transactionLifecycleRaceRepeats; iteration++ {
			t.Run(fmt.Sprintf("iteration_%02d", iteration), func(t *testing.T) {
				testBoltTransactionTimeoutWinsBeforeCommit(t)
			})
		}
	})
}

// testBoltCommitWinsBeforeTransactionTimeout forces COMMIT handling to enter
// before the deadline and holds it across that deadline. COMMIT must remain the
// sole terminal owner and its durable write must not be rolled back or relabeled.
func testBoltCommitWinsBeforeTransactionTimeout(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nornic")
	executor := newControlledTransactionExecutor(store)
	releaseCommit := make(chan struct{})
	released := false
	t.Cleanup(func() {
		if !released {
			close(releaseCommit)
		}
	})
	executor.commitRelease = releaseCommit
	port := startControlledTransactionServer(t, executor)
	baselineReaders := base.ActiveReaders()

	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleRaceTimeout / time.Millisecond),
	})
	runExplicitStatement(t, conn,
		"CREATE (r:Repository {repo_id: 'commit-wins'})", nil)
	require.NoError(t, SendCommit(t, conn))
	select {
	case <-executor.commitEntered:
	case <-time.After(transactionLifecycleRaceCleanup):
		t.Fatal("COMMIT did not enter the controlled production adapter")
	}

	time.Sleep(2 * transactionLifecycleRaceTimeout)
	close(releaseCommit)
	released = true
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(transactionLifecycleRaceCleanup)))
	require.NoError(t, ReadSuccess(t, conn))
	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleRaceCleanup, 10*time.Millisecond)

	fresh := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'commit-wins'}) RETURN count(r)")
	require.Equal(t, [][]any{{int64(1)}}, records)
}

// testBoltTransactionTimeoutWinsBeforeCommit waits until timeout rollback has
// released the reader before sending COMMIT, making the opposite ordering
// deterministic and requiring the Neo4j-compatible timeout failure.
func testBoltTransactionTimeoutWinsBeforeCommit(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() {
		require.NoError(t, base.Close())
	})
	store := storage.NewNamespacedEngine(base, "nornic")
	executor := newControlledTransactionExecutor(store)
	port := startControlledTransactionServer(t, executor)
	baselineReaders := base.ActiveReaders()

	conn := openBoltTestConn(t, port)
	beginExplicitTransaction(t, conn, map[string]any{
		"tx_timeout": int64(transactionLifecycleRaceTimeout / time.Millisecond),
	})
	runExplicitStatement(t, conn,
		"CREATE (r:Repository {repo_id: 'timeout-wins'})", nil)
	require.Greater(t, base.ActiveReaders(), baselineReaders)
	require.Eventually(t, func() bool {
		return base.ActiveReaders() == baselineReaders
	}, transactionLifecycleRaceCleanup, 10*time.Millisecond,
		"timeout must complete rollback before the competing COMMIT is sent")

	require.NoError(t, SendCommit(t, conn))
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(transactionLifecycleRaceCleanup)))
	code, _, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOutClientConfiguration", code)

	fresh := openBoltTestConn(t, port)
	records := runBoltQueryAndCollectRecords(t, fresh,
		"MATCH (r:Repository {repo_id: 'timeout-wins'}) RETURN count(r)")
	require.Equal(t, [][]any{{int64(0)}}, records)
}
