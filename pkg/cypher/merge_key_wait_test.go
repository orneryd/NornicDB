package cypher

import (
	"context"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMergeKeyWaitEndsAtTransactionTimeoutOnEveryRoute (NOR-284): a MERGE
// waiting for a key another transaction holds, with no deadline of its own,
// fails at the engine's transaction timeout with TransactionTimedOut on every
// route that admits MERGE keys: a single MERGE, one that writes the node, the
// pipeline (UNWIND … MERGE), a MERGE after MATCH, and a MERGE path. The
// statement's transaction ends and the key stays the holder's.
func TestMergeKeyWaitEndsAtTransactionTimeoutOnEveryRoute(t *testing.T) {
	const timeout = 200 * time.Millisecond
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	ctx := context.Background()
	setup := NewStorageExecutor(store)
	for _, statement := range []string{
		"CREATE CONSTRAINT w_k FOR (w:W) REQUIRE w.k IS UNIQUE",
		"CREATE (:Anchor)",
	} {
		_, err := setup.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}

	// The holder sets its own transaction timeout, which replaces the
	// server's: it stays idle across every wait without being ended by the
	// server's transaction timeout (NOR-284 follow-up, transactionEnd).
	holder := NewStorageExecutor(store)
	_, err = holder.Execute(WithClientTransactionTimeout(ctx), "BEGIN", nil)
	require.NoError(t, err)
	_, err = holder.Execute(ctx, "MERGE (w:W {k: 1}) RETURN w.k", nil)
	require.NoError(t, err)

	for _, query := range []string{
		"MERGE (w:W {k: 1}) RETURN w.k",
		"MERGE (w:W {k: 1}) SET w.seen = true RETURN w.k",
		"UNWIND [2, 1] AS i MERGE (w:W {k: i}) RETURN w.k",
		"MATCH (a:Anchor) MERGE (w:W {k: 1}) RETURN w.k",
		"UNWIND [1] AS i MERGE (a:W {k: i})-[:R]->(b:W {k: i + 100}) RETURN a.k",
	} {
		for _, explicit := range []bool{false, true} {
			session := NewStorageExecutor(store)
			if explicit {
				_, err := session.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
			}
			started := time.Now()
			_, err := session.Execute(ctx, query, nil)
			elapsed := time.Since(started)
			require.ErrorIs(t, err, storage.ErrLockWaitTimeout, "%s explicit=%v", query, explicit)
			code, _ := nornicerrors.Neo4jStatus(err)
			require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", code, "%s explicit=%v", query, explicit)
			require.GreaterOrEqual(t, elapsed, timeout, query)
			require.Less(t, elapsed, timeout+5*time.Second, query)
			if explicit {
				_, err := session.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			}
		}
	}
	result, err := setup.Execute(ctx, "MATCH (w:W) RETURN count(w)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows, "no timed-out statement wrote")

	_, err = holder.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	result, err = setup.Execute(ctx, "UNWIND [1, 2] AS i MERGE (w:W {k: i}) RETURN count(w)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
}

// mergeKeyWaitExecutors returns executors on an engine whose MERGE key waits
// end at timeout, with a UNIQUE constraint on (:U {k}): setup, and holder and
// waiter in explicit transactions, holder holding k = 1. Both transactions
// set their own timeout, which replaces the server's, so neither is ended
// for staying idle (transactionEnd): these tests are about the wait.
func mergeKeyWaitExecutors(t *testing.T, timeout time.Duration) (setup, holder, waiter *StorageExecutor) {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	setup, holder, waiter = NewStorageExecutor(store), NewStorageExecutor(store), NewStorageExecutor(store)
	_, err = setup.Execute(t.Context(), "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	for _, exec := range []*StorageExecutor{holder, waiter} {
		_, err = exec.Execute(WithClientTransactionTimeout(t.Context()), "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
	}
	_, err = holder.Execute(t.Context(), "MERGE (:U {k: 1})", nil)
	require.NoError(t, err)
	return setup, holder, waiter
}

// TestMergeKeyTimeoutEndsTheExplicitTransaction (NOR-284, review round 1): an
// explicit transaction whose MERGE hits the server's transaction timeout is
// rolled back at once, though its client hasn't ended it: an unrelated
// schema change, which needs every open transaction's write admission,
// proceeds once the key's holder ends. Its later statements are refused and
// its COMMIT rolls it back, as for any failed transaction.
func TestMergeKeyTimeoutEndsTheExplicitTransaction(t *testing.T) {
	setup, holder, waiter := mergeKeyWaitExecutors(t, 150*time.Millisecond)
	_, err := waiter.Execute(t.Context(), "MERGE (:U {k: 1})", nil)
	require.ErrorIs(t, err, storage.ErrLockWaitTimeout)
	_, err = holder.Execute(t.Context(), "ROLLBACK", nil)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = setup.Execute(ctx, "CREATE CONSTRAINT unrelated_k FOR (n:Unrelated) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err, "the timed-out transaction kept its write admission until its client ended it")

	_, err = waiter.Execute(t.Context(), "RETURN 1", nil)
	code, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.TransientError.Transaction.QueryExecutionFailedOnTransaction", code, "%v", err)
	_, err = waiter.Execute(t.Context(), "COMMIT", nil)
	code, _ = nornicerrors.Neo4jStatus(err)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionMarkedAsFailed", code, "%v", err)
	result, err := setup.Execute(t.Context(), "MATCH (u:U) RETURN count(u)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

// TestMergeKeyWaitBudgetIgnoresTransactionAge (NOR-284): the timeout bounds
// each wait, not the transaction's age, so a transaction already older than
// the timeout still waits for a brief holder and proceeds.
func TestMergeKeyWaitBudgetIgnoresTransactionAge(t *testing.T) {
	_, holder, waiter := mergeKeyWaitExecutors(t, 400*time.Millisecond)
	time.Sleep(500 * time.Millisecond)
	waits := runtimeFixtureOwner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 1)
	done := make(chan error, 1)
	go func() {
		_, err := waiter.Execute(t.Context(), "MERGE (:U {k: 1})", nil)
		done <- err
	}()
	<-waits
	_, err := holder.Execute(t.Context(), "ROLLBACK", nil)
	require.NoError(t, err)
	require.NoError(t, <-done)
}
