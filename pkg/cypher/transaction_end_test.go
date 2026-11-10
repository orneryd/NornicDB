package cypher

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// idleEndStore is an engine whose server transaction timeout is timeout
// (zero: none), with a UNIQUE constraint on (:U {k}).
func idleEndStore(t *testing.T, timeout time.Duration) storage.Engine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: timeout}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "test")
	_, err = NewStorageExecutor(store).Execute(t.Context(), "CREATE CONSTRAINT u_k FOR (n:U) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	return store
}

// idleEndHolder opens an explicit transaction on store that MERGEs (:U {k}).
func idleEndHolder(t *testing.T, ctx context.Context, store storage.Engine, k int) *StorageExecutor {
	t.Helper()
	holder := NewStorageExecutor(store)
	_, err := holder.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	_, err = holder.Execute(t.Context(), "MERGE (:U {k: $k})", map[string]any{"k": k})
	require.NoError(t, err)
	return holder
}

// requireSchemaChangeProceeds runs an unrelated CREATE CONSTRAINT, which
// needs every open transaction's write admission, under a short deadline.
func requireSchemaChangeProceeds(t *testing.T, store storage.Engine, name, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
	defer cancel()
	_, err := NewStorageExecutor(store).Execute(ctx, fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:%s) REQUIRE n.k IS UNIQUE", name, name), nil)
	require.NoError(t, err, what)
}

func requireEndStatus(t *testing.T, err error, code string, msgAndArgs ...any) {
	t.Helper()
	require.Error(t, err, msgAndArgs...)
	got, _ := nornicerrors.Neo4jStatus(err)
	require.Equal(t, code, got, "%v %v", err, msgAndArgs)
}

func countU(t *testing.T, store storage.Engine, k int) int64 {
	t.Helper()
	result, err := NewStorageExecutor(store).Execute(t.Context(), "MATCH (u:U {k: $k}) RETURN count(u)", map[string]any{"k": k})
	require.NoError(t, err)
	return result.Rows[0][0].(int64)
}

// startUMerge MERGEs (:U {k}) auto-commit and reports its error and duration.
func startUMerge(store storage.Engine, k int) <-chan keyWaitOutcome {
	done := make(chan keyWaitOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		started := time.Now()
		_, err := NewStorageExecutor(store).Execute(ctx, "MERGE (:U {k: $k})", map[string]any{"k": k})
		done <- keyWaitOutcome{err: err, elapsed: time.Since(started)}
	}()
	return done
}

type keyWaitOutcome struct {
	err     error
	elapsed time.Duration
}

// TestIdleTransactionEndsAtTransactionTimeout (NOR-284 follow-up of #1756):
// an explicit transaction that stays idle for the server's transaction
// timeout is ended at that timeout, though its client sends nothing: its
// storage is rolled back, so an unrelated schema change proceeds and a MERGE
// waiting for its key proceeds rather than timing out; its later statements
// and its COMMIT are refused with TransactionTimedOut, and nothing it wrote
// is kept.
func TestIdleTransactionEndsAtTransactionTimeout(t *testing.T) {
	const timeout = 300 * time.Millisecond
	store := idleEndStore(t, timeout)
	waits := runtimeFixtureOwner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 8)

	statementHolder := idleEndHolder(t, t.Context(), store, 1)
	holder := idleEndHolder(t, t.Context(), store, 2)
	time.Sleep(timeout / 2)
	waiter := startUMerge(store, 2)
	<-waits
	result := <-waiter
	require.NoError(t, result.err, "the MERGE waited for an idle transaction past its timeout and %v", result.elapsed)
	require.Less(t, result.elapsed, timeout, "the waiter proceeded only when its own wait timed out")
	requireSchemaChangeProceeds(t, store, "Unrelated", "the idle transaction kept its write admission past its timeout")

	_, err := statementHolder.Execute(t.Context(), "RETURN 1", nil)
	requireEndStatus(t, err, nornicerrors.TransactionTimedOut, "a statement of the timed-out transaction")
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	requireEndStatus(t, err, nornicerrors.TransactionTimedOut, "COMMIT of the timed-out transaction")
	require.Equal(t, int64(0), countU(t, store, 1), "the timed-out transaction's write was kept")
	require.Equal(t, int64(1), countU(t, store, 2))
}

// TestIdleTimeoutEndsAFailedTransaction: a transaction a statement already
// failed in (which keeps its storage for ROLLBACK) is still ended by the
// timeout once idle.
func TestIdleTimeoutEndsAFailedTransaction(t *testing.T) {
	const timeout = 200 * time.Millisecond
	store := idleEndStore(t, timeout)
	holder := idleEndHolder(t, t.Context(), store, 1)
	_, err := holder.Execute(t.Context(), "CREATE (:U {k: 1})", nil)
	require.Error(t, err, "a constraint violation fails the transaction")
	time.Sleep(timeout + 200*time.Millisecond)
	requireSchemaChangeProceeds(t, store, "AfterFailed", "the failed, idle transaction kept its write admission past its timeout")
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	requireEndStatus(t, err, nornicerrors.TransactionTimedOut)
}

// TestWorkingTransactionOutlivesTheTransactionTimeout: the timeout counts
// idle time, so a transaction that keeps running statements for longer than
// the timeout is not ended, and commits.
func TestWorkingTransactionOutlivesTheTransactionTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond
	store := idleEndStore(t, timeout)
	holder := idleEndHolder(t, t.Context(), store, 1)
	for i := 0; i < 8; i++ {
		time.Sleep(timeout / 4)
		_, err := holder.Execute(t.Context(), "MERGE (:U {k: $k})", map[string]any{"k": 100 + i})
		require.NoError(t, err, "statement %d", i)
	}
	_, err := holder.Execute(t.Context(), "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), countU(t, store, 107))
}

// TestClientTransactionTimeoutReplacesTheServerTimeout: a transaction whose
// client set its own timeout is not ended by the server's.
func TestClientTransactionTimeoutReplacesTheServerTimeout(t *testing.T) {
	const timeout = 150 * time.Millisecond
	store := idleEndStore(t, timeout)
	holder := idleEndHolder(t, WithClientTransactionTimeout(t.Context()), store, 1)
	time.Sleep(3 * timeout)
	_, err := holder.Execute(t.Context(), "COMMIT", nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), countU(t, store, 1))
}

// TestTerminateEndsAnIdleTransactionAtOnce (NOR-284 follow-up of #1756):
// TERMINATE TRANSACTIONS on an idle explicit transaction rolls back its
// storage before it returns: a MERGE waiting for its key proceeds and an
// unrelated schema change proceeds, though its client sends nothing; its
// later statements and COMMIT are refused with Terminated.
func TestTerminateEndsAnIdleTransactionAtOnce(t *testing.T) {
	store := idleEndStore(t, time.Minute)
	waits := runtimeFixtureOwner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 8)
	for _, then := range []string{"COMMIT", "RETURN 1"} {
		holder := idleEndHolder(t, t.Context(), store, 1)
		waiter := startUMerge(store, 1)
		<-waits
		result, err := NewStorageExecutor(store).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s' YIELD message RETURN message", holder.txContext.running.id()), nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"Transaction terminated."}}, result.Rows)
		select {
		case outcome := <-waiter:
			require.NoError(t, outcome.err)
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: the MERGE still waits for the terminated idle transaction's key", then)
		}
		requireSchemaChangeProceeds(t, store, "Terminated"+map[string]string{"COMMIT": "Commit", "RETURN 1": "Return"}[then], "the terminated idle transaction kept its write admission")
		_, err = holder.Execute(t.Context(), then, nil)
		requireEndStatus(t, err, transactionTerminatedCode, then)
		require.Equal(t, int64(1), countU(t, store, 1))
		_, err = NewStorageExecutor(store).Execute(t.Context(), "MATCH (u:U {k: 1}) DELETE u", nil)
		require.NoError(t, err)
	}
}

// TestTerminationRacingTheOwnerReleasesTheTransaction: TERMINATE and the
// transaction's own statements and COMMIT race. Whoever wins, the
// transaction ends exactly once and leaves nothing held: its key and the
// write admission are free afterwards.
func TestTerminationRacingTheOwnerReleasesTheTransaction(t *testing.T) {
	store := idleEndStore(t, 0)
	for i := 0; i < 40; i++ {
		holder := idleEndHolder(t, t.Context(), store, 1000+i)
		id := holder.txContext.running.id()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				if _, err := holder.Execute(t.Context(), "MERGE (u:U {k: $k}) SET u.n = $j", map[string]any{"k": 1000 + i, "j": j}); err != nil {
					break
				}
			}
			_, _ = holder.Execute(t.Context(), "COMMIT", nil)
		}()
		go func() {
			defer wg.Done()
			_, _ = NewStorageExecutor(store).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", id), nil)
		}()
		wg.Wait()
		_, _ = holder.Execute(t.Context(), "ROLLBACK", nil)
		require.False(t, holder.HasActiveTransaction(), "round %d", i)
		outcome := <-startUMerge(store, 1000+i)
		require.NoError(t, outcome.err, "round %d", i)
		require.Less(t, outcome.elapsed, 2*time.Second, "round %d: the key stayed held", i)
	}
	requireSchemaChangeProceeds(t, store, "AfterRaces", "a raced transaction kept its write admission")
}

// TestWorkingCompositeTransactionKeepsItsParticipant (review round 1 of
// #1763): a composite transaction that keeps writing its local constituent
// for longer than the server's transaction timeout keeps its participant's
// sub-transaction: the participant has no idle end of its own, the
// composite transaction decides it.
func TestWorkingCompositeTransactionKeepsItsParticipant(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: 300 * time.Millisecond}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed))
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "end_shard"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "end_cmp", []multidb.ConstituentRef{
		{Alias: "sh", DatabaseName: "end_shard", Type: "local", AccessMode: "read_write"},
	}))
	store, err := mgr.GetStorage("end_cmp")
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	for i := 0; i < 8; i++ {
		_, err = exec.Execute(ctx, "CALL { USE end_cmp.sh CREATE (:EndWorking {i: $i}) RETURN count(*) AS n } RETURN n", map[string]any{"i": i})
		require.NoError(t, err, "statement %d; no idle interval exceeds 75ms", i)
		time.Sleep(75 * time.Millisecond)
	}
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err, "a working composite transaction must not lose its participant")
	shard, err := mgr.GetStorage("end_shard")
	require.NoError(t, err)
	result, err := NewStorageExecutor(shard).Execute(ctx, "MATCH (n:EndWorking) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(8), result.Rows[0][0])
}

// endOwnerManager is a database manager whose transaction timeout is
// 300 ms.
func endOwnerManager(t *testing.T) *multidb.DatabaseManager {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		InMemory:      true,
		EngineOptions: storage.EngineOptions{LockWait: storage.LockWaitOptions{Timeout: 300 * time.Millisecond}},
	}))
	require.NoError(t, err)
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, &storage.MemoryEngine{BadgerEngine: engine}, storage.ErrStorageClosed))
	mgr, err := multidb.NewDatabaseManager(t.Context(), base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, mgr.Close()) })
	return mgr
}

// TestIdleCompositeTransactionEnds (review round 2 of #1763): an explicit
// composite transaction is a client transaction like any other. Left idle
// past the configured timeout, or terminated, it releases its participant's
// key at once, and its COMMIT is refused with the termination's status.
func TestIdleCompositeTransactionEnds(t *testing.T) {
	for _, reason := range []string{"timeout", "terminate"} {
		t.Run(reason, func(t *testing.T) {
			mgr := endOwnerManager(t)
			require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "endleaf"}))
			require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "endroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "endleaf", Type: "local", AccessMode: "read_write"}}))
			leaf, err := mgr.GetStorage("endleaf")
			require.NoError(t, err)
			_, err = NewStorageExecutor(leaf).Execute(t.Context(), "CREATE CONSTRAINT end_u FOR (n:EndHold) REQUIRE n.k IS UNIQUE", nil)
			require.NoError(t, err)
			root, err := mgr.GetStorage("endroot")
			require.NoError(t, err)
			holder := NewStorageExecutor(root)
			holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
			_, err = holder.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
			_, err = holder.Execute(t.Context(), "CALL { USE endroot.l MERGE (n:EndHold {k: 1}) RETURN n.k AS k } RETURN k", nil)
			require.NoError(t, err)
			code := nornicerrors.TransactionTimedOut
			if reason == "timeout" {
				time.Sleep(700 * time.Millisecond)
			} else {
				code = transactionTerminatedCode
				_, err = NewStorageExecutor(leaf).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", holder.txContext.running.id()), nil)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_, err = NewStorageExecutor(leaf).Execute(ctx, "MERGE (:EndHold {k: 1})", nil)
			require.NoError(t, err, "the ended composite transaction kept its participant's key")
			_, err = holder.Execute(t.Context(), "COMMIT", nil)
			requireEndStatus(t, err, code, "COMMIT after "+reason)
		})
	}
}

// TestAdministrationTransactionKeepsItsEndOwner (review round 2 of #1763):
// the first administration statement moves the transaction to the system
// database's snapshot, but the client's transaction stays the same: it keeps
// its SHOW/TERMINATE entry and its timeout, so idle past the timeout its
// COMMIT is refused with TransactionTimedOut.
func TestAdministrationTransactionKeepsItsEndOwner(t *testing.T) {
	mgr := endOwnerManager(t)
	store, err := mgr.GetStorage("nornic")
	require.NoError(t, err)
	holder := NewStorageExecutor(store)
	holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	_, err = holder.Execute(t.Context(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	registered := holder.txContext.running
	require.NotNil(t, registered)
	_, err = holder.Execute(t.Context(), "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.Same(t, registered, holder.txContext.running, "administration routing replaced the client's registered transaction")
	time.Sleep(700 * time.Millisecond)
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	requireEndStatus(t, err, nornicerrors.TransactionTimedOut, "administration transaction COMMIT past its timeout")
}

// TestTerminatedHandleSurvivesItsStatements (review round 2 of #1763): a
// statement refused because the transaction was ended (timeout or
// TERMINATE) leaves the client's failed transaction in place: a later write
// is refused too and commits nothing, and COMMIT reports the termination.
func TestTerminatedHandleSurvivesItsStatements(t *testing.T) {
	for _, reason := range []string{"timeout", "terminate"} {
		t.Run(reason, func(t *testing.T) {
			store := idleEndStore(t, 300*time.Millisecond)
			holder := idleEndHolder(t, t.Context(), store, 1)
			code := nornicerrors.TransactionTimedOut
			if reason == "timeout" {
				time.Sleep(700 * time.Millisecond)
			} else {
				code = transactionTerminatedCode
				_, err := NewStorageExecutor(store).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", holder.txContext.running.id()), nil)
				require.NoError(t, err)
			}
			_, err := holder.Execute(t.Context(), "RETURN 1", nil)
			requireEndStatus(t, err, code, "the first statement")
			require.True(t, holder.HasActiveTransaction(), "a refused statement must not detach the client's failed transaction")
			_, err = holder.Execute(t.Context(), "CREATE (:U {k: 99})", nil)
			requireEndStatus(t, err, code, "a later write")
			require.Equal(t, int64(0), countU(t, store, 99), "the later write auto-committed")
			_, err = holder.Execute(t.Context(), "COMMIT", nil)
			requireEndStatus(t, err, code, "COMMIT")
			require.False(t, holder.HasActiveTransaction(), "COMMIT retires the failed transaction")
		})
	}
}

// TestTerminateOfAListedParticipantEndsItsOwner (review round 3 of #1763): a
// composite transaction's local participant is listed by SHOW TRANSACTIONS
// with an id of its own, but it is never an end owner. TERMINATE of that id
// goes to the composite transaction, which ends at once: the participant's
// key is released, SHOW lists the participant as terminated, and the
// composite's COMMIT is refused with Terminated. The client's own timeout
// keeps the server's out of the measurement.
func TestTerminateOfAListedParticipantEndsItsOwner(t *testing.T) {
	mgr := endOwnerManager(t)
	require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "partleaf"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "partroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "partleaf", Type: "local", AccessMode: "read_write"}}))
	leaf, err := mgr.GetStorage("partleaf")
	require.NoError(t, err)
	observer := NewStorageExecutor(leaf)
	_, err = observer.Execute(t.Context(), "CREATE CONSTRAINT part_u FOR (n:PartHold) REQUIRE n.k IS UNIQUE", nil)
	require.NoError(t, err)
	root, err := mgr.GetStorage("partroot")
	require.NoError(t, err)
	holder := NewStorageExecutor(root)
	holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	_, err = holder.Execute(WithClientTransactionTimeout(t.Context()), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	_, err = holder.Execute(t.Context(), "CALL { USE partroot.l MERGE (n:PartHold {k: 1}) RETURN n.k AS k } RETURN k", nil)
	require.NoError(t, err)
	listed, err := observer.Execute(t.Context(), "SHOW TRANSACTIONS YIELD transactionId, database, currentQuery WHERE database = 'partleaf' AND currentQuery = '' RETURN transactionId", nil)
	require.NoError(t, err)
	require.Len(t, listed.Rows, 1)
	id := listed.Rows[0][0].(string)
	require.NotEqual(t, holder.txContext.running.id(), id, "the participant is listed with an id of its own")
	terminated, err := observer.Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", id), nil)
	require.NoError(t, err)
	require.Equal(t, "Transaction terminated.", terminated.Rows[0][2])
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err = observer.Execute(ctx, "MERGE (:PartHold {k: 1})", nil)
	require.NoError(t, err, "TERMINATE of the listed participant left its key held")
	status, err := observer.Execute(t.Context(), fmt.Sprintf("SHOW TRANSACTIONS YIELD transactionId, status WHERE transactionId = '%s' RETURN status", id), nil)
	require.NoError(t, err)
	require.Len(t, status.Rows, 1)
	require.Equal(t, "Terminated with reason: Transaction terminated.", status.Rows[0][0])
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	requireEndStatus(t, err, transactionTerminatedCode, "COMMIT of the composite transaction")
}

// TestEndedCompositeTransactionRefusesEveryRoute (review round 4 of #1763):
// after a composite transaction ends (timeout or TERMINATE), its next
// statements are refused with the end's status on every route: a Fabric
// statement on a constituent (read or write) as much as one the composite
// runs itself. Each is stopped at the transaction's statement gate, so none
// opens a participant or takes a key; COMMIT is refused and nothing was
// written.
func TestEndedCompositeTransactionRefusesEveryRoute(t *testing.T) {
	for _, reason := range []string{"terminate", "timeout"} {
		t.Run(reason, func(t *testing.T) {
			mgr := endOwnerManager(t)
			require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "gateleaf"}))
			require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "gateroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "gateleaf", Type: "local", AccessMode: "read_write"}}))
			root, err := mgr.GetStorage("gateroot")
			require.NoError(t, err)
			leaf, err := mgr.GetStorage("gateleaf")
			require.NoError(t, err)
			holder := NewStorageExecutor(root)
			holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
			_, err = holder.Execute(t.Context(), "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
			code := transactionTerminatedCode
			if reason == "timeout" {
				code = nornicerrors.TransactionTimedOut
				time.Sleep(700 * time.Millisecond)
			} else {
				_, err = NewStorageExecutor(leaf).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", holder.txContext.running.id()), nil)
				require.NoError(t, err)
			}
			for _, query := range []string{"USE gateroot.l RETURN 1 AS k", "USE gateroot.l CREATE (:GateRefused) RETURN 1 AS k", "CALL { USE gateroot.l CREATE (:GateRefused) RETURN 1 AS k } RETURN k", "RETURN 1"} {
				_, err := holder.Execute(t.Context(), query, nil)
				requireEndStatus(t, err, code, query)
			}
			_, err = holder.Execute(t.Context(), "COMMIT", nil)
			requireEndStatus(t, err, code, "COMMIT")
			counted, err := NewStorageExecutor(leaf).Execute(t.Context(), "MATCH (n:GateRefused) RETURN count(n)", nil)
			require.NoError(t, err)
			require.Equal(t, int64(0), counted.Rows[0][0])
		})
	}
}

// TestTerminateCancelsARunningCompositeStatement (review round 4 of #1763):
// a composite statement is registered at the transaction's statement gate
// like any other, so TERMINATE TRANSACTIONS cancels it while it runs, and it
// fails with Terminated instead of completing.
func TestTerminateCancelsARunningCompositeStatement(t *testing.T) {
	mgr := endOwnerManager(t)
	require.NoError(t, mgr.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "busyleaf"}))
	require.NoError(t, mgr.CreateCompositeDatabase(t.Context(), "busyroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "busyleaf", Type: "local", AccessMode: "read_write"}}))
	root, err := mgr.GetStorage("busyroot")
	require.NoError(t, err)
	leaf, err := mgr.GetStorage("busyleaf")
	require.NoError(t, err)
	holder := NewStorageExecutor(root)
	holder.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	_, err = holder.Execute(WithClientTransactionTimeout(t.Context()), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = holder.Execute(context.Background(), "ROLLBACK", nil) })
	started := make(chan struct{})
	newStatementGate(func(ctx context.Context) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}).install(t)
	done := make(chan error, 1)
	go func() {
		_, err := holder.Execute(t.Context(), "USE busyroot.l CREATE (:BusyWrite) RETURN "+statementGateFunction+"(1) AS k", nil)
		done <- err
	}()
	<-started
	_, err = NewStorageExecutor(leaf).Execute(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", holder.txContext.running.id()), nil)
	require.NoError(t, err)
	select {
	case statementErr := <-done:
		requireEndStatus(t, statementErr, transactionTerminatedCode, "the terminated composite statement")
	case <-time.After(3 * time.Second):
		t.Fatal("TERMINATE did not cancel the running composite statement")
	}
	_, err = holder.Execute(t.Context(), "COMMIT", nil)
	requireEndStatus(t, err, transactionTerminatedCode, "COMMIT")
	counted, err := NewStorageExecutor(leaf).Execute(t.Context(), "MATCH (n:BusyWrite) RETURN count(n)", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), counted.Rows[0][0])
}
