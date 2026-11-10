package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireHTTPSchemaChangeWithin runs an unrelated CREATE CONSTRAINT, which
// needs every open transaction's write admission, under a deadline of within.
func requireHTTPSchemaChangeWithin(t *testing.T, server *Server, name string, within time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), within)
	defer cancel()
	_, err := server.db.ExecuteCypher(ctx, fmt.Sprintf("CREATE CONSTRAINT %s FOR (n:%s) REQUIRE n.k IS UNIQUE", name, name), nil)
	require.NoError(t, err, what)
}

// TestHTTPIdleHolderEndsAtTransactionTimeout (NOR-284 follow-up of #1756): an
// explicit HTTP transaction that stays idle past the server's transaction
// timeout is ended at that timeout, though its client sends nothing: an
// unrelated schema change proceeds, a MERGE waiting for its key proceeds
// rather than timing out, and its COMMIT is refused with TransactionTimedOut.
func TestHTTPIdleHolderEndsAtTransactionTimeout(t *testing.T) {
	const timeout = time.Second
	server, token, _ := setupKeyWaitServer(t, timeout, 0)

	t.Run("unrelated schema change", func(t *testing.T) {
		holder := holdHTTPKeys(t, server, token, "idle-schema")
		time.Sleep(timeout + 300*time.Millisecond)
		requireHTTPSchemaChangeWithin(t, server, "HTTPIdleSchema", 750*time.Millisecond, "the idle holder kept its write admission past its timeout")
		requireHTTPCode(t, holder.end(true), "Neo.ClientError.Transaction.TransactionTimedOut", "COMMIT of the timed-out holder")
		require.Equal(t, int64(0), countHTTPDocs(t, server, "idle-schema"))
	})

	t.Run("waiting MERGE", func(t *testing.T) {
		holder := holdHTTPKeys(t, server, token, "idle-waited")
		time.Sleep(timeout / 2)
		result := awaitHTTPKeyWaiter(t, startHTTPKeyWaiter(context.Background(), t, server, token, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": "idle-waited"}), func() { holder.end(false) })
		require.Empty(t, result.errs, "the MERGE timed out behind an idle holder past its timeout")
		require.Less(t, result.elapsed, timeout, "the waiter proceeded only when its own wait timed out")
		requireHTTPCode(t, holder.end(true), "Neo.ClientError.Transaction.TransactionTimedOut", "COMMIT of the timed-out holder")
		require.Equal(t, int64(1), countHTTPDocs(t, server, "idle-waited"))
	})
}

// TestHTTPTerminateEndsAnIdleHolderAtOnce (NOR-284 follow-up of #1756):
// TERMINATE TRANSACTIONS on an idle HTTP transaction releases its key and
// write admission before it returns: the waiting MERGE proceeds at once, an
// unrelated schema change proceeds, and the holder's COMMIT is refused with
// Terminated.
func TestHTTPTerminateEndsAnIdleHolderAtOnce(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, 5*time.Second, 0)
	waits := fixtureowner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 64)
	holder := holdHTTPKeys(t, server, token, "terminated")
	drainHTTPKeyWaits(waits)
	done := startHTTPKeyWaiter(context.Background(), t, server, token, false, "MERGE (n:Doc {id: $k})", map[string]any{"k": "terminated"})
	fixtureowner.WaitForBarrier(t, waits, httpKeyWaitStuckAfter, nil, "the waiter never waited for the key")
	rows, err := server.db.ExecuteCypher(t.Context(), "SHOW TRANSACTIONS YIELD transactionId, currentQuery WHERE currentQuery = '' RETURN transactionId", nil)
	require.NoError(t, err)
	require.Len(t, rows.Rows, 1)
	_, err = server.db.ExecuteCypher(t.Context(), fmt.Sprintf("TERMINATE TRANSACTIONS '%s'", rows.Rows[0][0]), nil)
	require.NoError(t, err)
	terminated := time.Now()
	result := awaitHTTPKeyWaiter(t, done, func() { holder.end(false) })
	require.Empty(t, result.errs, "the waiter didn't get the terminated holder's key")
	require.Less(t, time.Since(terminated), time.Second, "the key was released only later")
	requireHTTPSchemaChangeWithin(t, server, "HTTPAfterTerminate", 750*time.Millisecond, "the terminated holder kept its write admission")
	requireHTTPCode(t, holder.end(true), "Neo.ClientError.Transaction.Terminated", "COMMIT of the terminated holder")
	require.Equal(t, int64(1), countHTTPDocs(t, server, "terminated"))
}

// TestHTTPWorkingHolderIsNotEndedEarly: an HTTP transaction that keeps
// running statements is not ended by the server's transaction timeout,
// however long it runs, and commits.
func TestHTTPWorkingHolderIsNotEndedEarly(t *testing.T) {
	const timeout = time.Second
	server, token, _ := setupKeyWaitServer(t, timeout, 0)
	holder := holdHTTPKeys(t, server, token, "working")
	for i := 0; i < 8; i++ {
		time.Sleep(timeout / 5)
		require.Empty(t, holder.run("MERGE (n:Doc {id: $k})", map[string]any{"k": fmt.Sprintf("working-%d", i)}), "step %d", i)
	}
	require.Empty(t, holder.end(true), "the working holder was ended")
	require.Equal(t, int64(1), countHTTPDocs(t, server, "working-7"))
}

// TestHTTPConfiguredTimeoutOutlivesTheSessionExpiry (review round 1 of
// #1763): the HTTP transport session doesn't end a transaction before the
// configured transaction timeout: an idle transaction past the session's
// 30 s expiry, within a 60 s timeout, still commits.
func TestHTTPConfiguredTimeoutOutlivesTheSessionExpiry(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, time.Minute, 0)
	holder := holdHTTPKeys(t, server, token, "http-minute")
	time.Sleep(32 * time.Second)
	require.Empty(t, holder.end(true), "the configured 60 s timeout has not elapsed")
	require.Equal(t, int64(1), countHTTPDocs(t, server, "http-minute"))
}

// TestHTTPCompositeConfiguredTimeoutOutlivesTheSessionExpiry (review round 2
// of #1763): a composite HTTP transaction follows the same configured
// transaction timeout as any other: idle 32 s, past the session's 30 s
// expiry, within a 60 s timeout, it still commits.
func TestHTTPCompositeConfiguredTimeoutOutlivesTheSessionExpiry(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, time.Minute, 0)
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "httpendleaf"}))
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "httpendroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "httpendleaf", Type: "local", AccessMode: "read_write"}}))
	status, opened := lifecycleTransactionPost(t, server, token, "/db/httpendroot/tx", "CALL { USE httpendroot.l CREATE (:HTTPEndMinute) RETURN 1 AS v } RETURN v")
	require.Equal(t, http.StatusCreated, status)
	require.Empty(t, opened.Errors)
	time.Sleep(32 * time.Second)
	status, committed := lifecycleTransactionPost(t, server, token, opened.Commit)
	require.Equal(t, http.StatusOK, status, "%+v", committed.Errors)
	require.Empty(t, committed.Errors, "the configured 60 s timeout governs the composite transaction")
}

// TestHTTPEndedCompositeTransactionRefusesWrites (review round 4 of #1763):
// an HTTP composite transaction that its timeout ended refuses a later
// Fabric write on a constituent with TransactionTimedOut, so it takes no key
// another writer would wait for. As for any statement that fails in an HTTP
// transaction, the HTTP API then rolls the transaction back and retires it,
// as Neo4j's does: the COMMIT after it finds no transaction.
func TestHTTPEndedCompositeTransactionRefusesWrites(t *testing.T) {
	server, token, _ := setupKeyWaitServer(t, 400*time.Millisecond, 0)
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "endedroot", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"}}))
	status, opened := lifecycleTransactionPost(t, server, token, "/db/endedroot/tx")
	require.Equal(t, http.StatusCreated, status)
	require.Empty(t, opened.Errors)
	time.Sleep(900 * time.Millisecond)
	_, written := lifecycleTransactionPost(t, server, token, strings.TrimSuffix(opened.Commit, "/commit"), "USE endedroot.l MERGE (:Doc {id: 'after-end'}) RETURN 1 AS k")
	require.NotEmpty(t, written.Errors, "the ended transaction accepted a write")
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", written.Errors[0].Code)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := server.db.ExecuteCypher(ctx, "MERGE (:Doc {id: 'after-end'})", nil)
	require.NoError(t, err, "the ended transaction took a key")
	_, committed := lifecycleTransactionPost(t, server, token, opened.Commit)
	require.NotEmpty(t, committed.Errors)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", committed.Errors[0].Code)
}
