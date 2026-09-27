package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestShowDefaultAndHomeDatabase: SHOW DEFAULT / HOME DATABASE list the
// default database with SHOW DATABASES' columns except default and home, as
// in Neo4j 5.26 (#718).
func TestShowDefaultAndHomeDatabase(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	manager := newMockDatabaseManager()
	require.NoError(t, manager.CreateDatabase("nornic"))
	require.NoError(t, manager.CreateDatabase("tenant_a"))
	exec.SetDatabaseManager(manager)
	ctx := context.Background()
	for _, query := range []string{"SHOW DEFAULT DATABASE", "SHOW HOME DATABASE"} {
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Equal(t, showSingleDatabaseDefaultColumns, result.Columns, query)
		require.Len(t, result.Rows, 1, query)
		require.Equal(t, "nornic", result.Rows[0][0], query)

		result, err = exec.Execute(ctx, query+" YIELD *", nil)
		require.NoError(t, err, query)
		require.NotContains(t, result.Columns, "default", query)
		require.NotContains(t, result.Columns, "home", query)
		require.Contains(t, result.Columns, "databaseID", query)
	}
	result, err := exec.Execute(ctx, "SHOW DEFAULT DATABASE YIELD name WHERE name = 'x' RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}

// TestShowUsersAndCurrentUser: SHOW USERS lists the user directory by name,
// SHOW CURRENT USER the signed-in user; without a signed-in user SHOW
// CURRENT USER lists nothing (#718).
func TestShowUsersAndCurrentUser(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	users := func() []UserListing {
		return []UserListing{
			{Name: "zoe", Roles: []string{"viewer"}},
			{Name: "admin", Roles: []string{"editor", "admin"}, PasswordChangeRequired: true},
			{Name: "sam", Suspended: true},
		}
	}
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{Users: users})
	signedIn := WithRequestIdentity(context.Background(), &RequestIdentity{
		Users: users,
		User:  &AuthenticatedUser{Name: "admin", Roles: []string{"admin", "editor"}},
	})

	result, err := exec.Execute(signedIn, "SHOW USERS", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"user", "roles", "passwordChangeRequired", "suspended", "home"}, result.Columns)
	require.Equal(t, [][]interface{}{
		{"admin", []string{"admin", "editor"}, true, false, nil},
		{"sam", []string{}, false, true, nil},
		{"zoe", []string{"viewer"}, false, false, nil},
	}, result.Rows)

	result, err = exec.Execute(signedIn, "SHOW CURRENT USER YIELD user, roles", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"admin", []string{"admin", "editor"}}}, result.Rows)

	result, err = exec.Execute(ctx, "SHOW CURRENT USER", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	result, err = exec.Execute(signedIn, "SHOW USERS YIELD user WHERE user STARTS WITH 's' RETURN count(*) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// TestUnsupportedAdministrationCommands: the administration commands NornicDB
// doesn't offer fail with Neo4j Community's status codes (#718).
func TestUnsupportedAdministrationCommands(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := context.Background()
	for query, code := range map[string]string{
		"SHOW ROLES":                 "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"SHOW PRIVILEGES":            "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"SHOW USER neo4j PRIVILEGES": "Neo.ClientError.Statement.UnsupportedAdministrationCommand",
		"SHOW SERVERS":               "Neo.ClientError.Statement.NotSystemDatabaseError",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Contains(t, statusText(err), code, query)
	}
}

// TestShowAndTerminateTransactions: SHOW TRANSACTIONS lists the running
// statement and open explicit transactions (an idle one with an empty
// currentQuery); TERMINATE TRANSACTIONS reports each id, and the terminated
// transaction's next statement and COMMIT fail with Neo4j's Terminated error
// and write nothing (#718).
func TestShowAndTerminateTransactions(t *testing.T) {
	engine := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(engine, "test")
	session := NewStorageExecutor(store)
	observer := NewStorageExecutor(store)
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		Connection: ClientConnection{ID: "bolt-7", Address: "10.0.0.1:5000", Protocol: "bolt"},
		User:       &AuthenticatedUser{Name: "alice"},
	})

	// The registry is process-wide; other tests' sessions run on other
	// connections.
	result, err := observer.Execute(ctx, "SHOW TRANSACTIONS YIELD currentQuery, username, connectionId, clientAddress, status WHERE connectionId = 'bolt-7'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"SHOW TRANSACTIONS YIELD currentQuery, username, connectionId, clientAddress, status WHERE connectionId = 'bolt-7'", "alice", "bolt-7", "10.0.0.1:5000", "Running"}}, result.Rows)

	_, err = session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "CREATE (:Terminated {x: 1})", nil)
	require.NoError(t, err)
	result, err = observer.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId, currentQuery, connectionId WHERE currentQuery = '' AND connectionId = 'bolt-7' RETURN transactionId", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	id := result.Rows[0][0].(string)
	require.Regexp(t, `^test-transaction-\d+$`, id)

	result, err = observer.Execute(ctx, "SHOW TRANSACTIONS $ids YIELD transactionId", map[string]interface{}{"ids": []interface{}{id, "nope"}})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{id}}, result.Rows)

	result, err = observer.Execute(ctx, "TERMINATE TRANSACTIONS $ids", map[string]interface{}{"ids": []interface{}{id, "test-transaction-0"}})
	require.NoError(t, err)
	require.Equal(t, []string{"transactionId", "username", "message"}, result.Columns)
	require.Equal(t, [][]interface{}{
		{id, "alice", "Transaction terminated."},
		{"test-transaction-0", nil, "Transaction not found."},
	}, result.Rows)

	_, err = session.Execute(ctx, "RETURN 1", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")
	_, err = session.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)

	result, err = observer.Execute(ctx, "MATCH (n:Terminated) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	result, err = observer.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId WHERE transactionId = $id RETURN count(*) AS c", map[string]interface{}{"id": id})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)

	// A terminated explicit transaction's COMMIT fails too.
	_, err = session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, err = observer.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId, currentQuery, connectionId WHERE currentQuery = '' AND connectionId = 'bolt-7' RETURN transactionId", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	_, err = observer.Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": result.Rows[0][0]})
	require.NoError(t, err)
	_, err = session.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")
	_, err = session.Execute(ctx, "RETURN 1 AS x", nil)
	require.NoError(t, err)

	_, err = observer.Execute(ctx, "TERMINATE TRANSACTIONS", nil)
	require.Error(t, err)
}

// TestTerminateTransactionIDsAreChecked: TERMINATE TRANSACTIONS reads each id
// as <databasename>-transaction-<number>, as Neo4j 5.26.30 does; SHOW
// TRANSACTIONS doesn't check its ids.
func TestTerminateTransactionIDsAreChecked(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	ctx := context.Background()
	for id, message := range map[string]string{
		"foo":                 "Could not parse id (expected format: <databasename>-transaction-<id>)",
		"neo4j-transaction-x": "Could not parse id (expected format: <databasename>-transaction-<id>)",
		"":                    "Could not parse id (expected format: <databasename>-transaction-<id>)",
		"ab-transaction-1":    "The provided database name must have a length between 3 and 63 characters.",
	} {
		_, err := exec.Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": id})
		require.Error(t, err, id)
		require.Contains(t, statusText(err), "Neo.ClientError.General.InvalidArguments", id)
		require.Contains(t, err.Error(), message, id)
	}
	result, err := exec.Execute(ctx, "TERMINATE TRANSACTION 'Abc-transaction-1'", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"abc-transaction-1", nil, "Transaction not found."}}, result.Rows)
	result, err = exec.Execute(ctx, "SHOW TRANSACTION 'foo'", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
}

// TestRunningStatementContextCancelsDerivedContexts: a context derived from
// a running statement's context (a subquery's, a storage call's) is
// cancelled when the statement is terminated, and still finds the
// statement's transaction and the request's values.
func TestRunningStatementContextCancelsDerivedContexts(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	type probeKey struct{}
	parent := context.WithValue(context.Background(), probeKey{}, "request")
	statementCtx, running, err := exec.withRunningStatement(parent, "RETURN 1")
	require.NoError(t, err)
	defer running.done()
	derived, cancelDerived := context.WithCancel(statementCtx)
	defer cancelDerived()
	require.Equal(t, "request", derived.Value(probeKey{}))
	require.Same(t, running.tx, derived.Value(ctxKeyRunningTransaction{}))

	_, found := runningTransactions.terminate(running.tx.id())
	require.True(t, found)
	select {
	case <-derived.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a context derived from the statement's was not cancelled by TERMINATE")
	}
}

// TestShowAndTerminateRunningAutoCommitStatement: an auto-commit statement is
// listed by SHOW TRANSACTIONS while it runs, TERMINATE TRANSACTIONS stops it
// (Neo4j's Terminated error, nothing written), and a statement answered from
// the result cache returns the same rows and leaves nothing listed. An
// auto-commit statement is registered after the result-cache lookup; a
// statement of an explicit transaction before it, so a terminated
// transaction refuses even a statement whose result is cached.
func TestShowAndTerminateRunningAutoCommitStatement(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	started := make(chan struct{}, 1)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name:      "test.waitForTermination",
		Signature: "test.waitForTermination() :: (done :: BOOLEAN)",
		Mode:      ProcedureModeRead,
		Returns:   []ProcedureColumn{{Name: "done", Type: "BOOLEAN"}},
	}, func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
		return &ExecuteResult{Columns: []string{"done"}, Rows: [][]interface{}{{true}}}, nil
	}))

	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	session := NewStorageExecutor(store)
	observer := NewStorageExecutor(store)
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		Connection: ClientConnection{ID: "bolt-autocommit", Address: "10.0.0.2:5000", Protocol: "bolt"},
		User:       &AuthenticatedUser{Name: "alice"},
	})
	listed := "SHOW TRANSACTIONS YIELD transactionId, currentQuery, connectionId, status WHERE connectionId = 'bolt-autocommit' AND NOT currentQuery STARTS WITH 'SHOW' RETURN transactionId, currentQuery, status"

	const statement = "CALL test.waitForTermination() YIELD done CREATE (:AutoCommitTerminated) RETURN done"
	finished := make(chan error, 1)
	go func() {
		_, err := session.Execute(ctx, statement, nil)
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the auto-commit statement did not start")
	}

	result, err := observer.Execute(ctx, listed, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, statement, result.Rows[0][1])
	require.Equal(t, "Running", result.Rows[0][2])
	id := result.Rows[0][0].(string)

	result, err = observer.Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": id})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{id, "alice", "Transaction terminated."}}, result.Rows)
	select {
	case err = <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("TERMINATE did not stop the auto-commit statement")
	}
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")

	result, err = observer.Execute(ctx, "MATCH (n:AutoCommitTerminated) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
	result, err = observer.Execute(ctx, listed, nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	// A statement answered from the result cache: the same rows, nothing
	// left listed.
	_, err = session.Execute(ctx, "CREATE (:Cached {v: 1})", nil)
	require.NoError(t, err)
	const cachedQuery = "MATCH (n:Cached) RETURN n.v AS v"
	first, err := session.Execute(ctx, cachedQuery, nil)
	require.NoError(t, err)
	hitsBefore, _, _, _, _ := session.cache.Stats()
	second, err := session.Execute(ctx, cachedQuery, nil)
	require.NoError(t, err)
	hitsAfter, _, _, _, _ := session.cache.Stats()
	require.Equal(t, hitsBefore+1, hitsAfter, "the second run is answered from the result cache")
	require.Equal(t, [][]interface{}{{int64(1)}}, first.Rows)
	require.Equal(t, first.Rows, second.Rows)
	result, err = observer.Execute(ctx, listed, nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	// A terminated explicit transaction refuses a statement whose result
	// is cached.
	_, err = session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, cachedQuery, nil)
	require.NoError(t, err)
	result, err = observer.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId, currentQuery, connectionId WHERE currentQuery = '' AND connectionId = 'bolt-autocommit' RETURN transactionId", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	_, err = observer.Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": result.Rows[0][0]})
	require.NoError(t, err)
	_, err = session.Execute(ctx, cachedQuery, nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")
	_, _ = session.Execute(ctx, "ROLLBACK", nil)
}

// TestTerminatedStatementFailsWhateverNoticedIt: a statement whose
// transaction TERMINATE TRANSACTIONS terminated while it ran fails with
// Neo4j's Neo.ClientError.Transaction.Terminated and stores nothing: an
// auto-commit read (which returned its rows), an auto-commit write (whose
// checked write path returned the bare cancellation), and a statement of
// an explicit transaction, whose COMMIT then fails too (#751).
func TestTerminatedStatementFailsWhateverNoticedIt(t *testing.T) {
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	started := make(chan struct{}, 1)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name:      "test.waitUntilTerminated",
		Signature: "test.waitUntilTerminated() :: (done :: BOOLEAN)",
		Mode:      ProcedureModeRead,
		Returns:   []ProcedureColumn{{Name: "done", Type: "BOOLEAN"}},
	}, func(ctx context.Context, exec *StorageExecutor, cypher string, args []interface{}) (*ExecuteResult, error) {
		started <- struct{}{}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Second):
		}
		return &ExecuteResult{Columns: []string{"done"}, Rows: [][]interface{}{{true}}}, nil
	}))

	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	observer := NewStorageExecutor(store)
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		Connection: ClientConnection{ID: "bolt-terminated", Address: "10.0.0.3:5000", Protocol: "bolt"},
	})
	terminateRunning := func(t *testing.T) {
		t.Helper()
		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("the statement did not start")
		}
		result, err := observer.Execute(ctx, "SHOW TRANSACTIONS YIELD transactionId, currentQuery, connectionId WHERE connectionId = 'bolt-terminated' AND currentQuery STARTS WITH 'CALL test.' RETURN transactionId", nil)
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		_, err = observer.Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": result.Rows[0][0]})
		require.NoError(t, err)
	}
	run := func(exec *StorageExecutor, statement string) <-chan error {
		finished := make(chan error, 1)
		go func() {
			_, err := exec.Execute(ctx, statement, nil)
			finished <- err
		}()
		return finished
	}
	requireTerminated := func(t *testing.T, finished <-chan error) {
		t.Helper()
		select {
		case err := <-finished:
			require.Error(t, err)
			require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")
		case <-time.After(10 * time.Second):
			t.Fatal("TERMINATE did not stop the statement")
		}
	}

	for _, statement := range []string{
		"CALL test.waitUntilTerminated() YIELD done RETURN done",
		"CALL test.waitUntilTerminated() YIELD done CREATE (:TerminatedWrite) RETURN done",
	} {
		finished := run(NewStorageExecutor(store), statement)
		terminateRunning(t)
		requireTerminated(t, finished)
	}

	session := NewStorageExecutor(store)
	_, err := session.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = session.Execute(ctx, "CREATE (:TerminatedWrite)", nil)
	require.NoError(t, err)
	finished := run(session, "CALL test.waitUntilTerminated() YIELD done RETURN done")
	terminateRunning(t)
	requireTerminated(t, finished)
	_, err = session.Execute(ctx, "COMMIT", nil)
	require.Error(t, err)
	require.Contains(t, statusText(err), "Neo.ClientError.Transaction.Terminated")

	result, err := observer.Execute(ctx, "MATCH (n:TerminatedWrite) RETURN count(n) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
}
