// Package server: NornicDB issue #563 defect 1 regression coverage.
//
// pkg/bolt/server.go's newDatabaseScopedCypherExecutor and cmd/nornicdb/main.go's
// DBQueryExecutor.ConfigureDatabaseExecutor build per-database StorageExecutors
// but historically never called SetLogger/SetSlowQueryThreshold on them — only
// the primary (base) executor got them, threaded once from main.go's bootstrap.
// The effect: every Bolt query and every HTTP /db/<name>/tx/commit query logs
// to io.Discard, so NORNICDB_SLOW_QUERY_THRESHOLD emits no event="slow_query"
// record for a slow database-scoped query.
//
// TestDatabaseScopedExecutor_InheritsLoggerAndSlowQueryThreshold exercises the
// HTTP construction path (pkg/server/server_db.go's newExecutorForDatabase),
// which had the identical gap: the "inherit from base executor" block that
// function already carries for embedder/inference copied everything except
// the logger and slow-query threshold.
//
// TestGetExecutorForDatabaseWithAuth_InheritsLoggerAndSlowQueryThreshold and
// TestHeimdallRouter_ExecutorForDatabase_InheritsLoggerAndSlowQueryThreshold
// cover the two other construction sites review finding C3 flagged as
// untested: server_db.go's composite/remote-auth path and the Heimdall
// router. Bolt's cmd/nornicdb/main.go ConfigureDatabaseExecutor site is
// covered separately in cmd/nornicdb, since it lives in package main.
package server

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/require"
)

// TestDatabaseScopedExecutor_InheritsLoggerAndSlowQueryThreshold proves a
// query executed against a NON-default, newly-created database (which is
// always served by a freshly constructed per-database StorageExecutor, never
// the primary one) still emits a slow_query record once the primary
// executor's logger + threshold are configured — exactly as main.go's
// bootstrap configures the primary executor in production.
func TestDatabaseScopedExecutor_InheritsLoggerAndSlowQueryThreshold(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	te := observability.NewTestEnv(t)
	te.CaptureRecords()

	// Mirrors cmd/nornicdb/main.go's Phase 2 D-01+D-04c bootstrap wiring:
	// thread the structured logger and slow-query threshold onto the base
	// (primary) executor before any per-database executor is constructed.
	baseExec := server.db.GetCypherExecutor()
	if baseExec == nil {
		t.Fatalf("server.db.GetCypherExecutor() returned nil")
	}
	baseExec.SetLogger(te.Logger)
	baseExec.SetSlowQueryThreshold(1 * time.Nanosecond)

	createDatabase(t, server, "obs_db_scoped", token)

	resp := makeRequest(t, server, "POST", "/db/obs_db_scoped/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Probe {name: 'trigger'}) RETURN n"},
		},
	}, "Bearer "+token)
	if resp.Code != 200 {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var found bool
	var planHash string
	for _, rec := range te.LoggedRecords() {
		if rec["event"] != "slow_query" {
			continue
		}
		found = true
		if ph, ok := rec["plan_hash"].(string); ok {
			planHash = ph
		}
		break
	}
	if !found {
		t.Fatalf("no slow_query record captured for a query against a per-database executor; logger/threshold did not propagate from the base executor (NornicDB issue #563 defect 1)")
	}
	if planHash == "" || planHash == "0000000000000000" {
		t.Errorf("expected a non-zero plan_hash, got %q (NornicDB issue #563 defect 2)", planHash)
	}
}

// executorEmitsSlowQueryLog runs statement directly against exec (threshold
// must already be near-zero) and reports whether a event="slow_query"
// record landed in te's captured buffer. Used to prove logger/threshold
// inheritance functionally: SetLogger wraps the base logger in a NEW
// *slog.Logger via .With("component", "cypher"), so comparing
// exec.Logger() against the base logger by pointer never matches even when
// inheritance is wired correctly — the only faithful proof is that a
// record this executor emits actually reaches the base logger's underlying
// sink.
func executorEmitsSlowQueryLog(t *testing.T, te *observability.TestEnv, exec *cypher.StorageExecutor, statement string) bool {
	t.Helper()
	_, err := exec.Execute(context.Background(), statement, nil)
	require.NoError(t, err)
	for _, rec := range te.LoggedRecords() {
		if rec["event"] == "slow_query" {
			return true
		}
	}
	return false
}

// TestGetExecutorForDatabaseWithAuth_InheritsLoggerAndSlowQueryThreshold
// covers the second of the 4 construction sites review finding C3 flagged
// as untested: getExecutorForDatabaseWithAuth's composite/remote-auth
// branch, which builds its own fresh *cypher.StorageExecutor rather than
// delegating to newExecutorForDatabase. Verified by mutation: commenting
// out either SetLogger or SetSlowQueryThreshold call in that function's
// "inherit from base executor" block fails this test.
func TestGetExecutorForDatabaseWithAuth_InheritsLoggerAndSlowQueryThreshold(t *testing.T) {
	server, _ := setupTestServer(t)

	baseExec := server.db.GetCypherExecutor()
	require.NotNil(t, baseExec)
	te := observability.NewTestEnv(t)
	te.CaptureRecords()
	baseExec.SetLogger(te.Logger)
	baseExec.SetSlowQueryThreshold(1 * time.Nanosecond)

	// A local constituent alongside a remote one: databaseHasRemoteConstituent
	// only needs ONE remote constituent to route through
	// getExecutorForDatabaseWithAuth's remote-auth branch (the code under
	// test), but querying the LOCAL constituent below keeps this test
	// in-process — no real dial to the remote constituent's URI needed.
	require.NoError(t, server.dbManager.CreateDatabase("obs_local_shard"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("obs_comp_remote", []multidb.ConstituentRef{
		{Alias: "local1", DatabaseName: "obs_local_shard", Type: "local", AccessMode: "read_write"},
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example",
		},
	}))

	require.True(t, server.databaseHasRemoteConstituent("obs_comp_remote"))
	exec, err := server.getExecutorForDatabaseWithAuth("obs_comp_remote", "Bearer remote-token")
	require.NoError(t, err)
	require.NotNil(t, exec)

	// Composite databases refuse ungrounded queries; target the local
	// constituent explicitly, same as TestCompositeExplicitTx_SecondWriteShardErrorCode.
	require.True(t, executorEmitsSlowQueryLog(t, te, exec, "CALL { USE obs_comp_remote.local1 RETURN 1 AS x } RETURN x"),
		"getExecutorForDatabaseWithAuth's composite/remote-auth executor did not inherit the base executor's logger/threshold (NornicDB issue #563 defect 1)")
}

// TestHeimdallRouter_ExecutorForDatabase_InheritsLoggerAndSlowQueryThreshold
// covers the third of the 4 construction sites review finding C3 flagged as
// untested: heimdallDBRouter.executorForDatabase, the Heimdall AI-assistant
// query path. Verified by mutation: commenting out either SetLogger or
// SetSlowQueryThreshold in that function's base-executor block fails this
// test.
func TestHeimdallRouter_ExecutorForDatabase_InheritsLoggerAndSlowQueryThreshold(t *testing.T) {
	server, _ := setupTestServer(t)

	baseExec := server.db.GetCypherExecutor()
	require.NotNil(t, baseExec)
	te := observability.NewTestEnv(t)
	te.CaptureRecords()
	baseExec.SetLogger(te.Logger)
	baseExec.SetSlowQueryThreshold(1 * time.Nanosecond)

	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	dbName, exec, _, err := router.executorForDatabase("nornic")
	require.NoError(t, err)
	require.NotNil(t, exec)
	require.Equal(t, "nornic", dbName)

	require.True(t, executorEmitsSlowQueryLog(t, te, exec, "RETURN 1"),
		"heimdallDBRouter.executorForDatabase's executor did not inherit the base executor's logger/threshold (NornicDB issue #563 defect 1)")
}
