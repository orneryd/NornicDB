// Package server: eshu-7014-cause-C defect 1 regression coverage.
//
// pkg/bolt/server.go's newDatabaseScopedCypherExecutor and cmd/nornicdb/main.go's
// DBQueryExecutor.ConfigureDatabaseExecutor build per-database StorageExecutors
// but historically never called SetLogger/SetSlowQueryThreshold on them — only
// the primary (base) executor got them, threaded once from main.go's bootstrap.
// The effect: every Bolt query and every HTTP /db/<name>/tx/commit query logs
// to io.Discard, so NORNICDB_SLOW_QUERY_THRESHOLD emits no event="slow_query"
// record for a slow database-scoped query.
//
// This test exercises the HTTP construction path (pkg/server/server_db.go's
// newExecutorForDatabase and getExecutorForDatabaseWithAuth), which had the
// identical gap: the "inherit from base executor" block those functions
// already carry for embedder/inference copied everything except the logger
// and slow-query threshold.
package server

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
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
		t.Fatalf("no slow_query record captured for a query against a per-database executor; logger/threshold did not propagate from the base executor (eshu-7014-cause-C defect 1)")
	}
	if planHash == "" || planHash == "0000000000000000" {
		t.Errorf("expected a non-zero plan_hash, got %q (eshu-7014-cause-C defect 2)", planHash)
	}
}
