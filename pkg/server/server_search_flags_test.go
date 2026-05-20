package server

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flagsResolver builds a per-DB search flags resolver that returns fixed
// values for one database name and defaults (true, true, startup, startup)
// for everything else. Used to dial in specific configurations on a single
// test database without touching global config.
func flagsResolver(target string, bm25, vector bool, bm25W, vecW string) nornicdb.DbSearchFlagsResolver {
	return func(dbName string) (bool, bool, string, string) {
		if dbName == target {
			return bm25, vector, bm25W, vecW
		}
		return true, true, "startup", "startup"
	}
}

// TestHandleSearch_BothDisabled_503 — when both indexes are off, the search
// handler returns a permanent 503 with request_status=search_disabled_for_database
// and retryable=false. Distinguishes the all-off shape from the warming /
// still-building shapes.
func TestHandleSearch_BothDisabled_503(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	const dbName = "search_off"
	require.NoError(t, server.dbManager.CreateDatabase(dbName))
	server.db.SetDbSearchFlagsResolver(flagsResolver(dbName, false, false, "startup", "startup"))

	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query":    "anything",
		"limit":    5,
		"database": dbName,
	}, "Bearer "+token)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code, resp.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "search_disabled_for_database", body["request_status"])
	assert.Equal(t, false, body["retryable"])
	assert.Equal(t, false, body["bm25_enabled"])
	assert.Equal(t, false, body["vector_enabled"])
}

// TestHandleSearch_BothLazy_FirstQueryTriggers503 — first request against a
// lazy database returns 503 search_index_warming_lazy with retryable=true.
func TestHandleSearch_BothLazy_FirstQueryTriggers503(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	const dbName = "search_lazy"
	require.NoError(t, server.dbManager.CreateDatabase(dbName))
	server.db.SetDbSearchFlagsResolver(flagsResolver(dbName, true, true, "lazy", "lazy"))

	// Boot loop should NOT have created+started a build for this DB. Probe
	// status directly to confirm the lazy state.
	st := server.db.GetDatabaseSearchStatus(dbName)
	assert.True(t, st.LazyTriggerNeeded, "expected lazy trigger pending before first query")

	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query":    "anything",
		"limit":    5,
		"database": dbName,
	}, "Bearer "+token)

	require.Equal(t, http.StatusServiceUnavailable, resp.Code, resp.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "search_index_warming_lazy", body["request_status"])
	assert.Equal(t, true, body["retryable"])
	assert.Equal(t, true, body["bm25_enabled"])
	assert.Equal(t, true, body["vector_enabled"])

	// After the lazy trigger fires, the next request hits the existing
	// "still building" or 200 path — which means LazyTriggerNeeded flips off.
	require.Eventually(t, func() bool {
		st := server.db.GetDatabaseSearchStatus(dbName)
		return !st.LazyTriggerNeeded
	}, 5*time.Second, 50*time.Millisecond, "lazy trigger should clear after ForceSearchIndexBuild")
}

// TestHandleSearch_GlobalOffPerDBOn_OverrideWins — confirm the load-bearing
// guarantee at the handler level: a database with per-DB override of true
// gets 200 (or 503 still-building) even though the global default is false.
func TestHandleSearch_GlobalOffPerDBOn_OverrideWins(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	const dbName = "tenant_with_search"
	require.NoError(t, server.dbManager.CreateDatabase(dbName))

	// Resolver: this DB gets enabled even though the "global" (default branch
	// in the closure) returns true,true. We simulate global-off by returning
	// false,false for OTHER DBs and true,true for our target — the inverse of
	// what we'd see in real prod with NORNICDB_SEARCH_VECTOR_ENABLED=false
	// globally and a per-DB override of true; the resolver is the single
	// chokepoint either way.
	server.db.SetDbSearchFlagsResolver(func(name string) (bool, bool, string, string) {
		if name == dbName {
			return true, true, "startup", "startup"
		}
		return false, false, "startup", "startup"
	})

	// With per-DB override=true the handler should NOT short-circuit with
	// search_disabled_for_database. It may 503 with search_not_ready while
	// the eager build is in flight, then 200 once ready.
	require.Eventually(t, func() bool {
		resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
			"query":    "anything",
			"limit":    5,
			"database": dbName,
		}, "Bearer "+token)
		switch resp.Code {
		case http.StatusOK:
			return true
		case http.StatusServiceUnavailable:
			var body map[string]interface{}
			_ = json.NewDecoder(resp.Body).Decode(&body)
			// Anything except search_disabled_for_database is acceptable —
			// "still building" is fine, "warming" is fine.
			rs, _ := body["request_status"].(string)
			require.NotEqual(t, "search_disabled_for_database", rs,
				"per-DB override of true must beat the global-off default")
			return false
		default:
			t.Fatalf("unexpected status %d: %s", resp.Code, resp.Body.String())
			return false
		}
	}, 10*time.Second, 100*time.Millisecond)
}
