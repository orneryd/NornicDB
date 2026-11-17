package server

import (
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricsCatalogUsesExistingAuthenticationAndLivePermissions(t *testing.T) {
	s, users := setupTestServer(t)
	require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "metrics_private"}))
	require.NoError(t, s.allowlistStore.SaveRoleDatabases(t.Context(), "viewer", []string{"nornic"}))
	registry := prometheus.NewRegistry()
	process := observability.NewGaugeVec(registry, observability.MetricOpts{Subsystem: "process", Name: "resident_memory_bytes", Help: "Process memory"}, nil)
	process.WithLabelValues().Set(4096)
	queries := observability.NewCypherMetrics(registry, true, nil)
	queries.BindSlowQueries("nornic").Add(2)
	queries.BindSlowQueries("metrics_private").Add(7)
	s.SetObsRegistry(registry)
	reader := "Bearer " + getAuthToken(t, users, "reader")
	admin := "Bearer " + getAuthToken(t, users, "admin")
	require.Equal(t, http.StatusUnauthorized, makeRequest(t, s, "GET", "/metrics", nil, "").Code)
	r := makeRequest(t, s, "GET", "/metrics", nil, reader)
	require.Equal(t, 200, r.Code, r.Body.String())
	require.Equal(t, "database", r.Header().Get("X-NornicDB-Metrics-Scope"))
	require.Equal(t, "no-store", r.Header().Get("Cache-Control"))
	require.NotContains(t, r.Body.String(), "nornicdb_process_resident_memory_bytes")
	require.NotContains(t, r.Body.String(), `database="metrics_private"`)
	require.Contains(t, r.Body.String(), `database="nornic"`)
	r = makeRequest(t, s, "GET", "/metrics", nil, admin)
	require.Equal(t, 200, r.Code)
	require.Equal(t, "server", r.Header().Get("X-NornicDB-Metrics-Scope"))
	require.Contains(t, r.Body.String(), "nornicdb_process_resident_memory_bytes 4096")
	require.Contains(t, r.Body.String(), `database="metrics_private"`)
	// The same authenticated reader token must observe entitlement updates.
	require.NoError(t, s.roleEntitlementsStore.Set(t.Context(), "viewer", []string{"read", "admin"}))
	r = makeRequest(t, s, "GET", "/metrics", nil, reader)
	require.Equal(t, 200, r.Code)
	require.Contains(t, r.Body.String(), "nornicdb_process_resident_memory_bytes 4096")
	require.NoError(t, s.roleEntitlementsStore.Set(t.Context(), "viewer", []string{"read"}))
	r = makeRequest(t, s, "GET", "/metrics", nil, reader)
	require.Equal(t, 200, r.Code)
	require.NotContains(t, r.Body.String(), "nornicdb_process_resident_memory_bytes")
}
