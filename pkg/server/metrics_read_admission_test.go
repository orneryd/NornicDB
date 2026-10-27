package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Matches cmd/nornicdb's production Badger probe, including system storage.
type metricsReadStorageProbe struct{ engine *storage.BadgerEngine }

func (p metricsReadStorageProbe) NodeCount() int64 { n, _ := p.engine.NodeCount(); return n }
func (p metricsReadStorageProbe) EdgeCount() int64 { n, _ := p.engine.EdgeCount(); return n }
func (p metricsReadStorageProbe) IDDictCounterNodes() uint64 {
	n, _ := p.engine.IDDictCounters()
	return n
}
func (p metricsReadStorageProbe) IDDictCounterEdges() uint64 {
	_, n := p.engine.IDDictCounters()
	return n
}
func (p metricsReadStorageProbe) IDDictFreelistNodes() int64 {
	n, _ := p.engine.IDDictFreelistPending()
	return n
}
func (p metricsReadStorageProbe) IDDictFreelistEdges() int64 {
	_, n := p.engine.IDDictFreelistPending()
	return n
}

func legacyMetricValue(t testing.TB, body, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, name+" ") {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name)), 64)
			require.NoError(t, err)
			return value
		}
	}
	t.Fatalf("missing metric %s in %s", name, body)
	return 0
}

func TestMetricsDatabaseReadAdmission(t *testing.T) {
	for _, tenantLabels := range []bool{false, true} {
		t.Run(strconv.FormatBool(tenantLabels), func(t *testing.T) {
			s, users := setupTestServer(t)
			ctx := context.Background()
			const role = "metrics-reader"
			require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "metrics_private"}))
			require.NoError(t, s.dbManager.CreateAlias(t.Context(), "metrics_alias", "nornic"))
			for database, size := range map[string]int{"nornic": 2, "metrics_private": 3} {
				engine, err := s.dbManager.GetStorage(database)
				require.NoError(t, err)
				for i := 0; i < size; i++ {
					_, err = engine.CreateNode(&storage.Node{ID: storage.NodeID(strconv.Itoa(i)), Labels: []string{"MetricSecret"}})
					require.NoError(t, err)
				}
				require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "edge", StartNode: "0", EndNode: "1", Type: "SECRET"}))
			}
			require.NoError(t, s.roleStore.CreateRole(ctx, role))
			require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read"}))
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "metrics_private", true, false))
			_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
			require.NoError(t, err)
			// Authenticate once: policy edits must apply to this same token.
			token := "Bearer " + getAuthToken(t, users, role)
			badger, ok := storage.UnwrapEngine(s.db.GetStorage()).(*storage.BadgerEngine)
			require.True(t, ok)
			registry := prometheus.NewRegistry()
			observability.NewStorageMetrics(registry, tenantLabels, metricsReadStorageProbe{badger})
			cypherMetrics := observability.NewCypherMetrics(registry, tenantLabels, nil)
			cypherMetrics.BindSlowQueries("nornic").Add(2)
			cypherMetrics.BindSlowQueries("metrics_private").Add(7)
			cypherMetrics.BindSlowQueries("system").Add(11)
			if tenantLabels {
				cypherMetrics.BindSlowQueries("metrics_alias").Add(4)
			}
			embedMetrics := observability.NewEmbedMetrics(registry, nil)
			embedMetrics.Processed.WithLabelValues("local", "test", "success", "cpu").Add(9)
			s.SetObsRegistry(registry)
			check := func(t *testing.T, nodes, edges, slow float64, denied ...bool) {
				t.Helper()
				response := makeRequest(t, s, http.MethodGet, "/metrics", nil, token)
				if len(denied) > 0 && denied[0] {
					require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
					require.NotContains(t, response.Body.String(), "nornicdb_nodes_total ")
					return
				}
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.Equal(t, observability.LegacyContentType, response.Header().Get("Content-Type"))
				require.Equal(t, nodes, legacyMetricValue(t, response.Body.String(), "nornicdb_nodes_total"))
				require.Equal(t, edges, legacyMetricValue(t, response.Body.String(), "nornicdb_edges_total"))
				require.Equal(t, slow, legacyMetricValue(t, response.Body.String(), "nornicdb_slow_queries_total"))
				require.Equal(t, float64(0), legacyMetricValue(t, response.Body.String(), "nornicdb_embeddings_processed"))
			}
			visibleSlow := float64(0)
			if tenantLabels {
				visibleSlow = 2
			}
			userSlow := float64(0)
			if tenantLabels {
				userSlow = 9
			}
			t.Run("omitted-allowlist", func(t *testing.T) { check(t, 2, 1, visibleSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "metrics_alias"}))
			aliasSlow := float64(0)
			if tenantLabels {
				aliasSlow = 6
			}
			t.Run("alias-granted", func(t *testing.T) { check(t, 2, 1, aliasSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
			t.Run("alias-revoked", func(t *testing.T) { check(t, 2, 1, visibleSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "metrics_private"}))
			t.Run("granted", func(t *testing.T) { check(t, 5, 2, userSlow) })
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "metrics_private", false, false))
			t.Run("read-revoked", func(t *testing.T) { check(t, 2, 1, visibleSlow) })
			private, err := s.dbManager.GetStorage("metrics_private")
			require.NoError(t, err)
			_, err = private.CreateNode(&storage.Node{ID: "new-private", Labels: []string{"MetricSecret"}})
			require.NoError(t, err)
			t.Run("unreadable-mutation", func(t *testing.T) { check(t, 2, 1, visibleSlow) })
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "metrics_private", true, false))
			t.Run("read-restored", func(t *testing.T) { check(t, 6, 2, userSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
			t.Run("allowlist-revoked", func(t *testing.T) { check(t, 2, 1, visibleSlow) })
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", false, false))
			t.Run("no-database-read", func(t *testing.T) { check(t, 0, 0, 0, true) })
			require.NoError(t, s.dbManager.CreateCompositeDatabase(t.Context(), "metrics_remote", []multidb.ConstituentRef{{
				Alias: "leaf", DatabaseName: "external_metrics", Type: "remote", AccessMode: "read", URI: "bolt://localhost:1",
			}}))
			cypherMetrics.BindSlowQueries("metrics_remote").Add(5)
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"metrics_remote", "external_metrics"}))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "metrics_remote", true, false))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "external_metrics", true, false))
			remoteSlow := float64(0)
			if tenantLabels {
				remoteSlow = 5
			}
			t.Run("remote-only", func(t *testing.T) { check(t, 0, 0, remoteSlow) })
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "external_metrics", false, false))
			t.Run("remote-revoked", func(t *testing.T) { check(t, 0, 0, 0, true) })
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "external_metrics", true, false))
			t.Run("remote-restored", func(t *testing.T) { check(t, 0, 0, remoteSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"system"}))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "system", true, false))
			system, err := s.dbManager.GetStorage("system")
			require.NoError(t, err)
			systemNodes, err := system.NodeCount()
			require.NoError(t, err)
			systemEdges, err := system.EdgeCount()
			require.NoError(t, err)
			systemSlow := float64(0)
			if tenantLabels {
				systemSlow = 11
			}
			t.Run("system-only", func(t *testing.T) { check(t, float64(systemNodes), float64(systemEdges), systemSlow) })
			require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "metrics_alias", "metrics_private", "system", "metrics_remote", "external_metrics"}))
			require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
			for _, control := range []string{"full-reader", "admin", "auth-off"} {
				t.Run(control, func(t *testing.T) {
					controlToken := "Bearer " + getAuthToken(t, users, "admin")
					if control == "full-reader" {
						controlToken = token
					}
					if control == "auth-off" {
						saved := s.auth
						s.auth = nil
						defer func() { s.auth = saved }()
						controlToken = ""
					}
					response := makeRequest(t, s, http.MethodGet, "/metrics", nil, controlToken)
					require.Equal(t, http.StatusOK, response.Code)
					require.Equal(t, float64(metricsReadStorageProbe{badger}.NodeCount()), legacyMetricValue(t, response.Body.String(), "nornicdb_nodes_total"))
					require.Equal(t, float64(metricsReadStorageProbe{badger}.EdgeCount()), legacyMetricValue(t, response.Body.String(), "nornicdb_edges_total"))
					fullSlow := float64(25)
					if tenantLabels {
						fullSlow = 29
					}
					require.Equal(t, fullSlow, legacyMetricValue(t, response.Body.String(), "nornicdb_slow_queries_total"))
					require.Equal(t, float64(9), legacyMetricValue(t, response.Body.String(), "nornicdb_embeddings_processed"))
				})
			}
			require.Equal(t, http.StatusUnauthorized, makeRequest(t, s, http.MethodGet, "/metrics", nil, "").Code)
		})
	}
}
