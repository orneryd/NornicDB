package server

import (
	"github.com/orneryd/nornicdb/pkg/multidb"

	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// Each calibration owns its server. Requests only scrape immutable fixtures;
// no subscribers, series, or stored objects accumulate in the timed loop.
func BenchmarkMetricsDatabaseReadAdmission(b *testing.B) {
	for _, profile := range []string{"reader", "admin", "auth-off"} {
		b.Run(profile, func(b *testing.B) {
			s, users := setupTestServer(b)
			ctx := context.Background()
			const role = "metrics-bench-reader"
			require.NoError(b, s.dbManager.CreateDatabase(b.Context(), multidb.DatabaseCreateRequest{Name: "metrics_private"}))
			require.NoError(b, s.roleStore.CreateRole(ctx, role))
			require.NoError(b, s.roleEntitlementsStore.Set(ctx, role, []string{"read"}))
			require.NoError(b, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic"}))
			require.NoError(b, s.privilegesStore.SavePrivilege(ctx, role, "nornic", true, false))
			_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
			require.NoError(b, err)
			for _, name := range []string{"nornic", "metrics_private"} {
				engine, err := s.dbManager.GetStorage(name)
				require.NoError(b, err)
				_, err = engine.CreateNode(&storage.Node{ID: "node", Labels: []string{"Metric"}})
				require.NoError(b, err)
			}
			badger, ok := storage.UnwrapEngine(s.db.GetStorage()).(*storage.BadgerEngine)
			require.True(b, ok)
			registry := prometheus.NewRegistry()
			observability.NewStorageMetrics(registry, true, metricsReadStorageProbe{badger})
			metrics := observability.NewCypherMetrics(registry, true, nil)
			metrics.BindSlowQueries("nornic").Add(2)
			metrics.BindSlowQueries("metrics_private").Add(7)
			s.SetObsRegistry(registry)
			token := "Bearer " + getAuthToken(b, users, role)
			if profile == "admin" {
				token = "Bearer " + getAuthToken(b, users, "admin")
			}
			if profile == "auth-off" {
				s.auth = nil
				token = ""
			}
			handler := s.buildRouter()
			for _, path := range []string{"/metrics", "/status"} {
				b.Run(path, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						request := httptest.NewRequest(http.MethodGet, path, nil)
						if token != "" {
							request.Header.Set("Authorization", token)
						}
						response := httptest.NewRecorder()
						handler.ServeHTTP(response, request)
						if response.Code != http.StatusOK {
							b.Fatalf("%s: %d %s", path, response.Code, response.Body.String())
						}
					}
				})
			}
		})
	}
}
