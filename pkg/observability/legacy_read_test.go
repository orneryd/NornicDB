package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func TestRenderLegacyReadScope(t *testing.T) {
	registry := prometheus.NewRegistry()
	cypher := NewCypherMetrics(registry, true, nil)
	cypher.BindSlowQueries("visible").Add(2)
	cypher.BindSlowQueries("private").Add(7)
	embed := NewEmbedMetrics(registry, nil)
	embed.Processed.WithLabelValues("local", "test", "success", "cpu").Add(9)
	allowed := false
	scope := LegacyReadScope{
		CanReadDatabase: func(database string) bool { return database == "visible" || allowed },
		Nodes: 2, Edges: 1,
	}
	for _, phase := range []bool{false, true, false} {
		allowed = phase
		scope.AllDatabasesReadable = phase
		body := string(RenderLegacyForRead(registry, time.Time{}, scope))
		require.Contains(t, body, "nornicdb_nodes_total 2\n")
		require.Contains(t, body, "nornicdb_edges_total 1\n")
		if phase {
			require.Contains(t, body, "nornicdb_slow_queries_total 9\n")
			require.Contains(t, body, "nornicdb_embeddings_processed 9\n")
		} else {
			require.Contains(t, body, "nornicdb_slow_queries_total 2\n")
			require.Contains(t, body, "nornicdb_embeddings_processed 0\n")
		}
		// A caller's view must never mutate the shared registry/exporter.
		require.Contains(t, string(RenderLegacy(registry, time.Time{})), "nornicdb_slow_queries_total 9\n")
	}
	require.Empty(t, RenderLegacyForRead(nil, time.Time{}, scope))
}

// The telemetry listener is deliberately unauthenticated (listener.go); both
// native Prometheus collectors and the OTel->Prometheus bridge remain global.
func TestTelemetryMetricsRemainProcessWideWithoutAuth(t *testing.T) {
	env := NewTestEnv(t)
	cypher := NewCypherMetrics(env.Registry, true, nil)
	cypher.BindSlowQueries("visible").Add(2)
	cypher.BindSlowQueries("private").Add(7)
	counter, err := env.Provider.MeterProvider().Meter("read-control").Int64Counter("database_reads")
	require.NoError(t, err)
	counter.Add(context.Background(), 2, metric.WithAttributes(attribute.String("database", "visible")))
	counter.Add(context.Background(), 7, metric.WithAttributes(attribute.String("database", "private")))
	listener, _ := newTestListener(t, env)
	t.Cleanup(func() { _ = listener.Shutdown(context.Background()) })
	response := httptest.NewRecorder()
	listener.srv.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Contains(t, response.Body.String(), "nornicdb_cypher_slow_queries_total{database=\"visible\"} 2")
	require.Contains(t, response.Body.String(), "nornicdb_cypher_slow_queries_total{database=\"private\"} 7")
	// OTel resource labels vary by environment; inspect its own private sample.
	found := false
	for _, line := range strings.Split(response.Body.String(), "\n") {
		if strings.HasPrefix(line, "nornicdb_otel_database_reads_total{") && strings.Contains(line, "database=\"private\"") {
			require.True(t, strings.HasSuffix(line, " 7"), line)
			found = true
		}
	}
	require.True(t, found, response.Body.String())
}
