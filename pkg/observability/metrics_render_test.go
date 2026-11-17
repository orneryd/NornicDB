package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestExpandedMetricsPreserveAliasesAndScope(t *testing.T) {
	registry := prometheus.NewRegistry()
	process := prometheus.NewGauge(prometheus.GaugeOpts{Name: "process_resident_memory_bytes", Help: "Process memory"})
	process.Set(4096)
	registry.MustRegister(process)
	cypher := NewCypherMetrics(registry, true, nil)
	cypher.BindSlowQueries("visible").Add(2)
	cypher.BindSlowQueries("private").Add(7)
	embed := NewEmbedMetrics(registry, nil)
	embed.Processed.WithLabelValues("local", "test", "success", "cpu").Add(9)
	allowed := false
	scope := &LegacyReadScope{CanReadDatabase: func(name string) bool { return name == "visible" || allowed }, Nodes: 3, Edges: 2}
	for _, all := range []bool{false, true, false} {
		allowed = all
		scope.AllDatabasesReadable = all
		body, err := RenderMetrics(registry, time.Time{}, scope, false)
		require.NoError(t, err)
		require.NotContains(t, string(body), "process_resident_memory_bytes")
		require.Contains(t, string(body), `nornicdb_cypher_slow_queries_total{database="visible"} 2`)
		if all {
			require.Contains(t, string(body), `database="private"`)
			require.Contains(t, string(body), "nornicdb_embeddings_processed 9\n")
		} else {
			require.NotContains(t, string(body), `database="private"`)
			require.Contains(t, string(body), "nornicdb_embeddings_processed 0\n")
		}
		require.Contains(t, string(body), "nornicdb_nodes_total 3\n")
		parser := expfmt.NewTextParser(model.LegacyValidation)
		_, err = parser.TextToMetricFamilies(strings.NewReader(string(body)))
		require.NoError(t, err)
	}
	body, err := RenderMetrics(registry, time.Time{}, nil, true)
	require.NoError(t, err)
	require.Contains(t, string(body), "process_resident_memory_bytes 4096\n")
	require.Contains(t, string(body), `database="private"`)
	require.Contains(t, string(body), "nornicdb_embeddings_processed 9\n")
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(strings.NewReader(string(body)))
	require.NoError(t, err)
	require.Contains(t, families, "nornicdb_embed_processed_total")
	// A full scrape following scoped scrapes must still see the complete registry.
	require.Contains(t, string(body), "nornicdb_slow_queries_total 9\n")
}

func TestExpandedMetricsDoNotDuplicateCompatibilityNames(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGauge(prometheus.GaugeOpts{Name: "nornicdb_requests_total", Help: "Compatibility name"}))
	body, err := RenderMetrics(registry, time.Time{}, nil, true)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(body), "# TYPE nornicdb_requests_total "))
}
