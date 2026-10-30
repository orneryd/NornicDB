package main

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerStartup_CypherMetricsRegistered verifies Plan 04-03-05:
// the Cypher bag (11 families per MET-08) coexists with the Phase-1
// go/process collectors, the Plan-04-01 Cache+Runtime bag, and the
// Plan-04-02 HTTP+Bolt bags on a single *prometheus.Registry. Mirrors
// the cache-bag startup smoke test from Plan 04-01 and the http+bolt
// smoke test from Plan 04-02.
//
// We do NOT boot the full server (CGO localllm dep unavailable without
// -tags localllm); we exercise the bag constructors directly against
// the same TestEnv-shaped registry the production startup uses.
func TestServerStartup_CypherMetricsRegistered(t *testing.T) {
	te := observability.NewTestEnv(t)

	// Construct all four Phase-4 bags on the same registry.
	cacheBag := observability.NewCacheMetrics(te.Registry)
	require.NotNil(t, cacheBag)
	httpBag := observability.NewHTTPMetrics(te.Registry, false)
	require.NotNil(t, httpBag)
	boltBag := observability.NewBoltMetrics(te.Registry)
	require.NotNil(t, boltBag)
	cypherBag := observability.NewCypherMetrics(te.Registry, false, func() float64 { return 5.0 })
	require.NotNil(t, cypherBag)

	// Materialize one series per *Vec so Gather() surfaces the family.
	cypherBag.Queries.WithLabelValues("read").Inc()
	cypherBag.QueryDuration.Bind("read").Observe(nil, 0.001)
	cypherBag.PlannerDuration.Bind("read").Observe(nil, 0.0001)
	cypherBag.PlannerCacheHits.WithLabelValues().Inc()
	cypherBag.PlannerCacheMisses.WithLabelValues().Inc()
	cypherBag.PlannerCacheSize.Set(0)
	cypherBag.RowsReturned.Bind("read").Observe(nil, 1)
	cypherBag.ActiveTransactions.Set(0)
	cypherBag.TransactionConflicts.WithLabelValues().Inc()
	cypherBag.SlowQueries.WithLabelValues().Inc()

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	got := make(map[string]bool, len(mfs))
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}

	cypherFamilies := []string{
		"nornicdb_cypher_queries_total",
		"nornicdb_cypher_query_duration_seconds",
		"nornicdb_cypher_planner_duration_seconds",
		"nornicdb_cypher_planner_cache_hits_total",
		"nornicdb_cypher_planner_cache_misses_total",
		"nornicdb_cypher_planner_cache_size",
		"nornicdb_cypher_rows_returned_rows",
		"nornicdb_cypher_active_transactions",
		"nornicdb_cypher_transaction_conflicts_total",
		"nornicdb_cypher_slow_queries_total",
		"nornicdb_cypher_slow_query_threshold_seconds",
	}
	for _, want := range cypherFamilies {
		assert.True(t, got[want],
			"MET-08: Cypher family %q must register at startup", want)
	}

	// Re-registering the same bag on the same registry MUST panic
	// (Pitfall 8 invariant — catches buggy double-init).
	require.Panics(t, func() {
		_ = observability.NewCypherMetrics(te.Registry, false, func() float64 { return 0 })
	}, "Re-registering Cypher bag on same registry must panic")
}
