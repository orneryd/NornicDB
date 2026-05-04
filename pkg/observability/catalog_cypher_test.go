package observability

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED: NewCypherMetrics + slow-query GaugeFunc land in
// Plan 04-03. Compile-fail until then.

// TestCypherMetrics_RegistersElevenFamilies asserts MET-08's eleven Cypher
// families per ADR §2.3.
func TestCypherMetrics_RegistersElevenFamilies(t *testing.T) {
	t.Skip("RED: pending Plan 04-03 (Cypher bag delivery; RISK-1 op_type chokepoint)")

	te := NewTestEnv(t)
	bag := NewCypherMetrics(te.Registry, false /* tenantLabelsEnabled */, func() float64 { return 1.0 })
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_cypher_queries_total",
		"nornicdb_cypher_query_duration_seconds",
		"nornicdb_cypher_planner_duration_seconds",
		"nornicdb_cypher_planner_cache_hits_total",
		"nornicdb_cypher_planner_cache_misses_total",
		"nornicdb_cypher_planner_cache_size",
		"nornicdb_cypher_rows_returned",
		"nornicdb_cypher_active_transactions",
		"nornicdb_cypher_transaction_conflicts_total",
		"nornicdb_cypher_slow_queries_total",
		"nornicdb_cypher_slow_query_threshold_seconds",
	} {
		assert.Contains(t, names, want, "MET-08: Cypher family %q must register", want)
	}
}

// TestSlowQueryThresholdGauge_GaugeFunc asserts CONTEXT D-15b: the gauge is
// a callback-driven NewGaugeFunc (not Set()) so config reload flows through
// without event wiring. Test invokes Gather and asserts the live cfg value
// is reflected.
func TestSlowQueryThresholdGauge_GaugeFunc(t *testing.T) {
	t.Skip("RED: pending Plan 04-03 (Cypher bag delivery)")

	te := NewTestEnv(t)
	current := 2.5
	bag := NewCypherMetrics(te.Registry, false, func() float64 { return current })
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_slow_query_threshold_seconds" {
			continue
		}
		require.Len(t, mf.Metric, 1)
		assert.InDelta(t, 2.5, mf.Metric[0].GetGauge().GetValue(), 0.0001)
	}

	// Reload simulation: callback returns new value on next scrape.
	current = 7.5
	mfs, err = te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_slow_query_threshold_seconds" {
			continue
		}
		assert.InDelta(t, 7.5, mf.Metric[0].GetGauge().GetValue(), 0.0001,
			"D-15b: GaugeFunc callback must read live cfg on every scrape")
	}
}
