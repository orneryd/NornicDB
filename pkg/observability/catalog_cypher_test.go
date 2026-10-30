package observability

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-03 GREEN: catalog_cypher.go ships the bag; this file's tests now
// exercise the eleven families per MET-08, the closed op_type enum (RISK-1
// corrected per CONTEXT D-04), the D-15b GaugeFunc-backed
// slow_query_threshold_seconds, the D-08 tenant-flag forward-compat, and
// forbidden-label discipline.

// TestCypherMetrics_RegistersElevenFamilies asserts MET-08's eleven Cypher
// families per ADR §2.3. All families MUST surface from a single Gather()
// call — including the GaugeFunc-backed slow_query_threshold_seconds and
// the bare prometheus.Gauge-backed planner_cache_size and active_transactions
// (which surface unconditionally because client_golang Gauge != GaugeVec
// emits the family on Gather even with zero observations).
func TestCypherMetrics_RegistersElevenFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCypherMetrics(te.Registry, false /* tenantLabelsEnabled */, func() float64 { return 1.0 })
	require.NotNil(t, bag)

	// Materialize one instance per *Vec so Gather() emits the family
	// (client_golang skips empty *Vec families). The bare Gauges
	// (PlannerCacheSize, ActiveTransactions) and GaugeFunc
	// (slow_query_threshold_seconds) surface unconditionally.
	bag.Queries.WithLabelValues("read").Inc()
	bag.QueryDuration.Bind("read").Observe(nil, 0.001)
	bag.PlannerDuration.Bind("read").Observe(nil, 0.001)
	bag.PlannerCacheHits.WithLabelValues().Inc()
	bag.PlannerCacheMisses.WithLabelValues().Inc()
	bag.RowsReturned.Bind("read").Observe(nil, 1)
	bag.TransactionConflicts.WithLabelValues().Inc()
	bag.SlowQueries.WithLabelValues().Inc()

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
		"nornicdb_cypher_rows_returned_rows",
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
	te := NewTestEnv(t)
	current := 2.5
	bag := NewCypherMetrics(te.Registry, false, func() float64 { return current })
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	found := false
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_slow_query_threshold_seconds" {
			continue
		}
		found = true
		require.Len(t, mf.Metric, 1)
		assert.InDelta(t, 2.5, mf.Metric[0].GetGauge().GetValue(), 0.0001)
	}
	require.True(t, found, "slow_query_threshold_seconds must surface")

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

// TestSlowQueryThresholdGauge_PanicSafe asserts RESEARCH RISK-8 / Pitfall 1:
// a panicking callback returns 0 from the GaugeFunc body so a buggy callback
// cannot poison the entire /metrics scrape (e.g., during graceful shutdown
// when the live cfg pointer is racing with cleanup).
func TestSlowQueryThresholdGauge_PanicSafe(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCypherMetrics(te.Registry, false, func() float64 {
		panic("intentional test panic — RISK-8 falsifiability")
	})
	require.NotNil(t, bag)

	// Gather should NOT propagate the panic.
	mfs, err := te.Registry.Gather()
	require.NoError(t, err, "RISK-8: panicking callback must not propagate")

	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_slow_query_threshold_seconds" {
			continue
		}
		require.Len(t, mf.Metric, 1)
		assert.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue(),
			"RISK-8: panicking callback must yield 0, not random memory")
	}
}

// TestSlowQueryThresholdGauge_NilCallback asserts the bag tolerates a nil
// slowQueryThresholdFn — returns 0 instead of panicking on the nil-call
// indirection.
func TestSlowQueryThresholdGauge_NilCallback(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCypherMetrics(te.Registry, false, nil)
	require.NotNil(t, bag)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_cypher_slow_query_threshold_seconds" {
			continue
		}
		require.Len(t, mf.Metric, 1)
		assert.Equal(t, 0.0, mf.Metric[0].GetGauge().GetValue(),
			"nil callback must return 0 instead of panicking")
	}
}

// TestCypherMetrics_TenantFlagOmitsLabel asserts CONTEXT D-08 forward-compat
// for the Cypher bag: when tenantLabelsEnabled=false, the `database` label
// is OMITTED from the labelnames slice on every tenant-tagged family
// (queries_total, query_duration_seconds, transaction_conflicts_total,
// slow_queries_total). When true, it is included.
func TestCypherMetrics_TenantFlagOmitsLabel(t *testing.T) {
	t.Run("flag_off_omits_database", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, false, func() float64 { return 1.0 })

		// 1-arity Bind succeeds when database label is absent.
		bag.Queries.WithLabelValues("read").Inc()
		bag.TransactionConflicts.WithLabelValues().Inc()
		bag.SlowQueries.WithLabelValues().Inc()

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			switch mf.GetName() {
			case "nornicdb_cypher_queries_total",
				"nornicdb_cypher_transaction_conflicts_total",
				"nornicdb_cypher_slow_queries_total":
				require.NotEmpty(t, mf.Metric, mf.GetName())
				labels := map[string]string{}
				for _, lp := range mf.Metric[0].Label {
					labels[lp.GetName()] = lp.GetValue()
				}
				assert.NotContains(t, labels, "database",
					"D-08: when tenantLabelsEnabled=false, database label must be omitted from %s",
					mf.GetName())
			}
		}
	})

	t.Run("flag_on_includes_database", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, true, func() float64 { return 1.0 })

		bag.Queries.WithLabelValues("read", "mydb").Inc()
		bag.TransactionConflicts.WithLabelValues("mydb").Inc()
		bag.SlowQueries.WithLabelValues("mydb").Inc()

		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			switch mf.GetName() {
			case "nornicdb_cypher_queries_total",
				"nornicdb_cypher_transaction_conflicts_total",
				"nornicdb_cypher_slow_queries_total":
				require.NotEmpty(t, mf.Metric, mf.GetName())
				labels := map[string]string{}
				for _, lp := range mf.Metric[0].Label {
					labels[lp.GetName()] = lp.GetValue()
				}
				assert.Contains(t, labels, "database",
					"D-08: when tenantLabelsEnabled=true, database label must be present on %s",
					mf.GetName())
				assert.Equal(t, "mydb", labels["database"])
			}
		}
	})
}

// TestCypherMetrics_BindHelperRespectsTenantFlag asserts the
// BindQueryDuration / BindQueries / BindTransactionConflicts /
// BindSlowQueries helpers drop the database arg when the bag was
// constructed with tenantLabelsEnabled=false (subsystems are bool-agnostic).
func TestCypherMetrics_BindHelperRespectsTenantFlag(t *testing.T) {
	t.Run("flag_off", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, false, func() float64 { return 1.0 })
		// Subsystem passes database unconditionally; helper drops it.
		bound := bag.BindQueryDuration("read", "ignored")
		bound.Observe(nil, 0.001)
		bag.BindQueries("read", "ignored").Inc()
		bag.BindTransactionConflicts("ignored").Inc()
		bag.BindSlowQueries("ignored").Inc()
		// No panic + Gather succeeds = success.
		_, err := te.Registry.Gather()
		require.NoError(t, err)
	})

	t.Run("flag_on", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, true, func() float64 { return 1.0 })
		bound := bag.BindQueryDuration("read", "mydb")
		bound.Observe(nil, 0.001)
		bag.BindQueries("read", "mydb").Inc()
		bag.BindTransactionConflicts("mydb").Inc()
		bag.BindSlowQueries("mydb").Inc()
		_, err := te.Registry.Gather()
		require.NoError(t, err)
	})
}

// TestCypherMetrics_RejectsRawQuery asserts CONTEXT D-03a defense-in-depth:
// passing "query" (raw Cypher text) as a label name panics at registration
// via the Phase-3 ForbiddenLabels guard. Subsystems MUST use the closed
// op_type enum and never raw query text or query_hash as a Prometheus label.
func TestCypherMetrics_RejectsRawQuery(t *testing.T) {
	te := NewTestEnv(t)
	require.Panics(t, func() {
		_ = NewCounterVec(te.Registry,
			MetricOpts{
				Subsystem: "cypher",
				Name:      "queries_total",
				Help:      "would-be cardinality bomb",
			},
			[]string{"query"}) // FORBIDDEN — must panic
	}, "D-03a: passing 'query' as a label name must panic at registration")
}

// TestOpType_ClosedEnum asserts the closed AllowedCypherOpTypes contract is
// six values exactly and matches the RISK-1 corrected enum: read, write,
// schema, admin, fabric, parse_error.
func TestOpType_ClosedEnum(t *testing.T) {
	require.Len(t, AllowedCypherOpTypes, 6,
		"D-04 corrected per RISK-1: closed enum is exactly 6 values")

	// Sort a copy so the test is order-independent against constructor edits.
	got := make([]string, len(AllowedCypherOpTypes))
	copy(got, AllowedCypherOpTypes)
	sort.Strings(got)
	want := []string{"admin", "fabric", "parse_error", "read", "schema", "write"}
	assert.Equal(t, want, got, "D-04b: parse_error is the sixth enum value")
}

// TestMetricCardinality_Cypher asserts the closed-enum cardinality ceiling
// for queries_total. Drives synthetic op_types — the closed enum bounds the
// cardinality regardless of how many call sites invoke. Parameterized on
// the D-08 tenant flag (CONTEXT D-08b) per MET-21.
func TestMetricCardinality_Cypher(t *testing.T) {
	t.Run("flag_off_ceiling_6", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, false, func() float64 { return 1.0 })
		// 6 closed-enum op_types; database axis omitted by D-08 flag.
		// Ceiling = 6. Drive 1k synthetic UUIDs as defense — tenant arg is
		// dropped by the Bind helper so cardinality stays bounded.
		te.AssertCardinalityCeiling(t, "nornicdb_cypher_queries_total", 6,
			func(tenant string) {
				for _, op := range AllowedCypherOpTypes {
					bag.BindQueries(op, tenant).Inc()
				}
			})
	})

	t.Run("flag_on_ceiling_higher", func(t *testing.T) {
		te := NewTestEnv(t)
		bag := NewCypherMetrics(te.Registry, true, func() float64 { return 1.0 })
		// With database axis included, 1000 synthetic tenants × 6 op_types
		// = 6000 series — ceiling 6000 (synthetic; production K8s autodetect
		// in Phase 5 caps databases to a small set, but this test proves
		// the cardinality wall scales linearly and predictably).
		te.AssertCardinalityCeiling(t, "nornicdb_cypher_queries_total", 6000,
			func(tenant string) {
				for _, op := range AllowedCypherOpTypes {
					bag.BindQueries(op, tenant).Inc()
				}
			})
	})
}
