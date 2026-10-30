package observability

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Plan 04-01 Wave-0 RED for Cache: this is the ONE catalog file that turns
// GREEN inside Plan 04-01 itself (Task 04-01-03 ships catalog_cache.go).
// All 11 RED files compile-fail until that GREEN bag lands; once it does,
// only this file passes — the other 9 stay skipped (t.Skip RED beacons)
// awaiting Plans 04-02..04-06.

// TestCacheMetrics_RegistersSixFamilies asserts MET-16's six Cache+Runtime
// families per ADR §2.3 / CONTEXT §domain:
//
//	cache_hits_total{cache}
//	cache_misses_total{cache}
//	cache_size_bytes{cache}
//	cache_evictions_total{cache, reason}
//	process_uptime_seconds
//	build_info
func TestCacheMetrics_RegistersSixFamilies(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCacheMetrics(te.Registry)
	require.NotNil(t, bag)

	// Materialize one instance per *Vec so Gather() emits the family
	// (client_golang skips empty *Vec families). GaugeFunc-based families
	// (build_info, process_uptime_seconds) surface unconditionally.
	bag.Hits.WithLabelValues("query_result").Inc()
	bag.Misses.WithLabelValues("query_result").Inc()
	bag.SizeBytes.WithLabelValues("query_result").Set(0)
	bag.Evictions.WithLabelValues("query_result", "lru").Inc()

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)
	names := metricNames(mfs)
	for _, want := range []string{
		"nornicdb_cache_hits_total",
		"nornicdb_cache_misses_total",
		"nornicdb_cache_size_bytes",
		"nornicdb_cache_evictions_total",
		"nornicdb_process_uptime_seconds",
		"nornicdb_build_info",
	} {
		assert.Contains(t, names, want, "MET-16: family %q must register", want)
	}
}

// TestCacheEnum_ClosedEnum asserts CONTEXT D-12 + RISK-4: the closed cache
// enum is restricted to {query_result, schema, label, node_lookup}. Plan
// 04-01 verifies each named cache has an actual increment site; if any
// don't, prune the enum here. RESEARCH §Q11 ceiling=4. Driving 1k synthetic
// tenants must NOT exceed 4 series.
func TestCacheEnum_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCacheMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_cache_hits_total", 4, func(tenant string) {
		for _, c := range AllowedCacheNames {
			bag.Hits.WithLabelValues(c).Inc()
		}
		_ = tenant
	})
}

// TestCacheEvictionsReason_ClosedEnum asserts CONTEXT D-12b: reason label
// accepts only {lru, ttl, capacity, manual}. With cache enum=4 and reason=4,
// total cardinality ceiling for cache_evictions_total = 16 (RESEARCH §Q11).
func TestCacheEvictionsReason_ClosedEnum(t *testing.T) {
	te := NewTestEnv(t)
	bag := NewCacheMetrics(te.Registry)
	require.NotNil(t, bag)
	te.AssertCardinalityCeiling(t, "nornicdb_cache_evictions_total", 16, func(tenant string) {
		for _, c := range AllowedCacheNames {
			for _, reason := range AllowedEvictionReasons {
				bag.Evictions.WithLabelValues(c, reason).Inc()
			}
		}
		_ = tenant
	})
}

// TestBuildInfo_ConstLabels asserts CONTEXT D-13a: build_info exposes
// version, commit, go_version, backend as const labels. The metric value
// is constant 1; the label values are interesting.
func TestBuildInfo_ConstLabels(t *testing.T) {
	te := NewTestEnv(t)
	_ = NewCacheMetrics(te.Registry)

	mfs, err := te.Registry.Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range mfs {
		if mf.GetName() != "nornicdb_build_info" {
			continue
		}
		found = true
		require.Len(t, mf.Metric, 1, "build_info is a single-series GaugeFunc")
		labels := map[string]string{}
		for _, lp := range mf.Metric[0].Label {
			labels[lp.GetName()] = lp.GetValue()
		}
		for _, want := range []string{"version", "commit", "go_version", "backend"} {
			assert.Contains(t, labels, want, "D-13a: build_info must expose const label %q", want)
			assert.NotEmpty(t, labels[want], "D-13a: const label %q must be populated", want)
		}
		assert.InDelta(t, 1.0, mf.Metric[0].GetGauge().GetValue(), 0.0001,
			"D-13a: build_info GaugeFunc returns constant 1")
	}
	require.True(t, found, "nornicdb_build_info must be registered")
}

// TestProcessUptime_PositiveAndIncreasing asserts CONTEXT MET-16:
// process_uptime_seconds returns a positive monotonically-increasing value
// derived from time.Since(processStart) per call.
func TestProcessUptime_PositiveAndIncreasing(t *testing.T) {
	te := NewTestEnv(t)
	_ = NewCacheMetrics(te.Registry)

	read := func() float64 {
		mfs, err := te.Registry.Gather()
		require.NoError(t, err)
		for _, mf := range mfs {
			if mf.GetName() == "nornicdb_process_uptime_seconds" {
				require.Len(t, mf.Metric, 1)
				return mf.Metric[0].GetGauge().GetValue()
			}
		}
		t.Fatal("nornicdb_process_uptime_seconds not registered")
		return 0
	}

	v1 := read()
	assert.GreaterOrEqual(t, v1, 0.0, "uptime must be non-negative")
	time.Sleep(10 * time.Millisecond)
	v2 := read()
	assert.Greater(t, v2, v1, "uptime must be monotonically increasing across scrapes")
}
