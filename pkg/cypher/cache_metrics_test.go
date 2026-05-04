// Package cypher — Plan 04-03-04 GREEN tests for the planner-cache and
// cross-cutting cache_hits_total wiring per CONTEXT D-12a.
//
// D-12a: planner_cache_hits/misses/size live under the CYPHER subsystem
// (nornicdb_cypher_planner_*), NOT the cross-cutting cache subsystem.
// The Cypher query-result cache ALSO bridges into the cross-cutting cache
// bag (nornicdb_cache_hits_total{cache="query_result"}) so the unified
// hit-ratio recording rule (Phase 10 K8S-11) sees the planner workload.
package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCacheBagsForTest(t *testing.T) (*observability.CypherMetrics, *observability.CacheMetrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	cm := observability.NewCypherMetrics(reg, false, func() float64 { return 1.0 })
	cache := observability.NewCacheMetrics(reg)
	return cm, cache, reg
}

// TestPlannerCache_HitsMisses asserts the QueryPlanCache emits planner-
// specific hits/misses through the CypherMetrics bag (D-12a — Cypher subsystem
// owns these counters).
func TestPlannerCache_HitsMisses(t *testing.T) {
	cm, _, _ := newCacheBagsForTest(t)
	pc := NewQueryPlanCache(8)
	pc.SetCypherMetrics(cm)

	// Miss → counter increments.
	_, _, found := pc.Get("MATCH (n) RETURN n")
	assert.False(t, found, "first Get is a miss")
	assert.Equal(t, 1.0, testutil.ToFloat64(cm.PlannerCacheMisses.WithLabelValues()),
		"miss → planner_cache_misses_total++")

	// Put then Get → hit.
	pc.Put("MATCH (n) RETURN n", nil, QueryMatch)
	_, _, found = pc.Get("MATCH (n) RETURN n")
	assert.True(t, found, "second Get is a hit")
	assert.Equal(t, 1.0, testutil.ToFloat64(cm.PlannerCacheHits.WithLabelValues()),
		"hit → planner_cache_hits_total++")
}

// TestPlannerCacheSize_Gauge asserts planner_cache_size tracks current cache
// occupancy via Set on Put/evict.
func TestPlannerCacheSize_Gauge(t *testing.T) {
	cm, _, _ := newCacheBagsForTest(t)
	pc := NewQueryPlanCache(2) // small max so eviction is observable
	pc.SetCypherMetrics(cm)

	pc.Put("Q1", nil, QueryMatch)
	assert.Equal(t, 1.0, testutil.ToFloat64(cm.PlannerCacheSize),
		"after one Put → size=1")

	pc.Put("Q2", nil, QueryMatch)
	assert.Equal(t, 2.0, testutil.ToFloat64(cm.PlannerCacheSize),
		"after two Puts → size=2")

	// Third Put triggers LRU eviction; size stays at maxSize.
	pc.Put("Q3", nil, QueryMatch)
	assert.Equal(t, 2.0, testutil.ToFloat64(cm.PlannerCacheSize),
		"after Put past capacity → size=2 (LRU evicted)")
}

// TestQueryResultCache_BridgeIntoCacheBag asserts the SmartQueryCache (Cypher
// query-result cache) emits into BOTH the Cypher PlannerCache* counters
// (NO — those are planner-only) AND the cross-cutting CacheMetrics
// `cache_hits_total{cache="query_result"}`. Per D-12a the Cypher result cache
// is a CACHE-subsystem citizen and DOES NOT emit into planner_cache_*. The
// planner cache (QueryPlanCache) is a separate concern.
func TestQueryResultCache_BridgeIntoCacheBag(t *testing.T) {
	_, cb, _ := newCacheBagsForTest(t)
	sc := NewSmartQueryCache(8)
	sc.SetCacheMetrics(cb)

	// Miss → cache_misses_total{cache="query_result"}++
	_, found := sc.Get("MATCH (n) RETURN n", nil)
	assert.False(t, found)
	assert.Equal(t, 1.0,
		testutil.ToFloat64(cb.Misses.WithLabelValues("query_result")),
		"D-12a bridge: miss increments cache_misses_total{cache=query_result}")

	sc.Put("MATCH (n) RETURN n", nil, &ExecuteResult{}, 60_000_000_000) // 60s ttl
	_, found = sc.Get("MATCH (n) RETURN n", nil)
	assert.True(t, found)
	assert.Equal(t, 1.0,
		testutil.ToFloat64(cb.Hits.WithLabelValues("query_result")),
		"D-12a bridge: hit increments cache_hits_total{cache=query_result}")
}

// TestCacheEvictions_ClosedReasonEnum asserts SmartQueryCache LRU eviction
// emits a cache_evictions_total{cache="query_result", reason="lru"} event.
// reason values come from the closed observability.AllowedEvictionReasons
// enum {lru, ttl, capacity, manual} — anything else would be a forbidden
// label cardinality bomb caught at registration via Phase 3 D-03a.
func TestCacheEvictions_ClosedReasonEnum(t *testing.T) {
	_, cb, _ := newCacheBagsForTest(t)
	sc := NewSmartQueryCache(2) // tiny so LRU eviction triggers
	sc.SetCacheMetrics(cb)

	sc.Put("Q1", nil, &ExecuteResult{}, 60_000_000_000)
	sc.Put("Q2", nil, &ExecuteResult{}, 60_000_000_000)
	// Third Put forces an LRU eviction.
	sc.Put("Q3", nil, &ExecuteResult{}, 60_000_000_000)

	got := testutil.ToFloat64(cb.Evictions.WithLabelValues("query_result", "lru"))
	require.GreaterOrEqual(t, got, 1.0,
		"D-12b: LRU eviction → cache_evictions_total{cache=query_result, reason=lru}")
}

// TestPlannerCache_NilBag_NoOp asserts the cache tolerates an unset metrics
// bag (forward-compat: Plan 04-03 wires it at startup; older fixtures may not).
func TestPlannerCache_NilBag_NoOp(t *testing.T) {
	pc := NewQueryPlanCache(8)
	// No SetCypherMetrics call.
	pc.Put("Q1", nil, QueryMatch)
	_, _, found := pc.Get("Q1")
	assert.True(t, found, "cache without metrics bag must still function")
}

// TestSmartQueryCache_NilBag_NoOp asserts the result cache tolerates nil bag.
func TestSmartQueryCache_NilBag_NoOp(t *testing.T) {
	sc := NewSmartQueryCache(8)
	sc.Put("Q1", nil, &ExecuteResult{}, 60_000_000_000)
	_, found := sc.Get("Q1", nil)
	assert.True(t, found, "result cache without metrics bag must still function")
}
