package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotCache/cache_* — MET-25 alloc-budget bench harness
// for the Cache+Runtime bag. Plan 04-07 absorbs this file's output into the
// per-subsystem evidence appendix.
//
// Parent name BenchmarkObserve_HotCache (rather than BenchmarkObserve_Hot)
// because exemplar_bench_test.go already owns BenchmarkObserve_Hot for the
// Phase-3 latency-histogram template (cold_no_span / hot_no_span /
// hot_with_span). Both functions match `go test -bench BenchmarkObserve_Hot`
// — Go's -bench is a regex that matches any function whose name contains
// the supplied pattern — so a single bench invocation runs both. The
// rename keeps subsystem-specific sub-benches grouped per file (per the
// per-subsystem evidence appendix Plan 04-07 will cumulate), without
// risking a duplicate-function-declaration compile error in pkg/observability.
//
// Mirrors the Phase-3 exemplar_bench_test.go template
// (BenchmarkObserve_Hot/hot_no_span ≤ 2 allocs/op):
//   - bound observer cached as a struct field at construction (MET-25);
//   - hot loop calls bound.Inc() — the inner WithLabelValues lookup is
//     amortized away from the per-event hot path.
//
// Budget: ≤ 2 allocs/op per Phase 3 D-02b. The verify command in 04-01-PLAN
// pipes through awk to enforce.
func BenchmarkObserve_HotCache(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewCacheMetrics(reg)

	b.Run("cache_hits_bound", func(b *testing.B) {
		// Pre-bound counter: WithLabelValues happens ONCE at construction;
		// the hot loop calls Inc() on the cached prometheus.Counter handle.
		// This is the hot-path discipline subsystems will use (Plan 04-03
		// pkg/cypher/cache.go wires this for "query_result").
		bound := bag.Hits.WithLabelValues("query_result")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Inc()
		}
	})

	b.Run("cache_evictions_bound", func(b *testing.B) {
		// Two-label bind: {cache, reason} per CONTEXT D-12b. Pre-bind on
		// the eviction-path constructor; hot loop only Inc()s.
		bound := bag.Evictions.WithLabelValues("query_result", "lru")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Inc()
		}
	})
}
