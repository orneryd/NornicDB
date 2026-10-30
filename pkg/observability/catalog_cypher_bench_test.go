package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotCypher measures the MET-25 alloc budget for the
// Cypher bag's hot-path observation. Mirrors Phase 3's exemplar_bench_test.go
// template + Plan 04-02's BenchmarkObserve_HotHTTP/HotBolt cadence; renamed
// (HotCypher suffix) to avoid duplicate-function-declaration with the
// Phase 3 BenchmarkObserve_Hot in the same package — Plan 04-01 established
// the per-subsystem suffix cadence.
//
// `go test -bench BenchmarkObserve_Hot ./pkg/observability/...` is a regex
// match — Phase 3's parent function and Plan 04-01..04-06 per-subsystem
// children all run from a single invocation.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5 / Plan 04-03 CLAUDE.md gate):
//   - cypher_query_duration_bound_read: ≤ 2 allocs/op (production
//     NeverSample default — exemplar fast path bypasses; pre-bound observer
//     holds the WithLabelValues lookup amortized away).
//   - cypher_query_duration_bound_write_tenant: ≤ 2 allocs/op (D-08
//     forward-compat: same hot-path budget when tenantLabelsEnabled=true).
//   - cypher_planner_cache_hits_inc: ≤ 2 allocs/op baseline (no labels;
//     bare *prometheus.CounterVec.WithLabelValues + Inc is the cheapest
//     metric type; should be 0 allocs/op).
//   - cypher_rows_returned_bound_read: ≤ 2 allocs/op (RowCountHistogram
//     hot path — exercised on every Execute that returns rows).
func BenchmarkObserve_HotCypher(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewCypherMetrics(reg, false /* tenantLabelsEnabled */, func() float64 { return 1.0 })

	b.Run("cypher_query_duration_bound_read", func(b *testing.B) {
		bound := bag.QueryDuration.Bind("read")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("cypher_query_duration_bound_write_tenant", func(b *testing.B) {
		// D-08 forward-compat: same hot-path budget when the bag is
		// constructed with tenantLabelsEnabled=true (Phase 5 K8s mode).
		regT := prometheus.NewRegistry()
		bagT := NewCypherMetrics(regT, true, func() float64 { return 1.0 })
		bound := bagT.QueryDuration.Bind("write", "mydb")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("cypher_planner_cache_hits_inc", func(b *testing.B) {
		// Baseline: bare *prometheus.CounterVec.WithLabelValues + Inc.
		// Pre-bound — should be 0 allocs/op (counter Inc is the cheapest
		// metric path in client_golang).
		ctr := bag.PlannerCacheHits.WithLabelValues()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("cypher_rows_returned_bound_read", func(b *testing.B) {
		// RowCountHistogram hot path — exercised on every Execute that
		// returns rows. Same chokepoint shape as QueryDuration so should
		// hit the same ≤ 2 allocs/op budget.
		bound := bag.RowsReturned.Bind("read")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 1)
		}
	})

	b.Run("cypher_queries_counter_bound", func(b *testing.B) {
		// Counter pre-bound via the bag's tenant-flag-aware helper.
		// Subsystem call site uses BindQueries, not raw WithLabelValues,
		// to honor the D-08 helper-arity contract.
		ctr := bag.BindQueries("read", "")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})
}
