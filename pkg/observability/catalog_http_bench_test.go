package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotHTTP measures the MET-25 alloc budget for the
// HTTP bag's hot-path observation. Mirrors Phase 3's exemplar_bench_test.go
// template; renamed (HotHTTP suffix) to avoid duplicate-function-declaration
// with the Phase 3 BenchmarkObserve_Hot in the same package — Plan 04-01
// established the per-subsystem suffix cadence (BenchmarkObserve_HotCache,
// BenchmarkObserve_HotHTTP, BenchmarkObserve_HotBolt, …).
//
// `go test -bench BenchmarkObserve_Hot ./pkg/observability/...` is a
// regex match — both this and Phase 3's parent function run from a
// single invocation.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - http_request_duration_bound: ≤ 2 allocs/op (production NeverSample
//     default — exemplar fast path bypasses; pre-bound observer holds
//     the WithLabelValues lookup amortized away).
func BenchmarkObserve_HotHTTP(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewHTTPMetrics(reg, false /* tenantLabelsEnabled */)

	b.Run("http_request_duration_bound", func(b *testing.B) {
		bound := bag.RequestDuration.Bind("GET", "/livez", "2xx")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("http_request_duration_bound_tenant", func(b *testing.B) {
		// D-08 forward-compat: same hot-path budget when the bag is
		// constructed with tenantLabelsEnabled=true (Phase 5 K8s mode).
		regT := prometheus.NewRegistry()
		bagT := NewHTTPMetrics(regT, true)
		bound := bagT.RequestDuration.Bind("GET", "/db/{database}/foo", "2xx", "mydb")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("http_requests_counter_bound", func(b *testing.B) {
		// Pre-bound counter via WithLabelValues — analog of Bind() for
		// raw *prometheus.CounterVec. Counter Inc() is allocation-free
		// when observer is pre-bound.
		ctr := bag.Requests.WithLabelValues("GET", "/livez", "2xx")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})
}
