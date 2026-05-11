package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// BenchmarkObserve_Hot is the MET-25 alloc-budget bench harness mandated by
// CONTEXT D-02c. Three sub-benches measure the cold path, the hot
// pre-bound path with no active span (production NeverSample default), and
// the hot path with an active sampled span (Phase 6 forward simulation).
//
// Budgets:
//   - hot_no_span:   ≤ 2 allocs/op (D-02b — exemplar fast path bypassed)
//   - hot_with_span: ≤ 4 allocs/op informational (Labels{} map + 2× String());
//     escalation to sync.Pool[*prometheus.Labels] (D-02b1) triggers if exceeded.
func BenchmarkObserve_Hot(b *testing.B) {
	reg := prometheus.NewRegistry()
	vec := NewLatencyHistogramVec(reg,
		MetricOpts{Subsystem: "cypher", Name: "query_duration_seconds", Help: "test"},
		[]string{"database", "op_type"})
	h := &LatencyHistogram{vec: vec}

	b.Run("cold_no_span", func(b *testing.B) {
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			h.Observe(ctx, []string{"db1", "read"}, 0.001)
		}
	})

	b.Run("hot_no_span", func(b *testing.B) {
		bound := h.Bind("db1", "read")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("hot_with_span", func(b *testing.B) {
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(tracetest.NewInMemoryExporter())),
		)
		b.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		ctx, span := tp.Tracer("bench").Start(context.Background(), "op")
		b.Cleanup(func() { span.End() })
		bound := h.Bind("db1", "read")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})
}
