package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotBolt measures the MET-25 alloc budget for the
// Bolt bag's hot-path observation. Mirrors Phase 3's exemplar_bench_test.go
// template; renamed (HotBolt suffix) per Plan 04-01's per-subsystem
// suffix cadence to avoid duplicate-function-declaration in the same
// package.
//
// `go test -bench BenchmarkObserve_Hot ./pkg/observability/...` is a
// regex match — both this and Phase 3's parent function run from a
// single invocation.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - bolt_message_duration_bound: ≤ 2 allocs/op (production NeverSample
//     default — exemplar fast path bypasses; per-op pre-bound observer
//     pulled from the BoltMetrics-cached map at session ctor time).
func BenchmarkObserve_HotBolt(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewBoltMetrics(reg)

	b.Run("bolt_message_duration_bound", func(b *testing.B) {
		bound := bag.BindMessageDuration("run")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("bolt_messages_total_bound", func(b *testing.B) {
		// Pre-bound counter via WithLabelValues — analog of Bind() for
		// raw *prometheus.CounterVec.
		ctr := bag.MessagesTotal.WithLabelValues("run", "success")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("bolt_session_duration_bound", func(b *testing.B) {
		// SessionDuration has no labels — Bind() returns the no-arg
		// observer which is the cheapest possible binding.
		bound := bag.SessionDuration.Bind()
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("bolt_packstream_decode_errors_bound", func(b *testing.B) {
		ctr := bag.PackstreamDecodeErrors.WithLabelValues("truncated")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})
}
