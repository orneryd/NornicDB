package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotEmbed measures the MET-25 alloc budget for the
// Embed bag's hot-path observation. Mirrors the Plan 04-04 HotStorage
// cadence — per-subsystem suffix avoids duplicate-function declarations
// in the same package.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - embed_processed_inc: ≤ 2 allocs/op (CounterVec.WithLabelValues +
//     Inc; closed-enum string literals at the call site amortize the
//     WithLabelValues lookup over a single per-call invocation).
//   - embed_duration_bound: ≤ 2 allocs/op (pre-bound observer per
//     (provider, model, mode) tuple; production NeverSample default
//     bypasses exemplar emission).
//   - embed_ffi_panic_inc: ≤ 2 allocs/op (closed mode enum; only fires
//     on actual FFI panic in production but the bench pins the cost).
func BenchmarkObserve_HotEmbed(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewEmbedMetrics(reg, benchEmbedProbe{})

	b.Run("embed_processed_inc", func(b *testing.B) {
		ctr := bag.Processed.WithLabelValues("ollama", "bge-m3", "success", "cpu")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("embed_duration_bound", func(b *testing.B) {
		bound := bag.Duration.Bind("ollama", "bge-m3", "cpu")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("embed_ffi_panic_inc", func(b *testing.B) {
		ctr := bag.FFIPanicTotal.WithLabelValues("cpu")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("embed_cache_hits_inc", func(b *testing.B) {
		// Flat counter (no labels) — should be the cheapest path.
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.CacheHits.Inc()
		}
	})
}

type benchEmbedProbe struct{}

func (benchEmbedProbe) QueueLen() int { return 0 }
