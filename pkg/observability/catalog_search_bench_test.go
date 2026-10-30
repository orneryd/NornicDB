package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotSearch measures the MET-25 alloc budget for the
// Search bag's hot-path observation. Per-subsystem suffix matches the
// established BenchmarkObserve_Hot{Cache,HTTP,Bolt,Cypher,Storage,MVCC}
// cadence; -bench BenchmarkObserve_Hot regex still matches all of them.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - search_duration_bound: ≤ 2 allocs/op (pre-bound observer cached
//     in pkg/search.Service struct fields per AttachMetrics —
//     production NeverSample default bypasses exemplar emission).
//   - search_requests_inc: ≤ 2 allocs/op (CounterVec.WithLabelValues +
//     Inc with closed-enum string literals).
//   - search_candidates_observe: ≤ 2 allocs/op (no-label histogram).
//   - search_index_size_set: ≤ 2 allocs/op (called every 30s by sweeper
//     in production; bench pins regression budget).
func BenchmarkObserve_HotSearch(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewSearchMetrics(reg, false /* tenant-OFF */, benchSearchProbe{})

	b.Run("search_duration_bound", func(b *testing.B) {
		bound := bag.BindDuration("", "hybrid", "embed")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("search_requests_inc", func(b *testing.B) {
		ctr := bag.Requests.WithLabelValues("hybrid", "success")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("search_candidates_observe", func(b *testing.B) {
		obs := bag.Candidates.Vec().WithLabelValues()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			obs.Observe(10)
		}
	})

	b.Run("search_index_size_set", func(b *testing.B) {
		gauge := bag.IndexSizeBytes.WithLabelValues("hnsw")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			gauge.Set(float64(i))
		}
	})
}

type benchSearchProbe struct{}

func (benchSearchProbe) IndexSizeBytes(kind string) uint64 { return 0 }
