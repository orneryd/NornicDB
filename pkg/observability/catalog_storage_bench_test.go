package observability

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotStorage measures the MET-25 alloc budget for the
// Storage bag's hot-path observation. Mirrors the Plan 04-03 HotCypher
// cadence — per-subsystem suffix avoids duplicate-function declarations
// in the same package.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - storage_op_duration_bound_get: ≤ 2 allocs/op (pre-bound observer
//     hot path; production NeverSample default bypasses exemplar emission).
//   - storage_index_rebuild_inc: ≤ 2 allocs/op (CounterVec.WithLabelValues
//   - Inc; closed-enum string literals at the call site).
//   - storage_bytes_set: ≤ 2 allocs/op (GaugeVec.WithLabelValues + Set,
//     called every 30s by the sweeper — not a hot path but the bench
//     pins the budget to catch regressions before they surface in prod).
func BenchmarkObserve_HotStorage(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewStorageMetrics(reg, false /* tenantLabelsEnabled */, benchStorageProbe{})

	b.Run("storage_op_duration_bound_get", func(b *testing.B) {
		bound := bag.OpDuration.Bind("get")
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bound.Observe(ctx, 0.001)
		}
	})

	b.Run("storage_index_rebuild_inc", func(b *testing.B) {
		ctr := bag.IndexRebuildTotal.WithLabelValues("label", "success")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("storage_bytes_set", func(b *testing.B) {
		gauge := bag.Bytes.WithLabelValues("nodes")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			gauge.Set(float64(i))
		}
	})
}

type benchStorageProbe struct{}

func (benchStorageProbe) NodeCount() int64           { return 0 }
func (benchStorageProbe) EdgeCount() int64           { return 0 }
func (benchStorageProbe) IDDictCounterNodes() uint64 { return 0 }
func (benchStorageProbe) IDDictCounterEdges() uint64 { return 0 }
func (benchStorageProbe) IDDictFreelistNodes() int64 { return 0 }
func (benchStorageProbe) IDDictFreelistEdges() int64 { return 0 }
