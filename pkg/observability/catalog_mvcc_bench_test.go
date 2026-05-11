package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotMVCC measures the MET-25 alloc budget for the MVCC
// bag's hot-path observation. The three GaugeFunc callbacks are exercised
// only at /metrics scrape time (cold path) so the bench focuses on the
// pressure_band indicator-set path called at MVCC reader open/close.
//
// Budgets (CONTEXT D-02b / Phase 3 SC #5):
//   - mvcc_pressure_band_set: ≤ 2 allocs/op for a single
//     WithLabelValues + Set on the indicator gauge.
//   - mvcc_update_band_full: ≤ 2 allocs/op AMORTIZED across the four
//     band gauges (one Set per band per call; pre-bound by tenant flag).
func BenchmarkObserve_HotMVCC(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewMVCCMetrics(reg, false /* tenantLabelsEnabled */, benchMVCCProbe{})

	b.Run("mvcc_pressure_band_set", func(b *testing.B) {
		gauge := bag.PressureBand.WithLabelValues("normal")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			gauge.Set(1)
		}
	})

	b.Run("mvcc_update_band_full", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.UpdateBand("", 0.30)
		}
	})
}

type benchMVCCProbe struct{}

func (benchMVCCProbe) PinnedBytes() int64              { return 0 }
func (benchMVCCProbe) OldestReaderAgeSeconds() float64 { return 0 }
func (benchMVCCProbe) ActiveReaders() int64            { return 0 }
