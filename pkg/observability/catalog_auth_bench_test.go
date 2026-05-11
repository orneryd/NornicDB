package observability

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// BenchmarkObserve_HotAuth measures the MET-25 alloc budget for the
// Auth bag's hot-path observation. Per-subsystem suffix matches the
// established BenchmarkObserve_Hot{...} cadence.
//
// Budget (CONTEXT D-02b / Phase 3 SC #5):
//
//   - auth_attempts_inc: ≤ 2 allocs/op. CounterVec.WithLabelValues +
//     Inc with closed-enum string literals; per-label cell is hit
//     repeatedly so client_golang's per-label cache avoids hashing.
func BenchmarkObserve_HotAuth(b *testing.B) {
	reg := prometheus.NewRegistry()
	bag := NewAuthMetrics(reg)

	b.Run("auth_attempts_inc", func(b *testing.B) {
		ctr := bag.AuthAttempts.WithLabelValues("success", "bolt")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ctr.Inc()
		}
	})

	b.Run("auth_attempts_with_label_inc", func(b *testing.B) {
		// The realistic chokepoint pattern — WithLabelValues on every Inc
		// (the call site doesn't cache because there are only 9 cells and
		// the closed enums are string literals at the call site).
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			bag.AuthAttempts.WithLabelValues("success", "bolt").Inc()
		}
	})
}
