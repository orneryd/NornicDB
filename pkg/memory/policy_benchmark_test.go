package memory

import (
	"testing"
	"time"
)

// BenchmarkPolicyAdmission measures the per-request reservation cost with the
// policy disabled and with a fresh cached process sample. Sampling itself is
// deliberately outside the measured loop, as it is outside query execution.
func BenchmarkPolicyAdmission(b *testing.B) {
	for _, tc := range []struct {
		name  string
		limit int64
	}{
		{name: "disabled"},
		{name: "enabled", limit: 1 << 30},
	} {
		b.Run(tc.name, func(b *testing.B) {
			policy := NewPolicy(tc.limit, 1<<20, time.Hour, func() (Sample, error) {
				return Sample{Used: 1 << 20, Ceiling: 2 << 30}, nil
			}, time.Now)
			if err := policy.Refresh(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				lease, err := policy.Reserve(1024)
				if err != nil {
					b.Fatal(err)
				}
				lease.Release()
			}
		})
	}
}
