package knowledgepolicy

import "testing"

func BenchmarkKalmanMutation_ManualR(b *testing.B) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}

	ProcessKalmanMutation("metric", 5.0, cfg, entry)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProcessKalmanMutation("metric", 5.0+float64(i%10)*0.1, cfg, entry)
	}
}

func BenchmarkKalmanMutation_AutoR(b *testing.B) {
	entry := &AccessMetaEntry{TargetID: "n1"}
	cfg := &KalmanConfig{Mode: KalmanModeAuto, Q: 0.05, R: 50.0, VarianceScale: 2.0, WindowSize: 50}

	ProcessKalmanMutation("metric", 5.0, cfg, entry)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ProcessKalmanMutation("metric", 5.0+float64(i%10)*0.1, cfg, entry)
	}
}

func BenchmarkKalmanMutation_VsPlainAccumulation(b *testing.B) {
	b.Run("plain", func(b *testing.B) {
		acc := NewAccessAccumulator(true)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			acc.IncrementAccess("n1")
		}
	})

	b.Run("kalman", func(b *testing.B) {
		entry := &AccessMetaEntry{TargetID: "n1"}
		cfg := &KalmanConfig{Mode: KalmanModeManual, Q: 0.05, R: 50.0}
		ProcessKalmanMutation("metric", 5.0, cfg, entry)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ProcessKalmanMutation("metric", 5.0, cfg, entry)
		}
	})
}
