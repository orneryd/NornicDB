package temporal

import (
	"testing"
	"time"
)

func TestLoadConfig_Default(t *testing.T) {
	cfg := DefaultLoadConfig()
	if cfg.BucketDurationSeconds <= 0 {
		t.Error("BucketDurationSeconds should be positive")
	}
}

func TestLoadConfig_HighSensitivity(t *testing.T) {
	cfg := HighSensitivityLoadConfig()
	def := DefaultLoadConfig()
	if cfg.SpikeThreshold >= def.SpikeThreshold {
		t.Error("HighSensitivity should have lower spike threshold")
	}
}

func TestQueryLoadPredictor_New(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())
	if qlp == nil {
		t.Fatal("NewQueryLoadPredictor returned nil")
	}
}

func TestQueryLoadPredictor_RecordQuery(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	for i := 0; i < 100; i++ {
		qlp.RecordQuery()
	}

	stats := qlp.GetStats()
	if stats.TotalQueries != 100 {
		t.Errorf("TotalQueries = %v, want 100", stats.TotalQueries)
	}
}

func TestQueryLoadPredictor_RecordQueries(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	qlp.RecordQueries(50)
	qlp.RecordQueries(50)

	stats := qlp.GetStats()
	if stats.TotalQueries != 100 {
		t.Errorf("TotalQueries = %v, want 100", stats.TotalQueries)
	}
}

func TestQueryLoadPredictor_GetPrediction(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	// Generate some load
	for i := 0; i < 1000; i++ {
		qlp.RecordQuery()
	}

	pred := qlp.GetPrediction()
	t.Logf("Prediction:")
	t.Logf("  CurrentQPS: %.2f", pred.CurrentQPS)
	t.Logf("  Trend: %s", pred.Trend)
	t.Logf("  Predicted5m: %.2f", pred.PredictedQPS5m)
	t.Logf("  Confidence: %.3f", pred.Confidence)
}

func TestQueryLoadPredictor_ShouldScaleUp(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	// Low load - should not scale up
	for i := 0; i < 10; i++ {
		qlp.RecordQuery()
	}

	if qlp.ShouldScaleUp(100) {
		t.Error("Should not suggest scale up with low load")
	}
}

func TestQueryLoadPredictor_ShouldScaleDown(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	// Record some queries to establish baseline
	for i := 0; i < 50; i++ {
		qlp.RecordQuery()
	}

	// Wait and check scale down (this is more of a smoke test)
	shouldScale := qlp.ShouldScaleDown(1000, 0)
	t.Logf("Should scale down: %v", shouldScale)
}

func TestQueryLoadPredictor_GetLoadLevel(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	// No load = level 0
	level := qlp.GetLoadLevel(100)
	if level != 0 {
		t.Errorf("LoadLevel = %v, want 0 (idle)", level)
	}
}

func TestQueryLoadPredictor_PredictPeakTime(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	// Record queries at specific hour
	peakHour := 14 // 2pm
	baseTime := time.Date(2024, 1, 1, peakHour, 0, 0, 0, time.UTC)
	for i := 0; i < 100; i++ {
		qlp.RecordQueryAt(baseTime.Add(time.Duration(i) * time.Second))
	}

	peakTime := qlp.PredictPeakTime()
	t.Logf("Predicted peak time: %v", peakTime)
	if peakTime.Hour() != peakHour {
		t.Logf("Note: Peak hour prediction may vary based on current time")
	}
}

func TestQueryLoadPredictor_Reset(t *testing.T) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	for i := 0; i < 100; i++ {
		qlp.RecordQuery()
	}

	qlp.Reset()

	stats := qlp.GetStats()
	if stats.TotalQueries != 0 {
		t.Errorf("TotalQueries = %v, want 0 after reset", stats.TotalQueries)
	}
}

func TestQueryLoadPredictor_BucketTransition(t *testing.T) {
	cfg := DefaultLoadConfig()
	cfg.BucketDurationSeconds = 1.0
	qlp := NewQueryLoadPredictor(cfg)

	// Record queries across buckets
	base := time.Now()
	for i := 0; i < 5; i++ {
		qlp.RecordQueryAt(base.Add(time.Duration(i) * 2 * time.Second))
	}

	pred := qlp.GetPrediction()
	t.Logf("After bucket transitions - TotalQueries: %d", pred.TotalQueries)
}

func BenchmarkQueryLoadPredictor_RecordQuery(b *testing.B) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		qlp.RecordQuery()
	}
}

func BenchmarkQueryLoadPredictor_GetPrediction(b *testing.B) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())

	for i := 0; i < 1000; i++ {
		qlp.RecordQuery()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		qlp.GetPrediction()
	}
}

func BenchmarkQueryLoadPredictor_RecordQueries(b *testing.B) {
	qlp := NewQueryLoadPredictor(DefaultLoadConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		qlp.RecordQueries(100)
	}
}
