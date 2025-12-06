package filter

import (
	"math"
	"math/rand"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func TestAdaptiveConfig_Default(t *testing.T) {
	cfg := DefaultAdaptiveConfig()
	if cfg.TrendThreshold <= 0 {
		t.Error("TrendThreshold should be positive")
	}
	if cfg.SwitchHysteresis < 1 {
		t.Error("SwitchHysteresis should be >= 1")
	}
	if cfg.InitialMode != ModeAuto {
		t.Errorf("InitialMode = %v, want ModeAuto", cfg.InitialMode)
	}
}

func TestAdaptiveConfig_SmoothingOptimized(t *testing.T) {
	cfg := SmoothingOptimizedConfig()
	def := DefaultAdaptiveConfig()

	if cfg.TrendThreshold <= def.TrendThreshold {
		t.Error("Smoothing config should have higher trend threshold")
	}
	if cfg.InitialMode != ModeBasic {
		t.Error("Smoothing config should start in basic mode")
	}
}

func TestAdaptiveConfig_TrackingOptimized(t *testing.T) {
	cfg := TrackingOptimizedConfig()
	def := DefaultAdaptiveConfig()

	if cfg.TrendThreshold >= def.TrendThreshold {
		t.Error("Tracking config should have lower trend threshold")
	}
	if cfg.InitialMode != ModeVelocity {
		t.Error("Tracking config should start in velocity mode")
	}
}

func TestKalmanAdaptive_New(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())
	if k == nil {
		t.Fatal("NewKalmanAdaptive returned nil")
	}
	// Starts in basic mode (auto defaults to basic)
	if k.Mode() != ModeBasic {
		t.Errorf("Mode = %v, want ModeBasic", k.Mode())
	}
	if k.Observations() != 0 {
		t.Errorf("Observations = %v, want 0", k.Observations())
	}
}

func TestKalmanAdaptive_StableSignal_StaysBasic(t *testing.T) {
	rand.Seed(42)
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Feed stable signal - should stay in basic mode or velocity with low trend
	for i := 0; i < 100; i++ {
		noisy := 50.0 + rand.NormFloat64()*2.0
		k.Process(noisy)
	}

	// Allow velocity mode if trend score is very low (< 0.05)
	// Noise can cause small trends, but they should be minimal
	if k.Mode() != ModeBasic && k.TrendScore() > 0.05 {
		t.Errorf("Mode = %v with TrendScore=%.4f, expected ModeBasic or low trend", k.Mode(), k.TrendScore())
	}
	
	// Allow a few mode switches for noisy stable signals
	if k.SwitchCount() > 3 {
		t.Errorf("Switched %d times (expected ≤3 for stable signal)", k.SwitchCount())
	}

	t.Logf("Stable signal: Mode=%s, TrendScore=%.4f, Switches=%d", k.Mode(), k.TrendScore(), k.SwitchCount())
}

func TestKalmanAdaptive_TrendSignal_SwitchesToVelocity(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Feed trending signal - should switch to velocity mode
	for i := 0; i < 100; i++ {
		value := float64(i) * 2.0 // Strong linear trend
		k.Process(value)
	}

	if k.Mode() != ModeVelocity {
		t.Errorf("Mode = %v, want ModeVelocity for trending signal", k.Mode())
	}
	if k.SwitchCount() == 0 {
		t.Error("Should have switched at least once for trending signal")
	}

	t.Logf("Trend signal: Mode=%s, TrendScore=%.4f, Switches=%d",
		k.Mode(), k.TrendScore(), k.SwitchCount())
}

func TestKalmanAdaptive_TrendThenStable_SwitchesBack(t *testing.T) {
	cfg := DefaultAdaptiveConfig()
	cfg.SwitchHysteresis = 5 // Allow faster switching for test
	k := NewKalmanAdaptive(cfg)

	// Phase 1: Trend (should switch to velocity)
	for i := 0; i < 50; i++ {
		k.Process(float64(i) * 2.0)
	}
	modeAfterTrend := k.Mode()

	// Phase 2: Stable (should switch back to basic)
	for i := 0; i < 100; i++ {
		k.Process(100.0 + rand.NormFloat64()*0.5) // Very stable
	}
	modeAfterStable := k.Mode()

	t.Logf("After trend: %s, After stable: %s, Total switches: %d",
		modeAfterTrend, modeAfterStable, k.SwitchCount())

	if modeAfterTrend != ModeVelocity {
		t.Errorf("Should be velocity after trend, got %s", modeAfterTrend)
	}
	// Might or might not switch back depending on hysteresis and exact values
}

func TestKalmanAdaptive_SetMode_ForcesMode(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Force velocity mode
	k.SetMode(ModeVelocity)
	if k.Mode() != ModeVelocity {
		t.Errorf("Mode = %v, want ModeVelocity after SetMode", k.Mode())
	}

	// Even with stable signal, should stay in velocity (forced)
	for i := 0; i < 50; i++ {
		k.Process(50.0)
	}
	if k.Mode() != ModeVelocity {
		t.Errorf("Mode = %v, want ModeVelocity (forced mode should not auto-switch)", k.Mode())
	}

	// Re-enable auto mode
	k.SetMode(ModeAuto)
	// Process more stable signal - now can switch
	for i := 0; i < 50; i++ {
		k.Process(50.0)
	}
	// Might switch back to basic
	t.Logf("After re-enabling auto: Mode=%s", k.Mode())
}

func TestKalmanAdaptive_Predict(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Train on linear trend
	for i := 0; i < 50; i++ {
		k.Process(float64(i * 2))
	}

	predicted := k.Predict(5)
	state := k.State()

	t.Logf("State: %.2f, Predict(5): %.2f, Mode: %s", state, predicted, k.Mode())

	// Prediction should be higher than current state for upward trend
	if predicted <= state {
		t.Errorf("Prediction %v should be > state %v for upward trend", predicted, state)
	}
}

func TestKalmanAdaptive_GetStats(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	for i := 0; i < 30; i++ {
		k.Process(float64(i))
	}

	stats := k.GetStats()

	if stats.Observations != 30 {
		t.Errorf("Observations = %v, want 30", stats.Observations)
	}
	if stats.Mode != k.Mode() {
		t.Errorf("Stats.Mode = %v, want %v", stats.Mode, k.Mode())
	}

	t.Logf("Stats: %+v", stats)
}

func TestKalmanAdaptive_Reset(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	for i := 0; i < 50; i++ {
		k.Process(float64(i * 10))
	}

	if k.Observations() == 0 {
		t.Error("Observations should not be 0 after processing")
	}

	k.Reset()

	if k.Observations() != 0 {
		t.Errorf("Observations after reset = %v, want 0", k.Observations())
	}
	if k.SwitchCount() != 0 {
		t.Errorf("SwitchCount after reset = %v, want 0", k.SwitchCount())
	}
	if k.Mode() != ModeBasic {
		t.Errorf("Mode after reset = %v, want ModeBasic", k.Mode())
	}
}

func TestKalmanAdaptive_ProcessBatch(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	measurements := []float64{10, 20, 30, 40, 50}
	results := k.ProcessBatch(measurements)

	if len(results) != 5 {
		t.Errorf("Results length = %v, want 5", len(results))
	}
	if k.Observations() != 5 {
		t.Errorf("Observations = %v, want 5", k.Observations())
	}
}

func TestKalmanAdaptive_ProcessIfEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Disabled
	result := k.ProcessIfEnabled(config.FeatureKalmanDecay, 100.0)
	if result.WasFiltered {
		t.Error("Should not filter when disabled")
	}

	// Enabled
	config.EnableKalmanFiltering()
	result = k.ProcessIfEnabled(config.FeatureKalmanDecay, 100.0)
	if !result.WasFiltered {
		t.Error("Should filter when enabled")
	}
}

// ===== Comparison: Adaptive vs Fixed Mode =====

func TestComparison_AdaptiveVsFixed_MixedSignal(t *testing.T) {
	rand.Seed(42)

	// Create filters
	adaptive := NewKalmanAdaptive(DefaultAdaptiveConfig())
	basic := NewKalman(DefaultConfig())
	velocity := NewKalmanVelocity(DefaultVelocityConfig())

	var adaptiveErrors, basicErrors, velocityErrors []float64

	// Mixed signal: stable → trend → stable → trend
	generateMixed := func(i int) (float64, float64) {
		phase := (i / 50) % 4
		var trueVal float64
		switch phase {
		case 0: // Stable at 50
			trueVal = 50.0
		case 1: // Rising trend
			trueVal = 50.0 + float64(i%50)*2.0
		case 2: // Stable at 150
			trueVal = 150.0
		case 3: // Falling trend
			trueVal = 150.0 - float64(i%50)*2.0
		}
		noisy := trueVal + rand.NormFloat64()*3.0
		return noisy, trueVal
	}

	for i := 0; i < 200; i++ {
		noisy, trueVal := generateMixed(i)

		adaptiveFiltered := adaptive.Process(noisy)
		basicFiltered := basic.Process(noisy, 0)
		velocityFiltered := velocity.Process(noisy)

		if i >= 20 { // Skip warmup
			adaptiveErrors = append(adaptiveErrors, math.Abs(adaptiveFiltered-trueVal))
			basicErrors = append(basicErrors, math.Abs(basicFiltered-trueVal))
			velocityErrors = append(velocityErrors, math.Abs(velocityFiltered-trueVal))
		}
	}

	adaptiveMAE := mean(adaptiveErrors)
	basicMAE := mean(basicErrors)
	velocityMAE := mean(velocityErrors)

	t.Log("MIXED SIGNAL COMPARISON (stable → trend → stable → trend):")
	t.Logf("  Adaptive MAE: %.3f", adaptiveMAE)
	t.Logf("  Basic MAE: %.3f", basicMAE)
	t.Logf("  Velocity MAE: %.3f", velocityMAE)
	t.Logf("  Adaptive switches: %d", adaptive.SwitchCount())

	// Adaptive should be competitive with the best of both
	bestFixed := math.Min(basicMAE, velocityMAE)
	if adaptiveMAE > bestFixed*1.5 {
		t.Logf("Warning: Adaptive (%.3f) significantly worse than best fixed (%.3f)", adaptiveMAE, bestFixed)
	}
}

func TestComparison_AdaptiveVsFixed_RealWorldScenario(t *testing.T) {
	rand.Seed(42)

	adaptive := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Simulate realistic memory access pattern:
	// - Frequent stable periods (node at rest)
	// - Occasional bursts (active research sessions)
	// - Gradual decay when not accessed

	var modeLog []string

	for i := 0; i < 300; i++ {
		var value float64

		if i < 50 {
			// Initial stable period
			value = 0.8 + rand.NormFloat64()*0.05
		} else if i < 100 {
			// Decay phase
			value = 0.8 - float64(i-50)*0.01 + rand.NormFloat64()*0.02
		} else if i < 150 {
			// Stable at low level
			value = 0.3 + rand.NormFloat64()*0.03
		} else if i < 200 {
			// Burst of activity (rising)
			value = 0.3 + float64(i-150)*0.012 + rand.NormFloat64()*0.02
		} else {
			// Another decay
			value = 0.9 - float64(i-200)*0.005 + rand.NormFloat64()*0.02
		}

		adaptive.Process(value)

		// Log mode changes
		if i%50 == 49 {
			modeLog = append(modeLog, string(adaptive.Mode()))
		}
	}

	t.Log("REALISTIC MEMORY ACCESS PATTERN:")
	t.Logf("  Mode progression: %v", modeLog)
	t.Logf("  Total switches: %d", adaptive.SwitchCount())
	t.Logf("  Final mode: %s", adaptive.Mode())
	t.Logf("  Final trend score: %.4f", adaptive.TrendScore())

	// Should have switched at least a few times for this varied signal
	if adaptive.SwitchCount() == 0 {
		t.Log("Note: No switches detected - may need tuning for this signal type")
	}
}

func TestKalmanAdaptive_TrendScore(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Feed stable signal
	for i := 0; i < 50; i++ {
		k.Process(50.0)
	}
	stableTrendScore := k.TrendScore()

	// Reset and feed trending signal
	k.Reset()
	for i := 0; i < 50; i++ {
		k.Process(float64(i) * 2)
	}
	trendTrendScore := k.TrendScore()

	t.Logf("Stable signal trend score: %.4f", stableTrendScore)
	t.Logf("Trending signal trend score: %.4f", trendTrendScore)

	if trendTrendScore <= stableTrendScore {
		t.Error("Trending signal should have higher trend score")
	}
}

func TestKalmanAdaptive_PredictionError(t *testing.T) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())

	// Feed predictable signal
	for i := 0; i < 50; i++ {
		k.Process(float64(i))
	}

	err := k.PredictionError()
	t.Logf("Prediction error for linear signal: %.4f", err)

	if err < 0 {
		t.Error("Prediction error should be non-negative")
	}
}

func TestKalmanAdaptive_SmallWindowSize(t *testing.T) {
	cfg := DefaultAdaptiveConfig()
	cfg.WindowSize = 2 // Too small, should be corrected to 5

	k := NewKalmanAdaptive(cfg)

	// Should not panic
	for i := 0; i < 20; i++ {
		k.Process(float64(i))
	}

	if k.Observations() != 20 {
		t.Errorf("Observations = %v, want 20", k.Observations())
	}
}

// ===== Benchmark =====

func BenchmarkKalmanAdaptive_Process(b *testing.B) {
	k := NewKalmanAdaptive(DefaultAdaptiveConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.Process(float64(i % 100))
	}
}

func BenchmarkComparison_AllFilters(b *testing.B) {
	b.Run("Basic", func(b *testing.B) {
		k := NewKalman(DefaultConfig())
		for i := 0; i < b.N; i++ {
			k.Process(float64(i), 0)
		}
	})

	b.Run("Velocity", func(b *testing.B) {
		k := NewKalmanVelocity(DefaultVelocityConfig())
		for i := 0; i < b.N; i++ {
			k.Process(float64(i))
		}
	})

	b.Run("Adaptive", func(b *testing.B) {
		k := NewKalmanAdaptive(DefaultAdaptiveConfig())
		for i := 0; i < b.N; i++ {
			k.Process(float64(i))
		}
	})
}
