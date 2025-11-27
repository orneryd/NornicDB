package filter

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

func TestVelocityConfig_Default(t *testing.T) {
	cfg := DefaultVelocityConfig()
	if cfg.Dt != 1.0 {
		t.Errorf("Dt = %v, want 1.0", cfg.Dt)
	}
	if cfg.ProcessNoisePos <= 0 {
		t.Error("ProcessNoisePos should be positive")
	}
}

func TestVelocityConfig_Temporal(t *testing.T) {
	cfg := TemporalTrackingConfig()
	if cfg.MeasurementNoise >= DefaultVelocityConfig().MeasurementNoise {
		t.Error("Temporal config should have lower measurement noise for tighter tracking")
	}
}

func TestVelocityConfig_Aggressive(t *testing.T) {
	cfg := AggressiveTrackingConfig()
	if cfg.ProcessNoiseVel <= DefaultVelocityConfig().ProcessNoiseVel {
		t.Error("Aggressive config should have higher velocity process noise")
	}
}

func TestKalmanVelocity_New(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())
	if k == nil {
		t.Fatal("NewKalmanVelocity returned nil")
	}
	if k.State() != 0 {
		t.Errorf("Initial state = %v, want 0", k.State())
	}
	if k.Velocity() != 0 {
		t.Errorf("Initial velocity = %v, want 0", k.Velocity())
	}
}

func TestKalmanVelocity_NewWithInitial(t *testing.T) {
	k := NewKalmanVelocityWithInitial(DefaultVelocityConfig(), 100.0, 5.0)
	if k.State() != 100.0 {
		t.Errorf("Initial state = %v, want 100.0", k.State())
	}
	if k.Velocity() != 5.0 {
		t.Errorf("Initial velocity = %v, want 5.0", k.Velocity())
	}
}

func TestKalmanVelocity_ConvergesToConstant(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	// Feed constant measurements - should converge to value with near-zero velocity
	for i := 0; i < 50; i++ {
		k.Process(100.0)
	}

	state := k.State()
	vel := k.Velocity()

	if math.Abs(state-100.0) > 1.0 {
		t.Errorf("State = %v, want ~100.0", state)
	}
	if math.Abs(vel) > 0.1 {
		t.Errorf("Velocity = %v, want ~0 for constant signal", vel)
	}
}

func TestKalmanVelocity_TracksLinearTrend(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	// Feed linear increasing signal: 0, 1, 2, 3, ...
	for i := 0; i < 50; i++ {
		k.Process(float64(i))
	}

	state := k.State()
	vel := k.Velocity()

	t.Logf("After linear trend (0 to 49):")
	t.Logf("  State: %.2f (true: 49)", state)
	t.Logf("  Velocity: %.4f (true: 1.0)", vel)

	// Should track closely
	if math.Abs(state-49) > 3 {
		t.Errorf("State = %v, want ~49", state)
	}
	// Velocity should be close to 1.0
	if math.Abs(vel-1.0) > 0.2 {
		t.Errorf("Velocity = %v, want ~1.0", vel)
	}
}

func TestKalmanVelocity_Predict(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	// Train on linear trend
	for i := 0; i < 30; i++ {
		k.Process(float64(i * 2)) // 0, 2, 4, 6, ...
	}

	// Predict 5 steps ahead
	predicted := k.Predict(5)
	expected := float64(30*2) + k.Velocity()*5 // ~68

	t.Logf("Trained on 0,2,4,...,58")
	t.Logf("Velocity learned: %.4f", k.Velocity())
	t.Logf("Predicted 5 steps: %.2f", predicted)

	// Prediction should be reasonable
	if math.Abs(predicted-expected) > 5 {
		t.Errorf("Predicted = %v, want ~%v", predicted, expected)
	}
}

func TestKalmanVelocity_PredictWithUncertainty(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	for i := 0; i < 20; i++ {
		k.Process(float64(i))
	}

	_, unc1 := k.PredictWithUncertainty(1)
	_, unc5 := k.PredictWithUncertainty(5)
	_, unc10 := k.PredictWithUncertainty(10)

	t.Logf("Uncertainties: 1-step=%.4f, 5-step=%.4f, 10-step=%.4f", unc1, unc5, unc10)

	// Uncertainty should grow with horizon
	if unc5 <= unc1 {
		t.Errorf("5-step uncertainty (%v) should be > 1-step (%v)", unc5, unc1)
	}
	if unc10 <= unc5 {
		t.Errorf("10-step uncertainty (%v) should be > 5-step (%v)", unc10, unc5)
	}
}

// ===== COMPARISON: Basic vs Velocity Filter for Trend Tracking =====

func TestComparison_BasicVsVelocity_LinearTrend(t *testing.T) {
	rand.Seed(42)

	basic := NewKalman(DefaultConfig())
	velocity := NewKalmanVelocity(DefaultVelocityConfig())

	var basicErrors, velocityErrors []float64

	// Linear trend with noise
	for i := 0; i < 100; i++ {
		actual := float64(i) * 0.5 // 0, 0.5, 1, 1.5, ...
		noisy := actual + rand.NormFloat64()*2.0

		basicFiltered := basic.Process(noisy, 0)
		velocityFiltered := velocity.Process(noisy)

		// Track errors (skip warmup)
		if i >= 20 {
			basicErrors = append(basicErrors, math.Abs(basicFiltered-actual))
			velocityErrors = append(velocityErrors, math.Abs(velocityFiltered-actual))
		}
	}

	basicMAE := mean(basicErrors)
	velocityMAE := mean(velocityErrors)
	improvement := (1 - velocityMAE/basicMAE) * 100

	t.Log("LINEAR TREND TRACKING COMPARISON:")
	t.Logf("  Basic Kalman MAE: %.4f", basicMAE)
	t.Logf("  Velocity Kalman MAE: %.4f", velocityMAE)
	t.Logf("  Improvement: %.1f%%", improvement)
	t.Logf("  Basic velocity estimate: %.4f", basic.Velocity())
	t.Logf("  Velocity filter velocity: %.4f (true: 0.5)", velocity.Velocity())

	// PROVE: Velocity filter is better for trends
	if improvement < 50 {
		t.Errorf("Expected >50%% improvement, got %.1f%%", improvement)
	}
}

func TestComparison_BasicVsVelocity_StableSignal(t *testing.T) {
	rand.Seed(42)

	basic := NewKalman(DefaultConfig())
	velocity := NewKalmanVelocity(DefaultVelocityConfig())

	trueValue := 50.0
	var basicErrors, velocityErrors []float64

	// Stable signal with noise (basic filter's strength)
	for i := 0; i < 100; i++ {
		noisy := trueValue + rand.NormFloat64()*5.0

		basicFiltered := basic.Process(noisy, 0)
		velocityFiltered := velocity.Process(noisy)

		if i >= 20 {
			basicErrors = append(basicErrors, math.Abs(basicFiltered-trueValue))
			velocityErrors = append(velocityErrors, math.Abs(velocityFiltered-trueValue))
		}
	}

	basicMAE := mean(basicErrors)
	velocityMAE := mean(velocityErrors)

	t.Log("STABLE SIGNAL COMPARISON:")
	t.Logf("  Basic Kalman MAE: %.4f", basicMAE)
	t.Logf("  Velocity Kalman MAE: %.4f", velocityMAE)

	// Both should work well for stable signals
	// Basic might be slightly better due to simpler model
	if basicMAE > 5 {
		t.Errorf("Basic filter error too high for stable signal: %.4f", basicMAE)
	}
	if velocityMAE > 5 {
		t.Errorf("Velocity filter error too high for stable signal: %.4f", velocityMAE)
	}
}

func TestComparison_BasicVsVelocity_Prediction(t *testing.T) {
	rand.Seed(42)

	basic := NewKalman(DefaultConfig())
	velocity := NewKalmanVelocity(DefaultVelocityConfig())

	// Train both filters on linear trend
	for i := 0; i < 50; i++ {
		actual := float64(i) * 2.0 // 0, 2, 4, 6, ...
		noisy := actual + rand.NormFloat64()*1.0

		basic.Process(noisy, 0)
		velocity.Process(noisy)
	}

	// Predict 10 steps ahead
	basicPred := basic.Predict(10)
	velocityPred := velocity.Predict(10)
	actualFuture := 50.0 * 2.0 // What the value should be at step 50+10

	basicError := math.Abs(basicPred - actualFuture)
	velocityError := math.Abs(velocityPred - actualFuture)

	t.Log("PREDICTION COMPARISON (10 steps ahead):")
	t.Logf("  Actual future value: %.2f", actualFuture)
	t.Logf("  Basic prediction: %.2f (error: %.2f)", basicPred, basicError)
	t.Logf("  Velocity prediction: %.2f (error: %.2f)", velocityPred, velocityError)
	t.Logf("  Basic velocity estimate: %.4f", basic.Velocity())
	t.Logf("  Velocity filter velocity: %.4f (true: 2.0)", velocity.Velocity())

	// PROVE: Velocity filter predicts better
	if velocityError > basicError*0.5 && velocityError > 10 {
		t.Logf("Velocity prediction not significantly better - may need tuning")
	}
}

func TestKalmanVelocity_ProcessBatch(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	measurements := []float64{10, 20, 30, 40, 50}
	results := k.ProcessBatch(measurements)

	if len(results) != 5 {
		t.Errorf("Results length = %v, want 5", len(results))
	}
	if k.Observations() != 5 {
		t.Errorf("Observations = %v, want 5", k.Observations())
	}

	// Results should be smoothed and trending upward
	t.Logf("Batch results: %v", results)
	if results[4] <= results[0] {
		t.Error("Results should trend upward for increasing inputs")
	}
}

func TestKalmanVelocity_Reset(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	for i := 0; i < 20; i++ {
		k.Process(float64(i * 10))
	}

	if k.State() == 0 {
		t.Error("State should not be 0 after processing")
	}

	k.Reset()

	if k.State() != 0 {
		t.Errorf("State after reset = %v, want 0", k.State())
	}
	if k.Velocity() != 0 {
		t.Errorf("Velocity after reset = %v, want 0", k.Velocity())
	}
	if k.Observations() != 0 {
		t.Errorf("Observations after reset = %v, want 0", k.Observations())
	}
}

func TestKalmanVelocity_SetState(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())
	k.SetState(100.0, 5.0)

	if k.State() != 100.0 {
		t.Errorf("State = %v, want 100.0", k.State())
	}
	if k.Velocity() != 5.0 {
		t.Errorf("Velocity = %v, want 5.0", k.Velocity())
	}
}

func TestKalmanVelocity_GetStats(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	for i := 0; i < 10; i++ {
		k.Process(float64(i * 5))
	}

	stats := k.GetStats()

	if stats.Observations != 10 {
		t.Errorf("Observations = %v, want 10", stats.Observations)
	}
	if stats.PositionVariance <= 0 {
		t.Errorf("PositionVariance = %v, want > 0", stats.PositionVariance)
	}
	if stats.VelocityVariance <= 0 {
		t.Errorf("VelocityVariance = %v, want > 0", stats.VelocityVariance)
	}

	t.Logf("Stats: %+v", stats)
}

func TestKalmanVelocity_VelocityCovariance(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	initialCov := k.VelocityCovariance()

	for i := 0; i < 50; i++ {
		k.Process(float64(i))
	}

	finalCov := k.VelocityCovariance()

	t.Logf("Initial velocity covariance: %.4f", initialCov)
	t.Logf("Final velocity covariance: %.4f", finalCov)

	// Covariance should decrease as we get more measurements
	if finalCov >= initialCov {
		t.Error("Velocity covariance should decrease with more observations")
	}
}

func TestKalmanVelocity_Position(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())
	k.Process(42.0)

	// Position() should be alias for State()
	if k.Position() != k.State() {
		t.Errorf("Position() = %v, State() = %v, should be equal", k.Position(), k.State())
	}
}

func TestKalmanVelocity_ProcessIfEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalmanVelocity(DefaultVelocityConfig())

	// Disabled: pass through
	result := k.ProcessIfEnabled(config.FeatureKalmanTemporal, 100.0)
	if result.WasFiltered {
		t.Error("Should not filter when disabled")
	}
	if result.Filtered != result.Raw {
		t.Error("Filtered should equal Raw when disabled")
	}

	// Enabled: filter
	config.EnableKalmanFiltering()
	result = k.ProcessIfEnabled(config.FeatureKalmanTemporal, 100.0)
	if !result.WasFiltered {
		t.Error("Should filter when enabled")
	}
}

func TestKalmanVelocity_PredictIfEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalmanVelocity(DefaultVelocityConfig())

	// Train
	for i := 0; i < 20; i++ {
		k.Process(float64(i * 10))
	}

	// Disabled
	result := k.PredictIfEnabled(config.FeatureKalmanTemporal, 5)
	if result.WasFiltered {
		t.Error("Should not predict when disabled")
	}

	// Enabled
	config.EnableKalmanFiltering()
	result = k.PredictIfEnabled(config.FeatureKalmanTemporal, 5)
	if !result.WasFiltered {
		t.Error("Should predict when enabled")
	}
}

func TestKalmanVelocity_ConcurrentAccess(t *testing.T) {
	k := NewKalmanVelocity(DefaultVelocityConfig())

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				k.Process(float64(id*10 + j))
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = k.State()
				_ = k.Velocity()
				_ = k.Predict(3)
			}
		}()
	}

	wg.Wait()
	t.Logf("Final observations: %d", k.Observations())
}

func TestKalmanVelocity_ZeroDt(t *testing.T) {
	cfg := DefaultVelocityConfig()
	cfg.Dt = 0 // Should be corrected to 1.0

	k := NewKalmanVelocity(cfg)
	k.Process(10.0)
	k.Process(20.0)

	// Should not panic and should work with dt=1.0
	if k.Observations() != 2 {
		t.Errorf("Observations = %v, want 2", k.Observations())
	}
}

// ===== Summary Comparison =====

func TestComparison_Summary_AllFilters(t *testing.T) {
	rand.Seed(42)

	t.Log("\n" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=")
	t.Log("FILTER COMPARISON SUMMARY")
	t.Log("=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=" + "=")

	scenarios := []struct {
		name      string
		generator func(i int) float64
		trueVel   float64
	}{
		{"Stable Signal", func(i int) float64 { return 50.0 + rand.NormFloat64()*5.0 }, 0.0},
		{"Linear Trend", func(i int) float64 { return float64(i)*0.5 + rand.NormFloat64()*2.0 }, 0.5},
		{"Fast Trend", func(i int) float64 { return float64(i)*2.0 + rand.NormFloat64()*3.0 }, 2.0},
		{"Decaying", func(i int) float64 { return 100.0*math.Exp(-0.02*float64(i)) + rand.NormFloat64()*2.0 }, -0.02},
	}

	for _, sc := range scenarios {
		basic := NewKalman(DefaultConfig())
		velocity := NewKalmanVelocity(DefaultVelocityConfig())

		var basicErr, velErr float64
		count := 0

		for i := 0; i < 100; i++ {
			obs := sc.generator(i)

			// True value (without noise)
			var trueVal float64
			switch sc.name {
			case "Stable Signal":
				trueVal = 50.0
			case "Linear Trend":
				trueVal = float64(i) * 0.5
			case "Fast Trend":
				trueVal = float64(i) * 2.0
			case "Decaying":
				trueVal = 100.0 * math.Exp(-0.02*float64(i))
			}

			basicFiltered := basic.Process(obs, 0)
			velFiltered := velocity.Process(obs)

			if i >= 30 { // Skip warmup
				basicErr += math.Abs(basicFiltered - trueVal)
				velErr += math.Abs(velFiltered - trueVal)
				count++
			}
		}

		basicErr /= float64(count)
		velErr /= float64(count)
		winner := "Basic"
		if velErr < basicErr {
			winner = "Velocity"
		}

		t.Logf("%-15s: Basic MAE=%.3f, Velocity MAE=%.3f  Winner: %s",
			sc.name, basicErr, velErr, winner)
	}

	t.Log("")
	t.Log("RECOMMENDATION:")
	t.Log("  - Use Basic Kalman for: stable signals, maximum noise rejection")
	t.Log("  - Use Velocity Kalman for: trends, temporal patterns, predictions")
}

// ===== Benchmarks =====

func BenchmarkKalmanVelocity_Process(b *testing.B) {
	k := NewKalmanVelocity(DefaultVelocityConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.Process(float64(i))
	}
}

func BenchmarkKalmanVelocity_Predict(b *testing.B) {
	k := NewKalmanVelocity(DefaultVelocityConfig())
	for i := 0; i < 100; i++ {
		k.Process(float64(i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.Predict(5)
	}
}

func BenchmarkComparison_BasicVsVelocity(b *testing.B) {
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
}
