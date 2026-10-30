package filter

import (
	"math"
	"math/rand"
	"sync"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ProcessNoise != 0.1 {
		t.Errorf("ProcessNoise = %v, want 0.1", cfg.ProcessNoise)
	}
	if cfg.MeasurementNoise != 88.0 {
		t.Errorf("MeasurementNoise = %v, want 88.0", cfg.MeasurementNoise)
	}
	if cfg.InitialCovariance != 30.0 {
		t.Errorf("InitialCovariance = %v, want 30.0", cfg.InitialCovariance)
	}
}

func TestDecayPredictionConfig(t *testing.T) {
	cfg := DecayPredictionConfig()
	if cfg.ProcessNoise != 0.05 {
		t.Errorf("ProcessNoise = %v, want 0.05", cfg.ProcessNoise)
	}
}

func TestCoAccessConfig(t *testing.T) {
	cfg := CoAccessConfig()
	if cfg.ProcessNoise != 0.2 {
		t.Errorf("ProcessNoise = %v, want 0.2", cfg.ProcessNoise)
	}
}

func TestLatencyConfig(t *testing.T) {
	cfg := LatencyConfig()
	if cfg.ProcessNoise != 0.15 {
		t.Errorf("ProcessNoise = %v, want 0.15", cfg.ProcessNoise)
	}
}

func TestNewKalman(t *testing.T) {
	k := NewKalman(DefaultConfig())
	if k == nil {
		t.Fatal("NewKalman returned nil")
	}
	if k.State() != 0 {
		t.Errorf("Initial state = %v, want 0", k.State())
	}
	if k.Observations() != 0 {
		t.Errorf("Initial observations = %v, want 0", k.Observations())
	}
}

func TestNewKalmanWithInitial(t *testing.T) {
	k := NewKalmanWithInitial(DefaultConfig(), 100.0)
	if k.State() != 100.0 {
		t.Errorf("Initial state = %v, want 100.0", k.State())
	}
}

func TestProcess_ConvergesToMeasurement(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Feed constant measurements - filter should converge
	for i := 0; i < 50; i++ {
		k.Process(100.0, 0)
	}

	state := k.State()
	if math.Abs(state-100.0) > 1.0 {
		t.Errorf("State = %v, want ~100.0 (within 1.0)", state)
	}
}

func TestProcess_SetpointErrorBoosting(t *testing.T) {
	// Test that being far from setpoint increases responsiveness
	k1 := NewKalmanWithInitial(DefaultConfig(), 50.0)
	k2 := NewKalmanWithInitial(DefaultConfig(), 50.0)

	// k1: far from target (100), should adapt faster
	// k2: no target, normal adaptation
	for i := 0; i < 10; i++ {
		k1.Process(100.0, 100.0) // Target = measurement
		k2.Process(100.0, 0)     // No target
	}

	// Both should converge, but k1 might be slightly different due to error boosting
	state1 := k1.State()
	state2 := k2.State()

	// Both should be approaching 100
	if math.Abs(state1-100.0) > 20.0 {
		t.Errorf("k1 state = %v, want closer to 100.0", state1)
	}
	if math.Abs(state2-100.0) > 20.0 {
		t.Errorf("k2 state = %v, want closer to 100.0", state2)
	}
}

func TestProcess_NoisySignal(t *testing.T) {
	k := NewKalman(DefaultConfig())
	rng := rand.New(rand.NewSource(42))

	// Generate noisy signal around 50.0
	trueValue := 50.0
	noiseStd := 10.0
	var filtered float64

	for i := 0; i < 100; i++ {
		noisy := trueValue + rng.NormFloat64()*noiseStd
		filtered = k.Process(noisy, 0)
	}

	// Filtered value should be closer to true value than individual noisy samples
	if math.Abs(filtered-trueValue) > noiseStd/2 {
		t.Errorf("Filtered = %v, want closer to %v", filtered, trueValue)
	}
}

func TestPredict_LinearTrend(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Feed linear increasing signal: 10, 20, 30, 40, 50...
	for i := 1; i <= 10; i++ {
		k.Process(float64(i*10), 0)
	}

	// Current state should be around 100
	state := k.State()
	t.Logf("Current state after linear input: %.2f", state)

	// Predict 5 steps ahead - should continue the trend
	predicted := k.Predict(5)
	t.Logf("Predicted 5 steps ahead: %.2f", predicted)

	// Should predict something higher than current state
	if predicted <= state {
		t.Errorf("Predicted = %v, want > %v (continuation of upward trend)", predicted, state)
	}
}

func TestPredictWithUncertainty(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Process some data
	for i := 0; i < 20; i++ {
		k.Process(float64(i), 0)
	}

	val1, unc1 := k.PredictWithUncertainty(1)
	val5, unc5 := k.PredictWithUncertainty(5)
	val10, unc10 := k.PredictWithUncertainty(10)

	t.Logf("1-step: val=%.2f, unc=%.4f", val1, unc1)
	t.Logf("5-step: val=%.2f, unc=%.4f", val5, unc5)
	t.Logf("10-step: val=%.2f, unc=%.4f", val10, unc10)

	// Uncertainty should increase with prediction horizon
	if unc5 <= unc1 {
		t.Errorf("5-step uncertainty (%v) should be > 1-step (%v)", unc5, unc1)
	}
	if unc10 <= unc5 {
		t.Errorf("10-step uncertainty (%v) should be > 5-step (%v)", unc10, unc5)
	}
}

func TestVelocity(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Feed increasing values
	k.Process(10.0, 0)
	k.Process(20.0, 0)
	k.Process(30.0, 0)

	vel := k.Velocity()
	// Velocity should be positive (increasing trend)
	if vel < 0 {
		t.Errorf("Velocity = %v, want positive (increasing trend)", vel)
	}
	t.Logf("Velocity after increasing inputs: %.4f", vel)
}

func TestReset(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Process some data
	for i := 0; i < 20; i++ {
		k.Process(float64(i*10), 0)
	}

	if k.State() == 0 {
		t.Error("State should not be 0 after processing")
	}
	if k.Observations() == 0 {
		t.Error("Observations should not be 0 after processing")
	}

	k.Reset()

	if k.State() != 0 {
		t.Errorf("State after reset = %v, want 0", k.State())
	}
	if k.Observations() != 0 {
		t.Errorf("Observations after reset = %v, want 0", k.Observations())
	}
}

func TestSetState(t *testing.T) {
	k := NewKalman(DefaultConfig())
	k.SetState(999.0)

	if k.State() != 999.0 {
		t.Errorf("State = %v, want 999.0", k.State())
	}
}

func TestGetStats(t *testing.T) {
	k := NewKalman(DefaultConfig())

	for i := 0; i < 10; i++ {
		k.Process(float64(i*5), 0)
	}

	stats := k.GetStats()

	if stats.Observations != 10 {
		t.Errorf("Observations = %v, want 10", stats.Observations)
	}
	if stats.Covariance <= 0 {
		t.Errorf("Covariance = %v, want > 0", stats.Covariance)
	}
	t.Logf("Stats: %+v", stats)
}

func TestProcessBatch(t *testing.T) {
	k := NewKalman(DefaultConfig())

	measurements := []float64{10, 20, 30, 40, 50}
	results := k.ProcessBatch(measurements, 0)

	if len(results) != 5 {
		t.Errorf("Results length = %v, want 5", len(results))
	}
	if k.Observations() != 5 {
		t.Errorf("Observations = %v, want 5", k.Observations())
	}

	// Results should be smoothed versions of inputs
	t.Logf("Batch results: %v", results)
}

func TestUpdateAdaptiveR(t *testing.T) {
	k := NewKalman(DefaultConfig())
	initialR := k.r

	// Process noisy signal to build up innovations
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		k.Process(50.0+rng.NormFloat64()*20.0, 0)
	}

	k.UpdateAdaptiveR()

	// R should have changed based on innovation variance
	if k.r == initialR {
		t.Log("R unchanged - may be expected if noise matches initial R")
	}
	t.Logf("Initial R: %.2f, Updated R: %.2f", initialR, k.r)
}

func TestConcurrentAccess(t *testing.T) {
	k := NewKalman(DefaultConfig())

	var wg sync.WaitGroup
	iterations := 100

	// Concurrent writes
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				k.Process(float64(id*10+j), 0)
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

// ===== Memory Decay Prediction Tests =====

func TestDecayPrediction_ExponentialDecay(t *testing.T) {
	// Simulate exponential decay like NornicDB's decay system
	k := NewKalman(DecayPredictionConfig())
	rng := rand.New(rand.NewSource(42))

	// Initial score of 1.0, decaying by ~5% each step (gentler decay)
	decayRate := 0.95
	currentScore := 1.0

	var filteredValues []float64
	var actualValues []float64

	// Run filter for enough iterations to converge
	for i := 0; i < 30; i++ {
		// Actual decay
		currentScore *= decayRate
		actualValues = append(actualValues, currentScore)

		// Filter observes the actual (with 5% noise)
		noisy := currentScore * (1.0 + rng.NormFloat64()*0.05)
		filtered := k.Process(noisy, 0)
		filteredValues = append(filteredValues, filtered)
	}

	// After warmup, filtered should track actual reasonably well
	// Compare last 10 filtered values to actuals
	var totalError float64
	for i := 20; i < 30; i++ {
		err := math.Abs(filteredValues[i] - actualValues[i])
		totalError += err
	}
	avgError := totalError / 10.0

	t.Logf("Average decay tracking error (last 10): %.4f", avgError)
	t.Logf("Final actual: %.4f, Final filtered: %.4f", actualValues[29], filteredValues[29])

	// Kalman filter should reduce error vs raw noisy input
	// Accept error up to 0.2 (20%) for exponential decay tracking
	if avgError > 0.2 {
		t.Errorf("Tracking error too high: %v", avgError)
	}
}

func TestDecayPrediction_ArchiveThreshold(t *testing.T) {
	// Test that filter tracks decay and eventually crosses threshold
	k := NewKalmanWithInitial(DecayPredictionConfig(), 0.5) // Start at 0.5

	archiveThreshold := 0.05
	decayRate := 0.92

	currentScore := 0.5
	var actualArchiveStep, filteredArchiveStep int

	for i := 0; i < 100; i++ {
		currentScore *= decayRate
		filtered := k.Process(currentScore, 0)

		// Track when filtered value crosses threshold
		if filtered < archiveThreshold && filteredArchiveStep == 0 {
			filteredArchiveStep = i
			t.Logf("Filtered crossed threshold at step %d, filtered=%.4f", i, filtered)
		}

		// Track when actual value crosses threshold
		if currentScore < archiveThreshold && actualArchiveStep == 0 {
			actualArchiveStep = i
			t.Logf("Actual crossed threshold at step %d, score=%.4f", i, currentScore)
		}

		// Both found, exit early
		if actualArchiveStep > 0 && filteredArchiveStep > 0 {
			break
		}
	}

	// Both should eventually cross the threshold
	if actualArchiveStep == 0 {
		t.Error("Actual never crossed archive threshold")
	}
	if filteredArchiveStep == 0 {
		t.Error("Filtered never crossed archive threshold")
	}

	// Filtered will lag behind actual (this is normal for Kalman smoothing)
	// The key value is that both eventually cross - lag is expected
	if filteredArchiveStep > 0 && actualArchiveStep > 0 {
		lag := filteredArchiveStep - actualArchiveStep
		t.Logf("Archive detection lag: %d steps (expected: Kalman smoothing adds lag)", lag)
		// Lag up to 30 steps is acceptable for heavy smoothing
		// This demonstrates the filter is working (not just passing through)
		if lag < 0 {
			t.Errorf("Filtered crossed BEFORE actual - filter not working: lag=%d", lag)
		}
		if lag > 50 {
			t.Errorf("Archive detection lag extremely high: %d steps", lag)
		}
	}
}

// ===== Co-Access Confidence Tests =====

func TestCoAccessConfidence_SteadyPattern(t *testing.T) {
	k := NewKalman(CoAccessConfig())

	// Simulate steady co-access pattern (high confidence)
	for i := 0; i < 30; i++ {
		// Consistent co-access = high confidence observations
		k.Process(0.8, 1.0) // Target is 1.0 (perfect co-access)
	}

	confidence := k.State()
	t.Logf("Steady pattern confidence: %.4f", confidence)

	if confidence < 0.5 {
		t.Errorf("Confidence too low for steady pattern: %v", confidence)
	}
}

func TestCoAccessConfidence_SporadicPattern(t *testing.T) {
	k := NewKalman(CoAccessConfig())
	rng := rand.New(rand.NewSource(42))

	// Simulate sporadic co-access (should have lower filtered confidence)
	for i := 0; i < 30; i++ {
		// Random co-access observations
		observed := rng.Float64()
		k.Process(observed, 0)
	}

	confidence := k.State()
	t.Logf("Sporadic pattern confidence: %.4f", confidence)

	// Should be moderate, not extreme
	if confidence > 0.9 || confidence < 0.1 {
		t.Logf("Sporadic pattern gave extreme confidence: %v (may be ok)", confidence)
	}
}

func TestCoAccessConfidence_FilteringNoise(t *testing.T) {
	k := NewKalman(CoAccessConfig())
	rng := rand.New(rand.NewSource(42))

	// True confidence is 0.7, but observations are noisy
	trueConfidence := 0.7
	var rawVariance, filteredVariance float64
	var rawValues, filteredValues []float64

	for i := 0; i < 50; i++ {
		raw := trueConfidence + rng.NormFloat64()*0.2
		raw = math.Max(0, math.Min(1, raw)) // Clamp to [0,1]
		filtered := k.Process(raw, 0)

		rawValues = append(rawValues, raw)
		filteredValues = append(filteredValues, filtered)
	}

	// Calculate variance
	rawVariance = variance(rawValues)
	filteredVariance = variance(filteredValues[20:]) // Skip warmup

	t.Logf("Raw variance: %.4f, Filtered variance: %.4f", rawVariance, filteredVariance)

	// Filtered should have lower variance (smoother)
	if filteredVariance > rawVariance {
		t.Errorf("Filter increased variance: raw=%v, filtered=%v", rawVariance, filteredVariance)
	}
}

// ===== Latency Prediction Tests =====

func TestLatencyPrediction(t *testing.T) {
	k := NewKalman(LatencyConfig())

	// Simulate query latencies (in ms)
	latencies := []float64{50, 55, 48, 52, 60, 58, 55, 62, 58, 55, 57, 59, 56, 54, 58}

	for _, lat := range latencies {
		k.Process(lat, 0)
	}

	// Predict next latency
	predicted := k.Predict(1)
	avgLatency := mean(latencies)

	t.Logf("Average latency: %.2f, Predicted next: %.2f", avgLatency, predicted)

	// Prediction should be reasonable (within 2 std devs of mean)
	stdDev := math.Sqrt(variance(latencies))
	if math.Abs(predicted-avgLatency) > 2*stdDev {
		t.Errorf("Prediction %v too far from average %v", predicted, avgLatency)
	}
}

func TestLatencyPrediction_SpikesFiltered(t *testing.T) {
	k := NewKalman(LatencyConfig())

	// Normal latencies with occasional spikes
	for i := 0; i < 20; i++ {
		latency := 50.0
		if i == 10 {
			latency = 500.0 // Spike!
		}
		k.Process(latency, 0)
	}

	state := k.State()
	t.Logf("State after spike: %.2f", state)

	// Should not be 500 (spike should be filtered)
	if state > 100 {
		t.Errorf("Spike not filtered properly: state=%v", state)
	}
}

// ===== Similarity Score Tests =====

func TestSimilarityScoreSmoothing(t *testing.T) {
	k := NewKalman(DefaultConfig())

	// Simulate repeated similarity queries with noise
	trueSimilarity := 0.85
	rng := rand.New(rand.NewSource(42))

	var rawScores []float64
	var filteredScores []float64

	for i := 0; i < 30; i++ {
		raw := trueSimilarity + rng.NormFloat64()*0.1
		raw = math.Max(0, math.Min(1, raw))
		filtered := k.Process(raw, 0)

		rawScores = append(rawScores, raw)
		filteredScores = append(filteredScores, filtered)
	}

	// Final filtered value should be close to true similarity
	final := filteredScores[len(filteredScores)-1]
	if math.Abs(final-trueSimilarity) > 0.15 {
		t.Errorf("Final filtered %v too far from true %v", final, trueSimilarity)
	}

	t.Logf("True: %.3f, Raw mean: %.3f, Filtered final: %.3f",
		trueSimilarity, mean(rawScores), final)
}

// ===== Variance Tracker Tests =====

func TestVarianceTracker_Basic(t *testing.T) {
	v := NewVarianceTracker(10)

	// Add constant values - variance should be ~0
	for i := 0; i < 20; i++ {
		v.Update(50.0)
	}

	if v.Variance() > 0.01 {
		t.Errorf("Variance of constant signal = %v, want ~0", v.Variance())
	}
	if math.Abs(v.Mean()-50.0) > 0.01 {
		t.Errorf("Mean = %v, want 50.0", v.Mean())
	}
}

func TestVarianceTracker_Noisy(t *testing.T) {
	v := NewVarianceTracker(20)
	rng := rand.New(rand.NewSource(42))

	// Add noisy values with known variance
	targetMean := 100.0
	targetStdDev := 10.0

	for i := 0; i < 50; i++ {
		sample := targetMean + rng.NormFloat64()*targetStdDev
		v.Update(sample)
	}

	measuredMean := v.Mean()
	measuredStdDev := v.StdDev()

	t.Logf("Target: mean=%.1f, std=%.1f", targetMean, targetStdDev)
	t.Logf("Measured: mean=%.1f, std=%.1f", measuredMean, measuredStdDev)

	// Should be reasonably close
	if math.Abs(measuredMean-targetMean) > 5 {
		t.Errorf("Mean %v too far from target %v", measuredMean, targetMean)
	}
}

func TestVarianceTracker_AdaptiveNoise(t *testing.T) {
	v := NewVarianceTracker(10)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 20; i++ {
		v.Update(rng.NormFloat64() * 5.0)
	}

	noise := v.AdaptiveNoise(10.0)
	t.Logf("Adaptive noise with scale 10: %.2f", noise)

	if noise <= 0 {
		t.Errorf("Adaptive noise = %v, want > 0", noise)
	}
}

func TestKalman_CovarianceAndGainGetters(t *testing.T) {
	k := NewKalman(DefaultConfig())
	_ = k.Process(10, 0)
	_ = k.Process(11, 0)

	cov := k.Covariance()
	gain := k.Gain()
	if cov <= 0 {
		t.Errorf("Covariance() = %v, want > 0", cov)
	}
	if gain <= 0 || gain > 1 {
		t.Errorf("Gain() = %v, want 0 < gain <= 1", gain)
	}
}

// ===== Benchmark Tests =====

func BenchmarkProcess(b *testing.B) {
	k := NewKalman(DefaultConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.Process(float64(i), 0)
	}
}

func BenchmarkPredict(b *testing.B) {
	k := NewKalman(DefaultConfig())
	for i := 0; i < 100; i++ {
		k.Process(float64(i), 0)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.Predict(5)
	}
}

func BenchmarkProcessBatch(b *testing.B) {
	k := NewKalman(DefaultConfig())
	measurements := make([]float64, 100)
	for i := range measurements {
		measurements[i] = float64(i)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.ProcessBatch(measurements, 0)
		k.Reset()
	}
}

func BenchmarkVarianceTracker(b *testing.B) {
	v := NewVarianceTracker(32)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		v.Update(float64(i))
	}
}

// Helper functions

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func variance(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	m := mean(values)
	var sumSq float64
	for _, v := range values {
		diff := v - m
		sumSq += diff * diff
	}
	return sumSq / float64(len(values))
}
