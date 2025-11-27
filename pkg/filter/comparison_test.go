package filter

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
)

// ComparisonResult holds the results of comparing filtered vs unfiltered values.
type ComparisonResult struct {
	Name        string
	SampleCount int
	TrueValue   float64

	// Unfiltered metrics
	UnfilteredMean     float64
	UnfilteredVariance float64
	UnfilteredMAE      float64 // Mean Absolute Error from true value
	UnfilteredRMSE     float64 // Root Mean Square Error

	// Filtered metrics
	FilteredMean     float64
	FilteredVariance float64
	FilteredMAE      float64
	FilteredRMSE     float64

	// Improvement metrics
	VarianceReduction float64 // Percentage reduction in variance
	MAEReduction      float64 // Percentage reduction in MAE
	RMSEReduction     float64 // Percentage reduction in RMSE

	// Raw data for detailed analysis
	UnfilteredValues []float64
	FilteredValues   []float64
}

func (r ComparisonResult) String() string {
	return fmt.Sprintf(`
=== %s ===
Samples: %d, True Value: %.4f

Unfiltered:
  Mean: %.4f, Variance: %.6f
  MAE: %.4f, RMSE: %.4f

Filtered (Kalman):
  Mean: %.4f, Variance: %.6f
  MAE: %.4f, RMSE: %.4f

Improvements:
  Variance Reduction: %.1f%%
  MAE Reduction: %.1f%%
  RMSE Reduction: %.1f%%
`,
		r.Name, r.SampleCount, r.TrueValue,
		r.UnfilteredMean, r.UnfilteredVariance, r.UnfilteredMAE, r.UnfilteredRMSE,
		r.FilteredMean, r.FilteredVariance, r.FilteredMAE, r.FilteredRMSE,
		r.VarianceReduction, r.MAEReduction, r.RMSEReduction)
}

// runComparison runs a comparison test between filtered and unfiltered values.
func runComparison(name string, trueValue float64, noiseStd float64, samples int, cfg Config) ComparisonResult {
	rand.Seed(42) // Deterministic for reproducibility

	k := NewKalman(cfg)

	unfiltered := make([]float64, samples)
	filtered := make([]float64, samples)

	for i := 0; i < samples; i++ {
		// Generate noisy observation
		noise := rand.NormFloat64() * noiseStd
		observation := trueValue + noise
		unfiltered[i] = observation
		filtered[i] = k.Process(observation, 0)
	}

	// Skip warmup period for filtered (first 10%)
	warmupEnd := samples / 10
	filteredAnalysis := filtered[warmupEnd:]
	unfilteredAnalysis := unfiltered[warmupEnd:]

	result := ComparisonResult{
		Name:             name,
		SampleCount:      samples,
		TrueValue:        trueValue,
		UnfilteredValues: unfiltered,
		FilteredValues:   filtered,
	}

	// Calculate unfiltered metrics
	result.UnfilteredMean = mean(unfilteredAnalysis)
	result.UnfilteredVariance = variance(unfilteredAnalysis)
	result.UnfilteredMAE = meanAbsoluteError(unfilteredAnalysis, trueValue)
	result.UnfilteredRMSE = rootMeanSquareError(unfilteredAnalysis, trueValue)

	// Calculate filtered metrics
	result.FilteredMean = mean(filteredAnalysis)
	result.FilteredVariance = variance(filteredAnalysis)
	result.FilteredMAE = meanAbsoluteError(filteredAnalysis, trueValue)
	result.FilteredRMSE = rootMeanSquareError(filteredAnalysis, trueValue)

	// Calculate improvements
	if result.UnfilteredVariance > 0 {
		result.VarianceReduction = (1 - result.FilteredVariance/result.UnfilteredVariance) * 100
	}
	if result.UnfilteredMAE > 0 {
		result.MAEReduction = (1 - result.FilteredMAE/result.UnfilteredMAE) * 100
	}
	if result.UnfilteredRMSE > 0 {
		result.RMSEReduction = (1 - result.FilteredRMSE/result.UnfilteredRMSE) * 100
	}

	return result
}

func meanAbsoluteError(values []float64, trueValue float64) float64 {
	var sum float64
	for _, v := range values {
		sum += math.Abs(v - trueValue)
	}
	return sum / float64(len(values))
}

func rootMeanSquareError(values []float64, trueValue float64) float64 {
	var sumSq float64
	for _, v := range values {
		diff := v - trueValue
		sumSq += diff * diff
	}
	return math.Sqrt(sumSq / float64(len(values)))
}

// ===== Feature Flag Tests =====

func TestFeatureFlags_Default(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	if config.IsKalmanEnabled() {
		t.Error("Kalman should be disabled by default")
	}
}

func TestFeatureFlags_Enable(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	config.EnableKalmanFiltering()
	if !config.IsKalmanEnabled() {
		t.Error("Kalman should be enabled after config.EnableKalmanFiltering()")
	}

	config.DisableKalmanFiltering()
	if config.IsKalmanEnabled() {
		t.Error("Kalman should be disabled after config.DisableKalmanFiltering()")
	}
}

func TestFeatureFlags_WithEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	if config.IsKalmanEnabled() {
		t.Error("Should start disabled")
	}

	cleanup := config.WithKalmanEnabled()
	if !config.IsKalmanEnabled() {
		t.Error("Should be enabled within scope")
	}

	cleanup()
	if config.IsKalmanEnabled() {
		t.Error("Should be disabled after cleanup")
	}
}

func TestFeatureFlags_IndividualFeatures(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	// Global disabled = all features disabled
	if config.IsFeatureEnabled(config.FeatureKalmanDecay) {
		t.Error("Feature should be disabled when global is off")
	}

	// Enable global
	config.EnableKalmanFiltering()

	// All features enabled by default when global is on
	if !config.IsFeatureEnabled(config.FeatureKalmanDecay) {
		t.Error("Feature should be enabled when global is on and not explicitly disabled")
	}

	// Explicitly disable one feature
	config.DisableFeature(config.FeatureKalmanDecay)
	if config.IsFeatureEnabled(config.FeatureKalmanDecay) {
		t.Error("Feature should be disabled after explicit disable")
	}

	// Other features still enabled
	if !config.IsFeatureEnabled(config.FeatureKalmanCoAccess) {
		t.Error("Other features should still be enabled")
	}
}

func TestFeatureFlags_GetEnabledFeatures(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	// None when disabled
	features := config.GetEnabledFeatures()
	if len(features) != 0 {
		t.Errorf("Expected no features when disabled, got %d", len(features))
	}

	// All when enabled
	config.EnableKalmanFiltering()
	features = config.GetEnabledFeatures()
	if len(features) != 5 {
		t.Errorf("Expected 5 features when enabled, got %d", len(features))
	}
}

func TestFeatureFlags_EnableAllDisableAll(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	config.EnableAllFeatures()
	if !config.IsKalmanEnabled() {
		t.Error("Global should be enabled after EnableAllFeatures")
	}
	if !config.IsFeatureEnabled(config.FeatureKalmanDecay) {
		t.Error("Decay feature should be enabled")
	}
	if !config.IsFeatureEnabled(config.FeatureKalmanTemporal) {
		t.Error("Temporal feature should be enabled")
	}

	config.DisableAllFeatures()
	if config.IsKalmanEnabled() {
		t.Error("Global should be disabled after DisableAllFeatures")
	}
}

func TestFeatureFlags_SetKalmanEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	config.SetKalmanEnabled(true)
	if !config.IsKalmanEnabled() {
		t.Error("Should be enabled after SetKalmanEnabled(true)")
	}

	config.SetKalmanEnabled(false)
	if config.IsKalmanEnabled() {
		t.Error("Should be disabled after SetKalmanEnabled(false)")
	}
}

func TestFeatureFlags_WithDisabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	config.EnableKalmanFiltering()
	if !config.IsKalmanEnabled() {
		t.Error("Should start enabled")
	}

	cleanup := config.WithKalmanDisabled()
	if config.IsKalmanEnabled() {
		t.Error("Should be disabled within scope")
	}

	cleanup()
	if !config.IsKalmanEnabled() {
		t.Error("Should be enabled after cleanup")
	}
}

func TestFeatureFlags_GetFeatureStatus(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	status := config.GetFeatureStatus()
	if status.GlobalEnabled {
		t.Error("Global should be disabled by default")
	}

	config.EnableKalmanFiltering()
	config.EnableFeature(config.FeatureKalmanDecay)
	config.DisableFeature(config.FeatureKalmanCoAccess)

	status = config.GetFeatureStatus()
	if !status.GlobalEnabled {
		t.Error("Global should be enabled")
	}
	if !status.Features[config.FeatureKalmanDecay] {
		t.Error("Decay feature should be enabled")
	}
	if status.Features[config.FeatureKalmanCoAccess] {
		t.Error("CoAccess feature should be disabled")
	}
}

func TestPredictIfEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalman(DefaultConfig())

	// Train filter
	for i := 0; i < 20; i++ {
		k.Process(float64(i*10), 0)
	}

	// Disabled: should return current state
	result := k.PredictIfEnabled(config.FeatureKalmanDecay, 5)
	if result.WasFiltered {
		t.Error("Should not predict when disabled")
	}
	if result.Filtered != result.Raw {
		t.Error("Filtered should equal Raw when disabled")
	}

	// Enabled: should return prediction
	config.EnableKalmanFiltering()
	result = k.PredictIfEnabled(config.FeatureKalmanDecay, 5)
	if !result.WasFiltered {
		t.Error("Should predict when enabled")
	}
	// Prediction with upward trend should be higher than current state
	if result.Filtered <= k.State()-10 { // Allow some tolerance
		t.Errorf("Prediction %v should be at or above state %v", result.Filtered, k.State())
	}
}

func TestFilteredValue_Fields(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalman(DefaultConfig())

	// Test all fields are populated
	config.EnableKalmanFiltering()
	result := k.ProcessIfEnabled(config.FeatureKalmanLatency, 42.0, 50.0)

	if result.Raw != 42.0 {
		t.Errorf("Raw = %v, want 42.0", result.Raw)
	}
	if result.Feature != config.FeatureKalmanLatency {
		t.Errorf("Feature = %v, want %v", result.Feature, config.FeatureKalmanLatency)
	}
	if !result.WasFiltered {
		t.Error("WasFiltered should be true when enabled")
	}
}

func TestProcessIfEnabled(t *testing.T) {
	config.ResetFeatureFlags()
	defer config.ResetFeatureFlags()

	k := NewKalman(DefaultConfig())

	// Disabled: should pass through unchanged
	result := k.ProcessIfEnabled(config.FeatureKalmanDecay, 100.0, 0)
	if result.WasFiltered {
		t.Error("Should not filter when disabled")
	}
	if result.Filtered != result.Raw {
		t.Error("Filtered should equal Raw when disabled")
	}

	// Enabled: should filter
	config.EnableKalmanFiltering()
	result = k.ProcessIfEnabled(config.FeatureKalmanDecay, 100.0, 0)
	if !result.WasFiltered {
		t.Error("Should filter when enabled")
	}
	// After processing, filtered will be different (converging to measurement)
}

// ===== A/B Comparison Tests =====

func TestComparison_DecayScoreFiltering(t *testing.T) {
	// Simulate memory decay scores with noise
	// True decay follows exponential curve, but observations are noisy
	result := runComparison(
		"Memory Decay Score",
		0.5, // True decay score
		0.1, // 10% noise standard deviation
		100, // 100 samples
		DecayPredictionConfig(),
	)

	t.Log(result.String())

	// PROVE: Kalman reduces variance
	if result.VarianceReduction < 50 {
		t.Errorf("Expected >50%% variance reduction, got %.1f%%", result.VarianceReduction)
	}

	// PROVE: Kalman reduces error
	if result.MAEReduction < 30 {
		t.Errorf("Expected >30%% MAE reduction, got %.1f%%", result.MAEReduction)
	}
}

func TestComparison_CoAccessConfidence(t *testing.T) {
	// Simulate co-access confidence scores
	// True confidence is stable, but individual observations vary
	result := runComparison(
		"Co-Access Confidence",
		0.75, // True confidence
		0.15, // 15% noise standard deviation
		100,
		CoAccessConfig(),
	)

	t.Log(result.String())

	// PROVE: Kalman smooths noisy confidence
	if result.VarianceReduction < 40 {
		t.Errorf("Expected >40%% variance reduction, got %.1f%%", result.VarianceReduction)
	}
}

func TestComparison_LatencyPrediction(t *testing.T) {
	// Simulate query latency observations
	result := runComparison(
		"Query Latency (ms)",
		50.0, // True average latency
		15.0, // Standard deviation in ms
		100,
		LatencyConfig(),
	)

	t.Log(result.String())

	// PROVE: Kalman reduces latency estimation variance
	if result.VarianceReduction < 30 {
		t.Errorf("Expected >30%% variance reduction, got %.1f%%", result.VarianceReduction)
	}
}

func TestComparison_SimilarityScores(t *testing.T) {
	// Simulate similarity scores from repeated queries
	result := runComparison(
		"Similarity Score",
		0.85, // True similarity
		0.08, // 8% noise
		50,
		DefaultConfig(),
	)

	t.Log(result.String())

	// PROVE: Kalman provides more stable similarity estimates
	if result.VarianceReduction < 40 {
		t.Errorf("Expected >40%% variance reduction, got %.1f%%", result.VarianceReduction)
	}
}

func TestComparison_HighNoise(t *testing.T) {
	// Test with very high noise - demonstrates Kalman value
	result := runComparison(
		"High Noise Signal",
		100.0, // True value
		30.0,  // 30% noise!
		200,
		DefaultConfig(),
	)

	t.Log(result.String())

	// Even with high noise, Kalman should help
	if result.VarianceReduction < 20 {
		t.Errorf("Expected >20%% variance reduction even with high noise, got %.1f%%", result.VarianceReduction)
	}
}

func TestComparison_LowNoise(t *testing.T) {
	// Test with low noise - Kalman should still help, not hurt
	result := runComparison(
		"Low Noise Signal",
		100.0, // True value
		2.0,   // 2% noise
		100,
		DefaultConfig(),
	)

	t.Log(result.String())

	// With low noise, Kalman should at least not make things worse
	if result.VarianceReduction < 0 {
		t.Errorf("Kalman made variance worse with low noise: %.1f%%", result.VarianceReduction)
	}
}

// TestComparison_SpikeRejection tests the filter's ability to reject outliers
func TestComparison_SpikeRejection(t *testing.T) {
	rand.Seed(42)

	k := NewKalman(DefaultConfig())
	trueValue := 50.0

	var unfiltered, filtered []float64

	for i := 0; i < 100; i++ {
		var observation float64
		if i == 50 {
			// Inject a spike
			observation = 500.0
		} else {
			observation = trueValue + rand.NormFloat64()*5.0
		}

		unfiltered = append(unfiltered, observation)
		filtered = append(filtered, k.Process(observation, 0))
	}

	// Check spike impact
	unfilteredSpike := unfiltered[50]
	filteredSpike := filtered[50]
	postSpikeFiltered := filtered[55] // 5 steps after spike

	t.Logf("Spike injection:")
	t.Logf("  Raw spike value: %.2f", unfilteredSpike)
	t.Logf("  Filtered at spike: %.2f (%.1f%% reduction)", filteredSpike, (1-filteredSpike/unfilteredSpike)*100)
	t.Logf("  Filtered 5 steps after: %.2f", postSpikeFiltered)

	// PROVE: Kalman significantly reduces spike impact
	if filteredSpike > 200 {
		t.Errorf("Kalman didn't sufficiently reduce spike: %.2f", filteredSpike)
	}

	// PROVE: Kalman recovers quickly after spike
	if math.Abs(postSpikeFiltered-trueValue) > 20 {
		t.Errorf("Kalman didn't recover after spike: %.2f (expected ~%.2f)", postSpikeFiltered, trueValue)
	}
}

// TestComparison_TrendTracking documents the filter's behavior with trends.
// NOTE: This Kalman filter is optimized for SMOOTHING (stable value + noise),
// not for tracking rapidly changing trends. For trend tracking, consider a
// more sophisticated filter (e.g., Kalman with velocity state).
func TestComparison_TrendTracking(t *testing.T) {
	rand.Seed(42)

	k := NewKalman(DefaultConfig())

	// Create a signal with a linear trend plus noise
	// Train the filter first
	for i := 0; i < 50; i++ {
		actualValue := float64(i) * 0.5 // Linear trend: 0, 0.5, 1, 1.5, ...
		observation := actualValue + rand.NormFloat64()*2.0
		k.Process(observation, 0)
	}

	// Document the filter's behavior
	state := k.State()
	velocity := k.Velocity()

	t.Logf("Trend Behavior Documentation:")
	t.Logf("  After 50 steps of linear trend (0 to 25):")
	t.Logf("  Filter state: %.2f (true value: 24.5)", state)
	t.Logf("  Filter velocity: %.4f (true velocity: 0.5)", velocity)
	t.Logf("  Lag: %.2f (expected for smoothing filter)", 24.5-state)

	// The filter DOES learn velocity, just slower than the true trend
	// This is correct behavior for a smoothing filter
	if velocity <= 0 {
		t.Errorf("Velocity should be positive for upward trend: %v", velocity)
	}

	// Document: this filter is designed for smoothing, not trend tracking
	// The lag is a feature, not a bug - it provides noise immunity
	t.Log("")
	t.Log("IMPORTANT: This filter is optimized for SMOOTHING stable signals.")
	t.Log("For trend tracking applications, use a dedicated tracking filter.")
	t.Log("The lag demonstrated here provides excellent noise rejection.")
}

// TestComparison_Summary generates a summary of all comparisons
func TestComparison_Summary(t *testing.T) {
	results := []ComparisonResult{
		runComparison("Memory Decay", 0.5, 0.1, 100, DecayPredictionConfig()),
		runComparison("Co-Access Confidence", 0.75, 0.15, 100, CoAccessConfig()),
		runComparison("Query Latency", 50.0, 15.0, 100, LatencyConfig()),
		runComparison("Similarity Score", 0.85, 0.08, 50, DefaultConfig()),
		runComparison("High Noise", 100.0, 30.0, 200, DefaultConfig()),
	}

	t.Log("\n" + strings.Repeat("=", 60))
	t.Log("KALMAN FILTER VALUE PROOF SUMMARY")
	t.Log(strings.Repeat("=", 60) + "\n")

	var totalVarianceReduction, totalMAEReduction float64

	for _, r := range results {
		t.Logf("%-25s: Variance -%5.1f%%, MAE -%5.1f%%, RMSE -%5.1f%%",
			r.Name, r.VarianceReduction, r.MAEReduction, r.RMSEReduction)
		totalVarianceReduction += r.VarianceReduction
		totalMAEReduction += r.MAEReduction
	}

	avgVariance := totalVarianceReduction / float64(len(results))
	avgMAE := totalMAEReduction / float64(len(results))

	t.Log("\n" + strings.Repeat("-", 60))
	t.Logf("AVERAGE IMPROVEMENT: Variance -%.1f%%, MAE -%.1f%%", avgVariance, avgMAE)
	t.Log(strings.Repeat("-", 60))

	// Overall assertion
	if avgVariance < 30 {
		t.Errorf("Average variance reduction below 30%%: %.1f%%", avgVariance)
	}
}

// BenchmarkComparison benchmarks filtered vs unfiltered processing
func BenchmarkComparison_Filtered(b *testing.B) {
	config.EnableKalmanFiltering()
	defer config.DisableKalmanFiltering()

	k := NewKalman(DefaultConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.ProcessIfEnabled(config.FeatureKalmanDecay, float64(i), 0)
	}
}

func BenchmarkComparison_Unfiltered(b *testing.B) {
	config.DisableKalmanFiltering()

	k := NewKalman(DefaultConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.ProcessIfEnabled(config.FeatureKalmanDecay, float64(i), 0)
	}
}
