// Package filter provides signal filtering and prediction algorithms for NornicDB.
//
// This package implements a lightweight Kalman filter based on the imu-f project
// (https://github.com/heliorc/imu-f) designed for real-time state estimation
// and future value prediction with minimal computational overhead.
//
// The filter is optimized for:
//   - Memory decay score prediction
//   - Co-access pattern confidence filtering
//   - Query latency prediction
//   - Similarity score smoothing
//
// Key Features:
//   - Adaptive measurement noise (R) based on signal variance
//   - Setpoint-based error boosting for faster convergence
//   - Velocity-based state projection for prediction
//   - No matrix operations - pure scalar math for speed
//
// Example Usage:
//
//	// Create a filter for memory decay prediction
//	filter := filter.NewKalman(filter.DefaultConfig())
//
//	// Process observations
//	for _, observation := range decayScores {
//		filtered := filter.Process(observation, targetScore)
//		fmt.Printf("Filtered: %.3f\n", filtered)
//	}
//
//	// Predict future value
//	futureScore := filter.Predict(5) // 5 steps ahead
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you're trying to guess where a ball will land. Each time you see the ball,
// you update your guess. But your eyes aren't perfect (measurement noise), and the
// ball might suddenly change direction (process noise).
//
// The Kalman filter is like having a really smart friend who:
// 1. Remembers where the ball was before
// 2. Guesses where it's going based on how fast it was moving
// 3. Updates the guess when they see new info, but doesn't completely forget the old guess
// 4. Trusts new info MORE when their guess was way off (error boosting)
//
// Original implementation: https://github.com/heliorc/imu-f/blob/master/src/filter/kalman.c
package filter

import (
	"math"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config"
)

// Config holds Kalman filter configuration.
type Config struct {
	// ProcessNoise (Q) - how much we expect the true state to change between measurements.
	// Higher values = more responsive to changes, but noisier output.
	// Default: 0.1 (scaled by 0.001 internally like imu-f)
	ProcessNoise float64

	// MeasurementNoise (R) - how much we distrust individual measurements.
	// Higher values = smoother output, but slower to respond.
	// Default: 88.0 (seed value from imu-f)
	MeasurementNoise float64

	// InitialCovariance (P) - initial uncertainty in our estimate.
	// Default: 30.0 (seed value from imu-f)
	InitialCovariance float64

	// VarianceScale - multiplier for adaptive R calculation.
	// Default: 10.0
	VarianceScale float64
}

// DefaultConfig returns sensible defaults based on imu-f tuning.
func DefaultConfig() Config {
	return Config{
		ProcessNoise:      0.1,
		MeasurementNoise:  88.0,
		InitialCovariance: 30.0,
		VarianceScale:     10.0,
	}
}

// DecayPredictionConfig returns config optimized for memory decay prediction.
func DecayPredictionConfig() Config {
	return Config{
		ProcessNoise:      0.05, // Decay is relatively stable
		MeasurementNoise:  50.0, // Access patterns can be noisy
		InitialCovariance: 20.0,
		VarianceScale:     8.0,
	}
}

// CoAccessConfig returns config optimized for co-access pattern filtering.
func CoAccessConfig() Config {
	return Config{
		ProcessNoise:      0.2,   // Co-access patterns can change
		MeasurementNoise:  100.0, // Individual accesses are noisy
		InitialCovariance: 40.0,
		VarianceScale:     12.0,
	}
}

// LatencyConfig returns config optimized for query latency prediction.
func LatencyConfig() Config {
	return Config{
		ProcessNoise:      0.15, // Latency varies with load
		MeasurementNoise:  60.0,
		InitialCovariance: 25.0,
		VarianceScale:     10.0,
	}
}

// Kalman implements a simple scalar Kalman filter with velocity-based prediction.
//
// Based on the imu-f flight controller implementation, this filter provides:
//   - State estimation with adaptive noise handling
//   - Setpoint-based error boosting for faster convergence
//   - Future state prediction using velocity
type Kalman struct {
	mu sync.RWMutex

	// State variables
	x     float64 // Current state estimate
	lastX float64 // Previous state (for velocity calculation)
	p     float64 // Estimate covariance (uncertainty)
	k     float64 // Kalman gain (how much to trust measurement)
	e     float64 // Setpoint error factor

	// Configuration
	q             float64 // Process noise (scaled)
	r             float64 // Measurement noise
	varianceScale float64

	// Statistics
	observations int
	innovations  []float64 // Recent innovations for adaptive R
	maxHistory   int
}

// NewKalman creates a new Kalman filter with the given configuration.
func NewKalman(cfg Config) *Kalman {
	return &Kalman{
		x:             0,
		lastX:         0,
		p:             cfg.InitialCovariance,
		k:             0,
		e:             1.0,
		q:             cfg.ProcessNoise * 0.001, // Scale like imu-f
		r:             cfg.MeasurementNoise,
		varianceScale: cfg.VarianceScale,
		observations:  0,
		innovations:   make([]float64, 0, 32),
		maxHistory:    32,
	}
}

// NewKalmanWithInitial creates a filter with an initial state estimate.
func NewKalmanWithInitial(cfg Config, initialState float64) *Kalman {
	k := NewKalman(cfg)
	k.x = initialState
	k.lastX = initialState
	return k
}

// Process updates the filter with a new measurement and optional setpoint target.
//
// Parameters:
//   - measurement: The observed value
//   - target: The desired setpoint (use 0 if no target)
//
// Returns the filtered state estimate.
//
// The setpoint affects error boosting: when far from target, the filter
// becomes more responsive to new measurements (from imu-f design).
func (k *Kalman) Process(measurement, target float64) float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.processInternal(measurement, target)
}

func (k *Kalman) processInternal(measurement, target float64) float64 {
	// Project state ahead using velocity (rate of change)
	// This is the key insight from imu-f: predict based on recent trend
	velocity := k.x - k.lastX
	k.x += velocity

	// Save for next velocity calculation
	k.lastX = k.x

	// Setpoint-based error boosting (from imu-f)
	// When far from target, increase process noise to trust measurements more
	if target != 0.0 && k.lastX != 0.0 {
		k.e = math.Abs(1.0 - (target / k.lastX))
	} else {
		k.e = 1.0
	}

	// Prediction update: increase uncertainty
	k.p = k.p + (k.q * k.e)

	// Measurement update
	k.k = k.p / (k.p + k.r) // Kalman gain

	// Innovation (measurement residual)
	innovation := measurement - k.x
	k.x += k.k * innovation

	// Update covariance
	k.p = (1.0 - k.k) * k.p

	// Track innovation for adaptive R
	k.trackInnovation(innovation)

	k.observations++
	return k.x
}

// ProcessBatch processes multiple measurements efficiently.
func (k *Kalman) ProcessBatch(measurements []float64, target float64) []float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	results := make([]float64, len(measurements))
	for i, m := range measurements {
		results[i] = k.processInternal(m, target)
	}
	return results
}

// Predict estimates the state n steps into the future.
//
// Uses the current velocity (rate of change) to project forward.
// Does not update the filter state.
func (k *Kalman) Predict(steps int) float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	velocity := k.x - k.lastX
	return k.x + (float64(steps) * velocity)
}

// PredictWithUncertainty returns the predicted value and its uncertainty.
func (k *Kalman) PredictWithUncertainty(steps int) (value, uncertainty float64) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	velocity := k.x - k.lastX
	value = k.x + (float64(steps) * velocity)

	// Uncertainty grows with prediction horizon
	// Each step adds process noise
	uncertainty = k.p
	for i := 0; i < steps; i++ {
		uncertainty += k.q * k.e
	}
	uncertainty = math.Sqrt(uncertainty)

	return value, uncertainty
}

// State returns the current state estimate.
func (k *Kalman) State() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.x
}

// Velocity returns the current rate of change.
func (k *Kalman) Velocity() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.x - k.lastX
}

// Covariance returns the current estimate uncertainty.
func (k *Kalman) Covariance() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.p
}

// Gain returns the current Kalman gain (0-1).
// Higher gain = trusting measurements more.
func (k *Kalman) Gain() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.k
}

// Observations returns the number of measurements processed.
func (k *Kalman) Observations() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.observations
}

// Reset resets the filter to initial state.
func (k *Kalman) Reset() {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.x = 0
	k.lastX = 0
	k.p = 30.0 // Reset to seed value
	k.k = 0
	k.e = 1.0
	k.observations = 0
	k.innovations = k.innovations[:0]
}

// SetState manually sets the current state (use sparingly).
func (k *Kalman) SetState(state float64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.x = state
	k.lastX = state
}

// ProcessIfEnabled applies filtering if the feature is enabled.
// If disabled, returns the raw measurement unchanged.
//
// Parameters:
//   - feature: The feature flag to check (e.g., FeatureKalmanDecay)
//   - measurement: The observed value
//   - target: The desired setpoint (use 0 if no target)
//
// Returns a config.FilteredValue containing both raw and filtered values.
func (k *Kalman) ProcessIfEnabled(feature string, measurement, target float64) config.FilteredValue {
	result := config.FilteredValue{
		Raw:     measurement,
		Feature: feature,
	}

	if config.IsFeatureEnabled(feature) {
		result.Filtered = k.Process(measurement, target)
		result.WasFiltered = true
	} else {
		result.Filtered = measurement
		result.WasFiltered = false
	}

	return result
}

// PredictIfEnabled returns a predicted value if the feature is enabled.
// If disabled, returns the current state unchanged.
func (k *Kalman) PredictIfEnabled(feature string, steps int) config.FilteredValue {
	currentState := k.State()
	result := config.FilteredValue{
		Raw:     currentState,
		Feature: feature,
	}

	if config.IsFeatureEnabled(feature) {
		result.Filtered = k.Predict(steps)
		result.WasFiltered = true
	} else {
		result.Filtered = currentState
		result.WasFiltered = false
	}

	return result
}

// trackInnovation tracks recent innovations for adaptive R calculation.
func (k *Kalman) trackInnovation(innovation float64) {
	k.innovations = append(k.innovations, innovation)
	if len(k.innovations) > k.maxHistory {
		k.innovations = k.innovations[1:]
	}
}

// UpdateAdaptiveR updates measurement noise based on innovation variance.
// Call periodically (e.g., every 10-20 observations) for adaptive filtering.
//
// This implements the variance-based R adaptation from imu-f.
func (k *Kalman) UpdateAdaptiveR() {
	k.mu.Lock()
	defer k.mu.Unlock()

	if len(k.innovations) < 5 {
		return // Need enough samples
	}

	// Calculate innovation variance
	var sum, sumSq float64
	n := float64(len(k.innovations))
	for _, inn := range k.innovations {
		sum += inn
		sumSq += inn * inn
	}
	mean := sum / n
	variance := math.Abs(sumSq/n - mean*mean)

	// Update R based on variance (from imu-f)
	k.r = math.Sqrt(variance) * k.varianceScale
	if k.r < 1.0 {
		k.r = 1.0 // Minimum noise floor
	}
}

// Stats returns filter statistics.
type Stats struct {
	State            float64
	Velocity         float64
	Covariance       float64
	Gain             float64
	MeasurementNoise float64
	Observations     int
}

// GetStats returns current filter statistics.
func (k *Kalman) GetStats() Stats {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return Stats{
		State:            k.x,
		Velocity:         k.x - k.lastX,
		Covariance:       k.p,
		Gain:             k.k,
		MeasurementNoise: k.r,
		Observations:     k.observations,
	}
}

// VarianceTracker tracks signal variance for adaptive filtering.
// Based on imu-f's update_kalman_covariance.
type VarianceTracker struct {
	mu sync.Mutex

	window    []float64
	windowIdx int
	windowLen int

	sumMean  float64
	sumVar   float64
	mean     float64
	variance float64

	inverseN float64
}

// NewVarianceTracker creates a variance tracker with the specified window size.
func NewVarianceTracker(windowSize int) *VarianceTracker {
	return &VarianceTracker{
		window:    make([]float64, windowSize),
		windowLen: windowSize,
		inverseN:  1.0 / float64(windowSize),
	}
}

// Update adds a new sample and updates variance statistics.
func (v *VarianceTracker) Update(sample float64) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Add new sample
	oldSample := v.window[v.windowIdx]
	v.window[v.windowIdx] = sample

	// Update running sums (sliding window from imu-f)
	v.sumMean += sample - oldSample
	v.sumVar += (sample * sample) - (oldSample * oldSample)

	// Move window index
	v.windowIdx++
	if v.windowIdx >= v.windowLen {
		v.windowIdx = 0
	}

	// Calculate mean and variance
	v.mean = v.sumMean * v.inverseN
	v.variance = math.Abs(v.sumVar*v.inverseN - (v.mean * v.mean))
}

// Mean returns the current mean.
func (v *VarianceTracker) Mean() float64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.mean
}

// Variance returns the current variance.
func (v *VarianceTracker) Variance() float64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.variance
}

// StdDev returns the current standard deviation.
func (v *VarianceTracker) StdDev() float64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return math.Sqrt(v.variance)
}

// AdaptiveNoise returns a noise value based on current variance.
func (v *VarianceTracker) AdaptiveNoise(scale float64) float64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return math.Sqrt(v.variance) * scale
}
