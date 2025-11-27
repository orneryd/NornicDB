// Package filter - Adaptive Kalman filter that auto-switches modes.
//
// KalmanAdaptive monitors signal characteristics and dynamically switches
// between basic (smoothing) and velocity (tracking) modes based on:
//   - Trend detection (is the signal drifting?)
//   - Variance analysis (is noise high or low?)
//   - Prediction error (is current mode working well?)
//
// This provides the best of both worlds:
//   - Use basic mode for stable signals → maximum noise rejection
//   - Use velocity mode for trends → accurate tracking
//   - Switch automatically when conditions change
//
// Example usage:
//
//	filter := NewKalmanAdaptive(DefaultAdaptiveConfig())
//	for _, observation := range data {
//		filtered := filter.Process(observation)
//		fmt.Printf("Mode: %s, Value: %.3f\n", filter.Mode(), filtered)
//	}
//
// ELI12 (Explain Like I'm 12):
//
// Imagine you have two friends helping you catch a ball:
//   - Friend A is great at catching balls thrown straight at you
//   - Friend B is great at catching balls that curve through the air
//
// The adaptive filter is like having a coach who watches and says
// "Hey, this ball is curving - Friend B, you take this one!"
// It automatically picks the best helper for each situation.
package filter

import (
	"math"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config"
)

// FilterMode represents the current filtering strategy.
type FilterMode string

const (
	// ModeBasic uses the scalar Kalman filter (optimal for stable signals)
	ModeBasic FilterMode = "basic"

	// ModeVelocity uses the 2-state Kalman filter (optimal for trends)
	ModeVelocity FilterMode = "velocity"

	// ModeAuto lets the filter decide automatically
	ModeAuto FilterMode = "auto"
)

// AdaptiveConfig holds configuration for the adaptive filter.
type AdaptiveConfig struct {
	// BasicConfig for the underlying basic filter
	BasicConfig Config

	// VelocityConfig for the underlying velocity filter
	VelocityConfig VelocityConfig

	// TrendThreshold - velocity magnitude above which we switch to velocity mode
	// Default: 0.1 (10% of signal range per step)
	TrendThreshold float64

	// StabilityThreshold - velocity magnitude below which we switch to basic mode
	// Default: 0.02 (2% of signal range per step)
	StabilityThreshold float64

	// ErrorThreshold - prediction error above which we consider switching
	// Default: 2.0 (2x expected noise)
	ErrorThreshold float64

	// SwitchHysteresis - minimum observations before mode can switch again
	// Prevents rapid oscillation between modes
	// Default: 10
	SwitchHysteresis int

	// WindowSize - number of observations for trend/variance detection
	// Default: 20
	WindowSize int

	// InitialMode - starting mode (ModeBasic, ModeVelocity, or ModeAuto)
	InitialMode FilterMode
}

// DefaultAdaptiveConfig returns sensible defaults for auto-switching.
func DefaultAdaptiveConfig() AdaptiveConfig {
	return AdaptiveConfig{
		BasicConfig:        DefaultConfig(),
		VelocityConfig:     DefaultVelocityConfig(),
		TrendThreshold:     0.1,
		StabilityThreshold: 0.02,
		ErrorThreshold:     2.0,
		SwitchHysteresis:   10,
		WindowSize:         20,
		InitialMode:        ModeAuto,
	}
}

// SmoothingOptimizedConfig favors basic mode, only switches for strong trends.
func SmoothingOptimizedConfig() AdaptiveConfig {
	cfg := DefaultAdaptiveConfig()
	cfg.TrendThreshold = 0.2      // Need stronger trend to switch
	cfg.StabilityThreshold = 0.05 // Quick return to basic
	cfg.SwitchHysteresis = 20     // More resistant to switching
	cfg.InitialMode = ModeBasic
	return cfg
}

// TrackingOptimizedConfig favors velocity mode, only switches for very stable signals.
func TrackingOptimizedConfig() AdaptiveConfig {
	cfg := DefaultAdaptiveConfig()
	cfg.TrendThreshold = 0.05     // Quick switch to velocity
	cfg.StabilityThreshold = 0.01 // Need very stable to switch to basic
	cfg.SwitchHysteresis = 5      // Quick adaptation
	cfg.InitialMode = ModeVelocity
	return cfg
}

// KalmanAdaptive wraps both filter types and switches dynamically.
type KalmanAdaptive struct {
	mu sync.RWMutex

	// Underlying filters
	basic    *Kalman
	velocity *KalmanVelocity

	// Current state
	mode         FilterMode
	forcedMode   FilterMode // If not ModeAuto, overrides auto-detection
	observations int
	lastSwitch   int // Observation count at last mode switch

	// Configuration
	trendThreshold     float64
	stabilityThreshold float64
	errorThreshold     float64
	hysteresis         int

	// Signal analysis
	window       []float64
	windowIdx    int
	windowSize   int
	lastValue    float64
	lastFiltered float64

	// Statistics for mode switching
	trendScore    float64 // Running estimate of trend strength
	predictionErr float64 // Running prediction error
	switchCount   int     // Total mode switches
}

// NewKalmanAdaptive creates a new adaptive filter.
func NewKalmanAdaptive(cfg AdaptiveConfig) *KalmanAdaptive {
	windowSize := cfg.WindowSize
	if windowSize < 5 {
		windowSize = 5
	}

	k := &KalmanAdaptive{
		basic:              NewKalman(cfg.BasicConfig),
		velocity:           NewKalmanVelocity(cfg.VelocityConfig),
		mode:               cfg.InitialMode,
		forcedMode:         cfg.InitialMode,
		trendThreshold:     cfg.TrendThreshold,
		stabilityThreshold: cfg.StabilityThreshold,
		errorThreshold:     cfg.ErrorThreshold,
		hysteresis:         cfg.SwitchHysteresis,
		window:             make([]float64, windowSize),
		windowSize:         windowSize,
	}

	// If starting in auto mode, default to basic
	if k.mode == ModeAuto {
		k.mode = ModeBasic
	}

	return k
}

// Process filters a new measurement, auto-switching modes if needed.
func (k *KalmanAdaptive) Process(measurement float64) float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.processInternal(measurement)
}

func (k *KalmanAdaptive) processInternal(measurement float64) float64 {
	// Update window for trend detection
	k.updateWindow(measurement)

	// Calculate prediction error (before processing)
	var predicted float64
	if k.observations > 0 {
		if k.mode == ModeVelocity {
			predicted = k.velocity.Predict(1)
		} else {
			predicted = k.basic.Predict(1)
		}
		k.predictionErr = k.predictionErr*0.9 + math.Abs(measurement-predicted)*0.1
	}

	// Consider mode switch (if in auto mode and hysteresis allows)
	if k.forcedMode == ModeAuto && k.observations-k.lastSwitch >= k.hysteresis {
		k.considerModeSwitch()
	}

	// Process with current mode
	var filtered float64
	if k.mode == ModeVelocity {
		filtered = k.velocity.Process(measurement)
		// Keep basic filter warm (in background)
		k.basic.Process(measurement, 0)
	} else {
		filtered = k.basic.Process(measurement, 0)
		// Keep velocity filter warm (in background)
		k.velocity.Process(measurement)
	}

	k.lastValue = measurement
	k.lastFiltered = filtered
	k.observations++

	return filtered
}

// updateWindow adds observation to sliding window for trend analysis.
func (k *KalmanAdaptive) updateWindow(value float64) {
	k.window[k.windowIdx] = value
	k.windowIdx = (k.windowIdx + 1) % k.windowSize
}

// considerModeSwitch evaluates whether to switch modes.
func (k *KalmanAdaptive) considerModeSwitch() {
	// Need enough data
	if k.observations < k.windowSize {
		return
	}

	// Detect trend strength
	trendStrength := k.detectTrend()
	k.trendScore = k.trendScore*0.8 + trendStrength*0.2 // Smooth

	// Decision logic
	shouldSwitch := false
	newMode := k.mode

	if k.mode == ModeBasic {
		// Consider switching to velocity if trend detected
		if k.trendScore > k.trendThreshold {
			shouldSwitch = true
			newMode = ModeVelocity
		}
		// Or if prediction error is high
		if k.predictionErr > k.errorThreshold && k.trendScore > k.stabilityThreshold {
			shouldSwitch = true
			newMode = ModeVelocity
		}
	} else {
		// Consider switching to basic if signal is stable
		if k.trendScore < k.stabilityThreshold {
			shouldSwitch = true
			newMode = ModeBasic
		}
	}

	if shouldSwitch && newMode != k.mode {
		k.switchMode(newMode)
	}
}

// detectTrend estimates the trend strength in the current window.
func (k *KalmanAdaptive) detectTrend() float64 {
	if k.observations < k.windowSize {
		return 0
	}

	// Simple linear regression slope estimation
	// y = mx + b, we estimate |m| relative to signal range
	n := float64(k.windowSize)
	var sumX, sumY, sumXY, sumX2 float64
	var minY, maxY float64 = math.MaxFloat64, -math.MaxFloat64

	for i := 0; i < k.windowSize; i++ {
		x := float64(i)
		y := k.window[(k.windowIdx+i)%k.windowSize]

		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x

		if y < minY {
			minY = y
		}
		if y > maxY {
			maxY = y
		}
	}

	// Slope
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)

	// Normalize by signal range
	signalRange := maxY - minY
	if signalRange < 0.001 {
		signalRange = 0.001
	}

	return math.Abs(slope) / signalRange
}

// switchMode transitions to a new filtering mode.
func (k *KalmanAdaptive) switchMode(newMode FilterMode) {
	// Synchronize state between filters for smooth transition
	if newMode == ModeVelocity {
		// Transfer state from basic to velocity
		k.velocity.SetState(k.basic.State(), k.basic.Velocity())
	} else {
		// Transfer state from velocity to basic
		k.basic.SetState(k.velocity.State())
	}

	k.mode = newMode
	k.lastSwitch = k.observations
	k.switchCount++
}

// SetMode forces a specific mode (ModeBasic, ModeVelocity) or re-enables auto (ModeAuto).
func (k *KalmanAdaptive) SetMode(mode FilterMode) {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.forcedMode = mode
	if mode != ModeAuto {
		k.mode = mode
		k.lastSwitch = k.observations
	}
}

// Mode returns the current filtering mode.
func (k *KalmanAdaptive) Mode() FilterMode {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.mode
}

// State returns the current filtered state.
func (k *KalmanAdaptive) State() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.mode == ModeVelocity {
		return k.velocity.State()
	}
	return k.basic.State()
}

// Velocity returns the current velocity estimate.
func (k *KalmanAdaptive) Velocity() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.mode == ModeVelocity {
		return k.velocity.Velocity()
	}
	return k.basic.Velocity()
}

// Predict estimates future state.
func (k *KalmanAdaptive) Predict(steps int) float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if k.mode == ModeVelocity {
		return k.velocity.Predict(steps)
	}
	return k.basic.Predict(steps)
}

// Observations returns total observations processed.
func (k *KalmanAdaptive) Observations() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.observations
}

// SwitchCount returns how many times the mode has switched.
func (k *KalmanAdaptive) SwitchCount() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.switchCount
}

// TrendScore returns the current trend strength estimate (0-1+).
func (k *KalmanAdaptive) TrendScore() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.trendScore
}

// PredictionError returns the smoothed prediction error.
func (k *KalmanAdaptive) PredictionError() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.predictionErr
}

// Reset resets both filters and statistics.
func (k *KalmanAdaptive) Reset() {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.basic.Reset()
	k.velocity.Reset()
	k.observations = 0
	k.lastSwitch = 0
	k.switchCount = 0
	k.trendScore = 0
	k.predictionErr = 0
	k.windowIdx = 0
	for i := range k.window {
		k.window[i] = 0
	}

	if k.forcedMode == ModeAuto {
		k.mode = ModeBasic
	}
}

// AdaptiveStats holds statistics for the adaptive filter.
type AdaptiveStats struct {
	Mode            FilterMode
	ForcedMode      FilterMode
	Observations    int
	SwitchCount     int
	TrendScore      float64
	PredictionError float64
	CurrentState    float64
	CurrentVelocity float64
}

// GetStats returns current statistics.
func (k *KalmanAdaptive) GetStats() AdaptiveStats {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return AdaptiveStats{
		Mode:            k.mode,
		ForcedMode:      k.forcedMode,
		Observations:    k.observations,
		SwitchCount:     k.switchCount,
		TrendScore:      k.trendScore,
		PredictionError: k.predictionErr,
		CurrentState:    k.State(),
		CurrentVelocity: k.Velocity(),
	}
}

// ProcessIfEnabled applies filtering if enabled.
func (k *KalmanAdaptive) ProcessIfEnabled(feature string, measurement float64) config.FilteredValue {
	result := config.FilteredValue{
		Raw:     measurement,
		Feature: feature,
	}

	if config.IsFeatureEnabled(feature) {
		result.Filtered = k.Process(measurement)
		result.WasFiltered = true
	} else {
		result.Filtered = measurement
		result.WasFiltered = false
	}

	return result
}

// ProcessBatch processes multiple measurements.
func (k *KalmanAdaptive) ProcessBatch(measurements []float64) []float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	results := make([]float64, len(measurements))
	for i, m := range measurements {
		results[i] = k.processInternal(m)
	}
	return results
}
