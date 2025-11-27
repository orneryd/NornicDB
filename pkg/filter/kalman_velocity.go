// Package filter - Velocity-state Kalman filter for trend tracking.
//
// This implements a 2-state Kalman filter that explicitly estimates both
// position and velocity, providing much better trend tracking than the
// basic scalar filter.
//
// Use KalmanVelocity when:
//   - Signal has trends or drift
//   - Prediction accuracy is critical
//   - Temporal patterns need tracking
//
// Use basic Kalman when:
//   - Signal is stationary (stable value + noise)
//   - Maximum noise rejection is priority
//   - Simplicity and speed are paramount
//
// The 2-state model:
//
//	State vector: [position, velocity]ᵀ
//	Transition:   x(k+1) = F * x(k) + process_noise
//	              where F = [1, dt; 0, 1]
//	Measurement:  z(k) = H * x(k) + measurement_noise
//	              where H = [1, 0] (we only measure position)
//
// ELI12 (Explain Like I'm 12):
//
// The basic Kalman filter is like guessing where a ball is, but forgetting
// how fast it was going. This velocity filter remembers BOTH where the ball
// is AND how fast it's moving. So when you predict where it'll be next,
// you say "it was HERE, moving THIS FAST, so it'll probably be THERE."
//
// This makes it WAY better at following moving targets!
package filter

import (
	"math"
	"sync"

	"github.com/orneryd/nornicdb/pkg/config"
)

// VelocityConfig holds configuration for the 2-state Kalman filter.
type VelocityConfig struct {
	// ProcessNoisePos - uncertainty in position prediction
	ProcessNoisePos float64

	// ProcessNoiseVel - uncertainty in velocity prediction
	ProcessNoiseVel float64

	// MeasurementNoise - uncertainty in measurements
	MeasurementNoise float64

	// InitialPosVariance - initial uncertainty in position
	InitialPosVariance float64

	// InitialVelVariance - initial uncertainty in velocity
	InitialVelVariance float64

	// Dt - time step between measurements (default: 1.0)
	Dt float64
}

// DefaultVelocityConfig returns sensible defaults for trend tracking.
func DefaultVelocityConfig() VelocityConfig {
	return VelocityConfig{
		ProcessNoisePos:    0.1,
		ProcessNoiseVel:    0.01,
		MeasurementNoise:   1.0,
		InitialPosVariance: 100.0,
		InitialVelVariance: 10.0,
		Dt:                 1.0,
	}
}

// TemporalTrackingConfig returns config optimized for temporal pattern tracking.
func TemporalTrackingConfig() VelocityConfig {
	return VelocityConfig{
		ProcessNoisePos:    0.05,
		ProcessNoiseVel:    0.005,
		MeasurementNoise:   0.5,
		InitialPosVariance: 50.0,
		InitialVelVariance: 5.0,
		Dt:                 1.0,
	}
}

// AggressiveTrackingConfig returns config for fast-changing signals.
func AggressiveTrackingConfig() VelocityConfig {
	return VelocityConfig{
		ProcessNoisePos:    0.5,
		ProcessNoiseVel:    0.1,
		MeasurementNoise:   1.0,
		InitialPosVariance: 100.0,
		InitialVelVariance: 50.0,
		Dt:                 1.0,
	}
}

// KalmanVelocity implements a 2-state Kalman filter with position and velocity.
type KalmanVelocity struct {
	mu sync.RWMutex

	// State vector [position, velocity]
	pos float64
	vel float64

	// 2x2 Covariance matrix (stored as individual elements)
	// P = [p00, p01; p10, p11]
	p00, p01, p10, p11 float64

	// Process noise covariance Q
	qPos, qVel float64

	// Measurement noise R
	r float64

	// Time step
	dt float64

	// Statistics
	observations int
}

// NewKalmanVelocity creates a new 2-state Kalman filter.
func NewKalmanVelocity(cfg VelocityConfig) *KalmanVelocity {
	dt := cfg.Dt
	if dt <= 0 {
		dt = 1.0
	}

	return &KalmanVelocity{
		pos:  0,
		vel:  0,
		p00:  cfg.InitialPosVariance,
		p01:  0,
		p10:  0,
		p11:  cfg.InitialVelVariance,
		qPos: cfg.ProcessNoisePos,
		qVel: cfg.ProcessNoiseVel,
		r:    cfg.MeasurementNoise,
		dt:   dt,
	}
}

// NewKalmanVelocityWithInitial creates a filter with initial state.
func NewKalmanVelocityWithInitial(cfg VelocityConfig, initialPos, initialVel float64) *KalmanVelocity {
	k := NewKalmanVelocity(cfg)
	k.pos = initialPos
	k.vel = initialVel
	return k
}

// Process updates the filter with a new measurement.
// Returns the filtered position estimate.
func (k *KalmanVelocity) Process(measurement float64) float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	return k.processInternal(measurement)
}

func (k *KalmanVelocity) processInternal(measurement float64) float64 {
	dt := k.dt

	// === PREDICT STEP ===
	// State prediction: x(k|k-1) = F * x(k-1|k-1)
	// F = [1, dt; 0, 1]
	predPos := k.pos + k.vel*dt
	predVel := k.vel // velocity assumed constant

	// Covariance prediction: P(k|k-1) = F * P(k-1|k-1) * F' + Q
	// After matrix multiplication:
	// P' = [p00 + dt*p10 + dt*p01 + dt²*p11 + qPos,  p01 + dt*p11]
	//      [p10 + dt*p11,                            p11 + qVel  ]
	predP00 := k.p00 + dt*k.p10 + dt*k.p01 + dt*dt*k.p11 + k.qPos
	predP01 := k.p01 + dt*k.p11
	predP10 := k.p10 + dt*k.p11
	predP11 := k.p11 + k.qVel

	// === UPDATE STEP ===
	// Innovation: y = z - H * x(k|k-1), where H = [1, 0]
	innovation := measurement - predPos

	// Innovation covariance: S = H * P(k|k-1) * H' + R = P00 + R
	s := predP00 + k.r

	// Kalman gain: K = P(k|k-1) * H' * S⁻¹
	// K = [P00/S, P10/S]'
	k0 := predP00 / s
	k1 := predP10 / s

	// State update: x(k|k) = x(k|k-1) + K * y
	k.pos = predPos + k0*innovation
	k.vel = predVel + k1*innovation

	// Covariance update: P(k|k) = (I - K*H) * P(k|k-1)
	// Using Joseph form for numerical stability:
	// P = (I - KH) * P * (I - KH)' + K * R * K'
	// Simplified (since H = [1,0]):
	k.p00 = (1 - k0) * predP00
	k.p01 = (1 - k0) * predP01
	k.p10 = predP10 - k1*predP00
	k.p11 = predP11 - k1*predP01

	k.observations++
	return k.pos
}

// ProcessBatch processes multiple measurements efficiently.
func (k *KalmanVelocity) ProcessBatch(measurements []float64) []float64 {
	k.mu.Lock()
	defer k.mu.Unlock()

	results := make([]float64, len(measurements))
	for i, m := range measurements {
		results[i] = k.processInternal(m)
	}
	return results
}

// Predict estimates the state n steps into the future.
func (k *KalmanVelocity) Predict(steps int) float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()

	// x(k+n) = pos + vel * (n * dt)
	return k.pos + k.vel*float64(steps)*k.dt
}

// PredictWithUncertainty returns predicted position and its uncertainty.
func (k *KalmanVelocity) PredictWithUncertainty(steps int) (position, uncertainty float64) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	n := float64(steps)
	dt := k.dt

	position = k.pos + k.vel*n*dt

	// Uncertainty grows with prediction horizon
	// Var(pos + vel*n*dt) = Var(pos) + n²*dt²*Var(vel) + 2*n*dt*Cov(pos,vel)
	// Plus accumulated process noise
	variance := k.p00 + n*n*dt*dt*k.p11 + 2*n*dt*k.p01
	// Add process noise accumulation
	for i := 0; i < steps; i++ {
		variance += k.qPos + float64(i)*float64(i)*k.qVel
	}
	uncertainty = math.Sqrt(variance)

	return position, uncertainty
}

// State returns the current position estimate.
func (k *KalmanVelocity) State() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.pos
}

// Position returns the current position estimate (alias for State).
func (k *KalmanVelocity) Position() float64 {
	return k.State()
}

// Velocity returns the current velocity estimate.
func (k *KalmanVelocity) Velocity() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.vel
}

// Covariance returns the position variance (uncertainty squared).
func (k *KalmanVelocity) Covariance() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.p00
}

// VelocityCovariance returns the velocity variance.
func (k *KalmanVelocity) VelocityCovariance() float64 {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.p11
}

// Observations returns the number of measurements processed.
func (k *KalmanVelocity) Observations() int {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.observations
}

// Reset resets the filter to initial state.
func (k *KalmanVelocity) Reset() {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.pos = 0
	k.vel = 0
	k.p00 = 100.0 // Reset to reasonable defaults
	k.p01 = 0
	k.p10 = 0
	k.p11 = 10.0
	k.observations = 0
}

// SetState manually sets position and velocity.
func (k *KalmanVelocity) SetState(pos, vel float64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.pos = pos
	k.vel = vel
}

// VelocityStats returns detailed filter statistics.
type VelocityStats struct {
	Position         float64
	Velocity         float64
	PositionVariance float64
	VelocityVariance float64
	CrossCovariance  float64
	MeasurementNoise float64
	Observations     int
}

// GetStats returns current filter statistics.
func (k *KalmanVelocity) GetStats() VelocityStats {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return VelocityStats{
		Position:         k.pos,
		Velocity:         k.vel,
		PositionVariance: k.p00,
		VelocityVariance: k.p11,
		CrossCovariance:  k.p01,
		MeasurementNoise: k.r,
		Observations:     k.observations,
	}
}

// ProcessIfEnabled applies filtering if the feature is enabled.
func (k *KalmanVelocity) ProcessIfEnabled(feature string, measurement float64) config.FilteredValue {
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

// PredictIfEnabled returns prediction if enabled.
func (k *KalmanVelocity) PredictIfEnabled(feature string, steps int) config.FilteredValue {
	result := config.FilteredValue{
		Raw:     k.State(),
		Feature: feature,
	}

	if config.IsFeatureEnabled(feature) {
		result.Filtered = k.Predict(steps)
		result.WasFiltered = true
	} else {
		result.Filtered = k.State()
		result.WasFiltered = false
	}

	return result
}
