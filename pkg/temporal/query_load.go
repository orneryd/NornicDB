// Package temporal - Query load prediction for resource scaling.
//
// QueryLoadPredictor tracks query volume trends and predicts future load
// using KalmanVelocity filters. This enables:
//   - Predicting upcoming query spikes
//   - Detecting load trends (increasing/decreasing)
//   - Triggering pre-emptive resource scaling
//   - Identifying query patterns (peak hours, burst events)
//
// Use cases:
//   - Auto-scale database connections
//   - Pre-warm caches before predicted spikes
//   - Alert on unusual load patterns
//   - Capacity planning
//
// Example usage:
//
//	predictor := temporal.NewQueryLoadPredictor(temporal.DefaultLoadConfig())
//
//	// Record each query
//	predictor.RecordQuery()
//
//	// Get current load prediction
//	prediction := predictor.GetPrediction()
//	fmt.Printf("Current QPS: %.1f, Predicted (5min): %.1f\n",
//	    prediction.CurrentQPS, prediction.PredictedQPS)
//
//	// Check if scaling needed
//	if predictor.ShouldScaleUp(100) { // threshold QPS
//	    triggerScaleUp()
//	}
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're running a lemonade stand. You want to know:
//
//	🍋 "How many customers are coming right now?" (Current QPS)
//	🍋 "Will it get busier or slower?" (Trend)
//	🍋 "Should I make more lemonade NOW?" (Scale up prediction)
//
// The QueryLoadPredictor counts how many "questions" (queries) the database
// gets every second. It's like counting customers:
//
//	Second 1: 10 queries
//	Second 2: 12 queries
//	Second 3: 15 queries
//	Second 4: 20 queries
//	→ "Whoa, we're getting BUSIER! Velocity is positive!" 📈
//
// The Kalman filter smooths out the bumps:
//
//	Raw counts:   10, 12, 50, 11, 13  (that 50 was a weird spike!)
//	Filtered:     10, 11, 15, 13, 13  (smoothed - ignores the spike)
//
// Why filter? Because ONE busy second doesn't mean you need to panic!
// The filter asks: "Is this a REAL trend or just random noise?"
//
// Predictions:
//
//	Current: 50 QPS (queries per second)
//	Velocity: +5 QPS/second (getting busier)
//	Predicted in 5 min: 50 + (5 × 300) = 1550 QPS! 😱
//	→ "Better scale up NOW before we're overwhelmed!"
//
// Anomaly detection:
//
//	Normal: 50 QPS
//	Suddenly: 500 QPS ← "SPIKE! Something's happening!"
//	Suddenly: 5 QPS ← "DROP! Did something break?"
//
// Peak hour detection:
//
//	Hour    0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23
//	Count   2  1  1  1  2  5  15 30 50 45 40 30 35 40 50 45 40 35 25 20 15 10 5  3
//	                              ^^^^^^^^^^^^
//	                    "Peak hours are 8-10am and 2-4pm"
//
// This lets us PREPARE before the rush! Pre-warm caches at 7:30am! 🚀
package temporal

import (
	"math"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/filter"
)

// LoadPrediction represents a query load prediction.
type LoadPrediction struct {
	// Current metrics
	CurrentQPS   float64 // Queries per second (smoothed)
	CurrentQPM   float64 // Queries per minute (smoothed)
	RawQPS       float64 // Unfiltered QPS
	TotalQueries int64

	// Trend
	Velocity float64 // Rate of change (positive = increasing load)
	Trend    string  // "increasing", "decreasing", "stable"

	// Predictions
	PredictedQPS5m  float64 // Predicted QPS in 5 minutes
	PredictedQPS15m float64 // Predicted QPS in 15 minutes
	PredictedQPS1h  float64 // Predicted QPS in 1 hour

	// Confidence
	Confidence float64

	// Time-of-day pattern
	PeakHour   int
	IsNearPeak bool

	// Anomaly detection
	IsAnomaly   bool
	AnomalyType string // "spike", "drop", "sustained_high", "sustained_low"

	// Timestamp
	Timestamp time.Time
}

// LoadConfig holds configuration for query load prediction.
type LoadConfig struct {
	// FilterConfig for the underlying Kalman velocity filter
	FilterConfig filter.VelocityConfig

	// BucketDurationSeconds - duration of each measurement bucket
	BucketDurationSeconds float64

	// SpikeThreshold - QPS velocity above which is a "spike"
	SpikeThreshold float64

	// DropThreshold - QPS velocity below which is a "drop"
	DropThreshold float64

	// AnomalyStdDevs - number of standard deviations for anomaly detection
	AnomalyStdDevs float64

	// ScaleUpThreshold - relative increase that suggests scaling up
	ScaleUpThreshold float64

	// ScaleDownThreshold - relative decrease that suggests scaling down
	ScaleDownThreshold float64

	// PeakDetectionWindow - hours to track for peak detection
	PeakDetectionWindow int
}

// DefaultLoadConfig returns sensible defaults.
func DefaultLoadConfig() LoadConfig {
	return LoadConfig{
		FilterConfig: filter.VelocityConfig{
			ProcessNoisePos:    0.5, // QPS can change quickly
			ProcessNoiseVel:    0.1,
			MeasurementNoise:   2.0, // Measurement has noise
			InitialPosVariance: 100.0,
			InitialVelVariance: 10.0,
			Dt:                 1.0,
		},
		BucketDurationSeconds: 1.0, // 1-second buckets
		SpikeThreshold:        5.0, // 5 QPS/sec increase
		DropThreshold:         -5.0,
		AnomalyStdDevs:        3.0,
		ScaleUpThreshold:      0.5,  // 50% increase
		ScaleDownThreshold:    -0.3, // 30% decrease
		PeakDetectionWindow:   24,
	}
}

// HighSensitivityLoadConfig returns config for high-sensitivity detection.
func HighSensitivityLoadConfig() LoadConfig {
	cfg := DefaultLoadConfig()
	cfg.SpikeThreshold = 2.0
	cfg.AnomalyStdDevs = 2.0
	cfg.ScaleUpThreshold = 0.3
	return cfg
}

// QueryLoadPredictor predicts query load using velocity tracking.
type QueryLoadPredictor struct {
	mu     sync.RWMutex
	config LoadConfig

	// Kalman filter for QPS tracking
	qpsFilter *filter.KalmanVelocity

	// Current bucket
	currentBucket time.Time
	bucketCount   int64

	// Statistics
	totalQueries int64
	startTime    time.Time

	// Rolling window for variance calculation
	recentQPS    []float64
	recentQPSIdx int
	windowSize   int

	// Hour-of-day tracking
	hourCounts [24]int64
	hourSums   [24]float64

	// Baseline for anomaly detection
	baselineQPS    float64
	baselineStdDev float64
}

// NewQueryLoadPredictor creates a new query load predictor.
func NewQueryLoadPredictor(cfg LoadConfig) *QueryLoadPredictor {
	return &QueryLoadPredictor{
		config:        cfg,
		qpsFilter:     filter.NewKalmanVelocity(cfg.FilterConfig),
		startTime:     time.Now(),
		currentBucket: time.Now().Truncate(time.Duration(cfg.BucketDurationSeconds) * time.Second),
		recentQPS:     make([]float64, 60), // Track last 60 measurements
		windowSize:    60,
	}
}

// RecordQuery records a query event.
func (qlp *QueryLoadPredictor) RecordQuery() {
	qlp.RecordQueryAt(time.Now())
}

// RecordQueryAt records a query at a specific time.
func (qlp *QueryLoadPredictor) RecordQueryAt(timestamp time.Time) {
	qlp.mu.Lock()
	defer qlp.mu.Unlock()

	qlp.totalQueries++

	// Track by hour
	hour := timestamp.Hour()
	qlp.hourCounts[hour]++

	// Check if we've moved to a new bucket
	bucket := timestamp.Truncate(time.Duration(qlp.config.BucketDurationSeconds) * time.Second)
	if bucket.After(qlp.currentBucket) {
		// Flush the old bucket
		qlp.flushBucket()
		qlp.currentBucket = bucket
		qlp.bucketCount = 0
	}

	qlp.bucketCount++
}

// RecordQueries records multiple query events (batch recording).
func (qlp *QueryLoadPredictor) RecordQueries(count int) {
	qlp.mu.Lock()
	defer qlp.mu.Unlock()

	qlp.totalQueries += int64(count)
	qlp.bucketCount += int64(count)

	hour := time.Now().Hour()
	qlp.hourCounts[hour] += int64(count)
}

// flushBucket processes the completed bucket.
func (qlp *QueryLoadPredictor) flushBucket() {
	if qlp.bucketCount == 0 {
		return
	}

	// Calculate QPS for this bucket
	qps := float64(qlp.bucketCount) / qlp.config.BucketDurationSeconds

	// Feed to Kalman filter
	qlp.qpsFilter.Process(qps)

	// Track in rolling window
	qlp.recentQPS[qlp.recentQPSIdx] = qps
	qlp.recentQPSIdx = (qlp.recentQPSIdx + 1) % qlp.windowSize

	// Update hour tracking
	hour := qlp.currentBucket.Hour()
	qlp.hourSums[hour] += qps

	// Update baseline
	qlp.updateBaseline()
}

// updateBaseline updates the baseline QPS statistics.
func (qlp *QueryLoadPredictor) updateBaseline() {
	// Calculate mean and std dev from recent window
	var sum, sumSq float64
	count := 0

	for _, qps := range qlp.recentQPS {
		if qps > 0 {
			sum += qps
			sumSq += qps * qps
			count++
		}
	}

	if count > 5 {
		mean := sum / float64(count)
		variance := sumSq/float64(count) - mean*mean
		if variance < 0 {
			variance = 0
		}

		qlp.baselineQPS = mean
		qlp.baselineStdDev = math.Sqrt(variance)
	}
}

// GetPrediction returns the current load prediction.
func (qlp *QueryLoadPredictor) GetPrediction() LoadPrediction {
	qlp.mu.RLock()
	defer qlp.mu.RUnlock()

	now := time.Now()

	// Get filtered QPS and velocity
	currentQPS := qlp.qpsFilter.State()
	velocity := qlp.qpsFilter.Velocity()

	// Calculate raw QPS from current bucket
	elapsed := now.Sub(qlp.currentBucket).Seconds()
	rawQPS := 0.0
	if elapsed > 0 {
		rawQPS = float64(qlp.bucketCount) / elapsed
	}

	// Determine trend
	var trend string
	if velocity > qlp.config.SpikeThreshold/10 {
		trend = "increasing"
	} else if velocity < qlp.config.DropThreshold/10 {
		trend = "decreasing"
	} else {
		trend = "stable"
	}

	// Predictions
	pred5m := qlp.qpsFilter.Predict(300)  // 5 minutes
	pred15m := qlp.qpsFilter.Predict(900) // 15 minutes
	pred1h := qlp.qpsFilter.Predict(3600) // 1 hour

	// Clamp predictions to reasonable values
	if pred5m < 0 {
		pred5m = 0
	}
	if pred15m < 0 {
		pred15m = 0
	}
	if pred1h < 0 {
		pred1h = 0
	}

	// Find peak hour
	peakHour := 0
	maxCount := int64(0)
	for h, count := range qlp.hourCounts {
		if count > maxCount {
			maxCount = count
			peakHour = h
		}
	}

	// Check if near peak (within 2 hours)
	currentHour := now.Hour()
	isNearPeak := abs(currentHour-peakHour) <= 2 ||
		abs(currentHour-peakHour) >= 22 // Handle wrap-around

	// Anomaly detection
	isAnomaly := false
	anomalyType := ""
	if qlp.baselineStdDev > 0 {
		deviation := (currentQPS - qlp.baselineQPS) / qlp.baselineStdDev
		if deviation > qlp.config.AnomalyStdDevs {
			isAnomaly = true
			if velocity > qlp.config.SpikeThreshold {
				anomalyType = "spike"
			} else {
				anomalyType = "sustained_high"
			}
		} else if deviation < -qlp.config.AnomalyStdDevs {
			isAnomaly = true
			if velocity < qlp.config.DropThreshold {
				anomalyType = "drop"
			} else {
				anomalyType = "sustained_low"
			}
		}
	}

	// Calculate confidence
	confidence := float64(qlp.totalQueries) / float64(qlp.totalQueries+1000)

	return LoadPrediction{
		CurrentQPS:      currentQPS,
		CurrentQPM:      currentQPS * 60,
		RawQPS:          rawQPS,
		TotalQueries:    qlp.totalQueries,
		Velocity:        velocity,
		Trend:           trend,
		PredictedQPS5m:  pred5m,
		PredictedQPS15m: pred15m,
		PredictedQPS1h:  pred1h,
		Confidence:      confidence,
		PeakHour:        peakHour,
		IsNearPeak:      isNearPeak,
		IsAnomaly:       isAnomaly,
		AnomalyType:     anomalyType,
		Timestamp:       now,
	}
}

// abs returns the absolute value of an int.
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// ShouldScaleUp checks if load is increasing and above threshold.
func (qlp *QueryLoadPredictor) ShouldScaleUp(thresholdQPS float64) bool {
	pred := qlp.GetPrediction()

	// Scale up if:
	// 1. Current QPS is near threshold
	// 2. Trend is increasing
	// 3. Predicted QPS exceeds threshold
	if pred.CurrentQPS > thresholdQPS*0.8 && pred.Trend == "increasing" {
		return true
	}
	if pred.PredictedQPS5m > thresholdQPS {
		return true
	}
	return false
}

// ShouldScaleDown checks if load is decreasing and below threshold.
func (qlp *QueryLoadPredictor) ShouldScaleDown(thresholdQPS float64, minQPS float64) bool {
	pred := qlp.GetPrediction()

	// Scale down if:
	// 1. Current QPS is well below threshold
	// 2. Trend is decreasing or stable
	// 3. Above minimum QPS
	if pred.CurrentQPS < thresholdQPS*0.5 &&
		pred.Trend != "increasing" &&
		pred.CurrentQPS > minQPS {
		return true
	}
	return false
}

// GetLoadLevel returns a simplified load level (0-5).
func (qlp *QueryLoadPredictor) GetLoadLevel(maxQPS float64) int {
	pred := qlp.GetPrediction()
	ratio := pred.CurrentQPS / maxQPS

	switch {
	case ratio < 0.1:
		return 0 // Idle
	case ratio < 0.3:
		return 1 // Low
	case ratio < 0.5:
		return 2 // Medium
	case ratio < 0.7:
		return 3 // High
	case ratio < 0.9:
		return 4 // Very High
	default:
		return 5 // Critical
	}
}

// PredictPeakTime predicts when the next peak will occur.
func (qlp *QueryLoadPredictor) PredictPeakTime() time.Time {
	qlp.mu.RLock()
	defer qlp.mu.RUnlock()

	now := time.Now()
	currentHour := now.Hour()

	// Find peak hour
	peakHour := 0
	maxCount := int64(0)
	for h, count := range qlp.hourCounts {
		if count > maxCount {
			maxCount = count
			peakHour = h
		}
	}

	// Calculate next occurrence of peak hour
	hoursUntilPeak := peakHour - currentHour
	if hoursUntilPeak <= 0 {
		hoursUntilPeak += 24
	}

	return now.Add(time.Duration(hoursUntilPeak) * time.Hour).Truncate(time.Hour)
}

// LoadStats holds statistics about query load.
type LoadStats struct {
	TotalQueries  int64
	UptimeSeconds float64
	AverageQPS    float64
	CurrentQPS    float64
	PeakQPS       float64
	PeakHour      int
}

// GetStats returns query load statistics.
func (qlp *QueryLoadPredictor) GetStats() LoadStats {
	qlp.mu.RLock()
	defer qlp.mu.RUnlock()

	uptime := time.Since(qlp.startTime).Seconds()

	// Find peak QPS from recent window
	peakQPS := 0.0
	for _, qps := range qlp.recentQPS {
		if qps > peakQPS {
			peakQPS = qps
		}
	}

	// Find peak hour
	peakHour := 0
	maxCount := int64(0)
	for h, count := range qlp.hourCounts {
		if count > maxCount {
			maxCount = count
			peakHour = h
		}
	}

	return LoadStats{
		TotalQueries:  qlp.totalQueries,
		UptimeSeconds: uptime,
		AverageQPS:    float64(qlp.totalQueries) / uptime,
		CurrentQPS:    qlp.qpsFilter.State(),
		PeakQPS:       peakQPS,
		PeakHour:      peakHour,
	}
}

// Reset clears all load data.
func (qlp *QueryLoadPredictor) Reset() {
	qlp.mu.Lock()
	defer qlp.mu.Unlock()

	qlp.qpsFilter = filter.NewKalmanVelocity(qlp.config.FilterConfig)
	qlp.totalQueries = 0
	qlp.startTime = time.Now()
	qlp.currentBucket = time.Now().Truncate(time.Duration(qlp.config.BucketDurationSeconds) * time.Second)
	qlp.bucketCount = 0
	qlp.recentQPS = make([]float64, qlp.windowSize)
	qlp.recentQPSIdx = 0
	qlp.hourCounts = [24]int64{}
	qlp.hourSums = [24]float64{}
	qlp.baselineQPS = 0
	qlp.baselineStdDev = 0
}
