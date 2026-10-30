// Package temporal - Pattern detection for cyclic access patterns.
//
// PatternDetector identifies recurring access patterns such as:
//   - Daily patterns (e.g., accessed every morning at 9am)
//   - Weekly patterns (e.g., accessed on Mondays)
//   - Burst patterns (clusters of rapid accesses)
//   - Decay patterns (gradually decreasing access)
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you have a favorite YouTube video. The PatternDetector notices:
//
//	📊 Daily Pattern: "You watch this video every day at 7pm after dinner"
//	📊 Weekly Pattern: "You binge-watch on Saturdays"
//	📊 Burst Pattern: "You're watching 10 videos right NOW - probably bored!"
//	📊 Growing: "You're watching more and more cat videos each week"
//	📊 Decaying: "You used to watch daily, now it's been 2 weeks..."
//
// The detector counts how often you do things at each hour (0-23) and each
// day (Sunday-Saturday). If one hour has WAY more than others, that's a pattern!
//
// Example:
//
//	Hour  0  1  2  3  4  5  6  7  8  9  10 11 12 13 14 15 16 17 18 19 20 21 22 23
//	Count 0  0  0  0  0  0  0  0  2  45  3  2  1  0  1  2  1  0  0  0  1  0  0  0
//	                                 ^^
//	                           "9am is clearly your jam!"
//
// The Kalman filter velocity tells us if you're watching MORE or LESS over time.
// Positive velocity = growing interest, Negative velocity = losing interest.
package temporal

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"sync"
	"time"
)

// PatternType represents detected pattern types.
type PatternType string

const (
	PatternNone     PatternType = "none"
	PatternDaily    PatternType = "daily"
	PatternWeekly   PatternType = "weekly"
	PatternBurst    PatternType = "burst"
	PatternDecaying PatternType = "decaying"
	PatternGrowing  PatternType = "growing"
)

// DetectedPattern holds information about a detected pattern.
type DetectedPattern struct {
	Type       PatternType
	Confidence float64
	PeakHour   int       // 0-23, for daily patterns
	PeakDay    int       // 0-6 (Sunday=0), for weekly patterns
	Period     float64   // Estimated period in seconds
	LastSeen   time.Time // When pattern was last observed
}

// PatternDetectorConfig holds configuration for pattern detection.
type PatternDetectorConfig struct {
	// MinSamplesForPattern - minimum accesses to detect patterns
	MinSamplesForPattern int

	// DailyConfidenceThreshold - min confidence to report daily pattern
	DailyConfidenceThreshold float64

	// WeeklyConfidenceThreshold - min confidence to report weekly pattern
	WeeklyConfidenceThreshold float64

	// BurstWindowSeconds - time window for burst detection
	BurstWindowSeconds float64

	// BurstMinAccesses - minimum accesses in window to be a burst
	BurstMinAccesses int

	// GrowthThreshold - velocity above which is "growing"
	GrowthThreshold float64

	// DecayThreshold - velocity below which is "decaying"
	DecayThreshold float64
}

// DefaultPatternDetectorConfig returns sensible defaults.
func DefaultPatternDetectorConfig() PatternDetectorConfig {
	return PatternDetectorConfig{
		MinSamplesForPattern:      10,
		DailyConfidenceThreshold:  0.3,
		WeeklyConfidenceThreshold: 0.4,
		BurstWindowSeconds:        60,
		BurstMinAccesses:          5,
		GrowthThreshold:           0.05,
		DecayThreshold:            -0.05,
	}
}

// PatternDetector detects access patterns for nodes.
type PatternDetector struct {
	mu     sync.RWMutex
	config PatternDetectorConfig

	// Per-node pattern data
	nodePatterns map[string]*nodePatternData
}

// nodePatternData holds pattern detection data for a single node.
type nodePatternData struct {
	// Hour-of-day histogram (24 buckets)
	hourCounts [24]int

	// Day-of-week histogram (7 buckets)
	dayCounts [7]int

	// Recent access times for burst detection
	recentAccesses []time.Time

	// Total accesses
	totalAccesses int

	// Detected patterns
	detectedPatterns []DetectedPattern

	// Last analysis time
	lastAnalysis time.Time
}

// NewPatternDetector creates a new pattern detector.
func NewPatternDetector(cfg PatternDetectorConfig) *PatternDetector {
	return &PatternDetector{
		config:       cfg,
		nodePatterns: make(map[string]*nodePatternData),
	}
}

// RecordAccess records an access for pattern analysis.
func (pd *PatternDetector) RecordAccess(nodeID string, timestamp time.Time) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	data, exists := pd.nodePatterns[nodeID]
	if !exists {
		data = &nodePatternData{
			recentAccesses: make([]time.Time, 0, 100),
		}
		pd.nodePatterns[nodeID] = data
	}

	// Update histograms
	hour := timestamp.Hour()
	day := int(timestamp.Weekday())
	data.hourCounts[hour]++
	data.dayCounts[day]++
	data.totalAccesses++

	// Track recent accesses for burst detection
	data.recentAccesses = append(data.recentAccesses, timestamp)
	// Keep only last 100
	if len(data.recentAccesses) > 100 {
		data.recentAccesses = data.recentAccesses[1:]
	}
}

// DetectPatterns analyzes access patterns for a node.
func (pd *PatternDetector) DetectPatterns(nodeID string, currentVelocity float64) []DetectedPattern {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	data, exists := pd.nodePatterns[nodeID]
	if !exists || data.totalAccesses < pd.config.MinSamplesForPattern {
		return nil
	}

	var patterns []DetectedPattern

	// Detect daily pattern
	if daily := pd.detectDailyPattern(data); daily != nil {
		patterns = append(patterns, *daily)
	}

	// Detect weekly pattern
	if weekly := pd.detectWeeklyPattern(data); weekly != nil {
		patterns = append(patterns, *weekly)
	}

	// Detect burst pattern
	if burst := pd.detectBurstPattern(data); burst != nil {
		patterns = append(patterns, *burst)
	}

	// Detect growth/decay from velocity
	if trend := pd.detectTrendPattern(currentVelocity); trend != nil {
		patterns = append(patterns, *trend)
	}

	return patterns
}

// detectDailyPattern checks for time-of-day patterns.
func (pd *PatternDetector) detectDailyPattern(data *nodePatternData) *DetectedPattern {
	total := 0
	maxHour := 0
	maxCount := 0

	for h, count := range data.hourCounts {
		total += count
		if count > maxCount {
			maxCount = count
			maxHour = h
		}
	}

	if total == 0 {
		return nil
	}

	// Calculate concentration (how peaked the distribution is)
	// If uniform: each hour would have total/24 accesses
	// High concentration = strong daily pattern
	expected := float64(total) / 24.0
	concentration := float64(maxCount) / expected

	// Normalize to 0-1 confidence
	confidence := (concentration - 1.0) / 3.0 // 4x concentration = 100% confidence
	if confidence > 1.0 {
		confidence = 1.0
	}
	if confidence < 0 {
		confidence = 0
	}

	if confidence < pd.config.DailyConfidenceThreshold {
		return nil
	}

	return &DetectedPattern{
		Type:       PatternDaily,
		Confidence: confidence,
		PeakHour:   maxHour,
		Period:     24 * 60 * 60, // 24 hours in seconds
		LastSeen:   time.Now(),
	}
}

// detectWeeklyPattern checks for day-of-week patterns.
func (pd *PatternDetector) detectWeeklyPattern(data *nodePatternData) *DetectedPattern {
	total := 0
	maxDay := 0
	maxCount := 0

	for d, count := range data.dayCounts {
		total += count
		if count > maxCount {
			maxCount = count
			maxDay = d
		}
	}

	if total == 0 {
		return nil
	}

	// Calculate concentration
	expected := float64(total) / 7.0
	concentration := float64(maxCount) / expected

	// Normalize to 0-1 confidence
	confidence := (concentration - 1.0) / 2.0 // 3x concentration = 100% confidence
	if confidence > 1.0 {
		confidence = 1.0
	}
	if confidence < 0 {
		confidence = 0
	}

	if confidence < pd.config.WeeklyConfidenceThreshold {
		return nil
	}

	return &DetectedPattern{
		Type:       PatternWeekly,
		Confidence: confidence,
		PeakDay:    maxDay,
		Period:     7 * 24 * 60 * 60, // 7 days in seconds
		LastSeen:   time.Now(),
	}
}

// detectBurstPattern checks for recent burst activity.
func (pd *PatternDetector) detectBurstPattern(data *nodePatternData) *DetectedPattern {
	if len(data.recentAccesses) < pd.config.BurstMinAccesses {
		return nil
	}

	now := time.Now()
	windowStart := now.Add(-time.Duration(pd.config.BurstWindowSeconds) * time.Second)

	// Count accesses in window
	countInWindow := 0
	for _, ts := range data.recentAccesses {
		if ts.After(windowStart) {
			countInWindow++
		}
	}

	if countInWindow < pd.config.BurstMinAccesses {
		return nil
	}

	// Calculate burst intensity
	accessesPerSecond := float64(countInWindow) / pd.config.BurstWindowSeconds
	confidence := math.Min(accessesPerSecond/10.0, 1.0) // 10+ per second = high confidence

	return &DetectedPattern{
		Type:       PatternBurst,
		Confidence: confidence,
		Period:     pd.config.BurstWindowSeconds,
		LastSeen:   now,
	}
}

// detectTrendPattern checks velocity for growth/decay.
func (pd *PatternDetector) detectTrendPattern(velocity float64) *DetectedPattern {
	if velocity > pd.config.GrowthThreshold {
		return &DetectedPattern{
			Type:       PatternGrowing,
			Confidence: math.Min(velocity/0.5, 1.0),
			LastSeen:   time.Now(),
		}
	}

	if velocity < pd.config.DecayThreshold {
		return &DetectedPattern{
			Type:       PatternDecaying,
			Confidence: math.Min(-velocity/0.5, 1.0),
			LastSeen:   time.Now(),
		}
	}

	return nil
}

// GetPeakAccessTime returns the predicted best time to access a node.
func (pd *PatternDetector) GetPeakAccessTime(nodeID string) (hour int, day int, confidence float64) {
	pd.mu.RLock()
	defer pd.mu.RUnlock()

	data, exists := pd.nodePatterns[nodeID]
	if !exists {
		return -1, -1, 0
	}

	// Find peak hour
	maxHour, maxHourCount := 0, 0
	totalHour := 0
	for h, count := range data.hourCounts {
		totalHour += count
		if count > maxHourCount {
			maxHour, maxHourCount = h, count
		}
	}

	// Find peak day
	maxDay, maxDayCount := 0, 0
	totalDay := 0
	for d, count := range data.dayCounts {
		totalDay += count
		if count > maxDayCount {
			maxDay, maxDayCount = d, count
		}
	}

	// Calculate combined confidence
	hourConf := 0.0
	if totalHour > 0 {
		hourConf = float64(maxHourCount) / float64(totalHour) * 24.0 / 10.0
	}
	dayConf := 0.0
	if totalDay > 0 {
		dayConf = float64(maxDayCount) / float64(totalDay) * 7.0 / 10.0
	}

	confidence = (hourConf + dayConf) / 2.0
	if confidence > 1.0 {
		confidence = 1.0
	}

	return maxHour, maxDay, confidence
}

// HasPattern checks if a node has a specific pattern type.
func (pd *PatternDetector) HasPattern(nodeID string, patternType PatternType, velocity float64) bool {
	patterns := pd.DetectPatterns(nodeID, velocity)
	for _, p := range patterns {
		if p.Type == patternType {
			return true
		}
	}
	return false
}

// Reset clears all pattern data.
func (pd *PatternDetector) Reset() {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.nodePatterns = make(map[string]*nodePatternData)
}

// ResetNode clears pattern data for a specific node.
func (pd *PatternDetector) ResetNode(nodeID string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	delete(pd.nodePatterns, nodeID)
}
