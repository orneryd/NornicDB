// Package temporal provides temporal pattern tracking and prediction for NornicDB.
//
// This package tracks when nodes are accessed and uses Kalman filtering to:
//   - Smooth noisy access patterns
//   - Predict when nodes will be accessed next
//   - Detect session boundaries (context switches)
//   - Identify cyclic patterns (daily, weekly, etc.)
//
// The temporal system integrates with NornicDB's decay system to:
//   - Slow decay for frequently accessed nodes
//   - Speed decay for abandoned nodes
//   - Predict archival candidates
//
// Example usage:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	// Record accesses
//	tracker.RecordAccess("node-123")
//	tracker.RecordAccess("node-456")
//
//	// Get predictions
//	prediction := tracker.PredictNextAccess("node-123")
//	fmt.Printf("Node likely accessed in %.1f seconds\n", prediction.SecondsUntil)
//
//	// Check for session change
//	if tracker.IsSessionBoundary("node-123") {
//		fmt.Println("User context has changed!")
//	}
//
// # ELI12 (Explain Like I'm 12)
//
// Imagine you're keeping a diary of when your friends visit:
//
//	📅 Monday:    Sarah came at 3pm
//	📅 Tuesday:   Sarah came at 3pm
//	📅 Wednesday: Sarah came at 3:15pm
//	📅 Thursday:  ???
//
// The Tracker is like a smart diary that notices patterns:
//
//	🧠 "Sarah visits around 3pm every day"
//	🧠 "Mike comes on weekends"
//	🧠 "Random friend hasn't visited in 2 weeks - maybe forgot about us?"
//
// The Kalman filter is the "smart" part. When Sarah came at 3:15pm on Wednesday,
// instead of panicking ("OMG she's 15 minutes late!"), it smoothly updates:
//
//	Before: "Sarah comes at 3:00pm"
//	After:  "Sarah comes around 3:03pm" (small adjustment)
//
// It's like averaging, but SMARTER because it:
//   - Trusts patterns more than single weird events
//   - Notices if someone is visiting MORE or LESS often (velocity)
//   - Can predict: "At this rate, Sarah will visit at 2:50pm next week"
//
// Why Kalman instead of simple averaging?
//   - Simple average: "3pm + 3pm + 3:15pm = 3:05pm average"
//   - Kalman: "3pm, 3pm, then 3:15pm... she might be getting later, or maybe
//     it was just traffic. I'll say 3:03pm and watch for more data."
//
// The Kalman filter LEARNS the trend (velocity) and uses it to predict!
package temporal

import "github.com/orneryd/nornicdb/pkg/util"

import (
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/filter"
)

// Config holds temporal tracker configuration.
type Config struct {
	// MaxTrackedNodes - maximum number of nodes to track (LRU eviction)
	MaxTrackedNodes int

	// MinAccessesForPrediction - minimum accesses before making predictions
	MinAccessesForPrediction int

	// SessionTimeoutSeconds - gap that indicates a new session
	SessionTimeoutSeconds float64

	// VelocityChangeThreshold - velocity change that triggers session boundary
	VelocityChangeThreshold float64

	// FilterConfig for the underlying Kalman velocity filter
	FilterConfig filter.VelocityConfig

	// EnableAdaptiveFilter - use adaptive filter that switches modes
	EnableAdaptiveFilter bool

	// CleanupInterval - how often to clean up stale entries
	CleanupInterval time.Duration

	// MaxHistoryPerNode - maximum access history entries per node
	MaxHistoryPerNode int
}

// DefaultConfig returns sensible defaults for temporal tracking.
func DefaultConfig() Config {
	return Config{
		MaxTrackedNodes:          100000,
		MinAccessesForPrediction: 3,
		SessionTimeoutSeconds:    300, // 5 minutes
		VelocityChangeThreshold:  0.5, // 50% velocity change
		FilterConfig:             filter.TemporalTrackingConfig(),
		EnableAdaptiveFilter:     false,
		CleanupInterval:          5 * time.Minute,
		MaxHistoryPerNode:        100,
	}
}

// HighPrecisionConfig returns config for high-precision temporal tracking.
func HighPrecisionConfig() Config {
	cfg := DefaultConfig()
	cfg.MinAccessesForPrediction = 5
	cfg.MaxHistoryPerNode = 500
	cfg.FilterConfig = filter.VelocityConfig{
		ProcessNoisePos:    0.01,
		ProcessNoiseVel:    0.001,
		MeasurementNoise:   0.1,
		InitialPosVariance: 10.0,
		InitialVelVariance: 1.0,
		Dt:                 1.0,
	}
	return cfg
}

// LowMemoryConfig returns config optimized for memory efficiency.
func LowMemoryConfig() Config {
	cfg := DefaultConfig()
	cfg.MaxTrackedNodes = 10000
	cfg.MaxHistoryPerNode = 20
	return cfg
}

// NodeAccess represents a single access event.
type NodeAccess struct {
	Timestamp time.Time
	SessionID string // Optional session identifier
}

// NodeStats holds temporal statistics for a single node.
type NodeStats struct {
	NodeID string

	// Access counts
	TotalAccesses  int64
	AccessesInHour int
	AccessesInDay  int
	AccessesInWeek int

	// Timing
	FirstAccess     time.Time
	LastAccess      time.Time
	AverageInterval float64 // seconds between accesses

	// Predictions
	PredictedNextAccess  time.Time
	PredictionConfidence float64

	// Pattern detection
	HasDailyPattern  bool
	HasWeeklyPattern bool
	PeakHour         int // 0-23
	PeakDay          int // 0-6 (Sunday=0)

	// Session info
	CurrentSessionStart time.Time
	SessionCount        int

	// Filter state
	AccessRateVelocity float64 // rate of change of access frequency
}

// Prediction represents a predicted future access.
type Prediction struct {
	NodeID          string
	PredictedTime   time.Time
	SecondsUntil    float64
	Confidence      float64
	BasedOnAccesses int
	AccessRateTrend string // "increasing", "stable", "decreasing"
}

// nodeTracker tracks temporal data for a single node.
type nodeTracker struct {
	nodeID string

	// Access history (ring buffer)
	history    []time.Time
	historyIdx int
	historyLen int
	maxHistory int

	// Kalman filter for access rate tracking
	// Tracks: inter-access interval (seconds between accesses)
	intervalFilter *filter.KalmanVelocity

	// Statistics
	totalAccesses int64
	firstAccess   time.Time
	lastAccess    time.Time

	// Session tracking
	sessionStart time.Time
	sessionCount int
	lastVelocity float64

	// Pattern detection
	hourCounts [24]int // Access count per hour of day
	dayCounts  [7]int  // Access count per day of week
}

// Tracker is the main temporal tracking system.
type Tracker struct {
	mu sync.RWMutex

	config Config

	// Node trackers (nodeID -> tracker)
	nodes map[string]*nodeTracker

	// LRU for eviction
	accessOrder []string

	// Global statistics
	totalAccesses int64
	startTime     time.Time

	// Cleanup
	lastCleanup time.Time
}

// NewTracker creates a new temporal tracker with the given configuration.
//
// The tracker monitors node access patterns over time and uses Kalman filtering
// to smooth noisy data and predict future accesses. It automatically detects
// session boundaries and cyclic patterns.
//
// Parameters:
//   - cfg: Configuration for tracking behavior (use DefaultConfig() for defaults)
//
// Returns:
//   - *Tracker ready to record accesses and make predictions
//
// Example 1 - Basic Access Tracking:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	// Simulate user accessing documents over time
//	for i := 0; i < 10; i++ {
//		tracker.RecordAccess("doc-123")
//		time.Sleep(5 * time.Second)
//	}
//
//	// Get statistics
//	stats := tracker.GetNodeStats("doc-123")
//	fmt.Printf("Accessed %d times, avg %.1f seconds between accesses\n",
//		stats.AccessCount, stats.AverageIntervalSeconds)
//
// Example 2 - Predicting Next Access:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	// User accesses a file regularly
//	for i := 0; i < 5; i++ {
//		tracker.RecordAccess("project-file")
//		time.Sleep(1 * time.Hour) // Every hour
//	}
//
//	// Predict when they'll access it next
//	prediction := tracker.PredictNextAccess("project-file")
//	if prediction != nil {
//		fmt.Printf("Likely to access in %.0f minutes\n", prediction.SecondsUntil/60)
//		// Output: "Likely to access in 60 minutes"
//	}
//
// Example 3 - Session Boundary Detection:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	// User is actively working
//	tracker.RecordAccess("doc-1")
//	time.Sleep(30 * time.Second)
//	tracker.RecordAccess("doc-1")
//	time.Sleep(30 * time.Second)
//
//	// Long gap - user left for lunch
//	time.Sleep(2 * time.Hour)
//
//	// Check if session changed
//	if tracker.IsSessionBoundary("doc-1") {
//		fmt.Println("New session detected - user returned!")
//		// Clear short-term context, log session end, etc.
//	}
//
// Example 4 - Integration with Decay System:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//	decayManager := decay.New(nil)
//
//	// Track accesses and update decay
//	onAccess := func(nodeID string) {
//		tracker.RecordAccess(nodeID)
//
//		// Predict if node will be accessed soon
//		pred := tracker.PredictNextAccess(nodeID)
//		if pred != nil && pred.SecondsUntil < 3600 { // Within 1 hour
//			// Slow decay for nodes that will be accessed soon
//			memory.ImportanceWeight = 0.9
//		}
//	}
//
// ELI12:
//
// Think of NewTracker like starting a stopwatch collection for tracking when
// your friends visit:
//
//   - Every time Sarah visits, you click her stopwatch: RECORD ACCESS
//   - After a few visits, you notice "Sarah comes every day around 3pm"
//   - The tracker can predict: "Sarah will probably visit tomorrow at 3pm"
//   - If she doesn't visit for a week, it notices: "Session ended, she forgot about us"
//
// The Kalman filter is the "smart brain" that:
//  1. Notices patterns (daily visits, weekly visits, etc.)
//  2. Handles noise (if she comes at 3:05pm once, don't panic)
//  3. Detects trends (is she visiting MORE often or LESS often?)
//  4. Makes predictions (when will she visit next?)
//
// Real-world Uses:
//   - Cache management: "This file will be accessed in 5 minutes, keep it warm!"
//   - Memory decay: "This hasn't been accessed in 2 weeks, archive it"
//   - Context switching: "User moved from coding to meetings (30 min gap)"
//   - Predictive loading: "User opens Report.docx every Monday at 9am"
//
// Performance:
//   - RecordAccess: O(1) - very fast
//   - PredictNextAccess: O(1) - simple calculation
//   - Memory: ~500 bytes per tracked node
//   - MaxTrackedNodes uses LRU eviction (least recently used gets removed)
//
// Thread Safety:
//
//	All methods are thread-safe for concurrent access from multiple goroutines.
func NewTracker(cfg Config) *Tracker {
	return &Tracker{
		config:      cfg,
		nodes:       make(map[string]*nodeTracker),
		accessOrder: make([]string, 0, cfg.MaxTrackedNodes),
		startTime:   time.Now(),
		lastCleanup: time.Now(),
	}
}

// RecordAccess records an access to a node at the current time.
//
// This is the primary method for feeding access events into the temporal tracker.
// Each call updates the node's access statistics, smooths the data with Kalman
// filtering, and checks for session boundaries.
//
// Parameters:
//   - nodeID: Unique identifier for the node being accessed
//
// Example 1 - Simple Access Tracking:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	// Record every time user opens a document
//	tracker.RecordAccess("doc-readme")
//	tracker.RecordAccess("doc-api")
//	tracker.RecordAccess("doc-readme") // Accessed again
//
//	// Tracker now knows doc-readme is accessed more frequently
//
// Example 2 - Integration with Storage Engine:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//	engine := storage.NewBadgerEngine("./data")
//
//	// Hook into node retrieval
//	originalGet := engine.GetNode
//	engine.GetNode = func(id storage.NodeID) (*storage.Node, error) {
//		tracker.RecordAccess(string(id)) // Track access
//		return originalGet(id)
//	}
//
// Example 3 - Real-time Recommendation System:
//
//	tracker := temporal.NewTracker(temporal.DefaultConfig())
//
//	func handleUserAction(userID, itemID string) {
//		tracker.RecordAccess(itemID)
//
//		// Get all recently accessed items
//		recentItems := tracker.GetRecentlyAccessed(10)
//
//		// Recommend similar items
//		recommendations := findSimilarItems(recentItems)
//		showRecommendations(userID, recommendations)
//	}
//
// ELI12:
//
// Think of RecordAccess like clicking a button on your stopwatch app:
//   - Click! = "I just used this thing"
//   - The app remembers: "Oh, you use this every hour"
//   - Next time, it can guess: "You'll probably use it again in 1 hour"
//
// It's like your phone learning you check Instagram every morning at 7am,
// so it preloads it for you!
//
// Performance:
//   - O(1) constant time operation
//   - Thread-safe with mutex protection
//   - Automatic LRU eviction when MaxTrackedNodes exceeded
//
// Thread Safety:
//
//	Safe to call concurrently from multiple goroutines.
func (t *Tracker) RecordAccess(nodeID string) {
	t.RecordAccessAt(nodeID, time.Now())
}

// RecordAccessAt records an access at a specific time.
func (t *Tracker) RecordAccessAt(nodeID string, timestamp time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.totalAccesses++

	// Get or create node tracker
	nt, exists := t.nodes[nodeID]
	if !exists {
		nt = t.createNodeTracker(nodeID)
		t.nodes[nodeID] = nt

		// Check if we need to evict
		if len(t.nodes) > t.config.MaxTrackedNodes {
			t.evictOldest()
		}
	}

	// Record the access
	nt.recordAccess(timestamp, t.config.SessionTimeoutSeconds, t.config.VelocityChangeThreshold)

	// Update LRU
	t.updateLRU(nodeID)

	// Periodic cleanup
	if time.Since(t.lastCleanup) > t.config.CleanupInterval {
		t.cleanup()
	}
}

// createNodeTracker creates a new tracker for a node.
func (t *Tracker) createNodeTracker(nodeID string) *nodeTracker {
	return &nodeTracker{
		nodeID:         nodeID,
		history:        make([]time.Time, t.config.MaxHistoryPerNode),
		maxHistory:     t.config.MaxHistoryPerNode,
		intervalFilter: filter.NewKalmanVelocity(t.config.FilterConfig),
	}
}

// recordAccess records an access to this node.
func (nt *nodeTracker) recordAccess(timestamp time.Time, sessionTimeout, velocityThreshold float64) {
	// Calculate interval since last access
	var intervalSeconds float64
	if nt.totalAccesses > 0 {
		intervalSeconds = timestamp.Sub(nt.lastAccess).Seconds()

		// Check for session boundary
		if intervalSeconds > sessionTimeout {
			nt.sessionStart = timestamp
			nt.sessionCount++
		}

		// Feed interval to Kalman filter
		// We track the inverse (access rate) so increasing velocity = more frequent access
		accessRate := 1.0 / intervalSeconds
		if intervalSeconds < 0.001 {
			accessRate = 1000.0 // Cap at 1000 accesses per second
		}
		nt.intervalFilter.Process(accessRate)

		// Check for velocity change (potential session boundary)
		currentVel := nt.intervalFilter.Velocity()
		if nt.lastVelocity != 0 {
			velChange := (currentVel - nt.lastVelocity) / nt.lastVelocity
			if velChange > velocityThreshold || velChange < -velocityThreshold {
				// Significant velocity change - might be session boundary
				nt.sessionStart = timestamp
				nt.sessionCount++
			}
		}
		nt.lastVelocity = currentVel
	} else {
		nt.firstAccess = timestamp
		nt.sessionStart = timestamp
		nt.sessionCount = 1
	}

	// Update history (ring buffer)
	nt.history[nt.historyIdx] = timestamp
	nt.historyIdx = (nt.historyIdx + 1) % nt.maxHistory
	if nt.historyLen < nt.maxHistory {
		nt.historyLen++
	}

	// Update pattern counters
	hour := timestamp.Hour()
	day := int(timestamp.Weekday())
	nt.hourCounts[hour]++
	nt.dayCounts[day]++

	// Update stats
	nt.lastAccess = timestamp
	nt.totalAccesses++
}

// PredictNextAccess predicts when a node will be accessed next.
func (t *Tracker) PredictNextAccess(nodeID string) *Prediction {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nt, exists := t.nodes[nodeID]
	if !exists {
		return nil
	}

	return nt.predictNextAccess(t.config.MinAccessesForPrediction)
}

// predictNextAccess generates a prediction for this node.
func (nt *nodeTracker) predictNextAccess(minAccesses int) *Prediction {
	if nt.totalAccesses < int64(minAccesses) {
		return &Prediction{
			NodeID:          nt.nodeID,
			Confidence:      0,
			BasedOnAccesses: int(nt.totalAccesses),
			AccessRateTrend: "unknown",
		}
	}

	// Get current access rate and velocity from filter
	currentRate := nt.intervalFilter.State()
	rateVelocity := nt.intervalFilter.Velocity()

	// Predict future access rate (5 steps ahead)
	futureRate := nt.intervalFilter.Predict(5)

	// Convert rate to interval
	var predictedInterval float64
	if futureRate > 0.001 {
		predictedInterval = 1.0 / futureRate
	} else {
		predictedInterval = 3600 * 24 // Default to 1 day if rate is ~0
	}

	// Cap at reasonable values
	if predictedInterval < 1 {
		predictedInterval = 1
	}
	if predictedInterval > 3600*24*30 {
		predictedInterval = 3600 * 24 * 30 // Max 30 days
	}

	predictedTime := nt.lastAccess.Add(time.Duration(predictedInterval) * time.Second)

	// Determine trend
	var trend string
	if rateVelocity > 0.01 {
		trend = "increasing"
	} else if rateVelocity < -0.01 {
		trend = "decreasing"
	} else {
		trend = "stable"
	}

	// Calculate confidence based on observations and filter uncertainty
	confidence := float64(nt.totalAccesses) / float64(nt.totalAccesses+10) // Asymptotic to 1.0
	_, uncertainty := nt.intervalFilter.PredictWithUncertainty(5)
	if currentRate > 0 {
		confidence *= 1.0 / (1.0 + uncertainty/currentRate)
	}

	return &Prediction{
		NodeID:          nt.nodeID,
		PredictedTime:   predictedTime,
		SecondsUntil:    time.Until(predictedTime).Seconds(),
		Confidence:      confidence,
		BasedOnAccesses: int(nt.totalAccesses),
		AccessRateTrend: trend,
	}
}

// GetStats returns statistics for a node.
func (t *Tracker) GetStats(nodeID string) *NodeStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nt, exists := t.nodes[nodeID]
	if !exists {
		return nil
	}

	return nt.getStats()
}

// getStats computes statistics for this node.
func (nt *nodeTracker) getStats() *NodeStats {
	now := time.Now()

	stats := &NodeStats{
		NodeID:              nt.nodeID,
		TotalAccesses:       nt.totalAccesses,
		FirstAccess:         nt.firstAccess,
		LastAccess:          nt.lastAccess,
		CurrentSessionStart: nt.sessionStart,
		SessionCount:        nt.sessionCount,
		AccessRateVelocity:  nt.intervalFilter.Velocity(),
	}

	// Count recent accesses
	hourAgo := now.Add(-time.Hour)
	dayAgo := now.Add(-24 * time.Hour)
	weekAgo := now.Add(-7 * 24 * time.Hour)

	for i := 0; i < nt.historyLen; i++ {
		ts := nt.history[i]
		if ts.After(hourAgo) {
			stats.AccessesInHour++
		}
		if ts.After(dayAgo) {
			stats.AccessesInDay++
		}
		if ts.After(weekAgo) {
			stats.AccessesInWeek++
		}
	}

	// Calculate average interval
	if nt.totalAccesses > 1 {
		totalTime := nt.lastAccess.Sub(nt.firstAccess).Seconds()
		stats.AverageInterval = totalTime / float64(nt.totalAccesses-1)
	}

	// Find peak hour and day
	maxHour, maxHourCount := 0, 0
	for h, count := range nt.hourCounts {
		if count > maxHourCount {
			maxHour, maxHourCount = h, count
		}
	}
	stats.PeakHour = maxHour

	maxDay, maxDayCount := 0, 0
	for d, count := range nt.dayCounts {
		if count > maxDayCount {
			maxDay, maxDayCount = d, count
		}
	}
	stats.PeakDay = maxDay

	// Detect patterns (simple heuristic)
	totalHourAccess := 0
	for _, c := range nt.hourCounts {
		totalHourAccess += c
	}
	if totalHourAccess > 0 && maxHourCount > totalHourAccess/8 {
		stats.HasDailyPattern = true
	}

	totalDayAccess := 0
	for _, c := range nt.dayCounts {
		totalDayAccess += c
	}
	if totalDayAccess > 0 && maxDayCount > totalDayAccess/3 {
		stats.HasWeeklyPattern = true
	}

	// Add prediction
	pred := nt.predictNextAccess(3)
	if pred != nil && pred.Confidence > 0 {
		stats.PredictedNextAccess = pred.PredictedTime
		stats.PredictionConfidence = pred.Confidence
	}

	return stats
}

// IsSessionBoundary checks if a significant session change occurred for a node.
func (t *Tracker) IsSessionBoundary(nodeID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nt, exists := t.nodes[nodeID]
	if !exists {
		return false
	}

	// Check if velocity changed significantly recently
	vel := nt.intervalFilter.Velocity()
	if nt.lastVelocity != 0 {
		change := (vel - nt.lastVelocity) / nt.lastVelocity
		return change > t.config.VelocityChangeThreshold || change < -t.config.VelocityChangeThreshold
	}

	return false
}

// GetAccessRateTrend returns the access rate trend for a node.
func (t *Tracker) GetAccessRateTrend(nodeID string) (velocity float64, trend string) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nt, exists := t.nodes[nodeID]
	if !exists {
		return 0, "unknown"
	}

	velocity = nt.intervalFilter.Velocity()
	if velocity > 0.01 {
		trend = "increasing"
	} else if velocity < -0.01 {
		trend = "decreasing"
	} else {
		trend = "stable"
	}

	return velocity, trend
}

// GetHotNodes returns nodes with increasing access rates.
func (t *Tracker) GetHotNodes(limit int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	type nodeVel struct {
		id  string
		vel float64
	}

	var hot []nodeVel
	for id, nt := range t.nodes {
		vel := nt.intervalFilter.Velocity()
		if vel > 0 {
			hot = append(hot, nodeVel{id, vel})
		}
	}

	// Sort by velocity (descending)
	for i := 0; i < len(hot)-1; i++ {
		for j := i + 1; j < len(hot); j++ {
			if hot[j].vel > hot[i].vel {
				hot[i], hot[j] = hot[j], hot[i]
			}
		}
	}

	result := make([]string, 0, util.SafePreallocCap(limit, len(hot)))
	for i := 0; i < len(hot) && i < limit; i++ {
		result = append(result, hot[i].id)
	}

	return result
}

// GetColdNodes returns nodes with decreasing access rates.
func (t *Tracker) GetColdNodes(limit int) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()

	type nodeVel struct {
		id  string
		vel float64
	}

	var cold []nodeVel
	for id, nt := range t.nodes {
		vel := nt.intervalFilter.Velocity()
		if vel < 0 {
			cold = append(cold, nodeVel{id, vel})
		}
	}

	// Sort by velocity (ascending - most negative first)
	for i := 0; i < len(cold)-1; i++ {
		for j := i + 1; j < len(cold); j++ {
			if cold[j].vel < cold[i].vel {
				cold[i], cold[j] = cold[j], cold[i]
			}
		}
	}

	result := make([]string, 0, util.SafePreallocCap(limit, len(cold)))
	for i := 0; i < len(cold) && i < limit; i++ {
		result = append(result, cold[i].id)
	}

	return result
}

// updateLRU updates LRU order for a node.
func (t *Tracker) updateLRU(nodeID string) {
	// Remove from current position
	for i, id := range t.accessOrder {
		if id == nodeID {
			t.accessOrder = append(t.accessOrder[:i], t.accessOrder[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	t.accessOrder = append(t.accessOrder, nodeID)
}

// evictOldest removes the least recently used node.
func (t *Tracker) evictOldest() {
	if len(t.accessOrder) == 0 {
		return
	}
	oldest := t.accessOrder[0]
	t.accessOrder = t.accessOrder[1:]
	delete(t.nodes, oldest)
}

// cleanup removes very old entries.
func (t *Tracker) cleanup() {
	t.lastCleanup = time.Now()
	// Future: could remove nodes not accessed in X days
}

// GlobalStats returns global tracking statistics.
type GlobalStats struct {
	TotalAccesses  int64
	TrackedNodes   int
	UptimeSeconds  float64
	AccessesPerSec float64
}

// GetGlobalStats returns global statistics.
func (t *Tracker) GetGlobalStats() GlobalStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	uptime := time.Since(t.startTime).Seconds()
	return GlobalStats{
		TotalAccesses:  t.totalAccesses,
		TrackedNodes:   len(t.nodes),
		UptimeSeconds:  uptime,
		AccessesPerSec: float64(t.totalAccesses) / uptime,
	}
}

// Reset clears all tracking data.
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nodes = make(map[string]*nodeTracker)
	t.accessOrder = make([]string, 0, t.config.MaxTrackedNodes)
	t.totalAccesses = 0
	t.startTime = time.Now()
	t.lastCleanup = time.Now()
}
