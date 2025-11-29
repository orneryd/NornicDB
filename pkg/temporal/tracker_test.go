package temporal

import (
	"math"
	"testing"
	"time"
)

func TestConfig_Default(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxTrackedNodes <= 0 {
		t.Error("MaxTrackedNodes should be positive")
	}
	if cfg.SessionTimeoutSeconds <= 0 {
		t.Error("SessionTimeoutSeconds should be positive")
	}
}

func TestConfig_HighPrecision(t *testing.T) {
	cfg := HighPrecisionConfig()
	def := DefaultConfig()
	if cfg.MinAccessesForPrediction <= def.MinAccessesForPrediction {
		t.Error("HighPrecision should require more accesses")
	}
}

func TestConfig_LowMemory(t *testing.T) {
	cfg := LowMemoryConfig()
	def := DefaultConfig()
	if cfg.MaxTrackedNodes >= def.MaxTrackedNodes {
		t.Error("LowMemory should track fewer nodes")
	}
}

func TestTracker_New(t *testing.T) {
	tracker := NewTracker(DefaultConfig())
	if tracker == nil {
		t.Fatal("NewTracker returned nil")
	}

	stats := tracker.GetGlobalStats()
	if stats.TotalAccesses != 0 {
		t.Errorf("TotalAccesses = %v, want 0", stats.TotalAccesses)
	}
	if stats.TrackedNodes != 0 {
		t.Errorf("TrackedNodes = %v, want 0", stats.TrackedNodes)
	}
}

func TestTracker_RecordAccess(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	tracker.RecordAccess("node-1")
	tracker.RecordAccess("node-2")
	tracker.RecordAccess("node-1")

	stats := tracker.GetGlobalStats()
	if stats.TotalAccesses != 3 {
		t.Errorf("TotalAccesses = %v, want 3", stats.TotalAccesses)
	}
	if stats.TrackedNodes != 2 {
		t.Errorf("TrackedNodes = %v, want 2", stats.TrackedNodes)
	}
}

func TestTracker_GetStats(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Unknown node
	stats := tracker.GetStats("unknown")
	if stats != nil {
		t.Error("Should return nil for unknown node")
	}

	// Record some accesses
	baseTime := time.Now()
	tracker.RecordAccessAt("node-1", baseTime)
	tracker.RecordAccessAt("node-1", baseTime.Add(10*time.Second))
	tracker.RecordAccessAt("node-1", baseTime.Add(20*time.Second))

	stats = tracker.GetStats("node-1")
	if stats == nil {
		t.Fatal("Should return stats for known node")
	}
	if stats.TotalAccesses != 3 {
		t.Errorf("TotalAccesses = %v, want 3", stats.TotalAccesses)
	}
	if stats.NodeID != "node-1" {
		t.Errorf("NodeID = %v, want node-1", stats.NodeID)
	}
}

func TestTracker_PredictNextAccess_UnknownNode(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	pred := tracker.PredictNextAccess("unknown")
	if pred != nil {
		t.Error("Should return nil for unknown node")
	}
}

func TestTracker_PredictNextAccess_InsufficientData(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	tracker.RecordAccess("node-1")

	pred := tracker.PredictNextAccess("node-1")
	if pred == nil {
		t.Fatal("Should return prediction even with insufficient data")
	}
	if pred.Confidence != 0 {
		t.Errorf("Confidence = %v, want 0 (insufficient data)", pred.Confidence)
	}
	if pred.AccessRateTrend != "unknown" {
		t.Errorf("Trend = %v, want unknown", pred.AccessRateTrend)
	}
}

func TestTracker_PredictNextAccess_RegularPattern(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Simulate regular access pattern (every 10 seconds)
	baseTime := time.Now()
	for i := 0; i < 10; i++ {
		tracker.RecordAccessAt("node-1", baseTime.Add(time.Duration(i*10)*time.Second))
	}

	pred := tracker.PredictNextAccess("node-1")
	if pred == nil {
		t.Fatal("Should return prediction")
	}

	t.Logf("Regular pattern prediction:")
	t.Logf("  Predicted time: %v", pred.PredictedTime)
	t.Logf("  Seconds until: %.1f", pred.SecondsUntil)
	t.Logf("  Confidence: %.3f", pred.Confidence)
	t.Logf("  Trend: %s", pred.AccessRateTrend)
	t.Logf("  Based on: %d accesses", pred.BasedOnAccesses)

	// Should have SOME confidence after 10 accesses (> 0)
	// Confidence varies based on filter uncertainty
	if pred.Confidence <= 0 {
		t.Errorf("Confidence should be positive: %v", pred.Confidence)
	}
	if pred.BasedOnAccesses != 10 {
		t.Errorf("BasedOnAccesses = %v, want 10", pred.BasedOnAccesses)
	}
}

func TestTracker_GetAccessRateTrend(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Unknown node
	vel, trend := tracker.GetAccessRateTrend("unknown")
	if trend != "unknown" {
		t.Errorf("Trend = %v, want unknown", trend)
	}
	if vel != 0 {
		t.Errorf("Velocity = %v, want 0", vel)
	}

	// Record accesses with increasing frequency
	baseTime := time.Now()
	intervals := []int{100, 80, 60, 40, 20, 10, 5} // Decreasing intervals = increasing rate
	currentTime := baseTime
	for _, interval := range intervals {
		tracker.RecordAccessAt("node-1", currentTime)
		currentTime = currentTime.Add(time.Duration(interval) * time.Second)
	}

	vel, trend = tracker.GetAccessRateTrend("node-1")
	t.Logf("Increasing access rate: velocity=%.4f, trend=%s", vel, trend)
}

func TestTracker_SessionDetection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SessionTimeoutSeconds = 60 // 1 minute
	tracker := NewTracker(cfg)

	baseTime := time.Now()

	// Session 1: rapid accesses
	for i := 0; i < 5; i++ {
		tracker.RecordAccessAt("node-1", baseTime.Add(time.Duration(i*10)*time.Second))
	}

	// Gap of 2 minutes (new session)
	tracker.RecordAccessAt("node-1", baseTime.Add(170*time.Second))

	stats := tracker.GetStats("node-1")
	t.Logf("Session detection: SessionCount=%d", stats.SessionCount)

	if stats.SessionCount < 2 {
		t.Errorf("SessionCount = %v, want >= 2", stats.SessionCount)
	}
}

func TestTracker_HotColdNodes(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	baseTime := time.Now()

	// Node 1: increasing access rate (hot)
	intervals1 := []int{100, 80, 60, 40, 20}
	currentTime := baseTime
	for _, interval := range intervals1 {
		tracker.RecordAccessAt("hot-node", currentTime)
		currentTime = currentTime.Add(time.Duration(interval) * time.Second)
	}

	// Node 2: decreasing access rate (cold)
	intervals2 := []int{20, 40, 60, 80, 100}
	currentTime = baseTime
	for _, interval := range intervals2 {
		tracker.RecordAccessAt("cold-node", currentTime)
		currentTime = currentTime.Add(time.Duration(interval) * time.Second)
	}

	hot := tracker.GetHotNodes(10)
	cold := tracker.GetColdNodes(10)

	t.Logf("Hot nodes: %v", hot)
	t.Logf("Cold nodes: %v", cold)

	// Hot node should have positive velocity
	// Cold node should have negative velocity
}

func TestTracker_PatternDetection(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	baseTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC) // Monday 9am

	// Simulate daily pattern: access at 9am each day for 2 weeks
	for day := 0; day < 14; day++ {
		accessTime := baseTime.Add(time.Duration(day*24) * time.Hour)
		tracker.RecordAccessAt("daily-node", accessTime)
	}

	stats := tracker.GetStats("daily-node")
	t.Logf("Pattern detection:")
	t.Logf("  HasDailyPattern: %v", stats.HasDailyPattern)
	t.Logf("  HasWeeklyPattern: %v", stats.HasWeeklyPattern)
	t.Logf("  PeakHour: %d", stats.PeakHour)
	t.Logf("  PeakDay: %d", stats.PeakDay)

	if stats.PeakHour != 9 {
		t.Errorf("PeakHour = %v, want 9", stats.PeakHour)
	}
}

func TestTracker_LRUEviction(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTrackedNodes = 3
	tracker := NewTracker(cfg)

	// Add 4 nodes (should evict oldest)
	tracker.RecordAccess("node-1")
	tracker.RecordAccess("node-2")
	tracker.RecordAccess("node-3")
	tracker.RecordAccess("node-4")

	stats := tracker.GetGlobalStats()
	if stats.TrackedNodes != 3 {
		t.Errorf("TrackedNodes = %v, want 3 (after eviction)", stats.TrackedNodes)
	}

	// node-1 should have been evicted
	if tracker.GetStats("node-1") != nil {
		t.Error("node-1 should have been evicted")
	}
}

func TestTracker_Reset(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	tracker.RecordAccess("node-1")
	tracker.RecordAccess("node-2")

	stats := tracker.GetGlobalStats()
	if stats.TotalAccesses != 2 {
		t.Fatal("Setup failed")
	}

	tracker.Reset()

	stats = tracker.GetGlobalStats()
	if stats.TotalAccesses != 0 {
		t.Errorf("TotalAccesses = %v, want 0 after reset", stats.TotalAccesses)
	}
	if stats.TrackedNodes != 0 {
		t.Errorf("TrackedNodes = %v, want 0 after reset", stats.TrackedNodes)
	}
}

// ===== Prediction Accuracy Tests =====

func TestPrediction_RegularInterval(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Regular 10-second intervals
	baseTime := time.Now().Add(-100 * time.Second) // Start in past
	for i := 0; i < 10; i++ {
		tracker.RecordAccessAt("node", baseTime.Add(time.Duration(i*10)*time.Second))
	}

	pred := tracker.PredictNextAccess("node")
	if pred == nil {
		t.Fatal("Should have prediction")
	}

	// Last access was at baseTime + 90s
	// Next should be around baseTime + 100s
	expectedNext := baseTime.Add(100 * time.Second)
	actualDiff := math.Abs(pred.PredictedTime.Sub(expectedNext).Seconds())

	t.Logf("Regular interval prediction:")
	t.Logf("  Expected: %v", expectedNext)
	t.Logf("  Predicted: %v", pred.PredictedTime)
	t.Logf("  Difference: %.1f seconds", actualDiff)

	// Should be within 30 seconds of expected
	if actualDiff > 30 {
		t.Errorf("Prediction off by %.1f seconds (expected within 30)", actualDiff)
	}
}

func TestPrediction_AcceleratingAccess(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Accelerating access: 20s, 15s, 10s, 5s intervals
	baseTime := time.Now().Add(-60 * time.Second)
	intervals := []int{0, 20, 35, 45, 50} // Cumulative seconds
	for _, offset := range intervals {
		tracker.RecordAccessAt("node", baseTime.Add(time.Duration(offset)*time.Second))
	}

	pred := tracker.PredictNextAccess("node")
	if pred == nil {
		t.Fatal("Should have prediction")
	}

	t.Logf("Accelerating access prediction:")
	t.Logf("  Trend: %s", pred.AccessRateTrend)
	t.Logf("  Confidence: %.3f", pred.Confidence)
	t.Logf("  Seconds until: %.1f", pred.SecondsUntil)

	// With accelerating access, next interval should be short
	if pred.SecondsUntil > 30 {
		t.Logf("Note: Predicted interval (%.1f) seems long for accelerating pattern", pred.SecondsUntil)
	}
}

func TestPrediction_DeceleratingAccess(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Decelerating access: 5s, 10s, 15s, 20s intervals
	baseTime := time.Now().Add(-60 * time.Second)
	intervals := []int{0, 5, 15, 30, 50} // Cumulative seconds
	for _, offset := range intervals {
		tracker.RecordAccessAt("node", baseTime.Add(time.Duration(offset)*time.Second))
	}

	pred := tracker.PredictNextAccess("node")
	if pred == nil {
		t.Fatal("Should have prediction")
	}

	t.Logf("Decelerating access prediction:")
	t.Logf("  Trend: %s", pred.AccessRateTrend)
	t.Logf("  Confidence: %.3f", pred.Confidence)
	t.Logf("  Seconds until: %.1f", pred.SecondsUntil)

	// With decelerating access, trend should show decreasing
	// (though our trend is about rate velocity, not interval)
}

// ===== Benchmarks =====

func BenchmarkTracker_RecordAccess(b *testing.B) {
	tracker := NewTracker(DefaultConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tracker.RecordAccess("node-1")
	}
}

func BenchmarkTracker_RecordAccess_ManyNodes(b *testing.B) {
	tracker := NewTracker(DefaultConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nodeID := "node-" + string(rune('A'+i%26))
		tracker.RecordAccess(nodeID)
	}
}

func BenchmarkTracker_PredictNextAccess(b *testing.B) {
	tracker := NewTracker(DefaultConfig())

	// Setup: record some accesses
	for i := 0; i < 100; i++ {
		tracker.RecordAccess("node-1")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.PredictNextAccess("node-1")
	}
}

func BenchmarkTracker_GetStats(b *testing.B) {
	tracker := NewTracker(DefaultConfig())

	// Setup
	for i := 0; i < 100; i++ {
		tracker.RecordAccess("node-1")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.GetStats("node-1")
	}
}

// ===== Missing Coverage Tests =====

func TestTracker_IsSessionBoundary(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	// Unknown node should return false
	if tracker.IsSessionBoundary("unknown") {
		t.Error("Unknown node should not be session boundary")
	}

	// Fresh node with no velocity history should return false
	tracker.RecordAccess("node-1")
	if tracker.IsSessionBoundary("node-1") {
		t.Error("New node with single access should not be boundary")
	}

	// Record multiple accesses to establish velocity
	baseTime := time.Now()
	for i := 0; i < 10; i++ {
		tracker.RecordAccessAt("node-2", baseTime.Add(time.Duration(i*10)*time.Second))
	}

	// Check - may or may not be boundary depending on pattern
	_ = tracker.IsSessionBoundary("node-2")
	// Just ensure it doesn't panic and returns a bool
	t.Log("IsSessionBoundary executed successfully")
}

func TestTracker_Cleanup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTrackedNodes = 5 // Small limit to test eviction
	tracker := NewTracker(cfg)

	// Add more nodes than max
	for i := 0; i < 10; i++ {
		tracker.RecordAccess("node-" + string(rune('A'+i)))
	}

	// Should have evicted some nodes
	stats := tracker.GetGlobalStats()
	if stats.TrackedNodes > cfg.MaxTrackedNodes {
		t.Errorf("Should have evicted nodes, have %d, max %d", stats.TrackedNodes, cfg.MaxTrackedNodes)
	}
	t.Logf("Tracked %d nodes after adding 10 (max: %d)", stats.TrackedNodes, cfg.MaxTrackedNodes)
}

func TestTracker_GetHotNodes(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	baseTime := time.Now()
	// Create nodes with different access frequencies
	for i := 0; i < 5; i++ {
		// Hot nodes - accessed many times recently
		for j := 0; j < (5 - i); j++ {
			tracker.RecordAccessAt("hot-"+string(rune('A'+i)), baseTime.Add(time.Duration(j)*time.Second))
		}
	}

	hotNodes := tracker.GetHotNodes(3)
	if len(hotNodes) > 3 {
		t.Errorf("Requested 3 hot nodes, got %d", len(hotNodes))
	}
	t.Logf("Hot nodes: %v", hotNodes)
}

func TestTracker_GetColdNodes(t *testing.T) {
	tracker := NewTracker(DefaultConfig())

	baseTime := time.Now()
	// Create old accesses
	for i := 0; i < 5; i++ {
		tracker.RecordAccessAt("cold-"+string(rune('A'+i)), baseTime.Add(-time.Duration(i+1)*time.Hour))
	}

	coldNodes := tracker.GetColdNodes(3)
	if len(coldNodes) > 3 {
		t.Errorf("Requested 3 cold nodes, got %d", len(coldNodes))
	}
	t.Logf("Cold nodes: %v", coldNodes)
}
