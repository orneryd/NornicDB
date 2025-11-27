package inference

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/temporal"
)

func TestDefaultKalmanAdapterConfig(t *testing.T) {
	cfg := DefaultKalmanAdapterConfig()

	if !cfg.EnableConfidenceSmoothing {
		t.Error("Expected EnableConfidenceSmoothing to be true")
	}
	if !cfg.EnableSessionTracking {
		t.Error("Expected EnableSessionTracking to be true")
	}
	if !cfg.EnableStrengthTracking {
		t.Error("Expected EnableStrengthTracking to be true")
	}
	if cfg.SessionCoAccessWeight != 0.4 {
		t.Errorf("Expected SessionCoAccessWeight 0.4, got %f", cfg.SessionCoAccessWeight)
	}
	if cfg.CrossSessionBoost != 1.3 {
		t.Errorf("Expected CrossSessionBoost 1.3, got %f", cfg.CrossSessionBoost)
	}
}

func TestNewKalmanAdapter(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.engine != engine {
		t.Error("Engine not set correctly")
	}
	if adapter.edgeFilters == nil {
		t.Error("edgeFilters should be initialized")
	}
	if adapter.cachedConfidence == nil {
		t.Error("cachedConfidence should be initialized")
	}
	if adapter.sessionCoAccess == nil {
		t.Error("sessionCoAccess should be initialized")
	}
}

func TestKalmanAdapter_OnAccess(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	ctx := context.Background()

	// Access some nodes
	suggestions1, err := adapter.OnAccess(ctx, "node-1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	suggestions2, err := adapter.OnAccess(ctx, "node-2")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Suggestions may be empty without similarity search configured
	// Just verify no error
	_ = suggestions1
	_ = suggestions2

	// Check stats
	stats := adapter.GetStats()
	if stats.TotalSuggestions < 0 {
		t.Error("TotalSuggestions should be non-negative")
	}
}

func TestKalmanAdapter_OnStore(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	ctx := context.Background()

	// Create embedding
	embedding := make([]float32, 1024)
	for i := range embedding {
		embedding[i] = float32(i) / 1024.0
	}

	suggestions, err := adapter.OnStore(ctx, "new-node", embedding)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Suggestions may be empty without similarity search configured
	_ = suggestions
}

func TestKalmanAdapter_WithSessionDetector(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create session detector
	sessionCfg := temporal.DefaultSessionDetectorConfig()
	session := temporal.NewSessionDetector(sessionCfg)
	adapter.SetSessionDetector(session)

	ctx := context.Background()

	// Record accesses via session detector first
	now := time.Now()
	session.RecordAccess("node-1", now)
	session.RecordAccess("node-2", now.Add(1*time.Second))
	session.RecordAccess("node-1", now.Add(2*time.Second)) // Re-access node-1

	// Now access via adapter
	suggestions, err := adapter.OnAccess(ctx, "node-1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Should have session-based suggestions since nodes are in same session
	_ = suggestions
}

func TestKalmanAdapter_WithTracker(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create tracker
	tracker := temporal.NewTracker(temporal.DefaultConfig())
	adapter.SetTracker(tracker)

	ctx := context.Background()

	// Access should also record in tracker
	_, _ = adapter.OnAccess(ctx, "tracked-node")

	// Verify tracker has the access
	stats := tracker.GetStats("tracked-node")
	if stats == nil {
		t.Error("Expected tracker to have stats for node")
	}
}

func TestKalmanAdapter_ConfidenceSmoothing(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create a suggestion and enhance it
	sug := EdgeSuggestion{
		SourceID:   "source-1",
		TargetID:   "target-1",
		Type:       "RELATES_TO",
		Confidence: 0.75,
		Reason:     "test",
		Method:     "similarity",
	}

	enhanced := adapter.enhanceSuggestion(sug)

	// Check that confidence was smoothed
	if enhanced.Confidence < 0 || enhanced.Confidence > 1 {
		t.Errorf("Smoothed confidence out of range: %f", enhanced.Confidence)
	}

	// Check cached
	strength := adapter.GetRelationshipStrength("source-1", "target-1")
	if strength == nil {
		t.Error("Expected cached strength")
	}
}

func TestKalmanAdapter_MakeEdgeKey(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Test that keys are consistent regardless of order
	key1 := adapter.makeEdgeKey("A", "B")
	key2 := adapter.makeEdgeKey("B", "A")

	if key1 != key2 {
		t.Error("Edge keys should be consistent regardless of order")
	}

	// Verify sorting
	key := adapter.makeEdgeKey("Z", "A")
	if key.Source != "A" || key.Target != "Z" {
		t.Error("Edge key should have smaller source first")
	}
}

func TestKalmanAdapter_GetStrengtheningRelationships(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create and enhance suggestions with increasing confidence
	for i := 0; i < 5; i++ {
		sug := EdgeSuggestion{
			SourceID:   "rising-src",
			TargetID:   "rising-tgt",
			Confidence: 0.3 + float64(i)*0.1, // Increasing
			Reason:     "test",
		}
		adapter.enhanceSuggestion(sug)
	}

	// Get strengthening relationships
	strengthening := adapter.GetStrengtheningRelationships(0.01)

	// The rising relationship may or may not be detected depending on filter convergence
	_ = strengthening
}

func TestKalmanAdapter_GetWeakeningRelationships(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create and enhance suggestions with decreasing confidence
	for i := 0; i < 5; i++ {
		sug := EdgeSuggestion{
			SourceID:   "falling-src",
			TargetID:   "falling-tgt",
			Confidence: 0.8 - float64(i)*0.1, // Decreasing
			Reason:     "test",
		}
		adapter.enhanceSuggestion(sug)
	}

	// Get weakening relationships
	weakening := adapter.GetWeakeningRelationships(-0.01)

	// May or may not detect depending on filter
	_ = weakening
}

func TestKalmanAdapter_PredictFutureRelationships(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create suggestions trending upward
	for i := 0; i < 10; i++ {
		sug := EdgeSuggestion{
			SourceID:   "predict-src",
			TargetID:   "predict-tgt",
			Confidence: 0.2 + float64(i)*0.05,
			Reason:     "test",
		}
		adapter.enhanceSuggestion(sug)
	}

	// Predict future relationships
	predictions := adapter.PredictFutureRelationships(0.7)

	// May or may not have predictions
	_ = predictions
}

func TestKalmanAdapter_MergeSuggestions(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	base := []EdgeSuggestion{
		{SourceID: "A", TargetID: "B", Confidence: 0.6, Reason: "similarity"},
		{SourceID: "C", TargetID: "D", Confidence: 0.5, Reason: "similarity"},
	}

	session := []EdgeSuggestion{
		{SourceID: "A", TargetID: "B", Confidence: 0.8, Reason: "session"},
		{SourceID: "E", TargetID: "F", Confidence: 0.4, Reason: "session"},
	}

	merged := adapter.mergeSuggestions(base, session)

	// Should have 3 unique edges
	if len(merged) != 3 {
		t.Errorf("Expected 3 merged suggestions, got %d", len(merged))
	}

	// A-B should have combined confidence
	for _, sug := range merged {
		if sug.SourceID == "A" && sug.TargetID == "B" {
			// Should be weighted average
			expected := 0.6*(1-0.4) + 0.8*0.4
			if sug.Confidence < expected-0.01 || sug.Confidence > expected+0.01 {
				t.Errorf("Expected combined confidence ~%.2f, got %.2f", expected, sug.Confidence)
			}
		}
	}
}

func TestKalmanAdapter_GetStats(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	stats := adapter.GetStats()

	if stats.TotalSuggestions != 0 {
		t.Errorf("Initial TotalSuggestions should be 0")
	}
	if stats.KalmanSmoothed != 0 {
		t.Errorf("Initial KalmanSmoothed should be 0")
	}
}

func TestKalmanAdapter_GetEngine(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	if adapter.GetEngine() != engine {
		t.Error("GetEngine should return the underlying engine")
	}
}

func TestKalmanAdapter_Reset(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Create some state
	sug := EdgeSuggestion{
		SourceID:   "reset-src",
		TargetID:   "reset-tgt",
		Confidence: 0.5,
		Reason:     "test",
	}
	adapter.enhanceSuggestion(sug)

	if len(adapter.edgeFilters) == 0 {
		t.Error("Expected filters to be created")
	}

	// Reset
	adapter.Reset()

	if len(adapter.edgeFilters) != 0 {
		t.Error("Filters should be cleared")
	}
	if len(adapter.cachedConfidence) != 0 {
		t.Error("Cached confidence should be cleared")
	}
	if len(adapter.sessionCoAccess) != 0 {
		t.Error("Session co-access should be cleared")
	}
}

func TestKalmanAdapter_DisabledSmoothing(t *testing.T) {
	// Disable Kalman filtering
	cleanup := config.WithKalmanDisabled()
	defer cleanup()

	engine := New(DefaultConfig())
	cfg := DefaultKalmanAdapterConfig()
	cfg.EnableConfidenceSmoothing = false
	adapter := NewKalmanAdapter(engine, cfg)

	sug := EdgeSuggestion{
		SourceID:   "no-smooth-src",
		TargetID:   "no-smooth-tgt",
		Confidence: 0.75,
		Reason:     "test",
	}

	enhanced := adapter.enhanceSuggestion(sug)

	// Confidence should remain unchanged
	if enhanced.Confidence != 0.75 {
		t.Errorf("Expected unchanged confidence 0.75, got %f", enhanced.Confidence)
	}
}

func TestKalmanAdapter_SessionConfidence(t *testing.T) {
	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	// Test nil data
	conf := adapter.calculateSessionConfidence(nil)
	if conf != 0 {
		t.Errorf("Expected 0 for nil data, got %f", conf)
	}

	// Test with data
	data := &crossSessionData{
		SessionIDs:    []string{"s1", "s2", "s3"},
		TotalCoAccess: 5,
		FirstSeen:     time.Now().Add(-24 * time.Hour),
		LastSeen:      time.Now(),
	}

	conf = adapter.calculateSessionConfidence(data)
	if conf <= 0 {
		t.Error("Expected positive confidence")
	}
	if conf > 1 {
		t.Errorf("Confidence should not exceed 1, got %f", conf)
	}
}

// Benchmark inference adapter
func BenchmarkKalmanAdapter_EnhanceSuggestion(b *testing.B) {
	config.EnableKalmanFiltering()
	defer config.DisableKalmanFiltering()

	engine := New(DefaultConfig())
	adapter := NewKalmanAdapter(engine, DefaultKalmanAdapterConfig())

	sug := EdgeSuggestion{
		SourceID:   "bench-src",
		TargetID:   "bench-tgt",
		Confidence: 0.75,
		Reason:     "benchmark",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.enhanceSuggestion(sug)
	}
}
