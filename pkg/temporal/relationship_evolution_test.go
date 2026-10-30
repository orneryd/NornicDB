package temporal

import (
	"testing"
	"time"
)

func TestRelationshipConfig_Default(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	if cfg.MaxTrackedRelationships <= 0 {
		t.Error("MaxTrackedRelationships should be positive")
	}
}

func TestRelationshipEvolution_New(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())
	if re == nil {
		t.Fatal("NewRelationshipEvolution returned nil")
	}
}

func TestRelationshipEvolution_RecordCoAccess(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	re.RecordCoAccess("node-1", "node-2", 1.0)
	re.RecordCoAccess("node-2", "node-3", 1.0)

	stats := re.GetStats()
	if stats.TrackedRelationships != 2 {
		t.Errorf("TrackedRelationships = %v, want 2", stats.TrackedRelationships)
	}
}

func TestRelationshipEvolution_GetTrend(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	// Record with increasing weights
	for i := 0; i < 10; i++ {
		re.RecordCoAccess("node-1", "node-2", float64(i+1)*0.1)
	}

	trend := re.GetTrend("node-1", "node-2")
	if trend == nil {
		t.Fatal("Should have trend")
	}

	t.Logf("Trend: direction=%s, velocity=%.4f, strength=%.3f",
		trend.Direction, trend.Velocity, trend.CurrentStrength)
}

func TestRelationshipEvolution_PredictStrength(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	for i := 0; i < 10; i++ {
		re.RecordCoAccess("node-1", "node-2", float64(i+1)*0.1)
	}

	predicted := re.PredictStrength("node-1", "node-2", 5)
	current := re.GetTrend("node-1", "node-2").CurrentStrength

	t.Logf("Current: %.3f, Predicted (5 steps): %.3f", current, predicted)
}

func TestRelationshipEvolution_GetStrengtheningRelationships(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	cfg.MinObservationsForTrend = 3
	re := NewRelationshipEvolution(cfg)

	// Create strengthening relationship
	for i := 0; i < 10; i++ {
		re.RecordCoAccess("strong-a", "strong-b", float64(i+1)*0.1)
	}

	strengthening := re.GetStrengtheningRelationships(10)
	t.Logf("Strengthening relationships: %d", len(strengthening))
}

func TestRelationshipEvolution_GetWeakeningRelationships(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	cfg.MinObservationsForTrend = 3
	re := NewRelationshipEvolution(cfg)

	// Create weakening relationship (decreasing weights)
	for i := 10; i > 0; i-- {
		re.RecordCoAccess("weak-a", "weak-b", float64(i)*0.1)
	}

	weakening := re.GetWeakeningRelationships(10)
	t.Logf("Weakening relationships: %d", len(weakening))
}

func TestRelationshipEvolution_ShouldPrune(t *testing.T) {
	cfg := DefaultRelationshipConfig()
	cfg.MinObservationsForTrend = 3
	re := NewRelationshipEvolution(cfg)

	// Create weakening relationship with low weight
	for i := 5; i > 0; i-- {
		re.RecordCoAccess("prune-a", "prune-b", float64(i)*0.01)
	}

	shouldPrune := re.ShouldPrune("prune-a", "prune-b", 0.5)
	t.Logf("Should prune: %v", shouldPrune)
}

func TestRelationshipEvolution_DecayIdleRelationships(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	// Record and then let it sit
	weekAgo := time.Now().Add(-7 * 24 * time.Hour)
	re.RecordCoAccessAt("idle-a", "idle-b", 1.0, weekAgo)

	decayed := re.DecayIdleRelationships(24) // Decay after 24 hours idle
	t.Logf("Decayed relationships: %d", decayed)
}

func TestRelationshipEvolution_Reset(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	re.RecordCoAccess("node-1", "node-2", 1.0)
	re.Reset()

	stats := re.GetStats()
	if stats.TrackedRelationships != 0 {
		t.Errorf("TrackedRelationships = %v, want 0", stats.TrackedRelationships)
	}
}

func TestRelationshipEvolution_EdgeKeyConsistency(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	// Record from both directions - should be same edge
	re.RecordCoAccess("node-a", "node-b", 1.0)
	re.RecordCoAccess("node-b", "node-a", 1.0)

	stats := re.GetStats()
	if stats.TrackedRelationships != 1 {
		t.Errorf("TrackedRelationships = %v, want 1 (same edge)", stats.TrackedRelationships)
	}
}

func TestRelationshipEvolution_EvictOldest(t *testing.T) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	re.edges["a->b"] = &edgeTracker{}
	re.edges["b->c"] = &edgeTracker{}
	re.accessOrder = []string{"a->b", "b->c"}

	re.evictOldest()
	if _, exists := re.edges["a->b"]; exists {
		t.Fatal("expected oldest edge to be evicted")
	}
	if len(re.accessOrder) != 1 || re.accessOrder[0] != "b->c" {
		t.Fatalf("unexpected access order after first eviction: %v", re.accessOrder)
	}

	re.evictOldest()
	if len(re.edges) != 0 {
		t.Fatalf("expected all edges evicted, got %d", len(re.edges))
	}

	// Empty path should be a no-op.
	re.evictOldest()
	if len(re.edges) != 0 || len(re.accessOrder) != 0 {
		t.Fatalf("expected empty state unchanged, edges=%d order=%d", len(re.edges), len(re.accessOrder))
	}
}

func BenchmarkRelationshipEvolution_RecordCoAccess(b *testing.B) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		re.RecordCoAccess("node-1", "node-2", 1.0)
	}
}

func BenchmarkRelationshipEvolution_GetTrend(b *testing.B) {
	re := NewRelationshipEvolution(DefaultRelationshipConfig())

	for i := 0; i < 100; i++ {
		re.RecordCoAccess("node-1", "node-2", float64(i)*0.1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		re.GetTrend("node-1", "node-2")
	}
}
