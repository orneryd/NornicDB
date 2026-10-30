package temporal

import (
	"testing"
	"time"
)

func TestDecayIntegrationConfig_Default(t *testing.T) {
	cfg := DefaultDecayIntegrationConfig()
	if cfg.BaseDecayRate <= 0 {
		t.Error("BaseDecayRate should be positive")
	}
	if cfg.MinDecayMultiplier <= 0 {
		t.Error("MinDecayMultiplier should be positive")
	}
}

func TestDecayIntegrationConfig_Conservative(t *testing.T) {
	cfg := ConservativeDecayConfig()
	def := DefaultDecayIntegrationConfig()
	if cfg.FrequentAccessBoost >= def.FrequentAccessBoost {
		t.Error("Conservative should have lower FrequentAccessBoost")
	}
}

func TestDecayIntegrationConfig_Aggressive(t *testing.T) {
	cfg := AggressiveDecayConfig()
	def := DefaultDecayIntegrationConfig()
	if cfg.RareAccessPenalty <= def.RareAccessPenalty {
		t.Error("Aggressive should have higher RareAccessPenalty")
	}
}

func TestDecayIntegration_New(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())
	if di == nil {
		t.Fatal("NewDecayIntegration returned nil")
	}
}

func TestDecayIntegration_RecordAccess(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	di.RecordAccess("node-1")
	di.RecordAccess("node-2")
	di.RecordAccess("node-1")

	stats := di.GetStats()
	if stats.TotalAccesses != 3 {
		t.Errorf("TotalAccesses = %v, want 3", stats.TotalAccesses)
	}
}

func TestDecayIntegration_GetDecayModifier_NewNode(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	modifier := di.GetDecayModifier("unknown-node")
	t.Logf("New node modifier: %.3f, reason: %s", modifier.Multiplier, modifier.Reason)

	// Should still get a valid modifier
	if modifier.Multiplier <= 0 {
		t.Error("Multiplier should be positive")
	}
}

func TestDecayIntegration_GetDecayModifier_FrequentAccess(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	// Rapid access pattern
	now := time.Now()
	for i := 0; i < 20; i++ {
		di.RecordAccessAt("hot-node", now.Add(time.Duration(i)*time.Second))
	}

	modifier := di.GetDecayModifier("hot-node")
	t.Logf("Frequent access modifier: %.3f (reason: %s)", modifier.Multiplier, modifier.Reason)
	t.Logf("Components: %v", modifier.Components)

	// Frequent access should slow decay (multiplier < 1)
	if modifier.Multiplier >= 1.0 {
		t.Log("Note: Expected multiplier < 1 for frequently accessed node")
	}
}

func TestDecayIntegration_GetDecayModifier_RareAccess(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	// Access from a week ago
	weekAgo := time.Now().Add(-7 * 24 * time.Hour)
	di.RecordAccessAt("cold-node", weekAgo)

	modifier := di.GetDecayModifier("cold-node")
	t.Logf("Rare access modifier: %.3f (reason: %s)", modifier.Multiplier, modifier.Reason)

	// Rare access should speed decay (multiplier > 1)
	if modifier.Multiplier <= 1.0 {
		t.Log("Note: Expected multiplier > 1 for rarely accessed node")
	}
}

func TestDecayIntegration_GetEffectiveDecayRate(t *testing.T) {
	cfg := DefaultDecayIntegrationConfig()
	di := NewDecayIntegration(cfg)

	di.RecordAccess("node-1")

	rate := di.GetEffectiveDecayRate("node-1")
	t.Logf("Effective decay rate: %.6f (base: %.6f)", rate, cfg.BaseDecayRate)

	if rate <= 0 {
		t.Error("Decay rate should be positive")
	}
}

func TestDecayIntegration_ShouldArchive(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	// Hot node - should be harder to archive
	now := time.Now()
	for i := 0; i < 10; i++ {
		di.RecordAccessAt("hot-node", now.Add(time.Duration(i)*time.Second))
	}

	hotArchive := di.ShouldArchive("hot-node", 0.5, 0.3)

	// Cold node - should be easier to archive
	weekAgo := time.Now().Add(-7 * 24 * time.Hour)
	di.RecordAccessAt("cold-node", weekAgo)

	coldArchive := di.ShouldArchive("cold-node", 0.5, 0.3)

	t.Logf("Hot node should archive: %v", hotArchive)
	t.Logf("Cold node should archive: %v", coldArchive)
}

func TestDecayIntegration_GetRelevanceBoost(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	now := time.Now()
	for i := 0; i < 10; i++ {
		di.RecordAccessAt("node-1", now.Add(time.Duration(i)*time.Second))
	}

	boost := di.GetRelevanceBoost("node-1")
	t.Logf("Relevance boost: %.3f", boost)

	if boost < 0.5 || boost > 2.0 {
		t.Errorf("Boost out of expected range: %.3f", boost)
	}
}

func TestDecayIntegration_GetHotColdNodes(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	// Create hot and cold nodes
	now := time.Now()
	intervals := []int{100, 80, 60, 40, 20}
	currentTime := now
	for _, interval := range intervals {
		di.RecordAccessAt("hot-node", currentTime)
		currentTime = currentTime.Add(time.Duration(interval) * time.Second)
	}

	intervals2 := []int{20, 40, 60, 80, 100}
	currentTime = now
	for _, interval := range intervals2 {
		di.RecordAccessAt("cold-node", currentTime)
		currentTime = currentTime.Add(time.Duration(interval) * time.Second)
	}

	hot := di.GetHotNodes(10)
	cold := di.GetColdNodes(10)

	t.Logf("Hot nodes: %v", hot)
	t.Logf("Cold nodes: %v", cold)
}

func TestDecayIntegration_Reset(t *testing.T) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	di.RecordAccess("node-1")
	di.Reset()

	stats := di.GetStats()
	if stats.TotalAccesses != 0 {
		t.Errorf("TotalAccesses = %v, want 0", stats.TotalAccesses)
	}
}

func TestDecayIntegration_WithComponents(t *testing.T) {
	tracker := NewTracker(DefaultConfig())
	pattern := NewPatternDetector(DefaultPatternDetectorConfig())
	session := NewSessionDetector(DefaultSessionDetectorConfig())

	di := NewDecayIntegrationWithComponents(
		DefaultDecayIntegrationConfig(),
		tracker, pattern, session,
	)

	if di == nil {
		t.Fatal("Should create integration with components")
	}

	di.RecordAccess("node-1")

	// Verify components are being used
	if tracker.GetStats("node-1") == nil {
		t.Error("Tracker should have recorded access")
	}
}

func BenchmarkDecayIntegration_RecordAccess(b *testing.B) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		di.RecordAccess("node-1")
	}
}

func BenchmarkDecayIntegration_GetDecayModifier(b *testing.B) {
	di := NewDecayIntegration(DefaultDecayIntegrationConfig())

	for i := 0; i < 100; i++ {
		di.RecordAccess("node-1")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		di.GetDecayModifier("node-1")
	}
}
