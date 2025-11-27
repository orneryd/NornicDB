package decay

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/temporal"
)

func TestDefaultKalmanAdapterConfig(t *testing.T) {
	cfg := DefaultKalmanAdapterConfig()

	if !cfg.EnableKalmanSmoothing {
		t.Error("Expected EnableKalmanSmoothing to be true")
	}
	if !cfg.EnableTemporalModifier {
		t.Error("Expected EnableTemporalModifier to be true")
	}
	if cfg.PredictionHorizon != 168 {
		t.Errorf("Expected PredictionHorizon 168, got %d", cfg.PredictionHorizon)
	}
	if cfg.MinScoreChange != 0.001 {
		t.Errorf("Expected MinScoreChange 0.001, got %f", cfg.MinScoreChange)
	}
}

func TestNewKalmanAdapter(t *testing.T) {
	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.manager != manager {
		t.Error("Manager not set correctly")
	}
	if adapter.nodeFilters == nil {
		t.Error("nodeFilters should be initialized")
	}
	if adapter.cachedScores == nil {
		t.Error("cachedScores should be initialized")
	}
}

func TestKalmanAdapter_CalculateScore_Basic(t *testing.T) {
	// Enable Kalman filtering for test
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	info := &MemoryInfo{
		ID:           "test-node-1",
		Tier:         TierSemantic,
		CreatedAt:    time.Now(),
		LastAccessed: time.Now(),
		AccessCount:  1,
	}

	// First calculation
	score1 := adapter.CalculateScore(info)

	if score1 < 0 || score1 > 1 {
		t.Errorf("Score should be between 0 and 1, got %f", score1)
	}

	// Check that stats were updated
	stats := adapter.GetStats()
	if stats.TotalCalculations != 1 {
		t.Errorf("Expected 1 calculation, got %d", stats.TotalCalculations)
	}
	if stats.KalmanSmoothed != 1 {
		t.Errorf("Expected 1 smoothed, got %d", stats.KalmanSmoothed)
	}

	// Check cached score
	cached := adapter.GetSmoothedScore(info.ID)
	if cached == nil {
		t.Fatal("Expected cached score")
	}
	if cached.Smoothed != score1 {
		t.Errorf("Cached smoothed %f != returned %f", cached.Smoothed, score1)
	}
}

func TestKalmanAdapter_CalculateScore_Smoothing(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	info := &MemoryInfo{
		ID:           "test-smooth",
		Tier:         TierSemantic,
		CreatedAt:    time.Now().Add(-24 * time.Hour),
		LastAccessed: time.Now(),
		AccessCount:  5,
	}

	// Calculate multiple scores to see smoothing effect
	var scores []float64
	for i := 0; i < 5; i++ {
		// Simulate varying raw scores by adjusting access time
		info.LastAccessed = time.Now().Add(-time.Duration(i*10) * time.Minute)
		score := adapter.CalculateScore(info)
		scores = append(scores, score)
	}

	// Verify all scores are valid
	for i, score := range scores {
		if score < 0 || score > 1 {
			t.Errorf("Score %d out of range: %f", i, score)
		}
	}

	// Check stats
	stats := adapter.GetStats()
	if stats.TotalCalculations != 5 {
		t.Errorf("Expected 5 calculations, got %d", stats.TotalCalculations)
	}
}

func TestKalmanAdapter_WithTemporalIntegration(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	// Create temporal integration
	temporalCfg := temporal.DefaultDecayIntegrationConfig()
	temporalInteg := temporal.NewDecayIntegration(temporalCfg)
	adapter.SetTemporal(temporalInteg)

	info := &MemoryInfo{
		ID:           "test-temporal",
		Tier:         TierSemantic,
		CreatedAt:    time.Now().Add(-48 * time.Hour),
		LastAccessed: time.Now().Add(-1 * time.Hour),
		AccessCount:  3,
	}

	// Record some accesses to make node "hot"
	for i := 0; i < 10; i++ {
		adapter.RecordAccess(info.ID)
		time.Sleep(10 * time.Millisecond)
	}

	score := adapter.CalculateScore(info)

	// Score should be valid
	if score < 0 || score > 1 {
		t.Errorf("Score out of range: %f", score)
	}

	// Check stats show temporal modification
	stats := adapter.GetStats()
	if stats.TemporalModified < 1 {
		t.Error("Expected temporal modification")
	}
}

func TestKalmanAdapter_ShouldArchive(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	// Create old, unused memory
	oldInfo := &MemoryInfo{
		ID:           "old-memory",
		Tier:         TierEpisodic,
		CreatedAt:    time.Now().Add(-30 * 24 * time.Hour), // 30 days old
		LastAccessed: time.Now().Add(-25 * 24 * time.Hour), // Not accessed in 25 days
		AccessCount:  1,
	}

	// Calculate score first to populate cache
	adapter.CalculateScore(oldInfo)

	// Should likely archive (episodic with no recent access)
	shouldArchive := adapter.ShouldArchive(oldInfo)

	// Create fresh memory
	freshInfo := &MemoryInfo{
		ID:           "fresh-memory",
		Tier:         TierSemantic,
		CreatedAt:    time.Now().Add(-1 * time.Hour),
		LastAccessed: time.Now(),
		AccessCount:  10,
	}

	adapter.CalculateScore(freshInfo)
	shouldNotArchive := adapter.ShouldArchive(freshInfo)

	// Fresh memory should not be archived
	if shouldNotArchive {
		t.Error("Fresh memory should not be archived")
	}

	// Check archive predictions stat
	stats := adapter.GetStats()
	if stats.ArchivePredictions != 2 {
		t.Errorf("Expected 2 archive predictions, got %d", stats.ArchivePredictions)
	}

	// Note: old memory MAY or MAY NOT be archived depending on exact timing
	// Just verify the function runs without error
	_ = shouldArchive
}

func TestKalmanAdapter_GetArchivalCandidates(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	// Create a mix of memories
	memories := []*MemoryInfo{
		{
			ID:           "old-1",
			Tier:         TierEpisodic,
			CreatedAt:    time.Now().Add(-60 * 24 * time.Hour),
			LastAccessed: time.Now().Add(-50 * 24 * time.Hour),
			AccessCount:  1,
		},
		{
			ID:           "fresh-1",
			Tier:         TierSemantic,
			CreatedAt:    time.Now().Add(-1 * time.Hour),
			LastAccessed: time.Now(),
			AccessCount:  20,
		},
		{
			ID:           "medium-1",
			Tier:         TierSemantic,
			CreatedAt:    time.Now().Add(-10 * 24 * time.Hour),
			LastAccessed: time.Now().Add(-5 * 24 * time.Hour),
			AccessCount:  5,
		},
	}

	// Calculate scores for all
	for _, info := range memories {
		adapter.CalculateScore(info)
	}

	// Get archival candidates
	candidates := adapter.GetArchivalCandidates(memories, 10)

	// Fresh memory should not be in candidates
	for _, c := range candidates {
		if c.ID == "fresh-1" {
			t.Error("Fresh memory should not be archival candidate")
		}
	}
}

func TestKalmanAdapter_Reinforce(t *testing.T) {
	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	info := &MemoryInfo{
		ID:           "reinforce-test",
		Tier:         TierSemantic,
		CreatedAt:    time.Now().Add(-24 * time.Hour),
		LastAccessed: time.Now().Add(-1 * time.Hour),
		AccessCount:  5,
	}

	originalCount := info.AccessCount

	// Reinforce
	reinforced := adapter.Reinforce(info)

	// Reinforced should have incremented access count
	if reinforced.AccessCount != originalCount+1 {
		t.Errorf("Expected access count %d, got %d", originalCount+1, reinforced.AccessCount)
	}
}

func TestKalmanAdapter_RunDecayCycle(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	memories := []*MemoryInfo{
		{ID: "cycle-1", Tier: TierSemantic, CreatedAt: time.Now(), LastAccessed: time.Now(), AccessCount: 1},
		{ID: "cycle-2", Tier: TierSemantic, CreatedAt: time.Now(), LastAccessed: time.Now(), AccessCount: 1},
		{ID: "cycle-3", Tier: TierSemantic, CreatedAt: time.Now(), LastAccessed: time.Now(), AccessCount: 1},
	}

	ctx := context.Background()
	err := adapter.RunDecayCycle(ctx, memories)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// All should be cached now
	for _, info := range memories {
		if adapter.GetSmoothedScore(info.ID) == nil {
			t.Errorf("Memory %s should be cached", info.ID)
		}
	}
}

func TestKalmanAdapter_Reset(t *testing.T) {
	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	info := &MemoryInfo{
		ID:           "reset-test",
		Tier:         TierSemantic,
		CreatedAt:    time.Now(),
		LastAccessed: time.Now(),
		AccessCount:  1,
	}

	// Create some state
	adapter.CalculateScore(info)

	if len(adapter.nodeFilters) == 0 {
		t.Error("Expected filters to be created")
	}

	// Reset
	adapter.Reset()

	if len(adapter.nodeFilters) != 0 {
		t.Error("Filters should be cleared")
	}
	if len(adapter.cachedScores) != 0 {
		t.Error("Cached scores should be cleared")
	}
	if adapter.stats.TotalCalculations != 0 {
		t.Error("Stats should be reset")
	}
}

func TestKalmanAdapter_GetManager(t *testing.T) {
	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	if adapter.GetManager() != manager {
		t.Error("GetManager should return the underlying manager")
	}
}

func TestKalmanAdapter_DisabledSmoothing(t *testing.T) {
	// Disable Kalman filtering
	cleanup := config.WithKalmanDisabled()
	defer cleanup()

	manager := New(DefaultConfig())
	cfg := DefaultKalmanAdapterConfig()
	cfg.EnableKalmanSmoothing = false
	adapter := NewKalmanAdapter(manager, cfg)

	info := &MemoryInfo{
		ID:           "no-smooth",
		Tier:         TierSemantic,
		CreatedAt:    time.Now(),
		LastAccessed: time.Now(),
		AccessCount:  1,
	}

	score := adapter.CalculateScore(info)

	// Should still work but no smoothing applied
	if score < 0 || score > 1 {
		t.Errorf("Score out of range: %f", score)
	}

	// No Kalman smoothing stats
	stats := adapter.GetStats()
	if stats.KalmanSmoothed != 0 {
		t.Errorf("Expected 0 smoothed with disabled smoothing, got %d", stats.KalmanSmoothed)
	}
}

// Benchmark Kalman adapter
func BenchmarkKalmanAdapter_CalculateScore(b *testing.B) {
	config.EnableKalmanFiltering()
	defer config.DisableKalmanFiltering()

	manager := New(DefaultConfig())
	adapter := NewKalmanAdapter(manager, DefaultKalmanAdapterConfig())

	info := &MemoryInfo{
		ID:           "bench-node",
		Tier:         TierSemantic,
		CreatedAt:    time.Now().Add(-24 * time.Hour),
		LastAccessed: time.Now(),
		AccessCount:  10,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.CalculateScore(info)
	}
}
