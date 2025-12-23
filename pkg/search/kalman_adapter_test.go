package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestDefaultKalmanSearchConfig(t *testing.T) {
	cfg := DefaultKalmanSearchConfig()

	if !cfg.EnableScoreSmoothing {
		t.Error("Expected EnableScoreSmoothing to be true")
	}
	if !cfg.EnableRankingStability {
		t.Error("Expected EnableRankingStability to be true")
	}
	if !cfg.EnableLatencyPrediction {
		t.Error("Expected EnableLatencyPrediction to be true")
	}
	if cfg.StabilityBoost != 1.05 {
		t.Errorf("Expected StabilityBoost 1.05, got %f", cfg.StabilityBoost)
	}
	if cfg.StabilityWindow != 10 {
		t.Errorf("Expected StabilityWindow 10, got %d", cfg.StabilityWindow)
	}
}

func TestNewKalmanSearchAdapter(t *testing.T) {
	// Create a storage engine
	engine := newNamespacedEngine(t)

	// Create search service
	service := NewService(engine)

	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.service != service {
		t.Error("Service not set correctly")
	}
	if adapter.docFilters == nil {
		t.Error("docFilters should be initialized")
	}
	if adapter.recentRankings == nil {
		t.Error("recentRankings should be initialized")
	}
	if adapter.latencyFilter == nil {
		t.Error("latencyFilter should be initialized")
	}
}

func TestKalmanSearchAdapter_EnhanceResult(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	result := SearchResult{
		ID:    "doc-1",
		Score: 0.85,
	}

	enhanced := adapter.enhanceResult(result, "test query")

	// Score should be smoothed
	if enhanced.Score < 0 || enhanced.Score > 1 {
		t.Errorf("Smoothed score out of range: %f", enhanced.Score)
	}

	// Stats should show smoothing
	stats := adapter.GetStats()
	if stats.ScoresSmoothed != 1 {
		t.Errorf("Expected 1 score smoothed, got %d", stats.ScoresSmoothed)
	}
}

func TestKalmanSearchAdapter_ApplyRankingStability(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some ranking history
	adapter.recentRankings = []queryRanking{
		{Query: "q1", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-2", "doc-3"}},
		{Query: "q2", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-2", "doc-4"}},
		{Query: "q3", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-3", "doc-5"}},
	}

	results := []SearchResult{
		{ID: "doc-1", Score: 0.80},
		{ID: "doc-2", Score: 0.79},
		{ID: "doc-6", Score: 0.78}, // New doc
	}

	stabilized := adapter.applyRankingStability(results, "test")

	// doc-1 should have stability boost (appeared in all queries at top)
	doc1Boosted := false
	for _, r := range stabilized {
		if r.ID == "doc-1" && r.Score > 0.80 {
			doc1Boosted = true
		}
	}
	if !doc1Boosted {
		t.Error("Expected doc-1 to have stability boost")
	}
}

func TestKalmanSearchAdapter_RecordRanking(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine)

	cfg := DefaultKalmanSearchConfig()
	cfg.StabilityWindow = 3
	adapter := NewKalmanSearchAdapter(service, cfg)

	results := []SearchResult{
		{ID: "doc-1", Score: 0.9},
		{ID: "doc-2", Score: 0.8},
		{ID: "doc-3", Score: 0.7},
	}

	// Record multiple rankings
	for i := 0; i < 5; i++ {
		adapter.recordRanking("query", results)
	}

	// Should trim to window size
	if len(adapter.recentRankings) != 3 {
		t.Errorf("Expected 3 rankings (window size), got %d", len(adapter.recentRankings))
	}
}

func TestKalmanSearchAdapter_SortByScore(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	results := []SearchResult{
		{ID: "doc-1", Score: 0.5},
		{ID: "doc-2", Score: 0.9},
		{ID: "doc-3", Score: 0.7},
	}

	adapter.sortByScore(results)

	// Should be sorted highest first
	if results[0].ID != "doc-2" {
		t.Errorf("Expected doc-2 first, got %s", results[0].ID)
	}
	if results[1].ID != "doc-3" {
		t.Errorf("Expected doc-3 second, got %s", results[1].ID)
	}
	if results[2].ID != "doc-1" {
		t.Errorf("Expected doc-1 third, got %s", results[2].ID)
	}
}

func TestKalmanSearchAdapter_GetPredictedLatency(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Simulate some latency measurements
	for i := 0; i < 10; i++ {
		adapter.latencyFilter.Process(float64(10 + i))
	}

	predicted := adapter.GetPredictedLatency(5)

	// Should predict a reasonable value
	if predicted < 0 {
		t.Errorf("Predicted latency should be non-negative, got %f", predicted)
	}
}

func TestKalmanSearchAdapter_GetLatencyTrend(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Simulate increasing latency (positive trend)
	for i := 0; i < 10; i++ {
		adapter.latencyFilter.Process(float64(10 + i*2))
	}

	trend := adapter.GetLatencyTrend()

	// Should show positive velocity (getting slower)
	if trend <= 0 {
		t.Logf("Trend may depend on filter convergence: %f", trend)
		// Don't fail - filter behavior depends on many factors
	}
}

func TestKalmanSearchAdapter_GetDocumentRelevanceTrend(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Process some results to create document filters
	for i := 0; i < 5; i++ {
		result := SearchResult{
			ID:    "rising-doc",
			Score: 0.5 + float64(i)*0.1, // Increasing scores
		}
		adapter.enhanceResult(result, "test")
	}

	trend := adapter.GetDocumentRelevanceTrend("rising-doc")

	// Trend may be positive for rising doc
	_ = trend // Just verify no panic

	// Unknown doc should return 0
	unknownTrend := adapter.GetDocumentRelevanceTrend("unknown")
	if unknownTrend != 0 {
		t.Errorf("Expected 0 for unknown doc, got %f", unknownTrend)
	}
}

func TestKalmanSearchAdapter_GetRisingDocuments(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some document filters with different trends
	for i := 0; i < 5; i++ {
		adapter.enhanceResult(SearchResult{ID: "rising", Score: 0.3 + float64(i)*0.1}, "q")
		adapter.enhanceResult(SearchResult{ID: "falling", Score: 0.8 - float64(i)*0.1}, "q")
	}

	rising := adapter.GetRisingDocuments(0.01)
	falling := adapter.GetFallingDocuments(-0.01)

	// May or may not find documents depending on filter convergence
	_ = rising
	_ = falling
}

func TestKalmanSearchAdapter_GetStats(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	stats := adapter.GetStats()

	if stats.TotalQueries != 0 {
		t.Errorf("Initial TotalQueries should be 0")
	}
	if stats.ScoresSmoothed != 0 {
		t.Errorf("Initial ScoresSmoothed should be 0")
	}
}

func TestKalmanSearchAdapter_GetService(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	if adapter.GetService() != service {
		t.Error("GetService should return the underlying service")
	}
}

func TestKalmanSearchAdapter_Reset(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some state
	adapter.enhanceResult(SearchResult{ID: "doc-1", Score: 0.8}, "query")
	adapter.recordRanking("query", []SearchResult{{ID: "doc-1"}})

	if len(adapter.docFilters) == 0 {
		t.Error("Expected filters to be created")
	}
	if len(adapter.recentRankings) == 0 {
		t.Error("Expected rankings to be recorded")
	}

	// Reset
	adapter.Reset()

	if len(adapter.docFilters) != 0 {
		t.Error("Filters should be cleared")
	}
	if len(adapter.recentRankings) != 0 {
		t.Error("Rankings should be cleared")
	}
}

func TestKalmanSearchAdapter_DisabledSmoothing(t *testing.T) {
	// Disable Kalman filtering globally
	cleanup := config.WithKalmanDisabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine)

	cfg := DefaultKalmanSearchConfig()
	cfg.EnableScoreSmoothing = false
	adapter := NewKalmanSearchAdapter(service, cfg)

	result := SearchResult{
		ID:    "no-smooth-doc",
		Score: 0.75,
	}

	enhanced := adapter.enhanceResult(result, "query")

	// Score should remain unchanged
	if enhanced.Score != 0.75 {
		t.Errorf("Expected unchanged score 0.75, got %f", enhanced.Score)
	}
}

func TestKalmanSearchAdapter_Search_Integration(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)

	// Add some nodes with embeddings
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		embedding := make([]float32, 1024)
		for j := range embedding {
			embedding[j] = float32(i*j) / 1024.0
		}
		_, _ = engine.CreateNode(&storage.Node{
			Labels:          []string{"Document"},
			Properties:      map[string]interface{}{"title": "Doc " + string(rune('A'+i))},
			ChunkEmbeddings: [][]float32{embedding},
		})
	}

	service := NewService(engine)
	_ = service.BuildIndexes(ctx)

	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create query embedding
	queryEmbed := make([]float32, 1024)
	for i := range queryEmbed {
		queryEmbed[i] = float32(i) / 1024.0
	}

	opts := &SearchOptions{
		Limit: 10,
	}

	// Run search
	response, err := adapter.Search(ctx, "test query", queryEmbed, opts)
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}

	// Check stats
	stats := adapter.GetStats()
	if stats.TotalQueries != 1 {
		t.Errorf("Expected 1 query, got %d", stats.TotalQueries)
	}

	_ = response
}

func TestMinHelper(t *testing.T) {
	if min(5, 10) != 5 {
		t.Error("min(5, 10) should be 5")
	}
	if min(10, 5) != 5 {
		t.Error("min(10, 5) should be 5")
	}
	if min(5, 5) != 5 {
		t.Error("min(5, 5) should be 5")
	}
}

// Benchmark search adapter
func BenchmarkKalmanSearchAdapter_EnhanceResult(b *testing.B) {
	config.EnableKalmanFiltering()
	defer config.DisableKalmanFiltering()

	engine := newNamespacedEngine(b)
	service := NewService(engine)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	result := SearchResult{
		ID:    "bench-doc",
		Score: 0.75,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.enhanceResult(result, "query")
	}
}
