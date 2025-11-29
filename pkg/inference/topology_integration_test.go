package inference

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestTopologyIntegrationBasic verifies basic topology integration.
func TestTopologyIntegrationBasic(t *testing.T) {
	// Create storage with test graph
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	// Create topology integration
	config := DefaultTopologyConfig()
	config.Enabled = true
	config.Algorithm = "adamic_adar"
	config.TopK = 10
	config.MinScore = 0.1

	topo := NewTopologyIntegration(engine, config)

	// Test suggestions
	suggestions, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Failed to get suggestions: %v", err)
	}

	if len(suggestions) == 0 {
		t.Error("Expected topological suggestions, got none")
	}

	// Verify suggestion structure
	for _, sug := range suggestions {
		if sug.SourceID != "alice" {
			t.Errorf("Wrong source ID: %s", sug.SourceID)
		}
		if sug.Method != "topology_adamic_adar" {
			t.Errorf("Wrong method: %s", sug.Method)
		}
		if sug.Confidence < 0 || sug.Confidence > 1 {
			t.Errorf("Confidence out of range: %.3f", sug.Confidence)
		}
		t.Logf("Suggestion: %s -> %s (%.3f)", sug.SourceID, sug.TargetID, sug.Confidence)
	}
}

// TestTopologyAlgorithms tests different topology algorithms.
func TestTopologyAlgorithms(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	algorithms := []string{
		"adamic_adar",
		"jaccard",
		"common_neighbors",
		"resource_allocation",
		"preferential_attachment",
	}

	for _, algo := range algorithms {
		t.Run(algo, func(t *testing.T) {
			config := DefaultTopologyConfig()
			config.Enabled = true
			config.Algorithm = algo
			config.TopK = 5
			config.MinScore = 0.0

			topo := NewTopologyIntegration(engine, config)
			suggestions, err := topo.SuggestTopological(context.Background(), "alice")

			if err != nil {
				t.Fatalf("Algorithm %s failed: %v", algo, err)
			}

			t.Logf("Algorithm %s: %d suggestions", algo, len(suggestions))
			for _, sug := range suggestions {
				t.Logf("  %s: %.3f", sug.TargetID, sug.Confidence)
			}
		})
	}
}

// TestCombinedSuggestions tests merging semantic + topological.
func TestCombinedSuggestions(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.Weight = 0.5 // Equal weight
	topo := NewTopologyIntegration(engine, config)

	// Mock semantic suggestions
	semantic := []EdgeSuggestion{
		{SourceID: "alice", TargetID: "diana", Confidence: 0.9, Method: "similarity"},
		{SourceID: "alice", TargetID: "eve", Confidence: 0.6, Method: "similarity"},
	}

	// Get topological suggestions
	topological, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Failed to get topology suggestions: %v", err)
	}

	// Combine
	combined := topo.CombinedSuggestions(semantic, topological)

	if len(combined) == 0 {
		t.Error("Expected combined suggestions")
	}

	// Verify diana has boosted score (appears in both)
	var dianaScore float64
	for _, sug := range combined {
		if sug.TargetID == "diana" {
			dianaScore = sug.Confidence
			t.Logf("Diana combined score: %.3f", dianaScore)
			break
		}
	}

	// Diana should have higher score than if only one source
	if dianaScore < 0.4 {
		t.Errorf("Diana score too low: %.3f (expected boost from both sources)", dianaScore)
	}

	// Verify sorted
	for i := 1; i < len(combined); i++ {
		if combined[i].Confidence > combined[i-1].Confidence {
			t.Error("Suggestions not sorted by confidence")
			break
		}
	}
}

// TestInferenceEngineIntegration tests full integration with Engine.
func TestInferenceEngineIntegration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	// Create inference engine
	inferConfig := DefaultConfig()
	inferEngine := New(inferConfig)

	// Mock semantic search
	inferEngine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
		// Return diana as semantically similar
		return []SimilarityResult{
			{ID: "diana", Score: 0.9},
		}, nil
	})

	// Enable topology integration
	topoConfig := DefaultTopologyConfig()
	topoConfig.Enabled = true
	topoConfig.Weight = 0.4
	topo := NewTopologyIntegration(engine, topoConfig)
	inferEngine.SetTopologyIntegration(topo)

	// Test OnStore with both semantic and topological
	suggestions, err := inferEngine.OnStore(context.Background(), "alice", []float32{1, 2, 3})
	if err != nil {
		t.Fatalf("OnStore failed: %v", err)
	}

	if len(suggestions) == 0 {
		t.Error("Expected suggestions from integrated engine")
	}

	t.Logf("Integrated suggestions: %d", len(suggestions))
	for _, sug := range suggestions {
		t.Logf("  %s: %.3f (%s)", sug.TargetID, sug.Confidence, sug.Method)
	}

	// Verify diana appears (from semantic)
	foundDiana := false
	for _, sug := range suggestions {
		if sug.TargetID == "diana" {
			foundDiana = true
			// Should have high score (semantic + topology)
			if sug.Confidence < 0.5 {
				t.Errorf("Diana confidence too low: %.3f", sug.Confidence)
			}
		}
	}

	if !foundDiana {
		t.Error("Expected diana in suggestions")
	}
}

// TestCacheInvalidation tests graph cache management.
func TestCacheInvalidation(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 2 // Refresh after 2 predictions
	topo := NewTopologyIntegration(engine, config)

	// First prediction (builds cache)
	_, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("First prediction failed: %v", err)
	}

	if topo.cachedGraph == nil {
		t.Error("Expected graph to be cached")
	}

	// Second prediction (uses cache)
	_, err = topo.SuggestTopological(context.Background(), "bob")
	if err != nil {
		t.Fatalf("Second prediction failed: %v", err)
	}

	// Third prediction (should rebuild cache)
	_, err = topo.SuggestTopological(context.Background(), "charlie")
	if err != nil {
		t.Fatalf("Third prediction failed: %v", err)
	}

	// Manual invalidation
	topo.InvalidateCache()
	if topo.cachedGraph != nil {
		t.Error("Cache should be invalidated")
	}
}

// TestDisabledTopology verifies topology can be disabled.
func TestDisabledTopology(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = false // Disabled
	topo := NewTopologyIntegration(engine, config)

	suggestions, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(suggestions) != 0 {
		t.Error("Expected no suggestions when topology disabled")
	}
}

// TestMinScoreThreshold tests filtering by minimum score.
func TestMinScoreThreshold(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.MinScore = 0.7 // High threshold
	topo := NewTopologyIntegration(engine, config)

	suggestions, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Prediction failed: %v", err)
	}

	// All suggestions should meet threshold
	for _, sug := range suggestions {
		if sug.Confidence < config.MinScore {
			t.Errorf("Suggestion %.3f below threshold %.3f", sug.Confidence, config.MinScore)
		}
	}
}

// Helper: setupTestGraph creates test graph structure.
//
// Structure:
//
//	alice -- bob -- diana
//	alice -- charlie -- diana
//	eve (isolated)
func setupTestGraph(t *testing.T, engine storage.Engine) {
	nodes := []*storage.Node{
		{ID: "alice", Labels: []string{"Person"}},
		{ID: "bob", Labels: []string{"Person"}},
		{ID: "charlie", Labels: []string{"Person"}},
		{ID: "diana", Labels: []string{"Person"}},
		{ID: "eve", Labels: []string{"Person"}},
	}

	for _, node := range nodes {
		if err := engine.CreateNode(node); err != nil {
			t.Fatalf("Failed to create node: %v", err)
		}
	}

	edges := []*storage.Edge{
		{ID: "e1", StartNode: "alice", EndNode: "bob", Type: "KNOWS"},
		{ID: "e2", StartNode: "alice", EndNode: "charlie", Type: "KNOWS"},
		{ID: "e3", StartNode: "bob", EndNode: "diana", Type: "KNOWS"},
		{ID: "e4", StartNode: "charlie", EndNode: "diana", Type: "KNOWS"},
	}

	for _, edge := range edges {
		if err := engine.CreateEdge(edge); err != nil {
			t.Fatalf("Failed to create edge: %v", err)
		}
	}
}

// BenchmarkTopologyIntegration benchmarks topology suggestion performance.
func BenchmarkTopologyIntegration(b *testing.B) {
	engine := storage.NewMemoryEngine()

	// Create larger graph
	for i := 0; i < 100; i++ {
		node := &storage.Node{
			ID:     storage.NodeID("node-" + string(rune(i))),
			Labels: []string{"Test"},
		}
		engine.CreateNode(node)
	}

	// Create edges
	for i := 0; i < 100; i++ {
		for j := i + 1; j < i+6 && j < 100; j++ {
			edge := &storage.Edge{
				ID:        storage.EdgeID("e-" + string(rune(i)) + "-" + string(rune(j))),
				StartNode: storage.NodeID("node-" + string(rune(i))),
				EndNode:   storage.NodeID("node-" + string(rune(j))),
				Type:      "CONNECTS",
			}
			engine.CreateEdge(edge)
		}
	}

	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.SuggestTopological(ctx, "node-50")
	}
}

// TestNormalizeScoreEdgeCases tests that algorithm scores are normalized in [0, 1].
// Normalization is now done in pkg/linkpredict/topology.go, not here.
func TestNormalizeScoreEdgeCases(t *testing.T) {
	// Build a simple graph to test normalization
	engine := storage.NewMemoryEngine()
	ctx := context.Background()

	// Create some nodes
	engine.CreateNode(&storage.Node{ID: "a", Labels: []string{"Test"}})
	engine.CreateNode(&storage.Node{ID: "b", Labels: []string{"Test"}})
	engine.CreateNode(&storage.Node{ID: "c", Labels: []string{"Test"}})
	engine.CreateNode(&storage.Node{ID: "d", Labels: []string{"Test"}})
	engine.CreateNode(&storage.Node{ID: "e", Labels: []string{"Test"}})

	// Create edges: a-b, a-c, b-c, b-d, c-d, c-e (small connected graph)
	engine.CreateEdge(&storage.Edge{ID: "e1", StartNode: "a", EndNode: "b", Type: "KNOWS"})
	engine.CreateEdge(&storage.Edge{ID: "e2", StartNode: "a", EndNode: "c", Type: "KNOWS"})
	engine.CreateEdge(&storage.Edge{ID: "e3", StartNode: "b", EndNode: "c", Type: "KNOWS"})
	engine.CreateEdge(&storage.Edge{ID: "e4", StartNode: "b", EndNode: "d", Type: "KNOWS"})
	engine.CreateEdge(&storage.Edge{ID: "e5", StartNode: "c", EndNode: "d", Type: "KNOWS"})
	engine.CreateEdge(&storage.Edge{ID: "e6", StartNode: "c", EndNode: "e", Type: "KNOWS"})

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.MinScore = 0.0 // Accept all scores

	algorithms := []string{"adamic_adar", "jaccard", "common_neighbors", "resource_allocation", "preferential_attachment"}

	for _, algo := range algorithms {
		config.Algorithm = algo
		topo := NewTopologyIntegration(engine, config)

		suggestions, err := topo.SuggestTopological(ctx, "a")
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", algo, err)
		}

		for _, s := range suggestions {
			if s.Confidence < 0.0 || s.Confidence > 1.0 {
				t.Errorf("%s: score %.3f out of [0, 1] range for target %s",
					algo, s.Confidence, s.TargetID)
			}
		}
		t.Logf("%s: %d suggestions, all scores in [0, 1]", algo, len(suggestions))
	}
}

// TestEmptyGraphHandling tests handling of empty suggestions
func TestEmptyGraphHandling(t *testing.T) {
	engine := storage.NewMemoryEngine()
	// Empty graph

	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	suggestions, err := topo.SuggestTopological(context.Background(), "nonexistent")

	if err != nil {
		t.Fatalf("Expected no error for nonexistent node: %v", err)
	}

	if len(suggestions) != 0 {
		t.Errorf("Expected no suggestions for nonexistent node, got %d", len(suggestions))
	}
}

// TestCombinedSuggestionsEmpty tests combining with empty lists
func TestCombinedSuggestionsEmpty(t *testing.T) {
	config := DefaultTopologyConfig()
	config.Enabled = true
	config.Weight = 0.5
	topo := NewTopologyIntegration(nil, config)

	// Both empty
	combined := topo.CombinedSuggestions(nil, nil)
	if len(combined) != 0 {
		t.Error("Expected empty result for both empty inputs")
	}

	// Only semantic
	semantic := []EdgeSuggestion{
		{SourceID: "a", TargetID: "b", Confidence: 0.8},
	}
	combined = topo.CombinedSuggestions(semantic, nil)
	if len(combined) != 1 {
		t.Errorf("Expected 1 suggestion, got %d", len(combined))
	}

	// Only topological
	topological := []EdgeSuggestion{
		{SourceID: "a", TargetID: "c", Confidence: 0.7},
	}
	combined = topo.CombinedSuggestions(nil, topological)
	if len(combined) != 1 {
		t.Errorf("Expected 1 suggestion, got %d", len(combined))
	}
}

// TestInvalidateCache tests cache invalidation
func TestInvalidateCache(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 100
	topo := NewTopologyIntegration(engine, config)

	// Build cache
	_, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("First prediction failed: %v", err)
	}

	if topo.cachedGraph == nil {
		t.Error("Cache should be populated after first prediction")
	}

	// Invalidate
	topo.InvalidateCache()

	if topo.cachedGraph != nil {
		t.Error("Cache should be nil after invalidation")
	}

	if topo.predictionCount != 0 {
		t.Error("Prediction count should be reset after invalidation")
	}
}
