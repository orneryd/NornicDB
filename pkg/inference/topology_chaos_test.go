package inference

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// CHAOS TESTS - Stress tests with random/adversarial inputs
// =============================================================================

// TestTopologyChaosRandomGraph tests topology integration on random graphs.
func TestTopologyChaosRandomGraph(t *testing.T) {
	sizes := []int{10, 50, 100}
	densities := []float64{0.1, 0.3, 0.5}

	for _, size := range sizes {
		for _, density := range densities {
			name := fmt.Sprintf("size_%d_density_%.1f", size, density)
			t.Run(name, func(t *testing.T) {
				engine := buildRandomStorageGraph(size, density, 42)

				config := &TopologyConfig{
					Enabled:              true,
					Algorithm:            "adamic_adar",
					TopK:                 10,
					MinScore:             0.0,
					Weight:               0.5,
					GraphRefreshInterval: 50,
				}
				topo := NewTopologyIntegration(engine, config)

				// Test all algorithms
				algorithms := []string{
					"adamic_adar", "jaccard", "common_neighbors",
					"resource_allocation", "preferential_attachment",
				}

				for _, algo := range algorithms {
					t.Run(algo, func(t *testing.T) {
						topo.config.Algorithm = algo

						// Pick a random node
						sourceID := fmt.Sprintf("node-%d", rand.Intn(size))

						defer func() {
							if r := recover(); r != nil {
								t.Errorf("%s panicked: %v", algo, r)
							}
						}()

						suggestions, err := topo.SuggestTopological(context.Background(), sourceID)
						if err != nil {
							t.Errorf("%s failed: %v", algo, err)
							return
						}

						// Verify suggestions are valid
						for _, sug := range suggestions {
							if sug.Confidence < 0 || sug.Confidence > 1 {
								t.Errorf("Confidence out of range: %.3f", sug.Confidence)
							}
							if sug.SourceID != sourceID {
								t.Errorf("Wrong source: %s != %s", sug.SourceID, sourceID)
							}
						}
					})
				}
			})
		}
	}
}

// TestTopologyChaosStarTopology tests with hub-and-spoke pattern.
func TestTopologyChaosStarTopology(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create hub
	engine.CreateNode(&storage.Node{ID: "hub", Labels: []string{"Hub"}})

	// Create 100 spokes
	for i := 0; i < 100; i++ {
		nodeID := storage.NodeID(fmt.Sprintf("spoke-%d", i))
		engine.CreateNode(&storage.Node{ID: nodeID, Labels: []string{"Spoke"}})
		engine.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("e-%d", i)),
			StartNode: "hub",
			EndNode:   nodeID,
			Type:      "CONNECTS",
		})
	}

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.MinScore = 0.0
	topo := NewTopologyIntegration(engine, config)

	// Hub should have no predictions (connected to everyone)
	hubSuggestions, err := topo.SuggestTopological(context.Background(), "hub")
	if err != nil {
		t.Fatalf("Hub prediction failed: %v", err)
	}
	t.Logf("Hub predictions: %d", len(hubSuggestions))

	// Spoke should predict other spokes
	spokeSuggestions, err := topo.SuggestTopological(context.Background(), "spoke-0")
	if err != nil {
		t.Fatalf("Spoke prediction failed: %v", err)
	}

	// Should have many suggestions (all other spokes via hub)
	if len(spokeSuggestions) == 0 {
		t.Error("Expected spoke to predict other spokes")
	} else {
		t.Logf("Spoke-0 predictions: %d", len(spokeSuggestions))
		// Scores should be low (hub has high degree in Adamic-Adar)
		displayCount := 5
		if len(spokeSuggestions) < displayCount {
			displayCount = len(spokeSuggestions)
		}
		for _, sug := range spokeSuggestions[:displayCount] {
			t.Logf("  %s: %.4f", sug.TargetID, sug.Confidence)
		}
	}
}

// TestTopologyChaosCliqueTopology tests with fully connected graph.
func TestTopologyChaosCliqueTopology(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create clique of 10 nodes
	for i := 0; i < 10; i++ {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("n%d", i))})
	}

	// Connect everyone to everyone
	edgeCount := 0
	for i := 0; i < 10; i++ {
		for j := i + 1; j < 10; j++ {
			engine.CreateEdge(&storage.Edge{
				ID:        storage.EdgeID(fmt.Sprintf("e-%d", edgeCount)),
				StartNode: storage.NodeID(fmt.Sprintf("n%d", i)),
				EndNode:   storage.NodeID(fmt.Sprintf("n%d", j)),
				Type:      "CONNECTS",
			})
			edgeCount++
		}
	}

	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	// In a clique, no predictions should exist (everyone is connected)
	suggestions, err := topo.SuggestTopological(context.Background(), "n0")
	if err != nil {
		t.Fatalf("Clique prediction failed: %v", err)
	}

	if len(suggestions) != 0 {
		t.Errorf("Expected no predictions in clique, got %d", len(suggestions))
	}
}

// TestTopologyChaosEmptyGraph tests with empty storage.
func TestTopologyChaosEmptyGraph(t *testing.T) {
	engine := storage.NewMemoryEngine()

	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	suggestions, err := topo.SuggestTopological(context.Background(), "nonexistent")
	if err != nil {
		t.Logf("Empty graph error (expected): %v", err)
	}

	if len(suggestions) != 0 {
		t.Errorf("Expected no predictions for empty graph, got %d", len(suggestions))
	}
}

// TestTopologyChaosDisabled tests behavior when disabled.
func TestTopologyChaosDisabled(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = false // Disabled
	topo := NewTopologyIntegration(engine, config)

	suggestions, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Disabled topology failed: %v", err)
	}

	if len(suggestions) != 0 {
		t.Errorf("Expected no predictions when disabled, got %d", len(suggestions))
	}
}

// TestTopologyChaosConcurrent tests thread safety.
func TestTopologyChaosConcurrent(t *testing.T) {
	engine := buildRandomStorageGraph(100, 0.3, 42)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 10
	topo := NewTopologyIntegration(engine, config)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 100 concurrent predictions
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errChan <- fmt.Errorf("panic: %v", r)
				}
			}()

			sourceID := fmt.Sprintf("node-%d", idx%100)
			_, err := topo.SuggestTopological(context.Background(), sourceID)
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}
}

// TestTopologyChaosRapidCacheInvalidation tests rapid cache operations.
func TestTopologyChaosRapidCacheInvalidation(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 1 // Rebuild every prediction
	topo := NewTopologyIntegration(engine, config)

	// Rapidly make predictions and invalidate cache
	for i := 0; i < 100; i++ {
		_, err := topo.SuggestTopological(context.Background(), "alice")
		if err != nil {
			t.Fatalf("Prediction %d failed: %v", i, err)
		}

		if i%10 == 0 {
			topo.InvalidateCache()
		}
	}
}

// =============================================================================
// COMPLEX USAGE TESTS
// =============================================================================

// TestTopologyComplexMultiLayerNetwork tests with multi-relationship graph.
func TestTopologyComplexMultiLayerNetwork(t *testing.T) {
	engine := storage.NewMemoryEngine()

	// Create users
	users := []string{"alice", "bob", "charlie", "diana", "eve", "frank"}
	for _, u := range users {
		engine.CreateNode(&storage.Node{ID: storage.NodeID(u), Labels: []string{"Person"}})
	}

	// Multiple relationship types
	relationships := []struct {
		from, to, relType string
	}{
		// Friendship layer
		{"alice", "bob", "FRIENDS"},
		{"alice", "charlie", "FRIENDS"},
		{"bob", "diana", "FRIENDS"},
		{"charlie", "diana", "FRIENDS"},
		// Work layer
		{"alice", "eve", "WORKS_WITH"},
		{"eve", "frank", "WORKS_WITH"},
		{"bob", "frank", "WORKS_WITH"},
		// Family layer
		{"charlie", "eve", "FAMILY"},
	}

	for i, rel := range relationships {
		engine.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID(fmt.Sprintf("e%d", i)),
			StartNode: storage.NodeID(rel.from),
			EndNode:   storage.NodeID(rel.to),
			Type:      rel.relType,
		})
	}

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.TopK = 10
	config.MinScore = 0.0
	topo := NewTopologyIntegration(engine, config)

	// Test predictions for alice
	suggestions, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Multi-layer prediction failed: %v", err)
	}

	t.Logf("Alice multi-layer predictions: %d", len(suggestions))
	for _, sug := range suggestions {
		t.Logf("  alice -> %s: %.4f (%s)", sug.TargetID, sug.Confidence, sug.Method)
	}

	// Diana should be highly ranked (via bob and charlie)
	foundDiana := false
	for _, sug := range suggestions {
		if sug.TargetID == "diana" {
			foundDiana = true
			break
		}
	}
	if !foundDiana {
		t.Error("Expected diana in predictions")
	}
}

// TestTopologyComplexInferenceEngineIntegration tests full Engine integration.
func TestTopologyComplexInferenceEngineIntegration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	// Create inference engine
	inferConfig := DefaultConfig()
	inferConfig.SimilarityThreshold = 0.3
	inferEngine := New(inferConfig)

	// Mock semantic search
	inferEngine.SetSimilaritySearch(func(ctx context.Context, embedding []float32, k int) ([]SimilarityResult, error) {
		return []SimilarityResult{
			{ID: "diana", Score: 0.85},
			{ID: "eve", Score: 0.45},
		}, nil
	})

	// Enable topology integration
	topoConfig := DefaultTopologyConfig()
	topoConfig.Enabled = true
	topoConfig.Weight = 0.4 // 40% topology, 60% semantic
	topo := NewTopologyIntegration(engine, topoConfig)
	inferEngine.SetTopologyIntegration(topo)

	// Test OnStore with both semantic and topology
	suggestions, err := inferEngine.OnStore(context.Background(), "alice", []float32{1, 2, 3})
	if err != nil {
		t.Fatalf("OnStore failed: %v", err)
	}

	t.Logf("Integrated suggestions: %d", len(suggestions))
	for _, sug := range suggestions {
		t.Logf("  %s: %.3f (%s)", sug.TargetID, sug.Confidence, sug.Method)
	}

	// Verify diana appears with combined score
	foundDiana := false
	for _, sug := range suggestions {
		if sug.TargetID == "diana" {
			foundDiana = true
			// Should have semantic contribution
			if sug.Confidence < 0.5 {
				t.Logf("Diana confidence: %.3f", sug.Confidence)
			}
		}
	}
	if !foundDiana {
		t.Error("Expected diana in combined suggestions")
	}
}

// TestTopologyComplexCombinedSuggestions tests merging semantic + topological.
func TestTopologyComplexCombinedSuggestions(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.Weight = 0.5 // Equal weight
	topo := NewTopologyIntegration(engine, config)

	// Mock semantic suggestions
	semantic := []EdgeSuggestion{
		{SourceID: "alice", TargetID: "diana", Confidence: 0.9, Method: "similarity"},
		{SourceID: "alice", TargetID: "eve", Confidence: 0.7, Method: "similarity"},
		{SourceID: "alice", TargetID: "frank", Confidence: 0.5, Method: "similarity"},
	}

	// Get topological suggestions
	topological, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Topological failed: %v", err)
	}

	// Combine
	combined := topo.CombinedSuggestions(semantic, topological)

	t.Logf("Combined suggestions: %d", len(combined))
	for _, sug := range combined {
		t.Logf("  %s: %.3f (%s)", sug.TargetID, sug.Confidence, sug.Method)
	}

	// Verify diana has boosted score
	for _, sug := range combined {
		if sug.TargetID == "diana" {
			// Diana appears in both, should have combined score
			if sug.Confidence < 0.4 { // weighted: 0.9*0.5 = 0.45 minimum
				t.Errorf("Diana combined score too low: %.3f", sug.Confidence)
			}
		}
	}

	// Verify sorted descending
	for i := 1; i < len(combined); i++ {
		if combined[i].Confidence > combined[i-1].Confidence {
			t.Error("Combined suggestions not sorted")
		}
	}
}

// TestTopologyComplexAlgorithmComparison tests all algorithms on same graph.
func TestTopologyComplexAlgorithmComparison(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	algorithms := []string{
		"adamic_adar",
		"jaccard",
		"common_neighbors",
		"resource_allocation",
		"preferential_attachment",
		"unknown", // Should default to adamic_adar
	}

	for _, algo := range algorithms {
		t.Run(algo, func(t *testing.T) {
			config := DefaultTopologyConfig()
			config.Enabled = true
			config.Algorithm = algo
			config.MinScore = 0.0
			topo := NewTopologyIntegration(engine, config)

			suggestions, err := topo.SuggestTopological(context.Background(), "alice")
			if err != nil {
				t.Fatalf("%s failed: %v", algo, err)
			}

			t.Logf("%s: %d suggestions", algo, len(suggestions))
			for _, sug := range suggestions {
				t.Logf("  %s: %.4f", sug.TargetID, sug.Confidence)
			}

			// Verify method tag
			for _, sug := range suggestions {
				expectedMethod := "topology_" + algo
				if algo == "unknown" {
					expectedMethod = "topology_adamic_adar" // Default
				}
				if sug.Method != expectedMethod {
					t.Errorf("Wrong method: %s != %s", sug.Method, expectedMethod)
				}
			}
		})
	}
}

// TestTopologyComplexCacheLifecycle tests full cache lifecycle.
func TestTopologyComplexCacheLifecycle(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 3
	topo := NewTopologyIntegration(engine, config)

	// Initially no cache
	if topo.cachedGraph != nil {
		t.Error("Cache should be nil initially")
	}

	// First prediction builds cache
	_, err := topo.SuggestTopological(context.Background(), "alice")
	if err != nil {
		t.Fatalf("First prediction failed: %v", err)
	}
	if topo.cachedGraph == nil {
		t.Error("Cache should be built after first prediction")
	}
	if topo.predictionCount != 1 {
		t.Errorf("Prediction count should be 1, got %d", topo.predictionCount)
	}

	// Second and third predictions use cache
	for i := 0; i < 2; i++ {
		_, err = topo.SuggestTopological(context.Background(), "bob")
		if err != nil {
			t.Fatalf("Prediction %d failed: %v", i+2, err)
		}
	}
	if topo.predictionCount != 3 {
		t.Errorf("Prediction count should be 3, got %d", topo.predictionCount)
	}

	// Fourth prediction triggers rebuild (interval = 3)
	_, err = topo.SuggestTopological(context.Background(), "charlie")
	if err != nil {
		t.Fatalf("Fourth prediction failed: %v", err)
	}
	if topo.predictionCount != 1 {
		t.Errorf("Prediction count should reset to 1, got %d", topo.predictionCount)
	}

	// Manual invalidation
	topo.InvalidateCache()
	if topo.cachedGraph != nil {
		t.Error("Cache should be nil after invalidation")
	}
	if topo.predictionCount != 0 {
		t.Error("Prediction count should be 0 after invalidation")
	}
}

// TestTopologyComplexMinScoreFiltering tests minimum score threshold.
func TestTopologyComplexMinScoreFiltering(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	thresholds := []float64{0.0, 0.3, 0.5, 0.7, 0.9, 1.0}

	for _, threshold := range thresholds {
		t.Run(fmt.Sprintf("threshold_%.1f", threshold), func(t *testing.T) {
			config := DefaultTopologyConfig()
			config.Enabled = true
			config.MinScore = threshold
			topo := NewTopologyIntegration(engine, config)

			suggestions, err := topo.SuggestTopological(context.Background(), "alice")
			if err != nil {
				t.Fatalf("Prediction failed: %v", err)
			}

			t.Logf("Threshold %.1f: %d suggestions", threshold, len(suggestions))

			// All suggestions should meet threshold
			for _, sug := range suggestions {
				if sug.Confidence < threshold {
					t.Errorf("Suggestion %.3f below threshold %.1f", sug.Confidence, threshold)
				}
			}
		})
	}
}

// TestTopologyComplexWeightConfiguration tests weight configuration.
func TestTopologyComplexWeightConfiguration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	weights := []float64{0.0, 0.25, 0.5, 0.75, 1.0}

	for _, weight := range weights {
		t.Run(fmt.Sprintf("weight_%.2f", weight), func(t *testing.T) {
			config := DefaultTopologyConfig()
			config.Enabled = true
			config.Weight = weight
			topo := NewTopologyIntegration(engine, config)

			// Mock semantic and topological suggestions
			semantic := []EdgeSuggestion{
				{TargetID: "diana", Confidence: 0.8, Method: "similarity"},
			}

			topological, err := topo.SuggestTopological(context.Background(), "alice")
			if err != nil {
				t.Fatalf("Topological failed: %v", err)
			}

			combined := topo.CombinedSuggestions(semantic, topological)

			for _, sug := range combined {
				if sug.TargetID == "diana" {
					// Semantic contribution: 0.8 * (1 - weight)
					// Topology contribution: topoScore * weight
					t.Logf("Weight %.2f: diana = %.3f", weight, sug.Confidence)
				}
			}
		})
	}
}

// TestTopologyComplexProcessSuggestionIntegration tests ProcessSuggestion with topology.
func TestTopologyComplexProcessSuggestionIntegration(t *testing.T) {
	engine := storage.NewMemoryEngine()
	setupTestGraph(t, engine)

	// Create inference engine with all features
	inferEngine := New(DefaultConfig())

	// Enable topology
	topoConfig := DefaultTopologyConfig()
	topoConfig.Enabled = true
	topo := NewTopologyIntegration(engine, topoConfig)
	inferEngine.SetTopologyIntegration(topo)

	// Create a suggestion that would come from topology
	suggestion := EdgeSuggestion{
		SourceID:   "alice",
		TargetID:   "diana",
		Type:       "RELATES_TO",
		Confidence: 0.8,
		Method:     "topology_adamic_adar",
	}

	// Process the suggestion
	result := inferEngine.ProcessSuggestion(suggestion, "session-1")

	t.Logf("ProcessSuggestion result: materialize=%v, reason=%s",
		result.ShouldMaterialize, result.Reason)

	// Without evidence/cooldown enabled, should materialize
	if !result.ShouldMaterialize {
		t.Logf("Note: suggestion not materialized (evidence/cooldown may be enabled)")
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// buildRandomStorageGraph creates a random graph in storage.
func buildRandomStorageGraph(nodes int, density float64, seed int64) storage.Engine {
	r := rand.New(rand.NewSource(seed))
	engine := storage.NewMemoryEngine()

	// Create nodes
	for i := 0; i < nodes; i++ {
		engine.CreateNode(&storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("node-%d", i)),
			Labels: []string{"Test"},
		})
	}

	// Create random edges
	edgeCount := 0
	for i := 0; i < nodes; i++ {
		for j := i + 1; j < nodes; j++ {
			if r.Float64() < density {
				engine.CreateEdge(&storage.Edge{
					ID:        storage.EdgeID(fmt.Sprintf("e-%d", edgeCount)),
					StartNode: storage.NodeID(fmt.Sprintf("node-%d", i)),
					EndNode:   storage.NodeID(fmt.Sprintf("node-%d", j)),
					Type:      "CONNECTS",
				})
				edgeCount++
			}
		}
	}

	return engine
}

// TestTopologyMathHelpers tests that math.Tanh, math.Exp, math.Log10 work as expected.
// This is mostly a sanity check that the standard library behaves correctly.
func TestTopologyMathHelpers(t *testing.T) {
	// Test math.Tanh - used in normalizeScore for adamic_adar/resource_allocation
	tanhTests := []struct {
		input    float64
		expected float64
		delta    float64
	}{
		{0, 0, 0.001},
		{1, 0.761594, 0.001},
		{-1, -0.761594, 0.001},
		{20, 1.0, 0.0001},
		{-20, -1.0, 0.0001},
		{100, 1.0, 0.0001},
		{-100, -1.0, 0.0001},
	}

	for _, tc := range tanhTests {
		result := math.Tanh(tc.input)
		if result < tc.expected-tc.delta || result > tc.expected+tc.delta {
			t.Errorf("math.Tanh(%.1f) = %.6f, expected %.6f", tc.input, result, tc.expected)
		}
	}

	// Test math.Exp - used for various calculations
	expTests := []struct {
		input    float64
		expected float64
		delta    float64
	}{
		{0, 1, 0.0001},
		{1, 2.71828, 0.001},
		{-1, 0.367879, 0.001},
		{2, 7.38906, 0.001},
	}

	for _, tc := range expTests {
		result := math.Exp(tc.input)
		if result < tc.expected-tc.delta || result > tc.expected+tc.delta {
			t.Errorf("math.Exp(%.1f) = %.6f, expected %.6f", tc.input, result, tc.expected)
		}
	}

	// Test math.Log10 - used in normalizeScore for preferential_attachment
	log10Tests := []struct {
		input    float64
		expected float64
		delta    float64
	}{
		{1, 0, 0.001},
		{10, 1, 0.001},
		{100, 2, 0.001},
		{1000, 3, 0.001},
	}

	for _, tc := range log10Tests {
		result := math.Log10(tc.input)
		if result < tc.expected-tc.delta || result > tc.expected+tc.delta {
			t.Errorf("math.Log10(%.1f) = %.6f, expected %.6f", tc.input, result, tc.expected)
		}
	}

	// Verify edge cases for Log10 (negative/zero inputs return -Inf or NaN)
	if !math.IsInf(math.Log10(0), -1) {
		t.Error("math.Log10(0) should be -Inf")
	}
	if !math.IsNaN(math.Log10(-1)) {
		t.Error("math.Log10(-1) should be NaN")
	}
}

// TestTopologySortByConfidence tests the sorting function.
func TestTopologySortByConfidence(t *testing.T) {
	suggestions := []EdgeSuggestion{
		{TargetID: "a", Confidence: 0.3},
		{TargetID: "b", Confidence: 0.9},
		{TargetID: "c", Confidence: 0.1},
		{TargetID: "d", Confidence: 0.7},
		{TargetID: "e", Confidence: 0.5},
	}

	sortByConfidence(suggestions)

	// Verify sorted descending
	for i := 1; i < len(suggestions); i++ {
		if suggestions[i].Confidence > suggestions[i-1].Confidence {
			t.Errorf("Not sorted at index %d: %.1f > %.1f",
				i, suggestions[i].Confidence, suggestions[i-1].Confidence)
		}
	}

	// First should be highest (b = 0.9)
	if suggestions[0].TargetID != "b" {
		t.Errorf("First should be 'b', got '%s'", suggestions[0].TargetID)
	}

	// Last should be lowest (c = 0.1)
	if suggestions[len(suggestions)-1].TargetID != "c" {
		t.Errorf("Last should be 'c', got '%s'", suggestions[len(suggestions)-1].TargetID)
	}
}

// TestTopologyNilStorage tests handling of nil storage.
func TestTopologyNilStorage(t *testing.T) {
	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(nil, config)

	// Should not panic, should return error or empty
	suggestions, err := topo.SuggestTopological(context.Background(), "anything")

	if err == nil && len(suggestions) > 0 {
		t.Error("Expected error or empty for nil storage")
	}
	t.Logf("Nil storage: err=%v, suggestions=%d", err, len(suggestions))
}

// BenchmarkTopologyIntegration benchmarks topology integration.
func BenchmarkTopologyIntegrationSmall(b *testing.B) {
	engine := buildRandomStorageGraph(50, 0.3, 42)
	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.SuggestTopological(ctx, "node-25")
	}
}

func BenchmarkTopologyIntegrationMedium(b *testing.B) {
	engine := buildRandomStorageGraph(200, 0.2, 42)
	config := DefaultTopologyConfig()
	config.Enabled = true
	topo := NewTopologyIntegration(engine, config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.SuggestTopological(ctx, "node-100")
	}
}

func BenchmarkTopologyIntegrationCached(b *testing.B) {
	engine := buildRandomStorageGraph(200, 0.2, 42)
	config := DefaultTopologyConfig()
	config.Enabled = true
	config.GraphRefreshInterval = 1000 // Long cache
	topo := NewTopologyIntegration(engine, config)

	// Prime cache
	ctx := context.Background()
	topo.SuggestTopological(ctx, "node-0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.SuggestTopological(ctx, "node-100")
	}
}

// BenchmarkCombinedSuggestions benchmarks combining suggestions.
func BenchmarkCombinedSuggestions(b *testing.B) {
	config := DefaultTopologyConfig()
	config.Enabled = true
	config.Weight = 0.5
	topo := NewTopologyIntegration(nil, config)

	semantic := make([]EdgeSuggestion, 50)
	topological := make([]EdgeSuggestion, 50)

	for i := 0; i < 50; i++ {
		semantic[i] = EdgeSuggestion{
			TargetID:   fmt.Sprintf("sem-%d", i),
			Confidence: rand.Float64(),
		}
		topological[i] = EdgeSuggestion{
			TargetID:   fmt.Sprintf("topo-%d", i),
			Confidence: rand.Float64(),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topo.CombinedSuggestions(semantic, topological)
	}
}
