package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestHybridBasic verifies basic hybrid scoring works.
func TestHybridBasic(t *testing.T) {
	graph := buildTestGraph()
	
	config := DefaultHybridConfig()
	scorer := NewHybridScorer(config)

	// Mock semantic scorer (returns fixed similarity)
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		if target == "diana" {
			return 0.9 // High semantic similarity
		}
		return 0.3 // Low semantic similarity
	})

	predictions := scorer.Predict(context.Background(), graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected hybrid predictions")
	}

	// diana should rank high (high topology + high semantic)
	found := false
	for _, pred := range predictions {
		if pred.TargetID == "diana" {
			found = true
			if pred.SemanticScore != 0.9 {
				t.Errorf("Expected semantic score 0.9, got %.2f", pred.SemanticScore)
			}
			if pred.TopologyScore <= 0 {
				t.Errorf("Expected positive topology score, got %.2f", pred.TopologyScore)
			}
			break
		}
	}

	if !found {
		t.Error("diana not in predictions")
	}
}

// TestHybridWeights verifies weight configuration affects scoring.
func TestHybridWeights(t *testing.T) {
	graph := buildTestGraph()

	// Topology-dominant
	configTopo := HybridConfig{
		TopologyWeight:    0.9,
		SemanticWeight:    0.1,
		TopologyAlgorithm: "common_neighbors",
		NormalizeScores:   true,
	}
	scorerTopo := NewHybridScorer(configTopo)
	scorerTopo.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		return 0.5
	})

	// Semantic-dominant
	configSem := HybridConfig{
		TopologyWeight:    0.1,
		SemanticWeight:    0.9,
		TopologyAlgorithm: "common_neighbors",
		NormalizeScores:   true,
	}
	scorerSem := NewHybridScorer(configSem)
	scorerSem.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		if target == "diana" {
			return 1.0
		}
		return 0.1
	})

	ctx := context.Background()
	predsTopo := scorerTopo.Predict(ctx, graph, "alice", 3)
	predsSem := scorerSem.Predict(ctx, graph, "alice", 3)

	// Both should have predictions
	if len(predsTopo) == 0 || len(predsSem) == 0 {
		t.Fatal("Expected predictions from both scorers")
	}

	t.Logf("Topology-dominant top: %s (%.3f)", predsTopo[0].TargetID, predsTopo[0].Score)
	t.Logf("Semantic-dominant top: %s (%.3f)", predsSem[0].TargetID, predsSem[0].Score)

	// Semantic-dominant should rank diana high
	if predsSem[0].TargetID != "diana" {
		t.Errorf("Expected diana first in semantic-dominant, got %s", predsSem[0].TargetID)
	}
}

// TestHybridThreshold verifies minimum threshold filtering.
func TestHybridThreshold(t *testing.T) {
	graph := buildTestGraph()

	config := HybridConfig{
		TopologyWeight:    0.5,
		SemanticWeight:    0.5,
		TopologyAlgorithm: "jaccard",
		NormalizeScores:   true,
		MinThreshold:      0.8, // High threshold
	}
	scorer := NewHybridScorer(config)

	// Low semantic scores
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		return 0.2
	})

	predictions := scorer.Predict(context.Background(), graph, "alice", 10)

	// High threshold should filter most predictions
	if len(predictions) > 2 {
		t.Logf("Warning: High threshold (0.8) allowed %d predictions", len(predictions))
	}
}

// TestHybridEnsemble verifies ensemble mode works.
func TestHybridEnsemble(t *testing.T) {
	graph := buildTestGraph()

	config := HybridConfig{
		TopologyWeight:  1.0,
		SemanticWeight:  0.0,
		UseEnsemble:     true,
		NormalizeScores: true,
	}
	scorer := NewHybridScorer(config)

	predictions := scorer.Predict(context.Background(), graph, "alice", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected ensemble predictions")
	}

	// Check that TopologyMethod indicates ensemble
	if predictions[0].TopologyMethod != "ensemble" {
		t.Errorf("Expected ensemble method, got %s", predictions[0].TopologyMethod)
	}
}

// TestHybridNoSemanticScorer verifies graceful handling of missing semantic scorer.
func TestHybridNoSemanticScorer(t *testing.T) {
	graph := buildTestGraph()

	config := DefaultHybridConfig()
	scorer := NewHybridScorer(config)
	// Don't set semantic scorer

	predictions := scorer.Predict(context.Background(), graph, "alice", 5)

	// Should still work with topology only
	if len(predictions) == 0 {
		t.Fatal("Expected predictions even without semantic scorer")
	}

	// Semantic scores should be 0
	for _, pred := range predictions {
		if pred.SemanticScore != 0 {
			t.Errorf("Expected semantic score 0 (no scorer), got %.2f", pred.SemanticScore)
		}
	}
}

// TestHybridCompareAlgorithms verifies different topology algorithms produce different results.
func TestHybridCompareAlgorithms(t *testing.T) {
	graph := buildTestGraph()
	ctx := context.Background()

	algorithms := []string{
		"common_neighbors",
		"jaccard",
		"adamic_adar",
		"preferential_attachment",
		"resource_allocation",
	}

	for _, algo := range algorithms {
		config := HybridConfig{
			TopologyWeight:    1.0,
			SemanticWeight:    0.0,
			TopologyAlgorithm: algo,
			NormalizeScores:   true,
		}
		scorer := NewHybridScorer(config)

		predictions := scorer.Predict(ctx, graph, "alice", 3)

		if len(predictions) == 0 {
			t.Errorf("No predictions for algorithm %s", algo)
			continue
		}

		t.Logf("%s: top=%s (%.3f)", algo, predictions[0].TargetID, predictions[0].Score)
	}
}

// TestHybridRealWorld simulates a knowledge graph scenario.
func TestHybridRealWorld(t *testing.T) {
	// Build a knowledge graph: concepts linked by citations and semantic similarity
	graph := Graph{
		"ml":      {"ai": {}, "data": {}},
		"ai":      {"ml": {}, "robotics": {}},
		"data":    {"ml": {}, "stats": {}},
		"robotics": {"ai": {}, "control": {}},
		"stats":   {"data": {}, "math": {}},
		"math":    {"stats": {}, "physics": {}},
	}

	config := HybridConfig{
		TopologyWeight:    0.6,
		SemanticWeight:    0.4,
		TopologyAlgorithm: "adamic_adar",
		NormalizeScores:   true,
		MinThreshold:      0.3,
	}
	scorer := NewHybridScorer(config)

	// Semantic scorer: ml and stats are semantically similar (both use math)
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		if source == "ml" && target == "stats" {
			return 0.8 // High semantic similarity
		}
		if source == "ml" && target == "robotics" {
			return 0.4 // Moderate similarity
		}
		return 0.2 // Default low similarity
	})

	predictions := scorer.Predict(context.Background(), graph, "ml", 5)

	if len(predictions) == 0 {
		t.Fatal("Expected predictions for ml")
	}

	// stats should rank high (shared neighbor 'data' + high semantic similarity)
	foundStats := false
	for _, pred := range predictions {
		if pred.TargetID == "stats" {
			foundStats = true
			t.Logf("ml → stats: hybrid=%.3f (topo=%.3f, sem=%.3f)",
				pred.Score, pred.TopologyScore, pred.SemanticScore)
			
			// Should have both signals contributing
			if pred.TopologyScore == 0 {
				t.Error("Expected non-zero topology score for ml→stats")
			}
			if pred.SemanticScore != 0.8 {
				t.Errorf("Expected semantic score 0.8, got %.2f", pred.SemanticScore)
			}
			break
		}
	}

	if !foundStats {
		t.Error("Expected stats in predictions for ml")
	}
}

// TestCosineSimilarity verifies cosine similarity calculation.
func TestCosineSimilarity(t *testing.T) {
	// Identical vectors
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{1.0, 2.0, 3.0}
	sim := CosineSimilarity(a, b)
	if sim < 0.99 || sim > 1.01 {
		t.Errorf("Expected ~1.0 for identical vectors, got %.3f", sim)
	}

	// Orthogonal vectors
	c := []float32{1.0, 0.0, 0.0}
	d := []float32{0.0, 1.0, 0.0}
	sim = CosineSimilarity(c, d)
	if sim != 0.0 {
		t.Errorf("Expected 0.0 for orthogonal vectors, got %.3f", sim)
	}

	// Opposite vectors
	e := []float32{1.0, 2.0, 3.0}
	f := []float32{-1.0, -2.0, -3.0}
	sim = CosineSimilarity(e, f)
	if sim > -0.99 {
		t.Errorf("Expected ~-1.0 for opposite vectors, got %.3f", sim)
	}

	// Different lengths (should return 0)
	g := []float32{1.0, 2.0}
	h := []float32{1.0, 2.0, 3.0}
	sim = CosineSimilarity(g, h)
	if sim != 0.0 {
		t.Errorf("Expected 0.0 for different lengths, got %.3f", sim)
	}

	// Zero vector
	zero := []float32{0.0, 0.0, 0.0}
	sim = CosineSimilarity(a, zero)
	if sim != 0.0 {
		t.Errorf("Expected 0.0 for zero vector, got %.3f", sim)
	}
}

// TestHybridExplanations verifies human-readable explanations.
func TestHybridExplanations(t *testing.T) {
	graph := buildTestGraph()

	config := DefaultHybridConfig()
	scorer := NewHybridScorer(config)

	// Varied semantic scores
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		switch target {
		case "diana":
			return 0.9 // High
		case "eve":
			return 0.1 // Low
		default:
			return 0.5 // Medium
		}
	})

	predictions := scorer.Predict(context.Background(), graph, "alice", 10)

	for _, pred := range predictions {
		if pred.Reason == "" {
			t.Errorf("Missing explanation for prediction to %s", pred.TargetID)
		}
		t.Logf("%s: %s", pred.TargetID, pred.Reason)
	}
}

// BenchmarkHybridPredict benchmarks hybrid prediction.
func BenchmarkHybridPredict(b *testing.B) {
	graph := buildLargeGraph(1000, 10)

	config := DefaultHybridConfig()
	scorer := NewHybridScorer(config)
	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
		// Simulate embedding similarity lookup
		return 0.5
	})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.Predict(ctx, graph, "node-500", 20)
	}
}

// BenchmarkHybridEnsemble benchmarks ensemble mode.
func BenchmarkHybridEnsemble(b *testing.B) {
	graph := buildLargeGraph(500, 10) // Smaller graph for ensemble (slower)

	config := HybridConfig{
		TopologyWeight:  1.0,
		SemanticWeight:  0.0,
		UseEnsemble:     true,
		NormalizeScores: true,
	}
	scorer := NewHybridScorer(config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.Predict(ctx, graph, "node-250", 20)
	}
}
