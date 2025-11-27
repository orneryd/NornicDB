package linkpredict

import (
	"context"
	"math"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// HybridScorer combines topological and semantic link prediction signals.
//
// This is where topological algorithms (Jaccard, Adamic-Adar, etc.) meet
// NornicDB's existing semantic inference (embedding similarity, co-access).
//
// Usage Example:
//
//	scorer := linkpredict.NewHybridScorer(linkpredict.HybridConfig{
//		TopologyWeight: 0.5,
//		SemanticWeight: 0.5,
//		TopologyAlgorithm: "adamic_adar",
//	})
//
//	// Set semantic scoring function (from inference engine)
//	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
//		return getEmbeddingSimilarity(source, target)
//	})
//
//	// Get hybrid predictions
//	predictions := scorer.Predict(ctx, graph, "user-123", 10)
//
// Why Hybrid Matters:
//
// **Topology alone** captures structure but misses semantics:
//   - Two papers with many mutual citations (high topology score)
//   - But about completely different topics (low semantic score)
//   - Hybrid catches this mismatch
//
// **Semantics alone** captures meaning but misses social patterns:
//   - Two people with similar interests (high semantic score)
//   - But in completely different social circles (low topology score)
//   - Hybrid catches this too
//
// **Best of both worlds:**
//   - High topology + high semantic = very confident prediction
//   - High topology + low semantic = structurally likely, verify content fit
//   - Low topology + high semantic = semantically related, but socially distant
//   - Low topology + low semantic = no connection
//
// ELI12:
//
// Imagine recommending friends at school:
//
// **Topology**: "You and Sarah have 5 mutual friends" (structural signal)
// **Semantic**: "You and Sarah both love soccer and coding" (interest signal)
// **Hybrid**: "You should definitely meet Sarah!" (both signals agree)
//
// But also:
// **Topology only**: "You and Jake have 8 mutual friends" (strong structure)
// **Semantic**: "But Jake is really into art, not sports" (weak interest match)
// **Hybrid**: "Maybe you know Jake through school, but not sure you'd be close"
type HybridScorer struct {
	config          HybridConfig
	semanticScorer  SemanticScorerFunc
	ensembleWeights map[string]float64 // For ensemble topological scoring
}

// HybridConfig configures hybrid scoring behavior.
//
// Weights control the blend between topological and semantic signals:
//   - TopologyWeight = 1.0, SemanticWeight = 0.0 → Pure topology
//   - TopologyWeight = 0.0, SemanticWeight = 1.0 → Pure semantic
//   - TopologyWeight = 0.5, SemanticWeight = 0.5 → Balanced hybrid
//   - TopologyWeight = 0.7, SemanticWeight = 0.3 → Topology-dominant
//
// Example:
//
//	// Social network (structure matters more)
//	config := HybridConfig{
//		TopologyWeight:    0.7,
//		SemanticWeight:    0.3,
//		TopologyAlgorithm: "adamic_adar",
//	}
//
//	// Knowledge graph (semantics matter more)
//	config = HybridConfig{
//		TopologyWeight:    0.3,
//		SemanticWeight:    0.7,
//		TopologyAlgorithm: "jaccard",
//	}
//
//	// Balanced AI agent memory
//	config = HybridConfig{
//		TopologyWeight:    0.5,
//		SemanticWeight:    0.5,
//		TopologyAlgorithm: "adamic_adar",
//		UseEnsemble:       true, // Use all topology algorithms
//	}
type HybridConfig struct {
	// Weight for topological signal (0.0-1.0)
	TopologyWeight float64

	// Weight for semantic signal (0.0-1.0)
	SemanticWeight float64

	// Which topology algorithm to use
	// Options: "common_neighbors", "jaccard", "adamic_adar",
	//          "preferential_attachment", "resource_allocation"
	TopologyAlgorithm string

	// Use ensemble of all topology algorithms (ignores TopologyAlgorithm)
	UseEnsemble bool

	// Normalize scores to [0, 1] before blending
	NormalizeScores bool

	// Minimum threshold for combined score (0.0-1.0)
	MinThreshold float64
}

// DefaultHybridConfig returns balanced configuration for general use.
func DefaultHybridConfig() HybridConfig {
	return HybridConfig{
		TopologyWeight:    0.5,
		SemanticWeight:    0.5,
		TopologyAlgorithm: "adamic_adar",
		UseEnsemble:       false,
		NormalizeScores:   true,
		MinThreshold:      0.3,
	}
}

// SemanticScorerFunc computes semantic similarity between two nodes.
//
// This is typically implemented using embedding similarity, but could also
// incorporate co-access patterns, temporal proximity, etc.
//
// Parameters:
//   - ctx: Context for cancellation
//   - source: Source node ID
//   - target: Target node ID
//
// Returns:
//   - Similarity score (typically 0.0-1.0, but implementation-dependent)
//
// Example:
//
//	semanticScorer := func(ctx context.Context, source, target storage.NodeID) float64 {
//		sourceNode, _ := engine.GetNode(ctx, source)
//		targetNode, _ := engine.GetNode(ctx, target)
//		
//		if len(sourceNode.Embedding) == 0 || len(targetNode.Embedding) == 0 {
//			return 0.0
//		}
//		
//		return cosineSimilarity(sourceNode.Embedding, targetNode.Embedding)
//	}
type SemanticScorerFunc func(ctx context.Context, source, target storage.NodeID) float64

// NewHybridScorer creates a new hybrid scorer with the given configuration.
func NewHybridScorer(config HybridConfig) *HybridScorer {
	// Normalize weights if needed
	if config.TopologyWeight+config.SemanticWeight == 0 {
		config.TopologyWeight = 0.5
		config.SemanticWeight = 0.5
	}

	// Default ensemble weights (can be tuned based on algorithm performance)
	ensembleWeights := map[string]float64{
		"common_neighbors":        0.1,
		"jaccard":                 0.2,
		"adamic_adar":             0.3,
		"resource_allocation":     0.25,
		"preferential_attachment": 0.15,
	}

	return &HybridScorer{
		config:          config,
		ensembleWeights: ensembleWeights,
	}
}

// SetSemanticScorer sets the semantic scoring function.
//
// This must be called before using Predict(), or semantic scores will be 0.
//
// Example:
//
//	scorer.SetSemanticScorer(func(ctx context.Context, source, target storage.NodeID) float64 {
//		return inferenceEngine.GetSimilarity(source, target)
//	})
func (h *HybridScorer) SetSemanticScorer(fn SemanticScorerFunc) {
	h.semanticScorer = fn
}

// Predict computes hybrid link predictions for a source node.
//
// This method:
//  1. Runs topological algorithm(s) to get structural candidates
//  2. Computes semantic scores for those candidates
//  3. Blends scores according to configured weights
//  4. Returns top-K predictions sorted by hybrid score
//
// Parameters:
//   - ctx: Context for cancellation
//   - graph: Graph structure for topology algorithms
//   - source: Source node to predict edges from
//   - topK: Maximum number of predictions to return
//
// Returns:
//   - Sorted list of predictions with hybrid scores
//
// Example:
//
//	predictions := scorer.Predict(ctx, graph, "user-123", 10)
//	
//	for _, pred := range predictions {
//		fmt.Printf("→ %s: %.3f (topology: %.3f, semantic: %.3f)\n",
//			pred.TargetID, pred.Score,
//			pred.Metadata["topology_score"],
//			pred.Metadata["semantic_score"])
//	}
func (h *HybridScorer) Predict(ctx context.Context, graph Graph, source storage.NodeID, topK int) []HybridPrediction {
	// Get topological predictions
	var topologyPreds []Prediction

	if h.config.UseEnsemble {
		topologyPreds = h.ensembleTopology(graph, source, topK*3) // Get more for ensemble
	} else {
		topologyPreds = h.singleTopology(graph, source, topK*3)
	}

	if len(topologyPreds) == 0 {
		return nil
	}

	// Normalize topology scores if requested
	if h.config.NormalizeScores {
		topologyPreds = h.normalizeScores(topologyPreds)
	}

	// Compute semantic scores for candidates
	hybridPreds := make([]HybridPrediction, 0, len(topologyPreds))

	for _, topoPred := range topologyPreds {
		semanticScore := 0.0
		if h.semanticScorer != nil {
			semanticScore = h.semanticScorer(ctx, source, topoPred.TargetID)
		}

		// Blend scores
		hybridScore := h.config.TopologyWeight*topoPred.Score +
			h.config.SemanticWeight*semanticScore

		// Apply threshold
		if hybridScore < h.config.MinThreshold {
			continue
		}

		hybridPreds = append(hybridPreds, HybridPrediction{
			TargetID:       topoPred.TargetID,
			Score:          hybridScore,
			TopologyScore:  topoPred.Score,
			SemanticScore:  semanticScore,
			TopologyMethod: topoPred.Algorithm,
			Reason:         h.explainPrediction(topoPred.Score, semanticScore),
		})
	}

	// Sort by hybrid score
	sortPredictions(hybridPreds)

	// Return top K
	if topK > 0 && len(hybridPreds) > topK {
		hybridPreds = hybridPreds[:topK]
	}

	return hybridPreds
}

// HybridPrediction represents a prediction combining topology and semantics.
type HybridPrediction struct {
	TargetID       storage.NodeID
	Score          float64 // Combined hybrid score
	TopologyScore  float64 // Raw topology score
	SemanticScore  float64 // Raw semantic score
	TopologyMethod string  // Which topology algorithm was used
	Reason         string  // Human-readable explanation
}

// singleTopology runs a single topology algorithm.
func (h *HybridScorer) singleTopology(graph Graph, source storage.NodeID, k int) []Prediction {
	switch h.config.TopologyAlgorithm {
	case "common_neighbors":
		return CommonNeighbors(graph, source, k)
	case "jaccard":
		return Jaccard(graph, source, k)
	case "adamic_adar":
		return AdamicAdar(graph, source, k)
	case "preferential_attachment":
		return PreferentialAttachment(graph, source, k)
	case "resource_allocation":
		return ResourceAllocation(graph, source, k)
	default:
		// Default to Adamic-Adar
		return AdamicAdar(graph, source, k)
	}
}

// ensembleTopology combines multiple topology algorithms.
//
// This runs all 5 topology algorithms and blends their scores using
// configured weights. More robust than single algorithm, but slower.
func (h *HybridScorer) ensembleTopology(graph Graph, source storage.NodeID, k int) []Prediction {
	// Run all algorithms
	cn := CommonNeighbors(graph, source, k)
	jac := Jaccard(graph, source, k)
	aa := AdamicAdar(graph, source, k)
	pa := PreferentialAttachment(graph, source, k)
	ra := ResourceAllocation(graph, source, k)

	// Normalize each algorithm's scores independently
	cn = h.normalizeScores(cn)
	jac = h.normalizeScores(jac)
	aa = h.normalizeScores(aa)
	pa = h.normalizeScores(pa)
	ra = h.normalizeScores(ra)

	// Aggregate scores
	aggregated := make(map[storage.NodeID]float64)

	for _, pred := range cn {
		aggregated[pred.TargetID] += h.ensembleWeights["common_neighbors"] * pred.Score
	}
	for _, pred := range jac {
		aggregated[pred.TargetID] += h.ensembleWeights["jaccard"] * pred.Score
	}
	for _, pred := range aa {
		aggregated[pred.TargetID] += h.ensembleWeights["adamic_adar"] * pred.Score
	}
	for _, pred := range pa {
		aggregated[pred.TargetID] += h.ensembleWeights["preferential_attachment"] * pred.Score
	}
	for _, pred := range ra {
		aggregated[pred.TargetID] += h.ensembleWeights["resource_allocation"] * pred.Score
	}

	// Convert to predictions
	predictions := make([]Prediction, 0, len(aggregated))
	for nodeID, score := range aggregated {
		predictions = append(predictions, Prediction{
			TargetID:  nodeID,
			Score:     score,
			Algorithm: "ensemble",
			Reason:    "Ensemble of 5 topology algorithms",
		})
	}

	// Sort and return
	sortTopoPredictions(predictions)
	if len(predictions) > k {
		predictions = predictions[:k]
	}

	return predictions
}

// normalizeScores normalizes prediction scores to [0, 1] range.
func (h *HybridScorer) normalizeScores(predictions []Prediction) []Prediction {
	if len(predictions) == 0 {
		return predictions
	}

	// Find min and max
	minScore := predictions[0].Score
	maxScore := predictions[0].Score

	for _, pred := range predictions {
		if pred.Score < minScore {
			minScore = pred.Score
		}
		if pred.Score > maxScore {
			maxScore = pred.Score
		}
	}

	// Normalize
	scoreRange := maxScore - minScore
	if scoreRange == 0 {
		// All scores equal - set to 1.0
		for i := range predictions {
			predictions[i].Score = 1.0
		}
		return predictions
	}

	for i := range predictions {
		predictions[i].Score = (predictions[i].Score - minScore) / scoreRange
	}

	return predictions
}

// explainPrediction generates human-readable explanation.
func (h *HybridScorer) explainPrediction(topoScore, semanticScore float64) string {
	// Classify scores
	topoHigh := topoScore > 0.6
	semanticHigh := semanticScore > 0.6

	switch {
	case topoHigh && semanticHigh:
		return "Strong structural connection and semantic similarity"
	case topoHigh && !semanticHigh:
		return "Strong structural connection, moderate semantic match"
	case !topoHigh && semanticHigh:
		return "Weak structural connection, strong semantic similarity"
	default:
		return "Moderate structural and semantic signals"
	}
}

// sortPredictions sorts hybrid predictions by score descending.
func sortPredictions(predictions []HybridPrediction) {
	// Simple bubble sort for small lists
	for i := 0; i < len(predictions); i++ {
		for j := i + 1; j < len(predictions); j++ {
			if predictions[j].Score > predictions[i].Score {
				predictions[i], predictions[j] = predictions[j], predictions[i]
			}
		}
	}
}

// sortTopoPredictions sorts topology predictions by score descending.
func sortTopoPredictions(predictions []Prediction) {
	for i := 0; i < len(predictions); i++ {
		for j := i + 1; j < len(predictions); j++ {
			if predictions[j].Score > predictions[i].Score {
				predictions[i], predictions[j] = predictions[j], predictions[i]
			}
		}
	}
}

// CosineSimilarity computes cosine similarity between two embeddings.
//
// This is a utility function for semantic scoring. Returns value in [-1, 1],
// where 1 = identical, 0 = orthogonal, -1 = opposite.
//
// Example:
//
//	scorer := func(ctx context.Context, source, target storage.NodeID) float64 {
//		sourceEmb := getEmbedding(source)
//		targetEmb := getEmbedding(target)
//		return linkpredict.CosineSimilarity(sourceEmb, targetEmb)
//	}
func CosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0.0
	}

	dotProduct := 0.0
	normA := 0.0
	normB := 0.0

	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0.0
	}

	return dotProduct / (math.Sqrt(normA) * math.Sqrt(normB))
}
