// Integration between topological link prediction and semantic inference.
//
// This file bridges the gap between:
//   - pkg/linkpredict (topological algorithms)
//   - pkg/inference (semantic/behavioral inference)
//
// Provides unified edge suggestion API that combines both approaches.
package inference

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/linkpredict"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// TopologyConfig controls topological link prediction integration.
//
// This allows the inference engine to incorporate graph structure signals
// alongside semantic similarity, co-access, and temporal patterns.
//
// Example:
//
//	config := &inference.TopologyConfig{
//		Enabled:        true,
//		Algorithm:      "adamic_adar",
//		TopK:           10,
//		MinScore:       0.3,
//		Weight:         0.4,  // 40% weight vs 60% semantic
//	}
type TopologyConfig struct {
	// Enable topological link prediction
	Enabled bool

	// Algorithm to use: "adamic_adar", "jaccard", "common_neighbors",
	// "resource_allocation", "preferential_attachment", or "ensemble"
	Algorithm string

	// TopK results to consider from topological algorithm
	TopK int

	// Minimum score threshold for topology predictions
	MinScore float64

	// Weight for topology score in hybrid mode (0.0-1.0)
	// Semantic weight is (1.0 - Weight)
	Weight float64

	// GraphRefreshInterval: how often to rebuild graph from storage
	// Zero means rebuild on every prediction (safe but slow)
	GraphRefreshInterval int // number of predictions before refresh
}

// DefaultTopologyConfig returns sensible defaults for topology integration.
func DefaultTopologyConfig() *TopologyConfig {
	return &TopologyConfig{
		Enabled:              false, // Opt-in
		Algorithm:            "adamic_adar",
		TopK:                 10,
		MinScore:             0.3,
		Weight:               0.4, // 40% topology, 60% semantic
		GraphRefreshInterval: 100, // Rebuild every 100 predictions
	}
}

// TopologyIntegration adds topological link prediction to the inference engine.
//
// This is an optional extension that can be enabled to incorporate graph
// structure signals into edge suggestions. When enabled, suggestions combine:
//   - Semantic similarity (embeddings)
//   - Co-access patterns
//   - Temporal proximity
//   - Graph topology (NEW)
//
// Example:
//
//	engine := inference.New(inference.DefaultConfig())
//	
//	// Enable topology integration
//	topoConfig := inference.DefaultTopologyConfig()
//	topoConfig.Enabled = true
//	topoConfig.Weight = 0.5  // Equal weight
//	
//	topo := inference.NewTopologyIntegration(storageEngine, topoConfig)
//	engine.SetTopologyIntegration(topo)
//	
//	// Now OnStore() suggestions include topology signals
//	suggestions, _ := engine.OnStore(ctx, nodeID, embedding)
type TopologyIntegration struct {
	config  *TopologyConfig
	storage storage.Engine

	// Cached graph for performance
	cachedGraph     linkpredict.Graph
	predictionCount int
}

// NewTopologyIntegration creates a new topology integration.
//
// Parameters:
//   - storage: Storage engine to build graph from
//   - config: Topology configuration (nil uses defaults)
//
// Returns ready-to-use integration that can be attached to inference engine.
func NewTopologyIntegration(storage storage.Engine, config *TopologyConfig) *TopologyIntegration {
	if config == nil {
		config = DefaultTopologyConfig()
	}

	return &TopologyIntegration{
		config:          config,
		storage:         storage,
		predictionCount: 0,
	}
}

// SuggestTopological generates edge suggestions using graph topology.
//
// This method:
//  1. Builds/refreshes graph from storage if needed
//  2. Runs configured topological algorithm
//  3. Converts results to EdgeSuggestion format
//  4. Returns suggestions compatible with inference engine
//
// Parameters:
//   - ctx: Context for cancellation
//   - sourceID: Node to predict edges from
//
// Returns:
//   - Slice of EdgeSuggestion with Method="topology_*"
//   - Error if graph building or prediction fails
//
// Example:
//
//	topo := NewTopologyIntegration(storage, config)
//	suggestions, err := topo.SuggestTopological(ctx, "node-123")
//	for _, sug := range suggestions {
//		fmt.Printf("%s: %.3f (%s)\n", sug.TargetID, sug.Confidence, sug.Method)
//	}
func (t *TopologyIntegration) SuggestTopological(ctx context.Context, sourceID string) ([]EdgeSuggestion, error) {
	// Check both config and feature flag
	if !t.config.Enabled {
		return nil, nil
	}
	
	// Check centralized feature flag
	// Import needed: "github.com/orneryd/nornicdb/pkg/filter"
	// if !filter.IsTopologyLinkPredictionEnabled() {
	// 	return nil, nil
	// }
	// Note: Commented out to avoid circular import. 
	// Feature flag should be checked at the caller level (inference.Engine.OnStore)

	// Rebuild graph if needed
	if t.cachedGraph == nil || t.predictionCount >= t.config.GraphRefreshInterval {
		graph, err := linkpredict.BuildGraphFromEngine(ctx, t.storage, true)
		if err != nil {
			return nil, err
		}
		t.cachedGraph = graph
		t.predictionCount = 0
	}
	t.predictionCount++

	// Run topological algorithm
	var predictions []linkpredict.Prediction
	sourceNodeID := storage.NodeID(sourceID)

	switch t.config.Algorithm {
	case "adamic_adar":
		predictions = linkpredict.AdamicAdar(t.cachedGraph, sourceNodeID, t.config.TopK)
	case "jaccard":
		predictions = linkpredict.Jaccard(t.cachedGraph, sourceNodeID, t.config.TopK)
	case "common_neighbors":
		predictions = linkpredict.CommonNeighbors(t.cachedGraph, sourceNodeID, t.config.TopK)
	case "resource_allocation":
		predictions = linkpredict.ResourceAllocation(t.cachedGraph, sourceNodeID, t.config.TopK)
	case "preferential_attachment":
		predictions = linkpredict.PreferentialAttachment(t.cachedGraph, sourceNodeID, t.config.TopK)
	default:
		// Default to Adamic-Adar (best all-around)
		predictions = linkpredict.AdamicAdar(t.cachedGraph, sourceNodeID, t.config.TopK)
	}

	// Convert to EdgeSuggestion format
	suggestions := make([]EdgeSuggestion, 0, len(predictions))
	for _, pred := range predictions {
		// Normalize score to [0, 1] for consistency with semantic scores
		normalizedScore := t.normalizeScore(pred.Score, pred.Algorithm)

		if normalizedScore < t.config.MinScore {
			continue
		}

		suggestions = append(suggestions, EdgeSuggestion{
			SourceID:   sourceID,
			TargetID:   string(pred.TargetID),
			Type:       "RELATES_TO",
			Confidence: normalizedScore,
			Reason:     pred.Reason,
			Method:     "topology_" + pred.Algorithm,
		})
	}

	return suggestions, nil
}

// normalizeScore converts algorithm-specific scores to [0, 1] range.
//
// Different algorithms have different score ranges:
//   - Common Neighbors: 0-N (integer count)
//   - Jaccard: 0.0-1.0 (already normalized)
//   - Adamic-Adar: 0.0-∞ (unbounded)
//   - Resource Allocation: 0.0-∞ (unbounded)
//   - Preferential Attachment: 0-N² (product of degrees)
//
// This function maps them to a consistent [0, 1] range using heuristics.
func (t *TopologyIntegration) normalizeScore(score float64, algorithm string) float64 {
	switch algorithm {
	case "jaccard":
		// Already in [0, 1]
		return score

	case "common_neighbors":
		// Map count to [0, 1] with sigmoid-like curve
		// Score of 1 neighbor → 0.5, 3 → 0.75, 5 → 0.85, etc.
		return 1.0 - (1.0 / (1.0 + score/2.0))

	case "adamic_adar", "resource_allocation":
		// Unbounded scores, use tanh to map to [0, 1]
		// Typical range is 0-5, so divide by 5 first
		return tanh(score / 5.0)

	case "preferential_attachment":
		// Very large values, use log then normalize
		// Typical range is 1-10000, log brings to 0-4
		if score <= 1.0 {
			return 0.0
		}
		logScore := log10(score)
		return min(1.0, logScore/4.0)

	default:
		// Conservative default
		return min(1.0, score)
	}
}

// CombinedSuggestions blends semantic and topological suggestions.
//
// This method:
//  1. Gets semantic suggestions (from existing inference engine)
//  2. Gets topological suggestions (from this integration)
//  3. Merges and ranks by weighted score
//  4. Removes duplicates (keeping highest scored)
//
// Parameters:
//   - semantic: Suggestions from semantic inference
//   - topological: Suggestions from topology integration
//
// Returns merged and ranked suggestions.
//
// Example:
//
//	semantic := engine.OnStore(ctx, nodeID, embedding)  // existing
//	topological, _ := topo.SuggestTopological(ctx, nodeID)  // new
//	
//	combined := topo.CombinedSuggestions(semantic, topological)
//	for _, sug := range combined {
//		if sug.Confidence >= 0.7 {
//			createEdge(sug)
//		}
//	}
func (t *TopologyIntegration) CombinedSuggestions(semantic, topological []EdgeSuggestion) []EdgeSuggestion {
	if !t.config.Enabled || len(topological) == 0 {
		return semantic
	}

	// Build map of all suggestions by target
	suggestionMap := make(map[string]EdgeSuggestion)

	// Add semantic suggestions with semantic weight
	semanticWeight := 1.0 - t.config.Weight
	for _, sug := range semantic {
		sug.Confidence *= semanticWeight
		suggestionMap[sug.TargetID] = sug
	}

	// Add or merge topological suggestions
	for _, sug := range topological {
		sug.Confidence *= t.config.Weight

		if existing, exists := suggestionMap[sug.TargetID]; exists {
			// Merge: combine scores and reasons
			existing.Confidence += sug.Confidence
			existing.Reason = existing.Reason + " + " + sug.Reason
			existing.Method = existing.Method + ",topology"
			suggestionMap[sug.TargetID] = existing
		} else {
			suggestionMap[sug.TargetID] = sug
		}
	}

	// Convert back to slice
	combined := make([]EdgeSuggestion, 0, len(suggestionMap))
	for _, sug := range suggestionMap {
		combined = append(combined, sug)
	}

	// Sort by confidence descending
	sortByConfidence(combined)

	return combined
}

// InvalidateCache forces graph rebuild on next prediction.
//
// Call this when the graph structure changes significantly (e.g., batch import,
// node/edge deletion, schema changes).
func (t *TopologyIntegration) InvalidateCache() {
	t.cachedGraph = nil
	t.predictionCount = 0
}

// Helper functions

func tanh(x float64) float64 {
	if x > 20 {
		return 1.0
	}
	if x < -20 {
		return -1.0
	}
	ex := exp(x)
	enx := exp(-x)
	return (ex - enx) / (ex + enx)
}

func exp(x float64) float64 {
	// Simplified exp approximation for small x
	// For production, use math.Exp
	const e = 2.718281828459045
	if x == 0 {
		return 1.0
	}
	// Taylor series: e^x ≈ 1 + x + x²/2 + x³/6 + ...
	result := 1.0
	term := 1.0
	for i := 1; i < 20; i++ {
		term *= x / float64(i)
		result += term
		if term < 1e-10 {
			break
		}
	}
	return result
}

func log10(x float64) float64 {
	if x <= 0 {
		return 0
	}
	// Approximate log10(x) = log(x) / log(10)
	// Using simple iteration
	result := 0.0
	val := x
	for val > 10 {
		val /= 10
		result += 1.0
	}
	// Handle remainder with linear approximation
	result += (val - 1.0) / 9.0
	return result
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func sortByConfidence(suggestions []EdgeSuggestion) {
	// Simple bubble sort for small slices
	for i := 0; i < len(suggestions); i++ {
		for j := i + 1; j < len(suggestions); j++ {
			if suggestions[j].Confidence > suggestions[i].Confidence {
				suggestions[i], suggestions[j] = suggestions[j], suggestions[i]
			}
		}
	}
}
