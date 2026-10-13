package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestExplainPredictionFollowsTheScores (#1186).
func TestExplainPredictionFollowsTheScores(t *testing.T) {
	h := NewHybridScorer(DefaultHybridConfig())
	for _, tc := range []struct {
		topology, semantic float64
		want               string
	}{
		{0.9, 0, "Strong structural connection, no semantic match"},
		{0.9, 0.7, "Strong structural connection, strong semantic similarity"},
		{0.5, 0.4, "Moderate structural connection, moderate semantic similarity"},
		{0.1, 0.2, "Weak structural connection, weak semantic similarity"},
		{0, -0.3, "No structural connection, no semantic match"},
	} {
		require.Equal(t, tc.want, h.explainPrediction(tc.topology, tc.semantic))
	}
}

func TestBestCosineSimilarity(t *testing.T) {
	require.Equal(t, 0.0, BestCosineSimilarity(nil, [][]float32{{1}}))
	// Vectors of different lengths, or empty, are not compared.
	require.Equal(t, 0.0, BestCosineSimilarity([][]float32{{1, 0}, {}}, [][]float32{{1, 0, 0}, {}}))
	require.InDelta(t, 1.0, BestCosineSimilarity([][]float32{{1, 0}, {0, 1}}, [][]float32{{0, 0, 1}, {0, 2}}), 1e-6)
	// The best pair, even when it is negative.
	require.InDelta(t, -1.0, BestCosineSimilarity([][]float32{{1, 0}}, [][]float32{{-1, 0}}), 1e-6)
}

// TestEnsembleKeepsTheAlgorithmScalesWithoutNormalizing: without
// NormalizeScores, the ensemble's topology score is the weighted sum of each
// algorithm's own score, not rescaled among the candidates (#1186); with
// one candidate, rescaling would make every algorithm's score 1.
func TestEnsembleKeepsTheAlgorithmScalesWithoutNormalizing(t *testing.T) {
	graph := Graph{
		"a": {"b": {}},
		"b": {"a": {}, "c": {}},
		"c": {"b": {}},
	}
	config := DefaultHybridConfig()
	config.UseEnsemble = true
	config.NormalizeScores = false
	config.MinThreshold = 0
	scorer := NewHybridScorer(config)
	predictions := scorer.Predict(context.Background(), graph, storage.NodeID("a"), 10)
	require.Len(t, predictions, 1)
	want := 0.0
	for name, algorithm := range map[string]func(Graph, storage.NodeID, int) []Prediction{
		"common_neighbors": CommonNeighbors, "jaccard": Jaccard, "adamic_adar": AdamicAdar,
		"resource_allocation": ResourceAllocation, "preferential_attachment": PreferentialAttachment,
	} {
		for _, prediction := range algorithm(graph, "a", 0) {
			want += scorer.ensembleWeights[name] * prediction.Score
		}
	}
	require.InDelta(t, want, predictions[0].TopologyScore, 1e-12)
	require.Less(t, predictions[0].TopologyScore, 1.0)
}
