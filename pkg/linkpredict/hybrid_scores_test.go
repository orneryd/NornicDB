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
	group := func(vectors ...[]float32) map[string][][]float32 { return map[string][][]float32{"default": vectors} }
	require.Equal(t, 0.0, BestCosineSimilarity(nil, group([]float32{1})))
	// Vectors of different lengths, or empty, are not compared.
	require.Equal(t, 0.0, BestCosineSimilarity(group([]float32{1, 0}, []float32{}), group([]float32{1, 0, 0}, []float32{})))
	// The best pair: here the second chunks match.
	require.InDelta(t, 1.0, BestCosineSimilarity(group([]float32{1, 0}, []float32{0, 1}), group([]float32{0, 0, 1}, []float32{0, 2})), 1e-6)
	// The best pair, even when it is negative.
	require.InDelta(t, -1.0, BestCosineSimilarity(group([]float32{1, 0}), group([]float32{-1, 0})), 1e-6)
	// Named embeddings compare with their namesake only: a title never
	// matches a content vector, whatever their lengths.
	a := map[string][][]float32{"title": {{1, 0}}, "content": {{0, 1}}}
	b := map[string][][]float32{"title": {{0, 1}}, "content": {{1, 0}}}
	require.InDelta(t, 0.0, BestCosineSimilarity(a, b), 1e-6)
	require.InDelta(t, 1.0, BestCosineSimilarity(a, map[string][][]float32{"content": {{0, 3}}}), 1e-6)
}

// TestBestCosineSimilarityBoundsLargeGroups: past maxCosinePairs pairs, each
// vector is compared with the other side's mean, so a strong match still
// shows, at a cost linear in the vectors.
func TestBestCosineSimilarityBoundsLargeGroups(t *testing.T) {
	many := func(count int, last []float32) [][]float32 {
		vectors := make([][]float32, count)
		for i := range vectors {
			vectors[i] = []float32{0, 0, 1}
		}
		vectors[count-1] = last
		return vectors
	}
	a := map[string][][]float32{"default": many(100, []float32{1, 0, 0})}
	b := map[string][][]float32{"default": many(100, []float32{0, 0, 1})}
	require.Greater(t, 100*100, maxCosinePairs)
	// b's mean is (0, 0, 1), which a's first chunks match exactly.
	require.InDelta(t, 1.0, BestCosineSimilarity(a, b), 1e-6)
	require.Equal(t, []float32(nil), meanVector(nil))
	require.Equal(t, []float32{1, 2}, meanVector([][]float32{{1, 2}, {9}, {1, 2}}))
}

// BenchmarkBestCosineSimilarity: two nodes of 300 chunks of 1024 dimensions,
// past the all-pairs bound.
func BenchmarkBestCosineSimilarity(b *testing.B) {
	chunks := func(seed float32) [][]float32 {
		vectors := make([][]float32, 300)
		for i := range vectors {
			vectors[i] = make([]float32, 1024)
			for j := range vectors[i] {
				vectors[i][j] = seed + float32((i*31+j*17)%97)
			}
		}
		return vectors
	}
	x, y := map[string][][]float32{"default": chunks(1)}, map[string][][]float32{"default": chunks(2)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		BestCosineSimilarity(x, y)
	}
}

// TestEnsembleScoresDontDependOnTopK: the ensemble combines every candidate
// of every algorithm before topK, so a pair's score is the same whatever topK
// cuts (#1186 review).
func TestEnsembleScoresDontDependOnTopK(t *testing.T) {
	graph := Graph{
		"a": {"b": {}, "d": {}},
		"b": {"a": {}, "c": {}, "e": {}},
		"c": {"b": {}},
		"d": {"a": {}, "e": {}},
		"e": {"b": {}, "d": {}, "f": {}},
		"f": {"e": {}},
	}
	config := DefaultHybridConfig()
	config.UseEnsemble = true
	config.NormalizeScores = false
	config.MinThreshold = 0
	all := NewHybridScorer(config).Predict(context.Background(), graph, "a", 0)
	scores := map[storage.NodeID]float64{}
	for _, p := range all {
		scores[p.TargetID] = p.TopologyScore
	}
	require.Greater(t, len(all), 1)
	for k := 1; k < len(all); k++ {
		for _, p := range NewHybridScorer(config).Predict(context.Background(), graph, "a", k) {
			require.InDelta(t, scores[p.TargetID], p.TopologyScore, 1e-12, "topK %d", k)
		}
	}
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
