package linkpredict

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Equal scores come in target ID order, and a top-K cut keeps the same
// candidates on every run, for each algorithm, the ensemble and the hybrid
// scorer: in a star graph every other spoke ties for a spoke.
func TestPredictionTiesComeInTargetOrder(t *testing.T) {
	graph := buildStarGraph(12)
	source := storage.NodeID("spoke-0")
	want := []storage.NodeID{"spoke-1", "spoke-10", "spoke-11"}
	targets := func(predictions []Prediction) []storage.NodeID {
		ids := make([]storage.NodeID, len(predictions))
		for i, prediction := range predictions {
			ids[i] = prediction.TargetID
		}
		return ids
	}
	hybridTargets := func(predictions []HybridPrediction) []storage.NodeID {
		ids := make([]storage.NodeID, len(predictions))
		for i, prediction := range predictions {
			ids[i] = prediction.TargetID
		}
		return ids
	}
	for run := 0; run < 20; run++ {
		for name, predict := range map[string]func(Graph, storage.NodeID, int) []Prediction{
			"common_neighbors":        CommonNeighbors,
			"jaccard":                 Jaccard,
			"adamic_adar":             AdamicAdar,
			"preferential_attachment": PreferentialAttachment,
			"resource_allocation":     ResourceAllocation,
		} {
			require.Equal(t, want, targets(predict(graph, source, 3)), name)
		}
		for _, ensemble := range []bool{false, true} {
			scorer := NewHybridScorer(HybridConfig{TopologyWeight: 1, TopologyAlgorithm: "common_neighbors", UseEnsemble: ensemble, NormalizeScores: true})
			require.Equal(t, want, hybridTargets(scorer.Predict(context.Background(), graph, source, 3)), "ensemble=%v", ensemble)
		}
	}
}

// An ensemble with no top-K (0) returns every prediction, as a single
// algorithm does, not none.
func TestEnsembleWithoutTopKReturnsEveryPrediction(t *testing.T) {
	graph := buildStarGraph(5)
	scorer := NewHybridScorer(HybridConfig{TopologyWeight: 1, UseEnsemble: true, NormalizeScores: true})
	require.Len(t, scorer.ensembleTopology(graph, "spoke-0", 0), 4)
	require.Len(t, scorer.Predict(context.Background(), graph, "spoke-0", 0), 4)
}
