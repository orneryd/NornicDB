package linkpredict

import (
	"context"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestPairScoreIsGDSs: GDS 2.13.12's gds.alpha.linkprediction.* values on
// a→a, a→b twice, b→c, c→b, where a degree counts relationships (#1192).
func TestPairScoreIsGDSs(t *testing.T) {
	neighbors := map[storage.NodeID]NodeSet{
		"a": {"b": {}},
		"b": {"a": {}, "c": {}},
		"c": {"b": {}},
	}
	degrees := map[storage.NodeID]int{"a": 3, "b": 4, "c": 2}
	degree := func(id storage.NodeID) int { return degrees[id] }
	score := func(algorithm string, x, y storage.NodeID) float64 {
		return PairScore(algorithm, x, y, neighbors[x], neighbors[y], degree)
	}
	require.InDelta(t, 1.0, score("common_neighbors", "a", "c"), 1e-12)
	require.InDelta(t, 1/math.Log(4), score("adamic_adar", "a", "c"), 1e-12)
	require.InDelta(t, 0.25, score("resource_allocation", "a", "c"), 1e-12)
	require.InDelta(t, 6.0, score("preferential_attachment", "a", "c"), 1e-12)
	require.InDelta(t, 1.0, score("total_neighbors", "a", "c"), 1e-12)
	require.InDelta(t, 1.0, score("jaccard", "a", "c"), 1e-12)
	// A node and itself share no neighbours; their union is the node's own.
	require.InDelta(t, 0.0, score("common_neighbors", "b", "b"), 1e-12)
	require.InDelta(t, 2.0, score("total_neighbors", "b", "b"), 1e-12)
	require.InDelta(t, 0.0, score("jaccard", "b", "b"), 1e-12)
	require.InDelta(t, 16.0, score("preferential_attachment", "b", "b"), 1e-12)
	// A common neighbour of degree 0 (as an OUTGOING read can give) adds
	// 1/ln 0 = -0, reported as 0.
	degrees["b"] = 0
	require.Equal(t, 0.0, score("adamic_adar", "a", "c"))
	require.False(t, math.Signbit(score("adamic_adar", "a", "c")))
}

// TestTopologyScoresTakeADegreeFunction: the degree-based scores read the
// given degrees; without one, the distinct neighbour count.
func TestTopologyScoresTakeADegreeFunction(t *testing.T) {
	graph := Graph{
		"a": {"b": {}},
		"b": {"a": {}, "c": {}},
		"c": {"b": {}},
	}
	require.InDelta(t, 1/math.Log(2), TopologyScores("adamic_adar", graph, nil, "a")["c"], 1e-12)
	four := func(storage.NodeID) int { return 4 }
	require.InDelta(t, 1/math.Log(4), TopologyScores("adamic_adar", graph, four, "a")["c"], 1e-12)
	require.InDelta(t, 0.25, TopologyScores("resource_allocation", graph, four, "a")["c"], 1e-12)
	require.InDelta(t, 16.0, TopologyScores("preferential_attachment", graph, four, "a")["c"], 1e-12)
	require.InDelta(t, 1/math.Log(4), TopologyScores("unknown", graph, four, "a")["c"], 1e-12)
	require.Nil(t, TopologyScores("adamic_adar", graph, four, "missing"))

	// Raw scores, best first, at most k; every one for k below 1.
	ranked := RankedPredictions(map[storage.NodeID]float64{"x": 9, "y": 2, "z": 5}, 2, "preferential_attachment")
	require.Equal(t, []storage.NodeID{"x", "z"}, []storage.NodeID{ranked[0].TargetID, ranked[1].TargetID})
	require.Equal(t, 9.0, ranked[0].Score)
	require.Len(t, RankedPredictions(map[storage.NodeID]float64{"x": 9, "y": 2}, 0, "x"), 2)
	require.Nil(t, RankedPredictions(nil, 2, "x"))

	// The hybrid scorer reads the same degrees.
	config := DefaultHybridConfig()
	config.TopologyAlgorithm = "resource_allocation"
	config.MinThreshold = 0
	config.NormalizeScores = false
	scorer := NewHybridScorer(config)
	scorer.SetDegreeFunction(four)
	predictions := scorer.Predict(context.Background(), graph, "a", 5)
	require.Len(t, predictions, 1)
	require.InDelta(t, math.Tanh(0.25/5), predictions[0].TopologyScore, 1e-12)
	config.TopologyAlgorithm = "unknown"
	require.Len(t, NewHybridScorer(config).Predict(context.Background(), graph, "a", 5), 1)
}
