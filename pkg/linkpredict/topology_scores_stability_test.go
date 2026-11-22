package linkpredict

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// stableNodeSet names one node per given degree, so the same multiset of
// contributions can be attached to differently named nodes.
func stableNodeSet(prefix string, degrees []int) (NodeSet, map[storage.NodeID]int) {
	set := make(NodeSet, len(degrees))
	byNode := make(map[storage.NodeID]int, len(degrees))
	for index, degree := range degrees {
		node := storage.NodeID(fmt.Sprintf("%s-%02d", prefix, index))
		set[node] = struct{}{}
		byNode[node] = degree
	}
	return set, byNode
}

// degreeOrderedSum is the score the accumulating algorithms must produce: one
// term per common-neighbour degree, added from the smallest degree up. The
// result therefore depends on the multiset of contributions, not on which node
// carries a degree or on Go's map iteration order.
func degreeOrderedSum(algorithm string, degrees []int) float64 {
	ordered := slices.Clone(degrees)
	slices.Sort(ordered)
	sum := 0.0
	for _, degree := range ordered {
		if algorithm == "adamic_adar" {
			sum += 1 / math.Log(float64(degree))
		} else {
			sum += 1 / float64(degree)
		}
	}
	return sum
}

// TestPairScoreAddsCommonNeighbourTermsInDegreeOrder pins the accumulation
// order of the scores that sum one term per common neighbour. The same degree
// multiset under different node identities must give the same score bits on
// every call, so identical contribution multisets stay exact ties and the
// ranked target-ID tie key can decide their order.
func TestPairScoreAddsCommonNeighbourTermsInDegreeOrder(t *testing.T) {
	const runs = 64
	layouts := []struct {
		name    string
		prefix  string
		degrees []int
	}{
		{"three-ascending", "asc", []int{4, 5, 6}},
		{"three-descending", "desc", []int{6, 5, 4}},
		{"eight-mixed", "mix", []int{13, 4, 17, 6, 5, 11, 9, 7}},
	}
	for _, size := range []int{32, 33, 256} {
		degrees := make([]int, size)
		for i := range degrees {
			degrees[i] = 3 + (i*17)%size
		}
		layouts = append(layouts, struct {
			name, prefix string
			degrees      []int
		}{fmt.Sprintf("size-%d", size), "large", degrees})
	}
	for _, algorithm := range []string{"adamic_adar", "resource_allocation"} {
		for _, layout := range layouts {
			t.Run(algorithm+"/"+layout.name, func(t *testing.T) {
				common, degreesByNode := stableNodeSet(layout.prefix, layout.degrees)
				degree := func(node storage.NodeID) int { return degreesByNode[node] }
				want := math.Float64bits(degreeOrderedSum(algorithm, layout.degrees))
				for run := 0; run < runs; run++ {
					got := PairScore(algorithm, "s", "x", common, common, degree)
					require.Equal(t, want, math.Float64bits(got),
						"run %d: the score must be the same bits for this degree multiset", run)
				}
			})
		}
	}
}

// TestTopologyScoresEqualContributionMultisetsTie is the public topology owner
// case: two candidates reach the source through differently named hubs of the
// same degrees 4, 5 and 6, so their Adamic-Adar scores are the same multiset of
// contributions. They must stay exact ties, and the top-1 cut must then keep the
// smaller target node ID.
func TestTopologyScoresEqualContributionMultisetsTie(t *testing.T) {
	const runs = 32
	source := storage.NodeID("s")
	graph := Graph{source: NodeSet{}}
	degrees := map[storage.NodeID]int{source: 6}
	for _, side := range []struct {
		candidate storage.NodeID
		prefix    string
		degrees   []int
	}{
		{"x", "x-hub", []int{4, 5, 6}},
		{"y", "y-hub", []int{6, 5, 4}},
	} {
		graph[side.candidate] = NodeSet{}
		for index, degree := range side.degrees {
			hub := storage.NodeID(fmt.Sprintf("%s-%d", side.prefix, index))
			graph[source][hub] = struct{}{}
			graph[side.candidate][hub] = struct{}{}
			graph[hub] = NodeSet{source: {}, side.candidate: {}}
			degrees[hub] = degree
		}
	}
	degree := func(node storage.NodeID) int { return degrees[node] }
	for run := 0; run < runs; run++ {
		scores := TopologyScores("adamic_adar", graph, degree, source)
		require.Len(t, scores, 2, "run %d", run)
		require.Equal(t, math.Float64bits(scores["x"]), math.Float64bits(scores["y"]),
			"run %d: equal contribution multisets must be the same score bits", run)
		ranked := RankedPredictions(scores, 1, "adamic_adar")
		require.Len(t, ranked, 1, "run %d", run)
		require.Equal(t, storage.NodeID("x"), ranked[0].TargetID,
			"run %d: the top-1 cut keeps the smaller target node ID of the tie", run)
	}
}

// TestPairScoreSmallSetsDoNotAllocate keeps small accumulating scores and
// count-only scores free of temporary heap storage.
func TestPairScoreSmallSetsDoNotAllocate(t *testing.T) {
	common, degreesByNode := stableNodeSet("hub", []int{3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18})
	degree := func(node storage.NodeID) int { return degreesByNode[node] }
	for _, algorithm := range []string{"common_neighbors", "jaccard", "total_neighbors", "preferential_attachment", "adamic_adar", "resource_allocation"} {
		allocs := testing.AllocsPerRun(50, func() {
			benchmarkScore = PairScore(algorithm, "s", "x", common, common, degree)
		})
		require.Zero(t, allocs, "%s must not allocate", algorithm)
	}
}
