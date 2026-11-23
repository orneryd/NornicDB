package linkpredict

import (
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// benchmarkScore keeps a benchmark's result reachable so the compiler cannot
// drop the call being measured.
var benchmarkScore float64

// topologyWorkload is one populated pair for a topology score: a source s
// whose neighbours are the common neighbors of the two candidates x and y.
// Both candidates are adjacent to every hub, so each score adds one term per
// common neighbour, which is the shape the Adamic-Adar accumulation bug needs.
type topologyWorkload struct {
	graph   Graph
	degree  DegreeFunc
	source  storage.NodeID
	a, b    NodeSet
	degrees map[storage.NodeID]int
}

// newTopologyWorkload builds a graph with the requested number of common
// neighbours. Hub degrees are distinct and at least 3 so the Adamic-Adar and
// resource-allocation terms differ from each other for explicit-degree callers.
// The nil-degree AdamicAdar workload instead uses Graph.Degree (all hubs degree 3).
// The degree function reads a
// precomputed map, as the named-projection public path does
// (pkg/cypher/linkprediction.go linkPredictionGraph).
func newTopologyWorkload(common int) topologyWorkload {
	source := storage.NodeID("s")
	x := storage.NodeID("x")
	y := storage.NodeID("y")
	neighbors := make(NodeSet, common)
	xNeighbors := make(NodeSet, common)
	yNeighbors := make(NodeSet, common)
	graph := Graph{source: neighbors, x: xNeighbors, y: yNeighbors}
	degrees := make(map[storage.NodeID]int, common+3)
	for index := 0; index < common; index++ {
		hub := storage.NodeID(fmt.Sprintf("hub-%03d", index))
		neighbors[hub] = struct{}{}
		xNeighbors[hub] = struct{}{}
		yNeighbors[hub] = struct{}{}
		graph[hub] = NodeSet{source: {}, x: {}, y: {}}
		degrees[hub] = index + 3
	}
	degrees[source] = common
	degrees[x] = common + 1
	degrees[y] = common + 1
	return topologyWorkload{
		graph:   graph,
		source:  source,
		a:       neighbors,
		b:       xNeighbors,
		degrees: degrees,
		degree:  func(node storage.NodeID) int { return degrees[node] },
	}
}

func topologyWorkloadSizes() []struct {
	name   string
	common int
} {
	return []struct {
		name   string
		common int
	}{
		{"small", 3},   // the N152-47 witness: three common neighbours
		{"medium", 32}, // a busy node's neighbourhood
		{"high", 256},  // a hub-scale neighbourhood
	}
}

// BenchmarkPairScore measures the shared scoring owner for the accumulating
// algorithms and for the count-only controls that must not pay a new
// allocation for the ordering fix.
func BenchmarkPairScore(b *testing.B) {
	for _, size := range topologyWorkloadSizes() {
		workload := newTopologyWorkload(size.common)
		for _, algorithm := range []string{"adamic_adar", "resource_allocation", "common_neighbors", "jaccard"} {
			b.Run(size.name+"/"+algorithm, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					benchmarkScore = PairScore(algorithm, workload.source, "x", workload.a, workload.b, workload.degree)
				}
			})
		}
	}
}

// BenchmarkTopologyScores measures the affected public topology owner end to
// end: candidate discovery, a pair score for each candidate, and the score map.
func BenchmarkTopologyScores(b *testing.B) {
	for _, size := range topologyWorkloadSizes() {
		workload := newTopologyWorkload(size.common)
		candidate := storage.NodeID("x")
		for _, algorithm := range []string{"adamic_adar", "common_neighbors"} {
			b.Run(size.name+"/"+algorithm, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scores := TopologyScores(algorithm, workload.graph, workload.degree, workload.source)
					if len(scores) != 2 {
						b.Fatalf("%s: scored %d candidates, want 2", algorithm, len(scores))
					}
					benchmarkScore = scores[candidate]
				}
			})
		}
	}
}

// BenchmarkAdamicAdarWorkload measures the public AdamicAdar entry point on the
// same workloads: score, normalise and rank the predictions.
func BenchmarkAdamicAdarWorkload(b *testing.B) {
	for _, size := range topologyWorkloadSizes() {
		workload := newTopologyWorkload(size.common)
		b.Run(size.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				predictions := AdamicAdar(workload.graph, workload.source, 1)
				if len(predictions) == 0 {
					b.Fatal("no prediction")
				}
				benchmarkScore = predictions[0].Score
			}
		})
	}
}
