package linkpredict

import (
	"math"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// DegreeFunc is a node's degree for the degree-based scores (Adamic-Adar,
// resource allocation, preferential attachment). GDS counts a node's
// relationships, a parallel one again and a self-loop once; Graph.Degree
// counts its distinct neighbours.
type DegreeFunc func(storage.NodeID) int

// TopologyScores is algorithm's raw score (not rescaled) for each candidate
// of source: common_neighbors, jaccard, adamic_adar, resource_allocation or
// preferential_attachment (anything else: adamic_adar). The candidates are
// the nodes two hops away that aren't neighbours; for preferential
// attachment, every node that isn't a neighbour. degree gives the
// degree-based scores' degrees; nil uses graph.Degree.
func TopologyScores(algorithm string, graph Graph, degree DegreeFunc, source storage.NodeID) map[storage.NodeID]float64 {
	neighbors, exists := graph[source]
	if !exists {
		return nil
	}
	if degree == nil {
		degree = graph.Degree
	}
	scores := make(map[storage.NodeID]float64)
	if algorithm == "preferential_attachment" {
		sourceDegree := float64(degree(source))
		for candidate := range graph {
			if _, isNeighbor := neighbors[candidate]; candidate == source || isNeighbor {
				continue
			}
			scores[candidate] = sourceDegree * float64(degree(candidate))
		}
		return scores
	}
	candidates := make(map[storage.NodeID]struct{})
	for neighbor := range neighbors {
		for candidate := range graph[neighbor] {
			if _, isNeighbor := neighbors[candidate]; candidate == source || isNeighbor {
				continue
			}
			candidates[candidate] = struct{}{}
		}
	}
	if algorithm != "common_neighbors" && algorithm != "jaccard" && algorithm != "resource_allocation" {
		algorithm = "adamic_adar"
	}
	for candidate := range candidates {
		if score := PairScore(algorithm, source, candidate, neighbors, graph[candidate], degree); score > 0 {
			scores[candidate] = score
		}
	}
	return scores
}

// PairScore is GDS's score for one pair of nodes (gds.alpha.linkprediction.*,
// #1192) from their neighbour sets a and b (other nodes, each once) and
// degree, which gives the degree-based scores' degrees: common_neighbors
// (|a ∩ b|), total_neighbors (|a ∪ b|), jaccard (|a ∩ b| / |a ∪ b|),
// adamic_adar (Σ 1/ln degree over the common neighbours), resource_allocation
// (Σ 1/degree) and preferential_attachment (degree(first) · degree(second)).
// The accumulating scores add their terms from the smallest common-neighbour
// degree up, so a score depends on the multiset of contributions rather than on
// Go's map iteration order or on which node carries a degree; equal multisets
// compare exactly equal and stay ties. A node and itself share no neighbours,
// as GDS scores them.
func PairScore(algorithm string, first, second storage.NodeID, a, b NodeSet, degree DegreeFunc) float64 {
	if algorithm == "preferential_attachment" {
		return float64(degree(first) * degree(second))
	}
	// Adamic-Adar and resource allocation add one term per common neighbour.
	// Floating-point addition is not associative, so the same terms in Go's map
	// iteration order can sum to adjacent values, which would turn an exactly
	// equal score into a ranking and defeat the target-ID tie key. Their
	// degrees are collected and added in one canonical order below; the
	// count-only scores collect nothing.
	degreeBased := algorithm == "adamic_adar" || algorithm == "resource_allocation"
	var commonDegrees []int
	if degreeBased {
		var degreeBuffer [32]int
		commonDegrees = degreeBuffer[:0]
	}
	common := 0
	if first != second {
		for neighbor := range a {
			if _, shared := b[neighbor]; !shared {
				continue
			}
			common++
			if degreeBased {
				commonDegrees = append(commonDegrees, degree(neighbor))
			}
		}
	}
	if !degreeBased {
		// The union is of the sets, whoever the pair is.
		union := len(a)
		for neighbor := range b {
			if _, counted := a[neighbor]; !counted {
				union++
			}
		}
		switch algorithm {
		case "common_neighbors":
			return float64(common)
		case "total_neighbors":
			return float64(union)
		case "jaccard":
			if common == 0 {
				return 0
			}
			return float64(common) / float64(union)
		}
		return 0
	}

	slices.Sort(commonDegrees)
	sum := 0.0
	for _, commonDegree := range commonDegrees {
		if algorithm == "adamic_adar" {
			sum += 1 / math.Log(float64(commonDegree))
		} else {
			sum += 1 / float64(commonDegree)
		}
	}
	if sum == 0 {
		return 0 // not -0, from 1/ln 0
	}
	return sum
}

// RankedPredictions is the top k of scores (every one for k below 1) with
// their raw values, best first; ties go to the smaller node id.
func RankedPredictions(scores map[storage.NodeID]float64, k int, algorithm string) []Prediction {
	if scores == nil {
		return nil // no such source
	}
	predictions := make([]Prediction, 0, len(scores))
	for nodeID, score := range scores {
		predictions = append(predictions, Prediction{TargetID: nodeID, Score: score, Algorithm: algorithm, Reason: "Topological similarity"})
	}
	sortPredictions(predictions)
	if k > 0 && len(predictions) > k {
		predictions = predictions[:k]
	}
	return predictions
}
