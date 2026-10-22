package cypher

import (
	"context"
	"strconv"
	"testing"
)

// BenchmarkNodeSimilarityKernel scores two 64-neighbour vectors sharing 32
// neighbours. Without an explanation, JACCARD, OVERLAP and COSINE only count
// the shared neighbours, so they allocate nothing; RESOURCE_ALLOCATION keeps
// them for its ordered sum, and an explanation builds the shared list.
func BenchmarkNodeSimilarityKernel(b *testing.B) {
	nodes := make([]string, 96)
	for i := range nodes {
		nodes[i] = "n" + strconv.Itoa(i)
	}
	var left, right []gdsEdge
	for i := 0; i < 64; i++ {
		left = append(left, gdsEdge{target: i, weight: 1 / float64(i+1)})
		right = append(right, gdsEdge{target: i + 32, weight: 1 / float64(i+2)})
	}
	for _, metric := range []string{"JACCARD", "OVERLAP", "COSINE", "RESOURCE_ALLOCATION"} {
		for _, explain := range []bool{false, true} {
			b.Run(metric+"/explain="+strconv.FormatBool(explain), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					nodeSimilarity(context.Background(), left, right, metric, true, nodes, explain)
				}
			})
		}
	}
}
