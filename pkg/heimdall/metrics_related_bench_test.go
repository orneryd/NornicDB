package heimdall

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// relatedBenchSearcherFanout builds the named workload root -> branches ->
// leaves. branches*leaves exceeds the 100-record QueryExecutor cap while every
// single adjacency stays below the remaining budget, so the pre-convergence walk
// and the canonical walk publish the same correct population.
func relatedBenchSearcherFanout(branches, leaves int) *coverageSearcher {
	searcher := &coverageSearcher{
		edges: make(map[string][]*GraphEdge),
		nodes: make(map[string]*NodeData),
	}
	searcher.nodes["root"] = &NodeData{ID: "root", Labels: []string{"Root"}}
	for i := 0; i < branches; i++ {
		branch := fmt.Sprintf("branch-%02d", i)
		searcher.nodes[branch] = &NodeData{ID: branch, Labels: []string{"Branch"}, Properties: map[string]interface{}{"title": branch}}
		searcher.edges["root"] = append(searcher.edges["root"], &GraphEdge{ID: fmt.Sprintf("e-root-%02d", i), Type: "LINK", SourceID: "root", TargetID: branch})
		for j := 0; j < leaves; j++ {
			leaf := fmt.Sprintf("leaf-%02d-%02d", i, j)
			searcher.nodes[leaf] = &NodeData{ID: leaf, Labels: []string{"Leaf"}, Properties: map[string]interface{}{"title": leaf}}
			searcher.edges[branch] = append(searcher.edges[branch], &GraphEdge{ID: fmt.Sprintf("e-%02d-%02d", i, j), Type: "LINK", SourceID: branch, TargetID: leaf})
		}
	}
	return searcher
}

func benchmarkQueryExecutorRelatedNodes(b *testing.B, branches, leaves, want int) {
	exec := NewQueryExecutorWithSearch(nil, relatedBenchSearcherFanout(branches, leaves), nil, time.Second)
	var related []RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = exec.getRelatedNodes(context.Background(), "root", 2)
	}
	b.StopTimer()
	if len(related) != want {
		b.Fatalf("related population = %d, want %d", len(related), want)
	}
}

func BenchmarkQueryExecutorRelatedNodesSmall(b *testing.B) {
	benchmarkQueryExecutorRelatedNodes(b, 4, 4, 20)
}

func BenchmarkQueryExecutorRelatedNodesLarge(b *testing.B) {
	benchmarkQueryExecutorRelatedNodes(b, 10, 9, 100)
}

// BenchmarkQueryExecutorRelatedNodesLargeCancelableContext measures the same
// population with a cancelable context, so the amortized cancellation poll is
// included while nothing cancels. A background context takes the nil-Done
// fastpath.
func BenchmarkQueryExecutorRelatedNodesLargeCancelableContext(b *testing.B) {
	exec := NewQueryExecutorWithSearch(nil, relatedBenchSearcherFanout(10, 9), nil, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var related []RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = exec.getRelatedNodes(ctx, "root", 2)
	}
	b.StopTimer()
	if len(related) != 100 {
		b.Fatalf("related population = %d, want %d", len(related), 100)
	}
}
