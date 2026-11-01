package cypher

// Author: Louvain context API lane. Representative small/large baselines for the
// public apoc.algo.louvain CALL path, measured over the same graph shapes as
// apoc/community's BenchmarkLouvain and BenchmarkLouvainScale1024.

import (
	"context"
	"strconv"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// newLouvainBenchGraph creates blocks of width mutually adjacent nodes, each
// block joined to the next, with one weighted relationship per pair.
func newLouvainBenchGraph(tb testing.TB, blocks, width int) *StorageExecutor {
	tb.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(tb), "louvainbench")
	ids := make([]string, blocks*width)
	for i := range ids {
		ids[i] = "n" + strconv.Itoa(i)
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(ids[i]), Labels: []string{"L"}, Properties: map[string]interface{}{"seq": int64(i)}})
		if err != nil {
			tb.Fatalf("create node %s: %v", ids[i], err)
		}
	}
	edges := 0
	link := func(a, b int) {
		err := engine.CreateEdge(&storage.Edge{
			ID:         storage.EdgeID("e" + strconv.Itoa(edges)),
			Type:       "R",
			StartNode:  storage.NodeID(ids[a]),
			EndNode:    storage.NodeID(ids[b]),
			Properties: map[string]interface{}{"w": 1.0},
		})
		if err != nil {
			tb.Fatalf("create edge %d: %v", edges, err)
		}
		edges++
	}
	for block := 0; block < blocks; block++ {
		start := block * width
		for i := 0; i < width; i++ {
			for j := i + 1; j < width; j++ {
				link(start+i, start+j)
			}
		}
		link(start+width-1, ((block+1)%blocks)*width)
	}
	return NewStorageExecutor(engine)
}

func benchmarkApocAlgoLouvain(b *testing.B, blocks, width int) {
	exec := newLouvainBenchGraph(b, blocks, width)
	ctx := context.Background()
	query := "CALL apoc.algo.louvain(['L'], {weightProperty: 'w'}) YIELD node, community RETURN count(node) AS nodes"
	result, err := exec.Execute(ctx, query, nil)
	if err != nil {
		b.Fatalf("healthy call: %v", err)
	}
	if got := result.Rows[0][0]; got != int64(blocks*width) {
		b.Fatalf("healthy rows: got %v, want %d", got, blocks*width)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			b.Fatalf("run %d: %v", i, err)
		}
	}
}

// BenchmarkApocAlgoLouvainSmall is the public small graph: 8 nodes, 13
// relationships (apoc/community's createTestGraph shape).
func BenchmarkApocAlgoLouvainSmall(b *testing.B) {
	benchmarkApocAlgoLouvain(b, 2, 4)
}

// BenchmarkApocAlgoLouvainLarge is the public scale graph: 1024 nodes, 7744
// relationships (apoc/community's louvainScaleGraph shape).
func BenchmarkApocAlgoLouvainLarge(b *testing.B) {
	benchmarkApocAlgoLouvain(b, 64, 16)
}
