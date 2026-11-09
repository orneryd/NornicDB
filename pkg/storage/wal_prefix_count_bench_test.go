package storage

import (
	"context"
	"fmt"
	"strconv"
	"testing"
)

// BenchmarkWALEnginePrefixCount measures the WAL engine's prefix count through
// each source it selects: the wrapped engine's prefix counter, its streaming
// enumeration, and its whole-collection read, over a small and a large source.
func BenchmarkWALEnginePrefixCount(b *testing.B) {
	for _, size := range []int{8, 1024} {
		nodes := make([]*Node, 0, size)
		edges := make([]*Edge, 0, size)
		for i := 0; i < size; i++ {
			suffix := strconv.Itoa(i)
			nodes = append(nodes, &Node{ID: NodeID("test:n" + suffix), Labels: []string{"Doc"}})
			edges = append(edges, &Edge{ID: EdgeID("test:e" + suffix), Type: "REL", StartNode: "test:n0", EndNode: "test:n1"})
		}
		cases := []struct {
			source string
			engine Engine
		}{
			{source: "stats", engine: &walPrefixStatsEngine{Engine: newTestMemoryEngine(b), nodeCount: int64(size), edgeCount: int64(size)}},
			{source: "stream", engine: &walStreamingCountEngine{Engine: newTestMemoryEngine(b), nodes: nodes, edges: edges}},
			{source: "all", engine: &exportableOnlyEngine{Engine: newTestMemoryEngine(b), allNodes: nodes, allEdges: edges}},
		}
		for _, testCase := range cases {
			wal, err := NewWAL(b.TempDir(), &WALConfig{SyncMode: "none"})
			if err != nil {
				b.Fatal(err)
			}
			walEngine := NewWALEngine(testCase.engine, wal)
			b.Run(fmt.Sprintf("%s/nodes/%d", testCase.source, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					count, err := walEngine.NodeCountByPrefix(context.Background(), "test:")
					if err != nil || count != int64(size) {
						b.Fatalf("count=%d err=%v, want %d", count, err, size)
					}
				}
			})
			b.Run(fmt.Sprintf("%s/edges/%d", testCase.source, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					count, err := walEngine.EdgeCountByPrefix(context.Background(), "test:")
					if err != nil || count != int64(size) {
						b.Fatalf("count=%d err=%v, want %d", count, err, size)
					}
				}
			})
			b.Cleanup(func() { _ = wal.Close() })
		}
	}
}
