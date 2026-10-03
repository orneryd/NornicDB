package storage

import (
	"fmt"
	"testing"
)

// BenchmarkBadgerEngine_BulkOperations times one bulk create of n nodes, one
// of n relationships, and the bulk deletes of both, each on a fresh engine.
func BenchmarkBadgerEngine_BulkOperations(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		for _, op := range []string{"create_nodes", "create_edges", "delete_edges", "delete_nodes"} {
			b.Run(fmt.Sprintf("%s/n=%d", op, n), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir()})
					if err != nil {
						b.Fatal(err)
					}
					nodes := make([]*Node, n)
					nodeIDs := make([]NodeID, n)
					for j := range nodes {
						nodes[j] = &Node{ID: NodeID(fmt.Sprintf("bench:n-%d", j)), Labels: []string{"Bulk"}, Properties: map[string]any{"i": int64(j), "name": fmt.Sprintf("node-%d", j)}}
						nodeIDs[j] = nodes[j].ID
					}
					edges := make([]*Edge, n)
					edgeIDs := make([]EdgeID, n)
					for j := range edges {
						edges[j] = &Edge{ID: EdgeID(fmt.Sprintf("bench:e-%d", j)), StartNode: nodes[j].ID, EndNode: nodes[(j+1)%n].ID, Type: "NEXT", Properties: map[string]any{"w": int64(j)}}
						edgeIDs[j] = edges[j].ID
					}
					run := func(err error) {
						if err != nil {
							b.Fatal(err)
						}
					}
					if op != "create_nodes" {
						run(engine.BulkCreateNodes(nodes))
					}
					if op == "delete_edges" || op == "delete_nodes" {
						run(engine.BulkCreateEdges(edges))
					}
					b.StartTimer()
					switch op {
					case "create_nodes":
						run(engine.BulkCreateNodes(nodes))
					case "create_edges":
						run(engine.BulkCreateEdges(edges))
					case "delete_edges":
						run(engine.BulkDeleteEdges(edgeIDs))
					case "delete_nodes":
						run(engine.BulkDeleteNodes(nodeIDs))
					}
					b.StopTimer()
					run(engine.Close())
				}
			})
		}
	}
}
