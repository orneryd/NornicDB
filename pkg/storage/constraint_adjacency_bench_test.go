package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
)

// BenchmarkConstraintCurrentAdjacency measures write-integrity reads over
// committed and pending edges. It includes the read and detached pending images;
// fixture creation is excluded. Every result is consumed and counted.
func BenchmarkConstraintCurrentAdjacency(b *testing.B) {
	for _, size := range []int{8, 2048} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
			if engine != nil {
				fixtureowner.Own(b, engine)
			}
			if err != nil {
				b.Fatal(err)
			}
			for _, id := range []NodeID{"test:hub", "test:leaf"} {
				if _, err := engine.CreateNode(b.Context(), &Node{ID: id}); err != nil {
					b.Fatal(err)
				}
			}
			tx := &BadgerTransaction{engine: engine, pendingEdges: make(map[EdgeID]*Edge), deletedEdges: make(map[EdgeID]struct{})}
			for i := 0; i < size; i++ {
				edge := &Edge{ID: EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:hub", EndNode: "test:leaf", Type: "R"}
				if err := engine.CreateEdge(b.Context(), edge); err != nil {
					b.Fatal(err)
				}
				if i%2 == 0 {
					tx.pendingEdges[edge.ID] = copyEdge(edge)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				edges, err := tx.currentDirectedEdgesLocked(b.Context(), AdjacentEdgeRead{Direction: AdjacentOutgoing, Node: "test:hub"})
				if err != nil || len(edges) != size {
					b.Fatalf("edges=%d err=%v", len(edges), err)
				}
			}
		})
	}
}
