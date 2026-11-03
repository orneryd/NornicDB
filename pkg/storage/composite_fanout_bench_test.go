package storage

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"
)

// BenchmarkCompositeConstituentFanOut measures the converged materialising and
// streaming fan-outs over small and large constituent sets. Every iteration
// asserts the complete merged result, so a fan-out that silently drops a
// constituent is a benchmark failure instead of a faster number.
func BenchmarkCompositeConstituentFanOut(b *testing.B) {
	for _, workload := range []struct {
		name         string
		constituents int
		nodes        int
	}{
		{name: "small", constituents: 2, nodes: 50},
		{name: "large", constituents: 8, nodes: 500},
	} {
		composite := newCompositeFanOutBenchmark(b, workload.constituents, workload.nodes)
		total := workload.constituents * workload.nodes
		ctx := context.Background()

		b.Run(workload.name, func(b *testing.B) {
			b.Run("AllNodes", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					nodes, err := composite.AllNodes(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if len(nodes) != total {
						b.Fatalf("AllNodes returned %d nodes, want %d", len(nodes), total)
					}
				}
			})

			b.Run("AllEdges", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					edges, err := composite.AllEdges(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if len(edges) != total {
						b.Fatalf("AllEdges returned %d edges, want %d", len(edges), total)
					}
				}
			})

			b.Run("StreamNodes", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					streamed := 0
					if err := composite.StreamNodes(ctx, func(*Node) error {
						streamed++
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					if streamed != total {
						b.Fatalf("StreamNodes published %d nodes, want %d", streamed, total)
					}
				}
			})

			b.Run("StreamEdges", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					streamed := 0
					if err := composite.StreamEdges(ctx, func(*Edge) error {
						streamed++
						return nil
					}); err != nil {
						b.Fatal(err)
					}
					if streamed != total {
						b.Fatalf("StreamEdges published %d edges, want %d", streamed, total)
					}
				}
			})

			b.Run("GetNodesByLabel", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					nodes, err := composite.GetNodesByLabel(ctx, compositeFanOutBenchmarkLabel)
					if err != nil {
						b.Fatal(err)
					}
					if len(nodes) != total {
						b.Fatalf("GetNodesByLabel returned %d nodes, want %d", len(nodes), total)
					}
				}
			})

			b.Run("GetOutgoingEdges", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					edges, err := composite.GetOutgoingEdges(ctx, NodeID(compositeFanOutBenchmarkID(0)))
					if err != nil {
						b.Fatal(err)
					}
					if len(edges) != workload.constituents {
						b.Fatalf("GetOutgoingEdges returned %d edges, want %d", len(edges), workload.constituents)
					}
				}
			})
		})
	}
}

const compositeFanOutBenchmarkLabel = "Bench"

func compositeFanOutBenchmarkID(index int) string {
	return fmt.Sprintf("bench:node-%d", index)
}

func newCompositeFanOutBenchmark(b *testing.B, constituentCount, nodesPerConstituent int) *CompositeEngine {
	b.Helper()
	constituents := make(map[string]Engine, constituentCount)
	names := make(map[string]string, constituentCount)
	modes := make(map[string]string, constituentCount)
	for constituent := 0; constituent < constituentCount; constituent++ {
		alias := fmt.Sprintf("bench-%d", constituent)
		engine := fixtureowner.Track(b, NewMemoryEngine(), ErrStorageClosed)
		for index := 0; index < nodesPerConstituent; index++ {
			id := NodeID(compositeFanOutBenchmarkID(index))
			if _, err := engine.CreateNode(b.Context(), &Node{ID: id, Labels: []string{compositeFanOutBenchmarkLabel}, Properties: map[string]any{"n": int64(index)}}); err != nil {
				b.Fatal(err)
			}
			if err := engine.CreateEdge(b.Context(), &Edge{ID: EdgeID(fmt.Sprintf("bench:edge-%d", index)), StartNode: id, EndNode: id, Type: "LOOP"}); err != nil {
				b.Fatal(err)
			}
		}
		b.Cleanup(func() {
			if err := engine.Close(); err != nil {
				b.Errorf("close benchmark constituent: %v", err)
			}
		})
		constituents[alias] = engine
		names[alias] = alias
		modes[alias] = "read"
	}
	return NewCompositeEngine(constituents, names, modes)
}
