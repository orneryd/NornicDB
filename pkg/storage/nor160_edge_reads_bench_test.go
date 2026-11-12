// Bounded storage-delivery correction: Codex, NOR-160, 1 November 2026.
// NOR-160 lane (Claude), 31 October 2026: cost of the edge list and edge
// stream reads, the same file on the release base and the head.
package storage

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// BenchmarkNOR160EdgeReads measures AllEdges (the edge list) and StreamEdges
// (the edge stream) over the same committed edges.
func BenchmarkNOR160EdgeReads(b *testing.B) {
	const nodes, edges = 200, 2000
	engine, err := fixtureowner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	require.NoError(b, engine.EnsureNamespaceMVCC("bench"))
	for index := 0; index < nodes; index++ {
		_, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("bench:n%03d", index)), Labels: []string{"N"}})
		require.NoError(b, err)
	}
	for index := 0; index < edges; index++ {
		require.NoError(b, engine.CreateEdge(b.Context(), &Edge{
			ID:         EdgeID(fmt.Sprintf("bench:e%04d", index)),
			StartNode:  NodeID(fmt.Sprintf("bench:n%03d", index%nodes)),
			EndNode:    NodeID(fmt.Sprintf("bench:n%03d", (index*7+1)%nodes)),
			Type:       "R",
			Properties: map[string]any{"w": int64(index)},
		}))
	}
	b.Run("list", func(b *testing.B) {
		b.ReportAllocs()
		for iteration := 0; iteration < b.N; iteration++ {
			all, err := engine.AllEdges(context.Background())
			if err != nil || len(all) != edges {
				b.Fatalf("AllEdges: %d edges, %v", len(all), err)
			}
		}
	})
	b.Run("stream", func(b *testing.B) {
		b.ReportAllocs()
		ctx := context.Background()
		for iteration := 0; iteration < b.N; iteration++ {
			seen := 0
			if err := engine.StreamEdges(ctx, func(*Edge) error { seen++; return nil }); err != nil || seen != edges {
				b.Fatalf("StreamEdges: %d edges, %v", seen, err)
			}
		}
	})
	head, err := engine.GetEdgeCurrentHead(b.Context(), "bench:e1999")
	require.NoError(b, err)
	var expectedPairIDs [10]EdgeID
	for i := range expectedPairIDs {
		expectedPairIDs[i] = EdgeID(fmt.Sprintf("bench:e%04d", i*nodes))
	}
	for _, historical := range []bool{false, true} {
		name := "between-current"
		if historical {
			name = "between-snapshot"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				var found []*Edge
				var err error
				if historical {
					found, err = engine.ReadAdjacentEdgesVisibleAt(b.Context(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "bench:n000", Other: "bench:n001"}, head.Version)
				} else {
					found, err = engine.ReadAdjacentEdges(b.Context(), AdjacentEdgeRead{Direction: AdjacentBetween, Node: "bench:n000", Other: "bench:n001"})
				}
				if err != nil || len(found) != 10 {
					b.Fatalf("between rows: %d, %v", len(found), err)
				}
				var seen uint16
				for _, edge := range found {
					value, ok := edge.Properties["w"].(int64)
					if !ok || value < 0 || value >= edges || value%nodes != 0 || edge.StartNode != "bench:n000" || edge.EndNode != "bench:n001" || edge.ID != expectedPairIDs[value/nodes] {
						b.Fatalf("unexpected between row: %#v", edge)
					}
					bit := uint16(1) << uint(value/nodes)
					if seen&bit != 0 {
						b.Fatalf("duplicate edge %s", edge.ID)
					}
					seen |= bit
				}
				if seen != 1023 {
					b.Fatalf("incomplete row identities: %b", seen)
				}
			}
		})
	}

}
