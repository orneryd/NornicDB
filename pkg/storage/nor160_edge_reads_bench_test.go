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
	for index := 0; index < nodes; index++ {
		_, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("bench:n%03d", index)), Labels: []string{"N"}})
		require.NoError(b, err)
	}
	for index := 0; index < edges; index++ {
		require.NoError(b, engine.CreateEdge(&Edge{
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
			all, err := engine.AllEdges()
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
}
