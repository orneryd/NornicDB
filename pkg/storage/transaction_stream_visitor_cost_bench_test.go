package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
)

// BenchmarkTransactionStreamVisitorRetake measures what a stream pays to hand
// one record to its visitor with the transaction's lock released and retake
// that lock. All three stream owners run the same 4-record shape: three
// committed records and one pending write of the transaction, so every owner
// takes its per-record path. The label workload is the one
// BenchmarkTxReads_StreamNodesByLabelProjectedWithPendingWrites measures; the
// edge workload carries the population through StreamEdgesByType and the
// whole-graph workload through StreamNodesWithOptions without a label. Their
// reported numbers are only comparable with each other and across changes to
// the release/retake owner: they measure a small population, so the per-record
// cost dominates and no throughput claim is made from them.
func BenchmarkTransactionStreamVisitorRetake(b *testing.B) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](b, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(b, err)
	b.Cleanup(func() { _ = engine.Close() })
	for _, name := range []string{"alice", "bob", "carol"} {
		_, err := engine.CreateNode(b.Context(), &Node{
			ID:         NodeID("bench:" + name),
			Labels:     []string{"Person"},
			Properties: map[string]any{"name": name},
		})
		require.NoError(b, err)
		require.NoError(b, engine.CreateEdge(b.Context(), &Edge{
			ID:        EdgeID("bench:e-" + name),
			StartNode: "bench:alice",
			EndNode:   NodeID("bench:" + name),
			Type:      "KNOWS",
		}))
	}
	tx, err := engine.BeginTransaction(b.Context())
	require.NoError(b, err)
	b.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.CreateNode(b.Context(), &Node{ID: "bench:pending", Labels: []string{"Person"}})
	require.NoError(b, err)
	require.NoError(b, tx.CreateEdge(b.Context(), &Edge{
		ID:        "bench:e-pending",
		StartNode: "bench:alice",
		EndNode:   "bench:pending",
		Type:      "KNOWS",
	}))

	visited := 0
	visit := func(*Node) error {
		visited++
		return nil
	}
	visitEdge := func(*Edge) error {
		visited++
		return nil
	}
	properties := []string{"name"}

	b.Run("LabelProjected", func(b *testing.B) {
		visited = 0
		require.NoError(b, tx.StreamNodesByLabelProjected(context.Background(), "Person", properties, visit))
		require.Equal(b, 4, visited)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			visited = 0
			if err := tx.StreamNodesByLabelProjected(context.Background(), "Person", properties, visit); err != nil || visited != 4 {
				b.Fatalf("stream visited %d nodes: %v", visited, err)
			}
		}
	})

	b.Run("EdgesByType", func(b *testing.B) {
		visited = 0
		require.NoError(b, tx.StreamEdgesByType(context.Background(), "KNOWS", visitEdge))
		require.Equal(b, 4, visited)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			visited = 0
			if err := tx.StreamEdgesByType(context.Background(), "KNOWS", visitEdge); err != nil || visited != 4 {
				b.Fatalf("stream visited %d edges: %v", visited, err)
			}
		}
	})

	b.Run("WholeGraph", func(b *testing.B) {
		opts := StreamNodesOptions{}
		visited = 0
		require.NoError(b, tx.StreamNodesWithOptions(context.Background(), opts, visit))
		require.Equal(b, 4, visited)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			visited = 0
			if err := tx.StreamNodesWithOptions(context.Background(), opts, visit); err != nil || visited != 4 {
				b.Fatalf("stream visited %d nodes: %v", visited, err)
			}
		}
	})
}
