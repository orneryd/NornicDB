package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkNOR160IterateLatestVisibleNodes measures the engine's latest node
// iteration over committed rows.
func BenchmarkNOR160IterateLatestVisibleNodes(b *testing.B) {
	engine := seedStreamBenchNodes(b, 1000)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		visited := 0
		err := engine.IterateLatestVisibleNodes(func(*Node) error {
			visited++
			return nil
		})
		if err != nil || visited != 1000 {
			b.Fatalf("iterated %d nodes: %v", visited, err)
		}
	}
}

// BenchmarkNOR160TxCommittedNodeStream measures a transaction's stream of
// committed nodes, with and without a pending write to merge.
func BenchmarkNOR160TxCommittedNodeStream(b *testing.B) {
	for _, pending := range []bool{false, true} {
		b.Run(fmt.Sprintf("pending=%v", pending), func(b *testing.B) {
			engine := seedStreamBenchNodes(b, 1000)
			tx, err := engine.BeginTransaction()
			require.NoError(b, err)
			b.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(b, tx.SetNamespace("db"))
			want := 1000
			if pending {
				_, err = tx.CreateNode(&Node{ID: "db:pending", Labels: []string{"Doc"}})
				require.NoError(b, err)
				want++
			}
			options := StreamNodesOptions{WithEmbeddings: true, ApplyDecayFilter: true}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				visited := 0
				err := tx.StreamNodesWithOptions(context.Background(), options, func(*Node) error {
					visited++
					return nil
				})
				if err != nil || visited != want {
					b.Fatalf("streamed %d nodes: %v", visited, err)
				}
			}
		})
	}
}

// BenchmarkNOR160MatchEdgesBetweenCandidates measures the two paths whose
// candidates a predicate inspects without a fresh decode: edge bodies answered
// by the cache, and the transaction's pending edges. The setup requires every
// cached body to be present, so the benchmark reaches the cached path.
func BenchmarkNOR160MatchEdgesBetweenCandidates(b *testing.B) {
	const count = 32
	seed := func(b *testing.B) (*BadgerEngine, []EdgeID) {
		b.Helper()
		engine := seedStreamBenchNodes(b, 2)
		ids := make([]EdgeID, count)
		for index := range ids {
			ids[index] = EdgeID(fmt.Sprintf("db:e-%02d", index))
			require.NoError(b, engine.CreateEdge(&Edge{
				ID: ids[index], Type: "T", StartNode: "db:n-00000", EndNode: "db:n-00001",
				Properties: map[string]any{"payload": map[string]any{"index": int64(index)}, "other": "value"},
			}))
		}
		return engine, ids
	}
	properties := []string{"payload"}
	predicates := map[string]func(*Edge) bool{
		"rejected": func(*Edge) bool { return false },
		"accepted": func(*Edge) bool { return true },
	}
	wants := map[string]int{"rejected": 0, "accepted": count}
	for name, predicate := range predicates {
		b.Run("cached/"+name, func(b *testing.B) {
			engine, ids := seed(b)
			for _, id := range ids {
				_, err := engine.GetEdge(id)
				require.NoError(b, err)
				engine.edgeCacheMu.RLock()
				_, cached := engine.edgeCache[id]
				engine.edgeCacheMu.RUnlock()
				require.True(b, cached, "the read must fill the edge body cache")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				matched, err := engine.MatchEdgesBetween("db:n-00000", "db:n-00001", "T", properties, predicate)
				if err != nil || len(matched) != wants[name] {
					b.Fatalf("matched %d edges: %v", len(matched), err)
				}
			}
		})
		b.Run("pending/"+name, func(b *testing.B) {
			engine := seedStreamBenchNodes(b, 2)
			tx, err := engine.BeginTransaction()
			require.NoError(b, err)
			b.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(b, tx.SetNamespace("db"))
			for index := 0; index < count; index++ {
				require.NoError(b, tx.CreateEdge(&Edge{
					ID: EdgeID(fmt.Sprintf("db:e-%02d", index)), Type: "T", StartNode: "db:n-00000", EndNode: "db:n-00001",
					Properties: map[string]any{"payload": map[string]any{"index": int64(index)}, "other": "value"},
				}))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				matched, err := tx.MatchEdgesBetween("db:n-00000", "db:n-00001", "T", properties, predicate)
				if err != nil || len(matched) != wants[name] {
					b.Fatalf("matched %d edges: %v", len(matched), err)
				}
			}
		})
	}
}
