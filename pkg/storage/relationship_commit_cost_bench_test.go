package storage

import (
	"fmt"
	"testing"
	"time"
)

// This fixture uses only storage APIs shared by the pristine release and the
// correction. Setup and refreshing the unrelated list are excluded; the timed
// operation includes staging, durable commit/replay and its cache callbacks.
func BenchmarkRelationshipCommitCoveredWrites(b *testing.B) {
	for _, replay := range []bool{false, true} {
		for _, cached := range []int{0, 100000} {
			b.Run(fmt.Sprintf("replay=%t/unrelated=%d", replay, cached), func(b *testing.B) {
				const count = 128
				engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), LowMemory: true, NodeCacheMaxEntries: 100000 + count, EdgeTypeCacheMaxTypes: 8, WriteBehind: replay, WriteBehindInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				defer engine.Close()
				if replay {
					engine.writeBehind.Close()
				}
				for _, id := range []NodeID{"test:a", "test:b", "other:a", "other:b"} {
					if _, err := engine.CreateNode(&Node{ID: id}); err != nil {
						b.Fatal(err)
					}
				}
				unrelated := make([]*Edge, cached)
				for i := range unrelated {
					unrelated[i] = &Edge{ID: EdgeID(fmt.Sprintf("other:cached-%d", i)), StartNode: "other:a", EndNode: "other:b", Type: "UNRELATED"}
				}
				// Seed outside the timer in bounded commits: one 100k-row
				// buffered commit exceeds the pristine base's WAL payload cap.
				const seedBatch = 1024
				for start := 0; start < len(unrelated); start += seedBatch {
					end := min(start+seedBatch, len(unrelated))
					if err := engine.BulkCreateEdges(unrelated[start:end]); err != nil {
						b.Fatal(err)
					}
					if err := engine.FlushWriteBehind(); err != nil {
						b.Fatal(err)
					}
				}
				changed := make([]*Edge, count)
				for i := range changed {
					changed[i] = &Edge{ID: EdgeID(fmt.Sprintf("test:changed-%d", i)), StartNode: "test:a", EndNode: "test:b", Type: "CHANGED", Properties: map[string]any{"v": int64(0)}}
				}
				if err := engine.BulkCreateEdges(changed); err != nil {
					b.Fatal(err)
				}
				if err := engine.FlushWriteBehind(); err != nil {
					b.Fatal(err)
				}
				warm := func() {
					if cached > 0 {
						rows, err := engine.GetEdgesByType("UNRELATED")
						if err != nil {
							b.Fatal(err)
						}
						if len(rows) != cached {
							b.Fatalf("unrelated cache fixture: got%d want%d", len(rows), cached)
						}
						// Type reads bypass their cache during replay. Fill the
						// shared body-cache reader too, and prove the population.
						rows, err = engine.GetOutgoingEdges("other:a")
						if err != nil {
							b.Fatal(err)
						}
						if len(rows) != cached {
							b.Fatalf("unrelated adjacency fixture: got%d want%d", len(rows), cached)
						}
						for _, edge := range rows {
							if _, ok := engine.cacheLoadEdge(edge.ID); !ok {
								b.Fatalf("unrelated body not cached: %s", edge.ID)
							}
						}
					}
				}
				warm()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					warm()
					b.StartTimer()
					tx, err := engine.BeginTransaction()
					if err != nil {
						b.Fatal(err)
					}
					if err := tx.SetNamespace("test"); err != nil {
						b.Fatal(err)
					}
					if err := tx.SetImplicit(replay); err != nil {
						b.Fatal(err)
					}
					for _, original := range changed {
						edge := *original
						edge.Properties = map[string]any{"v": int64(i + 1)}
						if err := tx.UpdateEdge(&edge); err != nil {
							_ = tx.Rollback()
							b.Fatal(err)
						}
					}
					if err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
					if replay {
						if err := engine.FlushWriteBehind(); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(count, "edges/op")
			})
		}
	}
}
