package storage

import (
	"fmt"
	"runtime/pprof"
	"testing"
	"time"
)

// This measures the actual native buffered conflict preparation owner, excluding
// staging and publication. It is not whole commit throughput qualification.
func BenchmarkBufferedConflictPreparationScan(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine := newWriteBehindTestEngine(b, time.Hour)
			for i := range size {
				if _, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"payload": "before"}}); err != nil {
					b.Fatal(err)
				}
			}
			tx, err := engine.BeginTransaction()
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = tx.Rollback() })
			for i := range size {
				node, err := tx.GetNode(NodeID(fmt.Sprintf("test:n%d", i)))
				if err != nil {
					b.Fatal(err)
				}
				before := CopyNode(node)
				node.Properties["payload"] = "after"
				if err := tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
					b.Fatal(err)
				}
			}
			bufferedCreate(b, engine, &Node{ID: "test:peer"})
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := tx.bufferedDeleteConflictLocked(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// This public fixture includes receipt observation and immutable preparation
// with one real acknowledged peer commit landing after each transaction began.
func BenchmarkPublicBufferedEntityCommitPreparation(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine := newWriteBehindTestEngine(b, time.Hour)
			for i := range size {
				if _, err := engine.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"payload": "before"}}); err != nil {
					b.Fatal(err)
				}
			}
			baseContext := b.Context()
			profileContext := pprof.WithLabels(baseContext, pprof.Labels("phase", "public_prepare"))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tx, err := engine.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				for j := range size {
					node, err := tx.GetNode(NodeID(fmt.Sprintf("test:n%d", j)))
					if err != nil {
						b.Fatal(err)
					}
					before := CopyNode(node)
					node.Properties["payload"] = "after"
					if err := tx.UpdateGraphNode(NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
						b.Fatal(err)
					}
				}
				bufferedCreate(b, engine, &Node{ID: NodeID(fmt.Sprintf("test:peer%d", i))})
				if engine.writeBehind.readEpoch.Load()&1 == 0 {
					b.Fatal("fixture lost pending acknowledged generation before preparation")
				}
				pprof.SetGoroutineLabels(profileContext)
				b.StartTimer()
				prepared, err := tx.PrepareEntityCommit(baseContext, EntityCommitRequest{LogicalID: fmt.Sprintf("public-buffered-%d", i)})
				b.StopTimer()
				pprof.SetGoroutineLabels(baseContext)
				if err != nil {
					b.Fatal(err)
				}
				prepared.Abort()
				b.StartTimer()
			}
			b.StopTimer()
			count, countErr := engine.NodeCount()
			if countErr != nil {
				b.Fatal(countErr)
			}
			if count != int64(size+b.N) {
				b.Fatalf("fixture graph count=%d want=%d", count, size+b.N)
			}
			engine.writeBehind.mu.RLock()
			acknowledgments := len(engine.writeBehind.active.commits)
			for _, generation := range engine.writeBehind.draining {
				acknowledgments += len(generation.commits)
			}
			epoch := engine.writeBehind.readEpoch.Load()
			engine.writeBehind.mu.RUnlock()
			if acknowledgments != b.N {
				b.Fatalf("fixture acknowledged records=%d want=%d", acknowledgments, b.N)
			}
			b.Logf("fixture native_nodes=%d iterations=%d graph_nodes=%d acknowledgments=%d read_epoch=%d caller=testing.B.Context", size, b.N, count, acknowledgments, epoch)

		})
	}
}
