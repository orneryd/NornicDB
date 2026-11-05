package storage

import (
	"fmt"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// This measures the actual native buffered conflict preparation owner, excluding
// staging and publication. It is not whole commit throughput qualification.
func BenchmarkBufferedConflictPreparationScan(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine := newWriteBehindTestEngine(b, time.Hour)
			for i := range size {
				if _, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"payload": "before"}}); err != nil {
					b.Fatal(err)
				}
			}
			tx, err := engine.BeginTransaction(b.Context())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = tx.Rollback() })
			for i := range size {
				node, err := tx.GetNode(b.Context(), NodeID(fmt.Sprintf("test:n%d", i)))
				if err != nil {
					b.Fatal(err)
				}
				before := CopyNode(node)
				node.Properties["payload"] = "after"
				if err := tx.UpdateGraphNode(b.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
					b.Fatal(err)
				}
			}
			bufferedCreate(b, engine, &Node{ID: "test:peer"})
			ctx := b.Context()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := tx.bufferedWriteConflictLocked(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Same public/native timed owner as the preserved buffered preparation fixture.
// Added physical/ACK/order validation is untimed and identical on both sides.
func BenchmarkPublicBufferedEntityCommitPreparation(b *testing.B) {
	for _, size := range []int{1, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			engine := newWriteBehindTestEngine(b, time.Hour)
			for i := range size {
				if _, err := engine.CreateNode(b.Context(), &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Properties: map[string]any{"payload": "before"}}); err != nil {
					b.Fatal(err)
				}
			}
			baseContext := b.Context()
			profileContext := pprof.WithLabels(baseContext, pprof.Labels("phase", "public_prepare"))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tx, err := engine.BeginTransaction(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				for j := range size {
					node, err := tx.GetNode(b.Context(), NodeID(fmt.Sprintf("test:n%d", j)))
					if err != nil {
						b.Fatal(err)
					}
					before := CopyNode(node)
					node.Properties["payload"] = "after"
					if err := tx.UpdateGraphNode(b.Context(), NodeUpdateRequest{Before: before, After: node, Policy: ClientNodeEdits}); err != nil {
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
			count, countErr := engine.NodeCount(b.Context())
			if countErr != nil {
				b.Fatal(countErr)
			}
			if count != int64(size+b.N) {
				b.Fatalf("fixture graph count=%d want=%d", count, size+b.N)
			}
			engine.writeBehind.mu.RLock()
			acknowledgments := len(engine.writeBehind.active.commits)
			ackOperations := 0
			rawOrder := len(engine.writeBehind.active.order)
			for _, commit := range engine.writeBehind.active.commits {
				ackOperations += len(commit.ops)
			}
			for _, generation := range engine.writeBehind.draining {
				acknowledgments += len(generation.commits)
				rawOrder += len(generation.order)
				for _, commit := range generation.commits {
					ackOperations += len(commit.ops)
				}
			}
			epoch := engine.writeBehind.readEpoch.Load()
			engine.writeBehind.mu.RUnlock()
			if acknowledgments != b.N {
				b.Fatalf("fixture acknowledged records=%d want=%d", acknowledgments, b.N)
			}
			if ackOperations != b.N {
				b.Fatalf("ack domain operations=%d want=%d", ackOperations, b.N)
			}
			// Actual captured commits keep ops in commits, not raw generation.order.
			// Preserve that representation; never synthesize or clear buffer work.
			if rawOrder != 0 {
				b.Fatalf("captured-commit fixture raw order=%d want=0", rawOrder)
			}
			physicalNodes := 0
			if err := engine.db.View(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				iterator := txn.NewIterator(opts)
				defer iterator.Close()
				prefix := []byte{prefixNode}
				for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
					physicalNodes++
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			if physicalNodes != size {
				b.Fatalf("physical native nodes=%d want=%d", physicalNodes, size)
			}
			b.Logf("fixture physical_nodes=%d iterations=%d graph_nodes=%d acknowledgments=%d ack_ops=%d raw_generation_order=%d read_epoch=%d caller=testing.B.Context", physicalNodes, b.N, count, acknowledgments, ackOperations, rawOrder, epoch)

		})
	}
}
