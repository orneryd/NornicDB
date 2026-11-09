package storage

import (
	"fmt"
	"testing"
	"time"
)

// newNOR418BenchEngine opens a plain or write-behind store for the NOR-418
// update and mixed-import benchmarks.
func newNOR418BenchEngine(b *testing.B, writeBehind bool) *BadgerEngine {
	b.Helper()
	if writeBehind {
		return newWriteBehindTestEngine(b, time.Hour)
	}
	return newTestMemoryEngine(b).BadgerEngine
}

// BenchmarkNOR418MixedImport commits one observed node update plus 4,096
// node creates in one native transaction (a mixed import), and separately
// measures only the commit's predecessor-validation phase of that frame.
func BenchmarkNOR418MixedImport(b *testing.B) {
	for _, writeBehind := range []bool{false, true} {
		stage := func(b *testing.B, engine *BadgerEngine, round int) *BadgerTransaction {
			ctx := b.Context()
			tx, err := engine.BeginTransaction(ctx)
			if err != nil {
				b.Fatal(err)
			}
			before, err := tx.GetNode(ctx, "test:existing")
			if err != nil {
				b.Fatal(err)
			}
			after := CopyNode(before)
			after.Properties["n"] = int64(round)
			if err := tx.UpdateGraphNode(ctx, NodeUpdateRequest{Before: before, After: after}); err != nil {
				b.Fatal(err)
			}
			for i := range 4096 {
				if _, err := tx.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("test:new-%d-%d", round, i))}); err != nil {
					b.Fatal(err)
				}
			}
			return tx
		}
		seed := func(b *testing.B) *BadgerEngine {
			engine := newNOR418BenchEngine(b, writeBehind)
			if _, err := engine.CreateNode(b.Context(), &Node{ID: "test:existing", Properties: map[string]any{"n": int64(0)}}); err != nil {
				b.Fatal(err)
			}
			return engine
		}
		b.Run(fmt.Sprintf("validate/write-behind=%t", writeBehind), func(b *testing.B) {
			engine := seed(b)
			tx := stage(b, engine, 0)
			b.Cleanup(func() { _ = tx.Rollback() })
			b.ReportAllocs()
			for b.Loop() {
				if err := tx.validateMutationPreimagesLocked(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("commit/write-behind=%t", writeBehind), func(b *testing.B) {
			engine := seed(b)
			b.ReportAllocs()
			round := 0
			for b.Loop() {
				round++
				b.StopTimer()
				tx := stage(b, engine, round)
				b.StartTimer()
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNOR418ObservedUpdate updates one node or relationship through the
// namespaced public Engine, observed (Before supplied) and blind.
func BenchmarkNOR418ObservedUpdate(b *testing.B) {
	for _, writeBehind := range []bool{false, true} {
		for _, observed := range []bool{false, true} {
			b.Run(fmt.Sprintf("node/write-behind=%t/observed=%t", writeBehind, observed), func(b *testing.B) {
				ctx := b.Context()
				store := NewNamespacedEngine(newNOR418BenchEngine(b, writeBehind), "test")
				if _, err := store.CreateNode(ctx, &Node{ID: "n", Labels: []string{"Item"}, Properties: map[string]any{"n": int64(0)}}); err != nil {
					b.Fatal(err)
				}
				current, err := store.GetNode(ctx, "n")
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				i := int64(0)
				for b.Loop() {
					i++
					after := CopyNode(current)
					after.Properties["n"] = i
					request := NodeUpdateRequest{After: after}
					if observed {
						request.Before = current
					}
					if err := store.UpdateNode(ctx, request); err != nil {
						b.Fatal(err)
					}
					current = after
				}
			})
			b.Run(fmt.Sprintf("relationship/write-behind=%t/observed=%t", writeBehind, observed), func(b *testing.B) {
				ctx := b.Context()
				store := NewNamespacedEngine(newNOR418BenchEngine(b, writeBehind), "test")
				for _, id := range []NodeID{"a", "b"} {
					if _, err := store.CreateNode(ctx, &Node{ID: id}); err != nil {
						b.Fatal(err)
					}
				}
				if err := store.CreateEdge(ctx, &Edge{ID: "r", StartNode: "a", EndNode: "b", Type: "COUNT", Properties: map[string]any{"n": int64(0)}}); err != nil {
					b.Fatal(err)
				}
				current, err := store.GetEdge(ctx, "r")
				if err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				i := int64(0)
				for b.Loop() {
					i++
					after := CopyEdge(current)
					after.Properties["n"] = i
					request := EdgeUpdateRequest{After: after}
					if observed {
						request.Before = current
					}
					if err := store.UpdateEdge(ctx, request); err != nil {
						b.Fatal(err)
					}
					current = after
				}
			})
		}
	}
}
