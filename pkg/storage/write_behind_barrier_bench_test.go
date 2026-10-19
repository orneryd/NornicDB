package storage

import (
	"fmt"
	"testing"
	"time"
)

// The pending mode includes an unrelated buffered update and the following
// synchronous write. It measures the required loss of batching as well as
// barrier bookkeeping; empty isolates the enabled-but-idle fast path.
func BenchmarkWriteBehindSynchronousBarrier(b *testing.B) {
	for _, mode := range []string{"disabled", "empty", "pending"} {
		for _, writer := range []string{"node", "edge", "transaction"} {
			b.Run(mode+"/"+writer, func(b *testing.B) {
				e, err := NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: mode != "disabled", WriteBehindInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = e.Close() })
				for _, id := range []NodeID{"test:a", "test:b", "test:background"} {
					if _, err := e.CreateNode(&Node{ID: id, Labels: []string{"N"}}); err != nil {
						b.Fatal(err)
					}
				}
				if err := e.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}); err != nil {
					b.Fatal(err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					value := fmt.Sprint(i)
					if mode == "pending" {
						tx, err := e.BeginTransaction()
						if err != nil {
							b.Fatal(err)
						}
						if err := tx.SetNamespace("test"); err != nil {
							b.Fatal(err)
						}
						if err := tx.SetImplicit(true); err != nil {
							b.Fatal(err)
						}
						if err := tx.UpdateNode(&Node{ID: "test:background", Labels: []string{"N"}, Properties: map[string]any{"v": value}}); err != nil {
							b.Fatal(err)
						}
						if err := tx.Commit(); err != nil {
							b.Fatal(err)
						}
					}
					node := &Node{ID: "test:a", Labels: []string{"N"}, Properties: map[string]any{"v": value}}
					switch writer {
					case "node":
						err = e.UpdateNode(node)
					case "edge":
						err = e.UpdateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R", Properties: map[string]any{"v": value}})
					case "transaction":
						var tx *BadgerTransaction
						tx, err = e.BeginTransaction()
						if err == nil {
							err = tx.UpdateNode(node)
						}
						if err == nil {
							err = tx.Commit()
						}
					}
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
