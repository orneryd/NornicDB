package storage

import (
	"testing"
	"time"
)

// Include the first read of an untouched warm node after each affected writer.
// The vector is separately stored, so raw-key classification is measured too.
func BenchmarkNodeCacheKnownWriterRead(b *testing.B) {
	for _, kind := range []string{"EdgeCycle", "Sidecar", "TransactionVector", "BufferedVector"} {
		b.Run(kind, func(b *testing.B) {
			var e *BadgerEngine
			if kind == "BufferedVector" {
				var err error
				e, err = NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = e.Close() })
			} else {
				e = NewMemoryEngine().BadgerEngine
				b.Cleanup(func() { _ = e.Close() })
			}
			e.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}})
			target := &Node{ID: "bench:scope-target", Properties: map[string]any{"v": int64(7)}, ChunkEmbeddings: [][]float32{make([]float32, 128)}}
			owner := &Node{ID: "bench:scope-owner", Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{make([]float32, 20000)}}
			for _, node := range []*Node{target, owner} {
				if _, err := e.CreateNode(node); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := e.GetNode(target.ID); err != nil {
				b.Fatal(err)
			}
			edge := &Edge{ID: "bench:scope-edge", StartNode: owner.ID, EndNode: target.ID, Type: "SCOPE"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var err error
				switch kind {
				case "EdgeCycle":
					if err = e.CreateEdge(edge); err == nil {
						err = e.DeleteEdge(edge.ID)
					}
				case "Sidecar":
					owner.ChunkEmbeddings[0][0] = float32(1 + i%2)
					err = e.UpdateNodeEmbeddingSidecar(owner)
				default:
					var tx *BadgerTransaction
					tx, err = e.BeginTransaction()
					if err == nil {
						err = tx.SetNamespace("bench")
					}
					if err == nil {
						err = tx.SetImplicit(kind == "BufferedVector")
					}
					owner.Properties["v"] = int64(i)
					owner.ChunkEmbeddings[0][0] = float32(1 + i%2)
					if err == nil {
						err = tx.UpdateNode(owner)
					}
					if err == nil {
						err = tx.Commit()
					}
					if err == nil && kind == "BufferedVector" {
						err = e.FlushWriteBehind()
					}
				}
				if err != nil {
					b.Fatal(err)
				}
				got, err := e.GetNode(target.ID)
				if err != nil {
					b.Fatal(err)
				}
				if got.Properties["v"] != int64(7) || len(got.ChunkEmbeddings) != 1 {
					b.Fatal("lost unchanged node")
				}
			}
		})
	}
}
