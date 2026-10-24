package storage

import (
	"fmt"
	"testing"
)

// Review-only workload: commit a batch, then consume each changed node once.
// Reads include the first read after commit, rather than prewarming it away.
func BenchmarkReview1548WriteThenRead(b *testing.B) {
	for _, size := range []int{1, 16} {
		b.Run(fmt.Sprintf("Batch%d", size), func(b *testing.B) {
			e := NewMemoryEngine()
			b.Cleanup(func() { _ = e.Close() })
			e.SetEmbeddingTextPolicy(EmbeddingTextPolicy{Exclude: []string{"v"}, IncludeLabels: true})
			nodes := make([]*Node, size)
			for i := range nodes {
				vec := make([]float32, 128)
				vec[i%128] = 1
				nodes[i] = &Node{ID: NodeID(fmt.Sprintf("review:n%d", i)), Properties: map[string]any{"v": int64(0)}, ChunkEmbeddings: [][]float32{vec}}
				if _, err := e.CreateNode(nodes[i]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := e.BeginTransaction()
				if err != nil {
					b.Fatal(err)
				}
				for _, n := range nodes {
					n.Properties["v"] = int64(i)
					if err := tx.UpdateNode(n); err != nil {
						b.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					b.Fatal(err)
				}
				for _, n := range nodes {
					got, err := e.GetNode(n.ID)
					if err != nil {
						b.Fatal(err)
					}
					if got.Properties["v"] != int64(i) || len(got.ChunkEmbeddings) != 1 {
						b.Fatal("lost committed payload")
					}
				}
			}
		})
	}
}
