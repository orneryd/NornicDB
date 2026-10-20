package storage

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkManagedSourceWritePath(b *testing.B) {
	for _, mode := range []string{"text", "structured", "body-unconsumed"} {
		b.Run(mode, func(b *testing.B) {
			engine := NewMemoryEngine().BadgerEngine
			defer engine.Close()
			// Detect source-aware acceptance outside timing. Both trees use
			// this identical fixture; the base writes its original payload,
			// and the head writes the new production descriptor too.
			probe := &Node{Properties: map[string]any{"id": "old"}, EmbedMeta: map[string]any{"__nornic_embedding_source": "1:0:0:0:1:2:id"}}
			sourceAware := !engine.sameEmbeddingSource(probe, &Node{Properties: map[string]any{"id": "new"}})
			const pool = 128
			nodes := make([]*Node, pool)
			for i := range nodes {
				n := &Node{ID: NodeID(fmt.Sprintf("test:source-bench-%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "hello", "id": "old", "updatedAt": "old"}, UpdatedAt: time.Now()}
				if _, err := engine.CreateNode(n); err != nil {
					b.Fatal(err)
				}
				keys := "0:"
				if mode != "text" {
					keys = "1:2:id"
				}
				n.ChunkEmbeddings = [][]float32{{1, 2, 3}}
				n.EmbedMeta = map[string]any{"has_embedding": true, "chunk_count": 1}
				if sourceAware {
					n.EmbedMeta["__nornic_embedding_source"] = "1:1:1:4:text0:" + keys
				}
				if err := engine.UpdateNodeEmbeddingSidecar(n); err != nil {
					b.Fatal(err)
				}
				nodes[i] = n
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n := CopyNode(nodes[i%pool])
				var err error
				if mode == "body-unconsumed" {
					n.UpdatedAt = time.Now()
					n.Properties["updatedAt"] = i
					err = engine.UpdateNode(n)
				} else {
					err = engine.UpdateNodeEmbeddingSidecar(n)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
