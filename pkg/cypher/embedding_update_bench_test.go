package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// BenchmarkEmbeddedNodeUpdates times write statements over embedded nodes
// (#963, #965): a SET over a 1,000-node label scan inside the statement's
// transaction, a SET of one property on one node, and a label scan that
// only reads. Nodes carry managed embeddings. The result cache is off.
func BenchmarkEmbeddedNodeUpdates(b *testing.B) {
	engine := newTestMemoryEngine(b)
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(engine, "test"), 0, 0)
	ctx := context.Background()
	store := storage.NewNamespacedEngine(engine, "test")
	for i := 0; i < 1000; i++ {
		vector := make([]float32, 256)
		vector[i%256] = 1
		if _, err := store.CreateNode(&storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("n%d", i)),
			Labels:          []string{"E"},
			Properties:      map[string]interface{}{"k": int64(i), "text": "embedded text", "tag": "t"},
			EmbedMeta:       map[string]interface{}{"has_embedding": true, "chunk_count": 1},
			ChunkEmbeddings: [][]float32{vector},
		}); err != nil {
			b.Fatal(err)
		}
	}
	for _, query := range []struct{ name, cypher string }{
		{"set-over-scan-1000", "MATCH (n:E) SET n.tag = $v"},
		{"set-one", "MATCH (n:E {k: 7}) SET n.tag = $v"},
		{"scan-read-1000", "MATCH (n:E) RETURN count(n.tag)"},
	} {
		b.Run(query.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := exec.Execute(ctx, query.cypher, map[string]interface{}{"v": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
