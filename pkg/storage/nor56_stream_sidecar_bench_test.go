package storage

import (
	"context"
	"fmt"
	"testing"

	fixture "github.com/orneryd/nornicdb/internal/testfixture"
)

func BenchmarkNOR56StreamingSidecarPresent(b *testing.B) {
	base := fixture.Own(b, fixture.Track(b, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	for i := 0; i < 2000; i++ {
		_, err := base.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("bench:n%05d", i)), Properties: map[string]any{"text": "representative payload"}})
		if err != nil {
			b.Fatal(err)
		}
	}
	node, err := base.GetNode(ctx, "bench:n00000")
	if err != nil {
		b.Fatal(err)
	}
	node.EmbedMeta = map[string]any{"status": "ready", "chunk_count": 1}
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	if err = base.UpdateNodeEmbeddingSidecar(ctx, node); err != nil {
		b.Fatal(err)
	}
	traced := NewTracedEngine(base)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count, vectors := 0, 0
		err = StreamNodesWithFallback(ctx, traced, 1000, func(node *Node) error { count++; vectors += len(node.ChunkEmbeddings); return nil })
		if err != nil || count != 2000 || vectors != 1 {
			b.Fatalf("nodes=%d vectors=%d err=%v", count, vectors, err)
		}
	}
}

func BenchmarkNOR56StreamingEarlyStop(b *testing.B) {
	base := fixture.Own(b, fixture.Track(b, NewMemoryEngine(), ErrStorageClosed))
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		_, err := base.CreateNode(ctx, &Node{ID: NodeID(fmt.Sprintf("bench:n%05d", i)), Properties: map[string]any{"text": "representative payload"}})
		if err != nil {
			b.Fatal(err)
		}
	}
	traced := NewTracedEngine(base)
	for _, limit := range []int{1, 10} {
		b.Run(fmt.Sprintf("first-%d", limit), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				count := 0
				err := StreamNodesWithFallback(ctx, traced, 1000, func(*Node) error {
					count++
					if count == limit {
						return ErrIterationStopped
					}
					return nil
				})
				if err != nil || count != limit {
					b.Fatalf("count=%d err=%v", count, err)
				}
			}
		})
	}
}
