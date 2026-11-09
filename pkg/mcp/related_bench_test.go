package mcp

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testfixture/relatedgraph"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func benchmarkMCPRelatedNodes(b *testing.B, branches, leaves, want int) {
	engine := relatedgraph.Fanout(b.Context(), b, branches, leaves)
	server := NewServer(nil, nil)
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return engine, nil })
	ctx := ContextWithDatabase(context.Background(), "bench")
	var related []RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = server.getRelatedNodes(ctx, "bench:root", 3)
	}
	b.StopTimer()
	if len(related) != want {
		b.Fatalf("related population = %d, want %d", len(related), want)
	}
}

func BenchmarkMCPRelatedNodesSmall(b *testing.B) {
	benchmarkMCPRelatedNodes(b, 4, 4, 20)
}

func BenchmarkMCPRelatedNodesLarge(b *testing.B) {
	benchmarkMCPRelatedNodes(b, 5, 9, 50)
}

// BenchmarkMCPRelatedNodesLargeCancelableContext measures the same population
// with a cancelable context, so the amortized cancellation poll is included while
// nothing cancels. A background context takes the nil-Done fastpath.
func BenchmarkMCPRelatedNodesLargeCancelableContext(b *testing.B) {
	engine := relatedgraph.Fanout(b.Context(), b, 5, 9)
	server := NewServer(nil, nil)
	server.SetDatabaseScopedStorage(func(string) (storage.Engine, error) { return engine, nil })
	ctx, cancel := context.WithCancel(ContextWithDatabase(context.Background(), "bench"))
	defer cancel()
	var related []RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = server.getRelatedNodes(ctx, "bench:root", 3)
	}
	b.StopTimer()
	if len(related) != 50 {
		b.Fatalf("related population = %d, want %d", len(related), 50)
	}
}
