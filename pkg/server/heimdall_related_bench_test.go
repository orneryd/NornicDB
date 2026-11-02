package server

import (
	"context"
	"github.com/orneryd/nornicdb/internal/testfixture/relatedgraph"
	"testing"

	"github.com/orneryd/nornicdb/pkg/heimdall"
)

func benchmarkServerRelatedNodes(b *testing.B, branches, leaves, want int) {
	engine := relatedgraph.Fanout(b.Context(), b, branches, leaves)
	router := &heimdallDBRouter{}
	var related []heimdall.RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = router.getRelatedNodes(context.Background(), engine, "bench:root", 2)
	}
	b.StopTimer()
	if len(related) != want {
		b.Fatalf("related population = %d, want %d", len(related), want)
	}
}

func BenchmarkServerRelatedNodesSmall(b *testing.B) {
	benchmarkServerRelatedNodes(b, 4, 4, 20)
}

func BenchmarkServerRelatedNodesLarge(b *testing.B) {
	benchmarkServerRelatedNodes(b, 5, 9, 50)
}

// BenchmarkServerRelatedNodesLargeCancelableContext measures the same population
// with a cancelable context, so the amortized cancellation poll is included while
// nothing cancels. A background context takes the nil-Done fastpath.
func BenchmarkServerRelatedNodesLargeCancelableContext(b *testing.B) {
	engine := relatedgraph.Fanout(b.Context(), b, 5, 9)
	router := &heimdallDBRouter{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var related []heimdall.RelatedNode

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		related = router.getRelatedNodes(ctx, engine, "bench:root", 2)
	}
	b.StopTimer()
	if len(related) != 50 {
		b.Fatalf("related population = %d, want %d", len(related), 50)
	}
}
