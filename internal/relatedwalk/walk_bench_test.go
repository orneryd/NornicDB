package relatedwalk

import (
	"context"
	"testing"
)

// unusableAdjacencySource is a single wide adjacency made of records the walk
// examines but never publishes or expands: empty records and repeats of one
// already-visited neighbor. No node load happens, so the benchmark isolates the
// per-record cost of the walk, including the cancellation poll.
func unusableAdjacencySource(width int) *fakeSource {
	edges := make([]fakeEdge, width)
	for i := range edges {
		if i%2 == 1 {
			edges[i] = fakeEdge{neighbor: "seen", relationship: "OUT", direction: Outgoing}
		}
	}
	return &fakeSource{adjacency: map[string][]fakeEdge{"root": edges}}
}

func benchmarkWalkUnusableAdjacency(b *testing.B, ctx context.Context) {
	const width = 100000
	src := unusableAdjacencySource(width)
	accept := &recordingAcceptor{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		records, err := Walk[testRecord](ctx, src, accept, "root", Options{MaxHops: 1, Limit: 2})
		if err != nil {
			b.Fatalf("walk: %v", err)
		}
		if len(records) != 1 {
			b.Fatalf("records = %d, want 1", len(records))
		}
	}
}

// BenchmarkWalkUnusableAdjacencyBackground measures the nil-Done fastpath: the
// walk never selects on a done channel for an uncancelable context.
func BenchmarkWalkUnusableAdjacencyBackground(b *testing.B) {
	benchmarkWalkUnusableAdjacency(b, context.Background())
}

// BenchmarkWalkUnusableAdjacencyCancelable measures the amortized cancellation
// poll on every examined record of a wide adjacency that publishes nothing.
func BenchmarkWalkUnusableAdjacencyCancelable(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	benchmarkWalkUnusableAdjacency(b, ctx)
}
