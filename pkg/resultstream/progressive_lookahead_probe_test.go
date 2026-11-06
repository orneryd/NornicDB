package resultstream

// Author: NOR-406 retrieval-profile lane, 2026-11-01: instrumentation only.
// These probes characterize the Progressive stream's per-pull lookahead
// (expansion targets, buffered pulls, in-flight coalescing) without any
// storage, network, clock or sleep dependency. They do not change behavior.

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// probeRows returns a deterministic row prefix of the given depth.
func probeRows(depth int) [][]any {
	rows := make([][]any, depth)
	for index := range rows {
		rows[index] = []any{index}
	}
	return rows
}

// probeExpander records every requested expansion depth.
//
// An expansion runs inside a pull, not on the test goroutine, so the recorded
// depths are guarded. The two gate channels are written only at construction:
// entered is closed once, by whichever expansion enters first, and release is
// closed by the test to let that expansion finish.
type probeExpander struct {
	mu        sync.Mutex
	requested []int
	entered   chan struct{}
	release   chan struct{}
	gate      sync.Once
}

func (e *probeExpander) expand(_ context.Context, depth int) ([][]any, bool, error) {
	e.mu.Lock()
	e.requested = append(e.requested, depth)
	e.mu.Unlock()
	if e.entered != nil {
		e.gate.Do(func() { close(e.entered) })
		<-e.release
	}
	return probeRows(depth), false, nil
}

// depths returns the expansion depths requested so far.
func (e *probeExpander) depths() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.requested...)
}

// expansions returns how many expansions have been requested.
func (e *probeExpander) expansions() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.requested)
}

// TestProgressiveLookaheadExpandsOnlyAtTheBufferedEdge pins the current
// per-pull lookahead: a pull that ends before the buffered rows' end expands
// nothing, and a pull whose end is exactly the buffered length expands to
// max(depth*2, end+1).
func TestProgressiveLookaheadExpandsOnlyAtTheBufferedEdge(t *testing.T) {
	expander := &probeExpander{}
	stream, err := NewProgressive(probeRows(20), false, 20, expander.expand)
	if err != nil {
		t.Fatalf("NewProgressive: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	ctx := context.Background()

	// Ends 5 and 10 are inside the initial 20-row buffer: no expansion.
	for _, position := range []uint64{0, 5} {
		page, pullErr := stream.Pull(ctx, position, 5)
		if pullErr != nil {
			t.Fatalf("buffered pull at %d: %v", position, pullErr)
		}
		if len(page.Rows) != 5 || !page.HasMore {
			t.Fatalf("buffered pull at %d: rows=%d hasMore=%t", position, len(page.Rows), page.HasMore)
		}
	}
	if depths := expander.depths(); len(depths) != 0 {
		t.Fatalf("buffered pulls expanded: %v", depths)
	}

	// end == buffered length && !exhausted: this pull must know whether the
	// next row exists, so it expands geometrically (20 -> 40 -> 80 -> 160).
	for _, expected := range []int{40, 80, 160} {
		position := uint64(expected/2 - 5)
		page, pullErr := stream.Pull(ctx, position, 5)
		if pullErr != nil {
			t.Fatalf("edge pull at %d: %v", position, pullErr)
		}
		if len(page.Rows) != 5 || !page.HasMore {
			t.Fatalf("edge pull at %d: rows=%d hasMore=%t", position, len(page.Rows), page.HasMore)
		}
	}
	if got := fmt.Sprint(expander.depths()); got != "[40 80 160]" {
		t.Fatalf("expansion targets = %s, want [40 80 160]", got)
	}

	// A pull strictly inside the deepened buffer still expands nothing.
	before := expander.expansions()
	if _, err := stream.Pull(ctx, 45, 5); err != nil {
		t.Fatalf("interior pull: %v", err)
	}
	if depths := expander.depths(); len(depths) != before {
		t.Fatalf("interior pull expanded: %v", depths)
	}
}

// TestProgressiveLookaheadCoalescesConcurrentEdgePulls pins the other half of
// the per-pull lookahead: two pulls that need the same edge expansion while one
// is running produce one expansion, and the second pull is served from the
// buffer the first one published.
func TestProgressiveLookaheadCoalescesConcurrentEdgePulls(t *testing.T) {
	expander := &probeExpander{entered: make(chan struct{}), release: make(chan struct{})}
	stream, err := NewProgressive(probeRows(20), false, 20, expander.expand)
	if err != nil {
		t.Fatalf("NewProgressive: %v", err)
	}
	t.Cleanup(func() { _ = stream.Close() })
	ctx := context.Background()

	firstDone := make(chan error, 1)
	go func() {
		_, pullErr := stream.Pull(ctx, 15, 5)
		firstDone <- pullErr
	}()
	// The first pull is provably inside expand: it closed entered before blocking.
	<-expander.entered

	secondDone := make(chan error, 1)
	go func() {
		_, pullErr := stream.Pull(ctx, 15, 5)
		secondDone <- pullErr
	}()

	// Non-blocking observation only: the second pull has not published a page
	// while the expansion it depends on is still running.
	select {
	case err := <-secondDone:
		t.Fatalf("second edge pull returned while the expansion it needs was running: %v", err)
	default:
	}
	close(expander.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first edge pull: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second edge pull: %v", err)
	}
	if depths := expander.depths(); len(depths) != 1 {
		t.Fatalf("expansion targets = %v, want one expansion for concurrent edge pulls", depths)
	}
}

// BenchmarkProgressiveLookaheadExpansion measures one pull that consumes the
// last buffered row, so each operation pays one expansion of max(depth*2,
// end+1) rows. It isolates the stream's own lookahead cost (no storage, no
// ranking); the retrieval-side cost of that expansion is measured in
// pkg/search.
func BenchmarkProgressiveLookaheadExpansion(b *testing.B) {
	for _, startDepth := range []int{10, 50, 100, 400, 1600} {
		b.Run(fmt.Sprintf("depth=%d", startDepth), func(b *testing.B) {
			initial := probeRows(startDepth)
			expander := &probeExpander{}
			ctx := context.Background()
			position := uint64(startDepth - 1)
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				stream, err := NewProgressive(initial, false, startDepth, expander.expand)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := stream.Pull(ctx, position, 1); err != nil {
					b.Fatal(err)
				}
				_ = stream.Close()
			}
			b.StopTimer()
			depths := expander.depths()
			if len(depths) != b.N {
				b.Fatalf("expansions = %d, want %d", len(depths), b.N)
			}
			b.ReportMetric(float64(depths[len(depths)-1]), "final-depth")
		})
	}
}
