package resultstream

// These probes characterize the Progressive stream's demand-driven page
// contract without storage, network, clocks, timers or sleeps: a pull always
// publishes the rows it can already serve, and expansion runs only for a
// request the buffered rows cannot fill. Every ordering is established with
// channels, so each test is deterministic.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testlatch"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// probeRows returns a deterministic row prefix of the given depth. It is shared
// with the other resultstream tests.
func probeRows(depth int) [][]any {
	rows := make([][]any, depth)
	for index := range rows {
		rows[index] = []any{index}
	}
	return rows
}

// probeGate parks one expansion until the test releases it, so a test can
// observe what other pulls do while that expansion is unfinished. entered is
// closed once, by the first expansion that reaches the gate; release is closed
// once by the test. Cleanup also releases the gate, so a failing test never
// leaves a pull blocked inside the expander.
type probeGate struct {
	entered chan struct{}
	release *testlatch.Latch
	once    sync.Once
}

func newProbeGate(t *testing.T) *probeGate {
	t.Helper()
	gate := &probeGate{
		entered: make(chan struct{}),
		release: testlatch.New(),
	}
	t.Cleanup(gate.releaseGate)
	return gate
}

// wait blocks the expansion until the test releases the gate or the pull's
// context ends.
func (g *probeGate) wait(ctx context.Context) error {
	g.once.Do(func() { close(g.entered) })
	select {
	case <-g.release.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseGate unblocks the gated expansion. Repeated calls are no-ops, so the
// test may release explicitly and the cleanup release stays safe.
func (g *probeGate) releaseGate() {
	g.release.Release()
}

// waitEntered blocks until an expansion is provably parked at the gate.
func (g *probeGate) waitEntered(ctx context.Context, t *testing.T) {
	t.Helper()
	select {
	case <-g.entered:
	case <-ctx.Done():
		t.Fatalf("no expansion reached the gate: %v", ctx.Err())
	}
}

// probeExpander records every requested expansion depth and optionally parks
// the expansion at a gate. A pull runs the expansion on its own goroutine, not
// on the test goroutine, so the recorded depths are guarded.
type probeExpander struct {
	mu        sync.Mutex
	requested []int
	gate      *probeGate
}

func (e *probeExpander) expand(ctx context.Context, depth int) ([][]any, bool, error) {
	e.mu.Lock()
	e.requested = append(e.requested, depth)
	e.mu.Unlock()
	if e.gate != nil {
		if err := e.gate.wait(ctx); err != nil {
			return nil, false, err
		}
	}
	return probeRows(depth), false, nil
}

// depths returns the expansion depths requested so far.
func (e *probeExpander) depths() []int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]int(nil), e.requested...)
}

// newProbeStream builds a non-exhausted stream whose rows are buffered to depth
// and closes it when the test ends.
func newProbeStream(t *testing.T, rows [][]any, depth int, expander *probeExpander) *Progressive {
	t.Helper()
	stream, err := NewProgressive(rows, false, depth, expander.expand)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })
	return stream
}

// TestProgressiveBoundaryPageDoesNotExpand pins that a page the buffered rows
// already fill is published immediately, including the page that ends exactly
// at the buffered end. HasMore reports the unfinished search, so the stream does
// not have to discover the next row before answering.
func TestProgressiveBoundaryPageDoesNotExpand(t *testing.T) {
	expander := &probeExpander{}
	stream := newProbeStream(t, probeRows(20), 20, expander)
	ctx := t.Context()

	for _, pull := range []struct {
		position uint64
		n        int
	}{
		{position: 0, n: 20},
		{position: 15, n: 5},
		{position: 0, n: 5},
	} {
		page, err := stream.Pull(ctx, pull.position, pull.n)
		require.NoError(t, err, "position %d", pull.position)
		require.Equal(t, probeRows(20)[pull.position:pull.position+uint64(pull.n)], page.Rows)
		require.Equal(t, pull.position, page.Position)
		require.Equal(t, pull.position+uint64(pull.n), page.Next)
		require.True(t, page.HasMore, "search is not exhausted at position %d", pull.position)
		require.Nil(t, page.Total)
	}
	require.Empty(t, expander.depths(), "ready pages must not expand")
}

// TestProgressiveInsufficientPageExpandsToDoublingDepth pins the demand-driven
// expansion: a request that starts at the buffered end needs rows the buffer
// does not hold, so the stream expands to max(depth*2, end+1) and then
// publishes the page.
func TestProgressiveInsufficientPageExpandsToDoublingDepth(t *testing.T) {
	expander := &probeExpander{}
	stream := newProbeStream(t, probeRows(20), 20, expander)
	ctx := t.Context()

	for index, step := range []struct {
		position uint64
		depth    int
	}{
		{position: 20, depth: 40},
		{position: 40, depth: 80},
		{position: 80, depth: 160},
	} {
		page, err := stream.Pull(ctx, step.position, 5)
		require.NoError(t, err, "position %d", step.position)
		require.Equal(t, probeRows(step.depth)[step.position:step.position+5], page.Rows)
		require.Equal(t, step.position, page.Position)
		require.Equal(t, step.position+5, page.Next)
		require.True(t, page.HasMore)
		require.Nil(t, page.Total)
		require.Equal(t, []int{40, 80, 160}[:index+1], expander.depths())
	}

	// A full page inside the deepened rows is ready and expands nothing.
	page, err := stream.Pull(ctx, 100, 5)
	require.NoError(t, err)
	require.Equal(t, probeRows(160)[100:105], page.Rows)
	require.True(t, page.HasMore)
	require.Len(t, expander.depths(), 3, "ready pages must not expand")
}

// TestProgressiveServesReadyPageDuringExpansion pins that a pull waits only for
// the expansion its own request needs: rows buffered before an unfinished
// expansion are still published as a full page.
func TestProgressiveServesReadyPageDuringExpansion(t *testing.T) {
	gate := newProbeGate(t)
	defer gate.releaseGate()
	expander := &probeExpander{gate: gate}
	stream := newProbeStream(t, probeRows(20), 20, expander)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	type outcome struct {
		page *Page
		err  error
	}
	expanding := make(chan outcome, 1)
	go func() {
		page, err := stream.Pull(ctx, 20, 5)
		expanding <- outcome{page: page, err: err}
	}()
	gate.waitEntered(ctx, t)

	// Rows 15..20 are replayable before the gated expansion publishes, so this
	// pull must answer without waiting for that expansion.
	page, err := stream.Pull(ctx, 15, 5)
	require.NoError(t, err, "ready page must not wait for the unfinished expansion")
	require.Equal(t, probeRows(20)[15:20], page.Rows)
	require.True(t, page.HasMore)
	require.Nil(t, page.Total)

	select {
	case result := <-expanding:
		t.Fatalf("gated pull returned before its expansion was released: %v", result.err)
	default:
	}
	require.Equal(t, []int{40}, expander.depths())

	gate.releaseGate()
	select {
	case result := <-expanding:
		require.NoError(t, result.err)
		require.Equal(t, probeRows(40)[20:25], result.page.Rows)
		require.True(t, result.page.HasMore)
	case <-ctx.Done():
		t.Fatalf("gated pull did not complete: %v", ctx.Err())
	}
}

// TestProgressiveCoalescesConcurrentInsufficientPulls pins that pulls needing
// the same expansion share one: the second pull waits for the in-flight
// expansion and is then served from the rows it published.
func TestProgressiveCoalescesConcurrentInsufficientPulls(t *testing.T) {
	gate := newProbeGate(t)
	defer gate.releaseGate()
	expander := &probeExpander{gate: gate}
	stream := newProbeStream(t, probeRows(20), 20, expander)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	type outcome struct {
		page *Page
		err  error
	}
	startPull := func() chan outcome {
		done := make(chan outcome, 1)
		go func() {
			page, err := stream.Pull(ctx, 20, 5)
			done <- outcome{page: page, err: err}
		}()
		return done
	}

	first := startPull()
	gate.waitEntered(ctx, t)
	second := startPull()

	// Neither pull can publish rows 20..25 until the gated expansion does.
	select {
	case result := <-second:
		t.Fatalf("second insufficient pull returned while the shared expansion was unfinished: %v", result.err)
	default:
	}

	gate.releaseGate()
	for _, done := range []chan outcome{first, second} {
		select {
		case result := <-done:
			require.NoError(t, result.err)
			require.Equal(t, probeRows(40)[20:25], result.page.Rows)
			require.True(t, result.page.HasMore)
			require.Nil(t, result.page.Total)
		case <-ctx.Done():
			t.Fatalf("insufficient pull did not complete: %v", ctx.Err())
		}
	}
	require.Equal(t, []int{40}, expander.depths(), "concurrent insufficient pulls must share one expansion")
}

// BenchmarkProgressiveExpansionPerPull measures one pull that requests a row
// starting at the buffered end, so each operation must expand to
// max(depth*2, end+1) before it can publish. It isolates the stream's own
// expansion cost (no storage, no ranking); the retrieval-side cost of that
// expansion is measured in pkg/search.
func BenchmarkProgressiveExpansionPerPull(b *testing.B) {
	for _, startDepth := range []int{10, 50, 100, 400, 1600} {
		b.Run(fmt.Sprintf("depth=%d", startDepth), func(b *testing.B) {
			initial := probeRows(startDepth)
			expander := &probeExpander{}
			ctx := context.Background()
			position := uint64(startDepth)
			var last *Page
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				stream, err := NewProgressive(initial, false, startDepth, expander.expand)
				if err != nil {
					b.Fatal(err)
				}
				page, err := stream.Pull(ctx, position, 1)
				if err != nil {
					b.Fatal(err)
				}
				last = page
				_ = stream.Close()
			}
			b.StopTimer()
			depths := expander.depths()
			if len(depths) != b.N {
				b.Fatalf("expansions = %d, want %d", len(depths), b.N)
			}
			if want := 2 * startDepth; depths[len(depths)-1] != want {
				b.Fatalf("expansion depth = %d, want %d", depths[len(depths)-1], want)
			}
			if last == nil || len(last.Rows) != 1 || last.Rows[0][0] != startDepth {
				b.Fatalf("published page = %v, want the single row at the buffered end", last)
			}
			b.ReportMetric(float64(depths[len(depths)-1]), "expansion-depth")
		})
	}
}
