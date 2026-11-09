package louvain

// Author: Louvain context API lane. Deterministic cancellation, deadline and
// work-boundary fixtures for the shared engine itself.

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"reflect"
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

// A cancelled run must not publish anything: every boundary the engine stops
// on returns the zero Result with the caller's error, and a live run of the
// same shaped graph still produces the same partition.
func TestRunObservesContextAtItsWorkBoundaries(t *testing.T) {
	graph := func() *Graph {
		g := NewGraph(8)
		for i := 0; i < 8; i++ {
			g.Link(i, (i+1)%8, 1)
		}
		return g
	}
	live := mustRun(t, graph(), DefaultOptions())

	stopped := 0
	for budget := 1; budget <= 4096; budget++ {
		ctx := fixtureowner.NewCheckpointContext(budget, nil)
		result, err := Run(ctx, graph(), DefaultOptions())
		if ctx.Context.Err() == nil {
			// The whole run fits in fewer boundaries than this budget.
			if err != nil || !reflect.DeepEqual(live, result) {
				t.Fatalf("budget %d: live run differs (%v)", budget, err)
			}
			break
		}
		if err != context.Canceled {
			t.Fatalf("budget %d: got %v, want context.Canceled", budget, err)
		}
		if len(result.Levels) != 0 || len(result.Representatives) != 0 || len(result.Modularities) != 0 {
			t.Fatalf("budget %d: cancelled run published %+v", budget, result)
		}
		if got := ctx.Checks(); got != budget {
			t.Fatalf("budget %d: cancelled run reached %d checkpoints", budget, got)
		}
		stopped++
	}
	if stopped < 8*4 {
		t.Fatalf("only %d cancellation boundaries exercised", stopped)
	}
}

// The live run checks the context inside its passes, not only at its entry:
// an eight-node graph needs more boundaries than one per node per level.
func TestRunChecksInsideItsPasses(t *testing.T) {
	graph := NewGraph(8)
	for i := 0; i < 8; i++ {
		graph.Link(i, (i+1)%8, 1)
	}
	ctx := fixtureowner.NewCheckpointContext(1<<20, nil)
	if _, err := Run(ctx, graph, DefaultOptions()); err != nil {
		t.Fatalf("live run: %v", err)
	}
	if ctx.Checks() <= 4*8 {
		t.Fatalf("live run reached only %d work boundaries", ctx.Checks())
	}
}

// The adapter stops before it reserves the graph's adjacency buffer: the
// caller cancels from inside the last relationship of the counting pass, and a
// stopped adapter must not allocate the two arcs per relationship.
func TestRunUndirectedStopsBeforeReservingAdjacency(t *testing.T) {
	const (
		nodes         = 16
		relationships = 1 << 18
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapted := 0
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result, err := RunUndirected(ctx, nodes, relationships, func(i int) (int, int, float64, bool) {
		adapted++
		if adapted == relationships {
			cancel()
		}
		return i % nodes, (i + 1) % nodes, 1, true
	}, DefaultOptions())
	runtime.ReadMemStats(&after)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if len(result.Levels) != 0 || len(result.Representatives) != 0 || len(result.Modularities) != 0 {
		t.Fatalf("stopped adapter published %+v", result)
	}
	if adapted != relationships {
		t.Fatalf("adapted %d relationships, want the counting pass's %d", adapted, relationships)
	}
	// 2*relationships arcs of 16 bytes are 8 MiB; the counting pass itself
	// allocates only the per-node counts.
	if reserved := after.TotalAlloc - before.TotalAlloc; reserved > 1<<20 {
		t.Fatalf("stopped adapter allocated %d bytes for %d relationships", reserved, relationships)
	}
}

// A high-degree node's arc scan is work the input controls, so the run stops
// inside it at the next stride boundary rather than after the node's arcs.
func TestRunStopsInsideHighDegreeArcScans(t *testing.T) {
	const links = 1 << 16 // 2*links arcs on one node
	graph := func() *Graph {
		g := NewGraph(2)
		for i := 0; i < links; i++ {
			g.Link(0, 0, 1)
		}
		g.Link(0, 1, 1)
		return g
	}
	probe := fixtureowner.NewCheckpointContext(1<<30, nil)
	live, err := Run(probe, graph(), DefaultOptions())
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	// Two nodes cannot reach this many boundaries on their own: it takes two
	// scans of the 2*links arcs, one stride apart.
	minimum := 2 * (2 * links) / checkStride
	total := probe.Checks()
	if total < minimum {
		t.Fatalf("live run reached %d boundaries, want at least %d", total, minimum)
	}
	checkStopsAt(t, graph, []int{1, 3, 17, minimum, total / 2, total - 1, total})
	again, err := Run(context.Background(), graph(), DefaultOptions())
	if err != nil || !reflect.DeepEqual(live, again) {
		t.Fatalf("live run after cancellation: %v", err)
	}
}

// A level sums, numbers, renumbers, initialises and copies every node, so a run
// stopped inside those passes publishes nothing and stops at that boundary.
func TestRunStopsInsideNodeSizedLevelWork(t *testing.T) {
	const nodes = 4096
	graph := func() *Graph {
		g := NewGraph(nodes)
		for i := 0; i < nodes; i++ {
			g.Link(i, (i+1)%nodes, 1)
		}
		return g
	}
	probe := fixtureowner.NewCheckpointContext(1<<30, nil)
	live, err := Run(probe, graph(), DefaultOptions())
	if err != nil {
		t.Fatalf("live run: %v", err)
	}
	// Two arcs per node are below one stride, so no arc scan contributes, and
	// two nodes cannot supply the boundaries either. A level sums, numbers,
	// renumbers and copies every node, so one checked level reaches eight
	// boundaries per node.
	total := probe.Checks()
	if minimum := 8 * nodes * len(live.Levels); total < minimum {
		t.Fatalf("live run reached %d boundaries, want at least %d", total, minimum)
	}
	checkStopsAt(t, graph, []int{4 * nodes, 20 * nodes, 40 * nodes, total - 1, total})
}

// The caller's deadline is a real timer, not a fabricated error: the bubble's
// clock advances to it while a relationship callback runs, and the adapter
// stops in the pass that observes it without publishing a partition.
func TestRunUndirectedStopsOnCallerDeadlineInsideWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		const relationships = 8
		adapted := 0
		result, err := RunUndirected(ctx, 4, relationships, func(i int) (int, int, float64, bool) {
			adapted++
			if adapted == 3 {
				// This callback is the only runnable work, so the clock
				// advances to the caller's deadline and its timer cancels the
				// run before the sleep returns.
				time.Sleep(2 * time.Second)
			}
			return i % 4, (i + 1) % 4, 1, true
		}, DefaultOptions())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want context.DeadlineExceeded", err)
		}
		if len(result.Levels) != 0 || len(result.Representatives) != 0 || len(result.Modularities) != 0 {
			t.Fatalf("expired adapter published %+v", result)
		}
		if adapted != 3 {
			t.Fatalf("adapted %d relationships after the deadline, want 3", adapted)
		}
	})
}

// An expired caller deadline stops both entry points at their first boundary
// with the deadline's identity and publishes nothing.
func TestRunStoppedDeadlinePublishesNothing(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(0, 0))
	defer cancel()
	graph := NewGraph(4)
	graph.Link(0, 1, 1)
	graph.Link(2, 3, 1)
	result, err := Run(ctx, graph, DefaultOptions())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	if len(result.Levels) != 0 || len(result.Representatives) != 0 || len(result.Modularities) != 0 {
		t.Fatalf("expired run published %+v", result)
	}
	adapted := 0
	adaptedResult, err := RunUndirected(ctx, 4, 4, func(i int) (int, int, float64, bool) {
		adapted++
		return i, (i + 1) % 4, 1, true
	}, DefaultOptions())
	if !errors.Is(err, context.DeadlineExceeded) || adapted != 0 {
		t.Fatalf("expired adapter: %v, adapted %d", err, adapted)
	}
	if len(adaptedResult.Levels) != 0 {
		t.Fatalf("expired adapter published %+v", adaptedResult)
	}
}

// RunUndirected adapts one relationship per callback; after the cancellation
// boundary no further relationship is adapted.
func TestRunUndirectedStopsAdaptingRelationships(t *testing.T) {
	const relationships = 64
	edges := make([][3]int, relationships)
	for i := range edges {
		edges[i] = [3]int{i % 16, (i + 1) % 16, 1}
	}
	for _, budget := range []int{1, 2, 3, 33} {
		adapted := 0
		var atCancel int
		ctx := fixtureowner.NewCheckpointContext(budget, func() { atCancel = adapted })
		result, err := RunUndirected(ctx, 16, relationships, func(i int) (int, int, float64, bool) {
			adapted++
			edge := edges[i]
			return edge[0], edge[1], float64(edge[2]), true
		}, DefaultOptions())
		if err != context.Canceled {
			t.Fatalf("budget %d: got %v, want context.Canceled", budget, err)
		}
		if len(result.Levels) != 0 {
			t.Fatalf("budget %d: cancelled adapter published %+v", budget, result)
		}
		if adapted != atCancel {
			t.Fatalf("budget %d: adapted %d relationships, cancellation observed at %d", budget, adapted, atCancel)
		}
	}
}

// An already-cancelled caller adapts no relationship at all.
func TestRunUndirectedCancelledCallerAdaptsNothing(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	adapted := 0
	result, err := RunUndirected(canceled, 4, 8, func(int) (int, int, float64, bool) {
		adapted++
		return 0, 1, 1, true
	}, DefaultOptions())
	if err != context.Canceled || adapted != 0 || len(result.Levels) != 0 {
		t.Fatalf("got %+v, %v, adapted %d", result, err, adapted)
	}
}

// checkStopsAt asserts that a run the caller stops at each budget returns ctx's
// error, stops at exactly that boundary, and publishes no part of a result.
func checkStopsAt(t *testing.T, graph func() *Graph, budgets []int) {
	t.Helper()
	for _, budget := range budgets {
		ctx := fixtureowner.NewCheckpointContext(budget, nil)
		result, err := Run(ctx, graph(), DefaultOptions())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("budget %d: got %v, want context.Canceled", budget, err)
		}
		if len(result.Levels) != 0 || len(result.Representatives) != 0 || len(result.Modularities) != 0 {
			t.Fatalf("budget %d: cancelled run published %+v", budget, result)
		}
		if ctx.Checks() != budget {
			t.Fatalf("budget %d: cancelled run reached %d checkpoints", budget, ctx.Checks())
		}
	}
}
