//go:build closeout_l2_e38_6

package storage

// L2 close-out, report entry 38.6: an equality index on a property whose value
// every node shares. Each node update files the node again under the shared
// value (CompositeIndex.ReplaceNode -> fileValueLocked -> appendUnique,
// schema_index_entries.go:72-80 and schema.go:1239 at the pin), a linear
// search of the one posting that holds every node: K updates on a posting of
// B members cost about K x B comparisons.
//
// Scaled from the entry's 20,000 nodes x 20,000 updates: 64,000 nodes share
// the value, and 2,000 updates (the most recently created nodes, which sit at
// the end of the posting) are timed per round, with and without the index.

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

const (
	l2E38_6Large   = 64000
	l2E38_6Small   = 2000
	l2E38_6Updates = 2000
	l2E38_6Rounds  = 3
	// l2E38_6MaxRatio separates the two outcomes. With the linear search each
	// update at the end of a 64,000-member posting compares about 64,000
	// node IDs (~150-250 us) on top of an update's own cost (~90 us in the
	// entry): with/without about 2.5-4. A posting with constant-time
	// membership leaves the index's cost a few microseconds: about 1.0-1.15.
	l2E38_6MaxRatio = 1.5
)

// l2E38_6Engine opens an engine on disk, optionally declares the equality
// index on Doc.kind, and bulk-creates n nodes nornic:n<i> {kind: "same", v: 0}.
func l2E38_6Engine(t *testing.T, n int, indexed bool) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngine(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	if indexed {
		if err := engine.GetSchema().AddPropertyIndex("common", "Doc", []string{"kind"}); err != nil {
			t.Fatalf("AddPropertyIndex: %v", err)
		}
	}
	ctx := context.Background()
	for first := 0; first < n; first += 5000 {
		batch := make([]*Node, 0, min(5000, n-first))
		for i := first; i < min(first+5000, n); i++ {
			batch = append(batch, l2E38_6Node(i, 0))
		}
		if err := engine.BulkCreateNodes(ctx, batch); err != nil {
			t.Fatalf("bulk create: %v", err)
		}
	}
	return engine
}

func l2E38_6Node(i int, v int64) *Node {
	return &Node{ID: NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"kind": "same", "v": v}}
}

// l2E38_6Update times one synchronous UpdateNode per node for the last
// l2E38_6Updates nodes of an engine holding n nodes, setting v.
func l2E38_6Update(t *testing.T, engine *BadgerEngine, n int, v int64) time.Duration {
	t.Helper()
	ctx := context.Background()
	start := time.Now()
	for i := n - l2E38_6Updates; i < n; i++ {
		if err := engine.UpdateNode(ctx, NodeUpdateRequest{After: l2E38_6Node(i, v)}); err != nil {
			t.Fatalf("UpdateNode n%d: %v", i, err)
		}
	}
	return time.Since(start)
}

func l2E38_6MedianFloat(values []float64) float64 {
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return sorted[len(sorted)/2]
}

// l2E38_6Ratios runs the rounds on an indexed and an unindexed engine of n
// nodes, interleaved, and returns the with/without ratios and raw times.
func l2E38_6Ratios(t *testing.T, n int) ([]float64, []time.Duration, []time.Duration) {
	t.Helper()
	withIndex := l2E38_6Engine(t, n, true)
	withoutIndex := l2E38_6Engine(t, n, false)
	var ratios []float64
	var with, without []time.Duration
	for round := 1; round <= l2E38_6Rounds; round++ {
		w := l2E38_6Update(t, withIndex, n, int64(round))
		o := l2E38_6Update(t, withoutIndex, n, int64(round))
		with, without = append(with, w), append(without, o)
		ratios = append(ratios, float64(w)/float64(max(o, time.Microsecond)))
	}
	return ratios, with, without
}

func TestL2_e38_6_Repro(t *testing.T) {
	largeRatios, largeWith, largeWithout := l2E38_6Ratios(t, l2E38_6Large)
	smallRatios, smallWith, smallWithout := l2E38_6Ratios(t, l2E38_6Small)
	large, small := l2E38_6MedianFloat(largeRatios), l2E38_6MedianFloat(smallRatios)
	figures := fmt.Sprintf("%d updates; posting %d: with/without median=%.2f (rounds with=%v without=%v); posting %d: median=%.2f (with=%v without=%v); threshold %.2f",
		l2E38_6Updates, l2E38_6Large, large, largeWith, largeWithout, l2E38_6Small, small, smallWith, smallWithout, l2E38_6MaxRatio)
	if large > l2E38_6MaxRatio {
		t.Logf("L2RESULT E38.6 DEFECT %s", figures)
		t.Errorf("updates under an equality index on a shared value cost in proportion to the posting: %s", figures)
		return
	}
	t.Logf("L2RESULT E38.6 OK %s", figures)
}

// The control: the same updates without the index, and the index's content
// after the updates (one entry per node under the shared value, every update
// stored).
func TestL2_e38_6_Control(t *testing.T) {
	engine := l2E38_6Engine(t, l2E38_6Small, true)
	plain := l2E38_6Engine(t, l2E38_6Small, false)
	l2E38_6Update(t, engine, l2E38_6Small, 7)
	l2E38_6Update(t, plain, l2E38_6Small, 7)
	for _, e := range []*BadgerEngine{engine, plain} {
		node, err := e.GetNode(context.Background(), NodeID(fmt.Sprintf("nornic:n%d", l2E38_6Small-1)))
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		if got := fmt.Sprint(node.Properties["v"]); got != "7" {
			t.Fatalf("v=%v after update, want 7", node.Properties["v"])
		}
	}
	indexes := engine.GetSchema().GetCompositeIndexesForLabel("Doc")
	if len(indexes) != 1 {
		t.Fatalf("Doc has %d composite indexes, want 1", len(indexes))
	}
	members := l2E38_6PostingSizes(indexes[0])
	if len(members) != 1 || members[0] != l2E38_6Small {
		t.Fatalf("posting sizes %v, want one posting of %d", members, l2E38_6Small)
	}
	t.Logf("L2RESULT E38.6 CONTROL OK updates stored on both engines; index posting sizes %v", members)
}

func l2E38_6PostingSizes(idx *CompositeIndex) []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	sizes := make([]int, 0, len(idx.values))
	for _, ids := range idx.values {
		sizes = append(sizes, len(ids))
	}
	sort.Ints(sizes)
	return sizes
}

// The cause the entry names, shown on the index itself: refiling a node under
// an unchanged value costs in proportion to its position in the posting
// (appendUnique's linear search). ReplaceNode is called directly for the
// first 2,000 nodes created and for the last 2,000 of a 64,000-member posting.
// A bulk batch of 5,000 files its nodes in map order, so the first ones sit
// within the posting's first 5,000 positions and the last ones beyond 60,000.
func TestL2_e38_6_Cause(t *testing.T) {
	engine := l2E38_6Engine(t, l2E38_6Large, true)
	indexes := engine.GetSchema().GetCompositeIndexesForLabel("Doc")
	if len(indexes) != 1 {
		t.Fatalf("Doc has %d composite indexes, want 1", len(indexes))
	}
	idx := indexes[0]
	same := map[string]interface{}{"kind": "same"}
	ctx := context.Background()
	refile := func(from int) time.Duration {
		start := time.Now()
		for i := from; i < from+l2E38_6Updates; i++ {
			if err := idx.ReplaceNode(ctx, NodeID(fmt.Sprintf("nornic:n%d", i)), same, same); err != nil {
				t.Fatalf("ReplaceNode: %v", err)
			}
		}
		return time.Since(start)
	}
	var fronts, backs []float64
	for range l2E38_6Rounds {
		fronts = append(fronts, float64(refile(0)))
		backs = append(backs, float64(refile(l2E38_6Large-l2E38_6Updates)))
	}
	front, back := l2E38_6MedianFloat(fronts), l2E38_6MedianFloat(backs)
	t.Logf("L2CAUSE E38.6 posting sizes %v; %d ReplaceNode calls: front of posting median=%v back of posting median=%v ratio=%.1f",
		l2E38_6PostingSizes(idx), l2E38_6Updates, time.Duration(front), time.Duration(back), back/max(front, 1))
}
