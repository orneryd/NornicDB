//go:build closeout_l2_e38_3

package storage

// L2 close-out, report entry 38.3: the first use of a namespace after an
// engine start reads every node and relationship key of the store
// (EnsureNamespaceMVCC -> namespaceMVCC -> recoverNamespaceMVCCFloor). The
// claim is a cost that grows with the store: the first call on a reopened
// store of 20,000 nodes takes about twenty times the first call on a store of
// 1,000 nodes, while the second call is constant.

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

const (
	l2E38_3Small    = 1000
	l2E38_3Large    = 20000
	l2E38_3Reopens  = 3
	l2E38_3MinRatio = 5.0
	// l2E38_3MinLarge keeps a sub-millisecond first call from tripping the
	// ratio on noise: the defect costs tens of milliseconds at 20,000 nodes.
	l2E38_3MinLarge = 5 * time.Millisecond
)

// l2E38_3Build writes n nodes nornic:n<i> (label N, no properties) for each
// namespace given, in bulk batches of 5,000, then closes the store.
func l2E38_3Build(t *testing.T, dir string, counts map[string]int) {
	t.Helper()
	engine, err := NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	ctx := context.Background()
	names := make([]string, 0, len(counts))
	for namespace := range counts {
		names = append(names, namespace)
	}
	sort.Strings(names)
	for _, namespace := range names {
		n := counts[namespace]
		for first := 0; first < n; first += 5000 {
			batch := make([]*Node, 0, min(5000, n-first))
			for i := first; i < min(first+5000, n); i++ {
				batch = append(batch, &Node{ID: NodeID(fmt.Sprintf("%s:n%d", namespace, i)), Labels: []string{"N"}})
			}
			if err := engine.BulkCreateNodes(ctx, batch); err != nil {
				_ = engine.Close()
				t.Fatalf("bulk create %s: %v", namespace, err)
			}
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close %s: %v", dir, err)
	}
}

// l2E38_3Reopen opens dir, times the first and the second
// EnsureNamespaceMVCC(namespace), and closes it.
func l2E38_3Reopen(t *testing.T, dir, namespace string) (first, second time.Duration) {
	t.Helper()
	engine, err := NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("reopen %s: %v", dir, err)
	}
	defer func() { _ = engine.Close() }()
	start := time.Now()
	if err := engine.EnsureNamespaceMVCC(namespace); err != nil {
		t.Fatalf("first EnsureNamespaceMVCC: %v", err)
	}
	first = time.Since(start)
	start = time.Now()
	if err := engine.EnsureNamespaceMVCC(namespace); err != nil {
		t.Fatalf("second EnsureNamespaceMVCC: %v", err)
	}
	second = time.Since(start)
	return first, second
}

func l2E38_3Median(values []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// l2E38_3Measure returns the first-call and second-call times of every reopen.
func l2E38_3Measure(t *testing.T, dir, namespace string) (firsts, seconds []time.Duration) {
	t.Helper()
	for range l2E38_3Reopens {
		first, second := l2E38_3Reopen(t, dir, namespace)
		firsts = append(firsts, first)
		seconds = append(seconds, second)
	}
	return firsts, seconds
}

func TestL2_e38_3_Repro(t *testing.T) {
	smallDir := filepath.Join(t.TempDir(), "small")
	largeDir := filepath.Join(t.TempDir(), "large")
	l2E38_3Build(t, smallDir, map[string]int{"nornic": l2E38_3Small})
	l2E38_3Build(t, largeDir, map[string]int{"nornic": l2E38_3Large})
	smallFirsts, smallSeconds := l2E38_3Measure(t, smallDir, "nornic")
	largeFirsts, largeSeconds := l2E38_3Measure(t, largeDir, "nornic")
	small, large := l2E38_3Median(smallFirsts), l2E38_3Median(largeFirsts)
	ratio := float64(large) / float64(max(small, time.Microsecond))
	figures := fmt.Sprintf("first-call median %d nodes=%v %d nodes=%v ratio=%.1f (threshold %.0f and >= %v); firsts small=%v large=%v; seconds small=%v large=%v",
		l2E38_3Small, small, l2E38_3Large, large, ratio, l2E38_3MinRatio, l2E38_3MinLarge, smallFirsts, largeFirsts, smallSeconds, largeSeconds)
	// Threshold: a scan of every key makes the first call grow with the store,
	// about 20x for 20x the nodes (the entry: 7 ms at 1,000, 60 ms at 8,000).
	// A first call that does not read the store's keys stays near the
	// second call's tens of microseconds at both sizes, a ratio near 1.
	if ratio >= l2E38_3MinRatio && large >= l2E38_3MinLarge {
		t.Logf("L2RESULT E38.3 DEFECT %s", figures)
		t.Errorf("first EnsureNamespaceMVCC after reopen grows with the store: %s", figures)
		return
	}
	t.Logf("L2RESULT E38.3 OK %s", figures)
}

// The control: the second call on the same open engine is the hot path and
// stays small at both sizes, and the stores hold what was written.
func TestL2_e38_3_Control(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "large")
	l2E38_3Build(t, dir, map[string]int{"nornic": l2E38_3Large})
	_, seconds := l2E38_3Measure(t, dir, "nornic")
	if median := l2E38_3Median(seconds); median > 5*time.Millisecond {
		t.Fatalf("second EnsureNamespaceMVCC median %v, want under 5ms: %v", median, seconds)
	}
	engine, err := NewBadgerEngine(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = engine.Close() }()
	count, err := engine.NodeCount(context.Background())
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if count != l2E38_3Large {
		t.Fatalf("NodeCount=%d, want %d", count, l2E38_3Large)
	}
	t.Logf("L2RESULT E38.3 CONTROL OK second-call=%v nodes=%d", seconds, count)
}

// The cause the entry names: recoverNamespaceMVCCFloor
// (badger_mvcc_per_namespace.go:265 at the pin) walks every node and
// relationship key of the store, whatever its namespace. Two diagnostics:
// (1) the step itself, timed directly on the large store, accounts for the
// first call's time; (2) a namespace holding one node pays for another
// namespace's 20,000 nodes on its first use.
func TestL2_e38_3_Cause(t *testing.T) {
	largeDir := filepath.Join(t.TempDir(), "large")
	l2E38_3Build(t, largeDir, map[string]int{"nornic": l2E38_3Large})
	engine, err := NewBadgerEngine(largeDir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	start := time.Now()
	seq, highWater, err := engine.recoverNamespaceMVCCFloor("nornic")
	direct := time.Since(start)
	_ = engine.Close()
	if err != nil {
		t.Fatalf("recoverNamespaceMVCCFloor: %v", err)
	}
	firsts, _ := l2E38_3Measure(t, largeDir, "nornic")
	t.Logf("L2CAUSE E38.3 recoverNamespaceMVCCFloor(nornic) on %d nodes took %v (seq=%d highWater=%d); first EnsureNamespaceMVCC=%v",
		l2E38_3Large, direct, seq, highWater, firsts)

	aloneDir := filepath.Join(t.TempDir(), "alone")
	mixedDir := filepath.Join(t.TempDir(), "mixed")
	l2E38_3Build(t, aloneDir, map[string]int{"tiny": 1})
	l2E38_3Build(t, mixedDir, map[string]int{"tiny": 1, "other": l2E38_3Large})
	aloneFirsts, _ := l2E38_3Measure(t, aloneDir, "tiny")
	mixedFirsts, _ := l2E38_3Measure(t, mixedDir, "tiny")
	alone, mixed := l2E38_3Median(aloneFirsts), l2E38_3Median(mixedFirsts)
	t.Logf("L2CAUSE E38.3 first EnsureNamespaceMVCC(tiny): alone=%v (all %v) beside %d nodes of another namespace=%v (all %v) ratio=%.1f",
		alone, aloneFirsts, l2E38_3Large, mixed, mixedFirsts, float64(mixed)/float64(max(alone, time.Microsecond)))
}
