//go:build closeout_l2_e38_11

package cypher

import (
	"context"
	"runtime"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// E38.11: a warm full node read copies all of the node's vectors on every
// call. On the store of E38.10 with 4 documents, after three warm-up reads,
// GetNode(ctx, "test:src-0") (256 vectors of 2,048 values, 2.00 MiB)
// allocates 2.01 MiB per call over 10 calls; GetQueryNode of the same node
// 0.001 MiB.
//
// Threshold: a warm GetNode allocating more than a quarter of the node's
// vector bytes per call (0.5 MiB) is the defect: the entry's figure is
// 1.005x the vector bytes, and a warm read that shares or does not re-copy
// the cached vectors allocates the node's header and properties only (the
// GetQueryNode figure, about 1 KiB). No parser is involved.

const (
	l2E38_11Node  = storage.NodeID("test:src-0")
	l2E38_11Calls = 10
)

// perCall is the bytes per call of read over l2E38_11Calls calls, after
// three warm-up calls and a GC.
func l2E38_11PerCall(t *testing.T, read func() (*storage.Node, error)) (uint64, *storage.Node) {
	t.Helper()
	var last *storage.Node
	for i := 0; i < 3; i++ {
		n, err := read()
		if err != nil || n == nil {
			t.Fatalf("warm-up read: node=%v err=%v", n, err)
		}
		last = n
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < l2E38_11Calls; i++ {
		n, err := read()
		if err != nil || n == nil {
			t.Fatalf("read: node=%v err=%v", n, err)
		}
		last = n
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / l2E38_11Calls, last
}

func l2E38_11VectorBytes(n *storage.Node) uint64 {
	var total uint64
	for _, c := range n.ChunkEmbeddings {
		total += uint64(len(c)) * 4
	}
	for _, v := range n.NamedEmbeddings {
		total += uint64(len(v)) * 4
	}
	return total
}

func TestL2_e38_11_Repro(t *testing.T) {
	g := newNOR413Graph(t, 4, 256)
	ctx := context.Background()
	perCall, node := l2E38_11PerCall(t, func() (*storage.Node, error) { return g.engine.GetNode(ctx, l2E38_11Node) })
	vectors := l2E38_11VectorBytes(node)
	t.Logf("GetNode %s: %d chunk vectors, %d vector bytes; %d B per warm call", l2E38_11Node, len(node.ChunkEmbeddings), vectors, perCall)
	if len(node.ChunkEmbeddings) != 256 {
		t.Fatalf("GetNode returned %d chunk vectors, want 256 (the full read must keep returning the vectors)", len(node.ChunkEmbeddings))
	}
	bound := vectors / 4
	if perCall > bound {
		t.Logf("L2RESULT E38.11 DEFECT getNodePerCall=%dB (%.3f MiB) vectorBytes=%d bound=%d", perCall, float64(perCall)/(1<<20), vectors, bound)
		t.Errorf("a warm GetNode allocates %d B per call, %.2f times the node's %d vector bytes (bound %d)", perCall, float64(perCall)/float64(vectors), vectors, bound)
		return
	}
	t.Logf("L2RESULT E38.11 OK getNodePerCall=%dB (%.3f MiB) vectorBytes=%d bound=%d", perCall, float64(perCall)/(1<<20), vectors, bound)
}

// TestL2_e38_11_Control: GetQueryNode of the same node (metadata, no
// separately stored vectors) allocates far under the bound, and the store
// really holds the vectors (the Repro's figure is not an empty node's).
func TestL2_e38_11_Control(t *testing.T) {
	g := newNOR413Graph(t, 4, 256)
	ctx := context.Background()
	perCall, node := l2E38_11PerCall(t, func() (*storage.Node, error) { return g.engine.GetQueryNode(ctx, l2E38_11Node) })
	t.Logf("control GetQueryNode %s: %d B per warm call, %d chunk vectors returned; store vector bytes %d", l2E38_11Node, perCall, len(node.ChunkEmbeddings), g.vectorBytes)
	if perCall > 64<<10 {
		t.Errorf("control: GetQueryNode allocates %d B per call", perCall)
	}
	if g.vectorBytes < 2<<20 {
		t.Errorf("control: store holds only %d vector bytes", g.vectorBytes)
	}
}

// TestL2_e38_11_Cause: whether two warm GetNode results share the vectors'
// backing arrays (a shared cached image would allocate nothing for them) or
// each holds its own copy, and what one warm call allocates in mallocs. The
// owner read at the pin: BadgerEngine.GetNode -> getNodeSnapshotForRead
// (pkg/storage/badger_nodes.go:331-363), whose copyImage is copyNode for
// every full read, cache hits included.
func TestL2_e38_11_Cause(t *testing.T) {
	g := newNOR413Graph(t, 4, 256)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := g.engine.GetNode(ctx, l2E38_11Node); err != nil {
			t.Fatal(err)
		}
	}
	first, err := g.engine.GetNode(ctx, l2E38_11Node)
	if err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	second, err := g.engine.GetNode(ctx, l2E38_11Node)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	shared := len(first.ChunkEmbeddings) > 0 && len(second.ChunkEmbeddings) > 0 &&
		len(first.ChunkEmbeddings[0]) > 0 && len(second.ChunkEmbeddings[0]) > 0 &&
		&first.ChunkEmbeddings[0][0] == &second.ChunkEmbeddings[0][0]
	t.Logf("L2CAUSE E38.11 warm GetNode: %d B, %d mallocs; chunk vectors %d/%d; first vector shared between calls: %v",
		after.TotalAlloc-before.TotalAlloc, after.Mallocs-before.Mallocs, len(first.ChunkEmbeddings), len(second.ChunkEmbeddings), shared)
	// The same through the namespaced view the executor uses.
	perCall, _ := l2E38_11PerCall(t, func() (*storage.Node, error) { return g.store.GetNode(ctx, "src-0") })
	t.Logf("L2CAUSE E38.11 NamespacedEngine.GetNode(src-0): %d B per warm call", perCall)
}
