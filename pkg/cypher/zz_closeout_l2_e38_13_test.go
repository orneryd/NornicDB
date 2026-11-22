//go:build closeout_l2_e38_13

package cypher

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E38.13: an expression that takes the node as a value (id(n),
// n IS NOT NULL, labels(n), ...) allocates what returning the whole node
// allocates: on 2dbc67e96, for a node holding a 2,048-element list property,
// a 4,096-value named embedding and two 2,048-value chunk embeddings, each
// such expression costs +133 KB over the same statement on a small node,
// against +33.5 KB for `1` or `n.small`.
//
// The figure is the entry's: per-run TotalAlloc of MATCH (n:EIBig) RETURN e
// minus that of MATCH (n:EISmall) RETURN e (20 runs after 3 warm-ups and a
// GC). The assertion is on that difference minus the same difference for
// e = `1` (which reads the node's properties but not the node as a value).
//
// Threshold 40 KB: the entry's defect is +99.5 KB over `1` (133.0 - 33.5) for
// every node-valued expression listed, the vectors being 40 KB of float32;
// an expression that does not hydrate the node's vectors costs about 0 KB
// over `1` (b6d8e762a: -0.1 to +0.1 KB). The parser is the process's
// (NORNICDB_PARSER).

const (
	l2E38_13Bound  = 40 * 1024
	l2E38_13Warm   = 3
	l2E38_13Runs   = 20
	l2E38_13Smalls = "EISmall"
	l2E38_13Bigs   = "EIBig"
)

var l2E38_13NodeValued = []string{
	"id(n)",
	"n IS NOT NULL",
	"elementId(n)",
	"labels(n)",
	"size(keys(n))",
	"n['small']",
	"properties(n).small",
	"size([n])",
	"coalesce(n, 1) IS NULL",
	"CASE WHEN n IS NULL THEN 0 ELSE 1 END",
}

func l2E38_13Open(t *testing.T) *StorageExecutor {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "nornic")
	ctx := context.Background()
	if _, err := ns.CreateNode(ctx, &storage.Node{ID: "EISmall-1", Labels: []string{l2E38_13Smalls}, Properties: map[string]interface{}{"small": int64(1)}}); err != nil {
		t.Fatalf("small node: %v", err)
	}
	big := make([]interface{}, 2048)
	for i := range big {
		big[i] = int64(i)
	}
	node := &storage.Node{
		ID:              "EIBig-1",
		Labels:          []string{l2E38_13Bigs},
		Properties:      map[string]interface{}{"small": int64(1), "big": big},
		NamedEmbeddings: map[string][]float32{"default": make([]float32, 4096)},
		ChunkEmbeddings: [][]float32{make([]float32, 2048), make([]float32, 2048)},
	}
	if _, err := ns.CreateNode(ctx, node); err != nil {
		t.Fatalf("big node: %v", err)
	}
	return NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0, TTL: 0})
}

// perRun is the bytes per run of query, after warm-ups and a GC.
func l2E38_13PerRun(t *testing.T, exec *StorageExecutor, query string) int64 {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < l2E38_13Warm; i++ {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < l2E38_13Runs; i++ {
		if _, err := exec.Execute(ctx, query, nil); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	runtime.ReadMemStats(&after)
	return int64(after.TotalAlloc-before.TotalAlloc) / l2E38_13Runs
}

// delta is the big node's per-run figure minus the small node's for e.
func l2E38_13Delta(t *testing.T, exec *StorageExecutor, prefix, e string) int64 {
	t.Helper()
	small := l2E38_13PerRun(t, exec, fmt.Sprintf("MATCH (n:%s) %sRETURN %s AS v", l2E38_13Smalls, prefix, e))
	big := l2E38_13PerRun(t, exec, fmt.Sprintf("MATCH (n:%s) %sRETURN %s AS v", l2E38_13Bigs, prefix, e))
	return big - small
}

func TestL2_e38_13_Repro(t *testing.T) {
	exec := l2E38_13Open(t)
	base := l2E38_13Delta(t, exec, "", "1")
	whole := l2E38_13Delta(t, exec, "", "n")
	t.Logf("baseline `1`: %+.1f KB; whole node `n`: %+.1f KB", float64(base)/1000, float64(whole)/1000)
	var lines []string
	worst := int64(-1 << 62)
	worstExpr := ""
	check := func(prefix, e string) {
		d := l2E38_13Delta(t, exec, prefix, e)
		over := d - base
		label := prefix + e
		lines = append(lines, fmt.Sprintf("%q=%+.1fKB", label, float64(d)/1000))
		t.Logf("%s: %+.1f KB (%+.1f KB over `1`)", label, float64(d)/1000, float64(over)/1000)
		if over > worst {
			worst, worstExpr = over, label
		}
		if over > l2E38_13Bound {
			t.Errorf("%s allocates %+.1f KB more on the big node than `1` does (bound %d KB): it hydrates the whole node", label, float64(over)/1000, l2E38_13Bound/1024)
		}
	}
	for _, e := range l2E38_13NodeValued {
		check("", e)
	}
	for _, e := range []string{"1", "n.small", "n IS NOT NULL", "size(keys(n))"} {
		check("WITH n ", e)
	}
	verdict := "OK"
	if worst > l2E38_13Bound {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E38.13 %s base1=%+.1fKB n=%+.1fKB worstOver1=%+.1fKB(%s) %s", verdict, float64(base)/1000, float64(whole)/1000, float64(worst)/1000, worstExpr, strings.Join(lines, " "))
}

// TestL2_e38_13_Control: `n.small` (a property, not the node) and count(n)
// stay within the bound of `1`, and returning the whole node does cost the
// node's vectors and list (so the harness can see the difference).
func TestL2_e38_13_Control(t *testing.T) {
	exec := l2E38_13Open(t)
	base := l2E38_13Delta(t, exec, "", "1")
	for _, e := range []string{"n.small", "count(n)"} {
		d := l2E38_13Delta(t, exec, "", e)
		t.Logf("control %s: %+.1f KB (`1`: %+.1f KB)", e, float64(d)/1000, float64(base)/1000)
		if d-base > l2E38_13Bound {
			t.Errorf("control %s: %+.1f KB over `1`", e, float64(d-base)/1000)
		}
	}
	whole := l2E38_13Delta(t, exec, "", "n")
	t.Logf("control whole node `n`: %+.1f KB", float64(whole)/1000)
	if whole < 16*1024 {
		t.Errorf("control: returning the big node allocates only %+.1f KB more than the small node; the fixture does not hold the big values", float64(whole)/1000)
	}
}

// TestL2_e38_13_Cause measures, below the executor, the node reads a label
// scan can make for the big and the small node: GetNodeProjected with no
// property (labels only), GetQueryNode (metadata, no separate vectors) and
// GetNode (full). The entry's profile puts the cost under
// streamNodesByLabelProjectedInScope -> readNodeItemInTxn ->
// decodeNodeProjected (pkg/storage/badger_snapshot_body.go:52-60 at the
// pin), which decodes the whole body (vectors inline) when its property list
// is nil. Logged only: the read whose big-minus-small figure matches the
// statement's +133 KB is the one the statement takes.
func TestL2_e38_13_Cause(t *testing.T) {
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "nornic")
	ctx := context.Background()
	big := make([]interface{}, 2048)
	for i := range big {
		big[i] = int64(i)
	}
	if _, err := ns.CreateNode(ctx, &storage.Node{ID: "EISmall-1", Labels: []string{l2E38_13Smalls}, Properties: map[string]interface{}{"small": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.CreateNode(ctx, &storage.Node{ID: "EIBig-1", Labels: []string{l2E38_13Bigs}, Properties: map[string]interface{}{"small": int64(1), "big": big},
		NamedEmbeddings: map[string][]float32{"default": make([]float32, 4096)}, ChunkEmbeddings: [][]float32{make([]float32, 2048), make([]float32, 2048)}}); err != nil {
		t.Fatal(err)
	}
	measure := func(read func(id storage.NodeID) (*storage.Node, error), id storage.NodeID) (int64, *storage.Node) {
		var last *storage.Node
		for i := 0; i < 3; i++ {
			n, err := read(id)
			if err != nil {
				t.Fatalf("read %s: %v", id, err)
			}
			last = n
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < l2E38_13Runs; i++ {
			if _, err := read(id); err != nil {
				t.Fatalf("read %s: %v", id, err)
			}
		}
		runtime.ReadMemStats(&after)
		return int64(after.TotalAlloc-before.TotalAlloc) / l2E38_13Runs, last
	}
	reads := []struct {
		name string
		read func(id storage.NodeID) (*storage.Node, error)
	}{
		{"GetNodeProjected(labels only)", func(id storage.NodeID) (*storage.Node, error) { return ns.GetNodeProjected(ctx, id, []string{}) }},
		{"GetNodeProjected(nil)", func(id storage.NodeID) (*storage.Node, error) { return ns.GetNodeProjected(ctx, id, nil) }},
		{"GetQueryNode", func(id storage.NodeID) (*storage.Node, error) { return ns.GetQueryNode(ctx, id) }},
		{"GetNode", func(id storage.NodeID) (*storage.Node, error) { return ns.GetNode(ctx, id) }},
	}
	for _, r := range reads {
		small, _ := measure(r.read, "EISmall-1")
		bigBytes, node := measure(r.read, "EIBig-1")
		vectors := 0
		for _, c := range node.ChunkEmbeddings {
			vectors += len(c)
		}
		for _, v := range node.NamedEmbeddings {
			vectors += len(v)
		}
		t.Logf("L2CAUSE E38.13 %s: big %d B, small %d B, big-small %+.1f KB; big node returned %d vector values, %d properties",
			r.name, bigBytes, small, float64(bigBytes-small)/1000, vectors, len(node.Properties))
	}
}
