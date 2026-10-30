// Per-query profiling of the graphify push workload.
//
// graphify's push_to_neo4j emits two query shapes, one per node and one per
// edge, executed sequentially over Bolt:
//
//   MERGE (n:<Label> {id: $id}) SET n += $props
//   MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:<RelType>]->(b) SET r += $props
//
// Pushing a realistic NornicDB-scale graph (≈30k nodes / 80k edges) was
// observed to take many minutes over Bolt. This test runs the exact same
// statement shapes against an in-process StorageExecutor (no Bolt, no driver,
// no network) and reports per-class wall-clock latency, so we can attribute
// the slowness to one of:
//
//   A. The query plan / executor itself (per-statement cost is high regardless
//      of transport) — visible here as a high mean per-op in milliseconds.
//   B. The "one statement per network round-trip + implicit transaction commit"
//      harness (i.e. the Python loop + Bolt round-trip dominates, not Cypher) —
//      visible here as a low mean per-op (μs), implying the wire path is the
//      bottleneck.
//
// Two variants are profiled:
//
//   * WithIndexes — indexes on (:Code|:Document|:Entity)(id) are created
//     before the push, matching graphify's documented setup.
//   * WithoutIndexes — no indexes, exercising the scan fallback that graphify
//     hits on a fresh, unindexed database.
//
// Run with:
//
//   go test ./pkg/cypher/ -run TestGraphifyPushProfile -v -count=1 -timeout=10m
//
// or as a benchmark (loops over -benchtime):
//
//   go test ./pkg/cypher/ -run=^$ -bench=BenchmarkGraphifyPushProfile -benchtime=1x \
//       -cpuprofile=/tmp/push.prof -timeout=10m

package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// graphifyPushScale is the per-class population pushed during profiling.
// Defaults are sized to be representative of a real graphify push (mid-size
// repo) while still running in a few seconds per variant; `-short` shrinks it.
type graphifyPushScale struct {
	codeCount   int
	docCount    int
	entityCount int
}

func defaultPushScale(t testing.TB) graphifyPushScale {
	if testing.Short() {
		return graphifyPushScale{codeCount: 200, docCount: 50, entityCount: 30}
	}
	return graphifyPushScale{codeCount: 2000, docCount: 200, entityCount: 100}
}

// runGraphifyPushProfile pushes a payload at the requested scale and reports
// per-class timings. Returns the timings for the caller to log or assert on.
type pushTimings struct {
	nodeUpsertDur time.Duration
	nodeUpsertOps int
	edgeUpsertDur time.Duration
	edgeUpsertOps int
}

func (p pushTimings) report(tb testing.TB, label string) {
	tb.Helper()
	nodeAvg := time.Duration(0)
	if p.nodeUpsertOps > 0 {
		nodeAvg = p.nodeUpsertDur / time.Duration(p.nodeUpsertOps)
	}
	edgeAvg := time.Duration(0)
	if p.edgeUpsertOps > 0 {
		edgeAvg = p.edgeUpsertDur / time.Duration(p.edgeUpsertOps)
	}
	nodeOps := float64(p.nodeUpsertOps) / p.nodeUpsertDur.Seconds()
	edgeOps := float64(p.edgeUpsertOps) / p.edgeUpsertDur.Seconds()
	tb.Logf(
		"[%s] nodes: %d ops in %s — avg %s/op, %.0f ops/sec",
		label, p.nodeUpsertOps, p.nodeUpsertDur, nodeAvg, nodeOps,
	)
	tb.Logf(
		"[%s] edges: %d ops in %s — avg %s/op, %.0f ops/sec",
		label, p.edgeUpsertOps, p.edgeUpsertDur, edgeAvg, edgeOps,
	)
}

func runGraphifyPushProfile(tb testing.TB, withIndexes bool, scale graphifyPushScale) pushTimings {
	tb.Helper()
	base := newTestMemoryEngine(tb)
	ns := storage.NewNamespacedEngine(base, "profile")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	if withIndexes {
		for _, q := range []string{
			"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
			"CREATE INDEX document_id IF NOT EXISTS FOR (n:Document) ON (n.id)",
			"CREATE INDEX entity_id IF NOT EXISTS FOR (n:Entity) ON (n.id)",
		} {
			if _, err := exec.Execute(ctx, q, nil); err != nil {
				tb.Fatalf("index ddl %q: %v", q, err)
			}
		}
	}

	payload := buildGraphifyScenarioPayload(scale.codeCount, scale.docCount, scale.entityCount)

	// ---- per-node MERGE timing ---------------------------------------------
	nodeStart := time.Now()
	for _, n := range payload.nodes {
		q := fmt.Sprintf(graphifyNodeUpsertQuery, n.label)
		if _, err := exec.Execute(ctx, q, map[string]interface{}{"id": n.id, "props": n.props}); err != nil {
			tb.Fatalf("node upsert %s/%s: %v", n.label, n.id, err)
		}
	}
	nodeDur := time.Since(nodeStart)

	// ---- per-edge MATCH+MERGE timing ---------------------------------------
	edgeStart := time.Now()
	for _, e := range payload.edges {
		q := fmt.Sprintf(graphifyEdgeUpsertQuery, e.relType)
		if _, err := exec.Execute(ctx, q, map[string]interface{}{"src": e.src, "tgt": e.tgt, "props": e.props}); err != nil {
			tb.Fatalf("edge upsert %s:[%s->%s]: %v", e.relType, e.src, e.tgt, err)
		}
	}
	edgeDur := time.Since(edgeStart)

	return pushTimings{
		nodeUpsertDur: nodeDur,
		nodeUpsertOps: len(payload.nodes),
		edgeUpsertDur: edgeDur,
		edgeUpsertOps: len(payload.edges),
	}
}

// TestGraphifyPushProfile_WithIndexes profiles the graphify push workload with
// the documented (:Label)(id) indexes in place — the "happy path" graphify
// operators are told to set up before a push.
func TestGraphifyPushProfile_WithIndexes(t *testing.T) {
	requirePerformanceWorkload(t)
	scale := defaultPushScale(t)
	timings := runGraphifyPushProfile(t, true, scale)
	timings.report(t, "with-indexes")
}

// TestGraphifyPushProfile_WithoutIndexes profiles the same workload but with
// no indexes — the path graphify hits on a fresh, unindexed NornicDB.
// Smaller-scale to keep wall-clock bounded.
func TestGraphifyPushProfile_WithoutIndexes(t *testing.T) {
	requirePerformanceWorkload(t)
	scale := defaultPushScale(t)
	// Without indexes the per-op cost is O(label population); shrink the
	// edge population to keep the test under a couple of minutes.
	scale.codeCount = scale.codeCount / 4
	scale.docCount = scale.docCount / 4
	scale.entityCount = scale.entityCount / 4
	timings := runGraphifyPushProfile(t, false, scale)
	timings.report(t, "without-indexes")
}

// BenchmarkGraphifyPushProfile_WithIndexes is the same workload exposed as a
// Go benchmark so that -cpuprofile / -memprofile can attribute time per
// in-process Cypher path.
func BenchmarkGraphifyPushProfile_WithIndexes(b *testing.B) {
	scale := defaultPushScale(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runGraphifyPushProfile(b, true, scale)
	}
}

// TestGraphifyPushProfile_EdgeShapeHypothesis isolates the cost of graphify's
// labelless edge MERGE shape vs a labeled variant. The labelless shape is
// `MATCH (a {id: $src}), (b {id: $tgt}) MERGE ...` — the per-label
// (:Code)(id) index cannot fire because the pattern has no label, so the
// executor falls back to a full node scan twice per edge. If the labeled
// variant is materially faster, that confirms graphify's push shape is the
// bottleneck (not Nornic's per-statement cost) and points at a single-line
// fix in graphify/graphify/export.py.
func TestGraphifyPushProfile_EdgeShapeHypothesis(t *testing.T) {
	requirePerformanceWorkload(t)
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "edge_shape")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE INDEX code_id IF NOT EXISTS FOR (n:Code) ON (n.id)",
	} {
		if _, err := exec.Execute(ctx, q, nil); err != nil {
			t.Fatalf("index ddl: %v", err)
		}
	}

	// Seed a single-label population so both edge shapes can resolve through
	// the same set of nodes.
	const codeCount = 1000
	for i := 0; i < codeCount; i++ {
		id := fmt.Sprintf("node_%05d", i)
		if _, err := exec.Execute(ctx,
			"MERGE (n:Code {id: $id}) SET n.label = $lbl",
			map[string]interface{}{"id": id, "lbl": id}); err != nil {
			t.Fatal(err)
		}
	}

	// --- labelless MATCH (graphify's current shape) -------------------------
	const labellessQ = `MATCH (a {id: $src}), (b {id: $tgt}) MERGE (a)-[r:LINKS_A]->(b)`
	const edgeOps = 500
	startA := time.Now()
	for i := 0; i+1 < edgeOps; i++ {
		params := map[string]interface{}{
			"src": fmt.Sprintf("node_%05d", i),
			"tgt": fmt.Sprintf("node_%05d", i+1),
		}
		if _, err := exec.Execute(ctx, labellessQ, params); err != nil {
			t.Fatalf("labelless edge %d: %v", i, err)
		}
	}
	labellessDur := time.Since(startA)

	// --- labeled MATCH (the index-driven variant) ---------------------------
	const labeledQ = `MATCH (a:Code {id: $src}), (b:Code {id: $tgt}) MERGE (a)-[r:LINKS_B]->(b)`
	startB := time.Now()
	for i := 0; i+1 < edgeOps; i++ {
		params := map[string]interface{}{
			"src": fmt.Sprintf("node_%05d", i),
			"tgt": fmt.Sprintf("node_%05d", i+1),
		}
		if _, err := exec.Execute(ctx, labeledQ, params); err != nil {
			t.Fatalf("labeled edge %d: %v", i, err)
		}
	}
	labeledDur := time.Since(startB)

	t.Logf("[edge-shape] labelless: %d ops in %s — avg %s/op",
		edgeOps-1, labellessDur, labellessDur/time.Duration(edgeOps-1))
	t.Logf("[edge-shape] labeled:   %d ops in %s — avg %s/op",
		edgeOps-1, labeledDur, labeledDur/time.Duration(edgeOps-1))
	if labellessDur > 0 {
		t.Logf("[edge-shape] labeled is %.1fx %s than labelless",
			float64(labellessDur)/float64(labeledDur),
			pickWord(labellessDur > labeledDur, "faster", "slower"))
	}
}

func pickWord(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}
