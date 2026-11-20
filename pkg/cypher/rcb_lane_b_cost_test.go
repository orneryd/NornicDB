package cypher

// Root-cause diagnostics, v1.5.2 verification campaign, lane B (test-only).
//
//   TestRCBChainedMatchCostSteps   G-chained-match (NOR-118): the status
//     statement split into its steps on the NOR-118 fixture, each step timed,
//     plus CPU and allocation profiles of the whole statement and its PROFILE.
//   TestRCBTraversalFarNodeLoads   G-traversal-far-node-load: PROFILE and time
//     of single-hop expansions from one hub, relationship filter in WHERE
//     against an inline property map.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func rcbPprof(t *testing.T, tag string, args ...string) {
	t.Helper()
	goBin, err := osexec.LookPath("go")
	if err != nil {
		goBin = filepath.Join(runtime.GOROOT(), "bin", "go")
	}
	out, err := osexec.Command(goBin, append([]string{"tool", "pprof"}, args...)...).CombinedOutput()
	t.Logf("%s pprof %v err=%v", tag, args, err)
	for _, line := range strings.Split(string(out), "\n") {
		if strings.TrimSpace(line) != "" {
			t.Logf("%s | %s", tag, line)
		}
	}
}

func rcbMetadata(result *ExecuteResult, limit int) string {
	if result == nil {
		return "<nil>"
	}
	raw, err := json.Marshal(result.Metadata)
	text := string(raw)
	if err != nil {
		text = fmt.Sprintf("%v", result.Metadata)
	}
	if len(text) > limit {
		text = text[:limit] + "…"
	}
	return text
}

func TestRCBChainedMatchCostSteps(t *testing.T) {
	k := nor118EnvInt(t, "RCB118_PER_VERSION", 300)
	engine, err := storage.NewBadgerEngine(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	e := NewStorageExecutor(storage.NewNamespacedEngine(engine, "nor118"))
	e.cache = nil
	want := nor118Fixture(t, e, k)
	logical := 5 * k * k
	head := "MATCH (u:Occurrence {batch_id: $batch_id})\nMATCH (v:Version {id: u.version_id})\nMATCH (f:Occurrence)-[:OF_VERSION]->(v)\n"
	variants := []struct{ name, q string }{
		{"join-count", head + "RETURN count(*) AS n"},
		{"join-with-ufv", head + "WITH u, v, f RETURN count(*) AS n"},
		{"join-order", head + "WITH u, v, f ORDER BY f.uploaded_at, f.id RETURN count(*) AS n"},
		{"join-collect-no-order", head + "WITH u, v, collect(f.batch_id)[0] AS first_batch_id RETURN count(*) AS n"},
		{"join-order-collect", head + "WITH u, v, f ORDER BY f.uploaded_at, f.id WITH u, v, collect(f.batch_id)[0] AS first_batch_id RETURN count(*) AS n"},
		{"join-scalars-order-collect", head + "WITH u.id AS uid, v.id AS vid, f.uploaded_at AS at, f.id AS fid, f.batch_id AS fb ORDER BY at, fid WITH uid, vid, collect(fb)[0] AS first_batch_id RETURN count(*) AS n"},
		{"control", nor118DedupedControl},
		{"status", nor118Status},
		// Bypass of the per-row subscript validation (row_subscript_validation.go:58):
		// the same first element without a trailing subscript.
		{"status-head-collect", strings.Replace(nor118Status, "collect(f.batch_id)[0]", "head(collect(f.batch_id))", 1)},
		{"join-head-collect-no-order", head + "WITH u, v, head(collect(f.batch_id)) AS first_batch_id RETURN count(*) AS n"},
	}
	for _, variant := range variants {
		m := nor118Run(e, variant.q)
		first := ""
		if len(m.rows) > 0 {
			first = fmt.Sprint(m.rows[0])
			if len(first) > 80 {
				first = first[:80]
			}
		}
		t.Logf("RCB118 STEP k=%d logical_rows=%d variant=%s seconds=%.3f alloc_bytes=%d alloc_per_logical_row=%.0f peak_heap_mib=%.1f rows=%d first=%s err=%v",
			k, logical, variant.name, m.elapsed.Seconds(), m.allocated, float64(m.allocated)/float64(logical), float64(m.peakHeap)/(1<<20), len(m.rows), first, m.err)
		if variant.name == "status" || variant.name == "control" || variant.name == "status-head-collect" {
			nor118Compare(t, variant.name, m.rows, want)
		}
	}

	dir := t.TempDir()
	cpuFile := filepath.Join(dir, "status.cpu.pprof")
	beforeFile := filepath.Join(dir, "allocs.before.pprof")
	afterFile := filepath.Join(dir, "allocs.after.pprof")
	runtime.GC()
	if f, err := os.Create(beforeFile); err == nil {
		_ = pprof.Lookup("allocs").WriteTo(f, 0)
		_ = f.Close()
	}
	cpu, err := os.Create(cpuFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(cpu); err != nil {
		t.Fatal(err)
	}
	m := nor118Run(e, nor118Status)
	pprof.StopCPUProfile()
	_ = cpu.Close()
	if f, err := os.Create(afterFile); err == nil {
		_ = pprof.Lookup("allocs").WriteTo(f, 0)
		_ = f.Close()
	}
	t.Logf("RCB118 PROFILED status seconds=%.3f alloc_bytes=%d err=%v", m.elapsed.Seconds(), m.allocated, m.err)
	rcbPprof(t, "RCB118 CPU-CUM", "-top", "-cum", "-nodecount=45", cpuFile)
	rcbPprof(t, "RCB118 CPU-FLAT", "-top", "-nodecount=25", cpuFile)
	rcbPprof(t, "RCB118 ALLOC-CUM", "-top", "-cum", "-sample_index=alloc_space", "-nodecount=45", "-base", beforeFile, afterFile)
	rcbPprof(t, "RCB118 ALLOC-FLAT", "-top", "-sample_index=alloc_space", "-nodecount=25", "-base", beforeFile, afterFile)

	profiled, err := e.Execute(context.Background(), "PROFILE "+nor118Status, map[string]interface{}{"batch_id": "batch"})
	t.Logf("RCB118 PLAN err=%v metadata=%s", err, rcbMetadata(profiled, 12000))
}

func TestRCBTraversalFarNodeLoads(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	e := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"))
	e.cache = nil
	run := func(q string, params map[string]interface{}) (*ExecuteResult, time.Duration) {
		start := time.Now()
		result, err := e.Execute(ctx, q, params)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		return result, time.Since(start)
	}
	const degree = 4000
	run("CREATE INDEX rcb_hub_key FOR (n:Hub) ON (n.key)", nil)
	run("CREATE (:Hub {key:'h0'})", nil)
	for from := 0; from < degree; from += 1000 {
		run("MATCH (h:Hub {key:'h0'}) UNWIND range($from, $to - 1) AS i CREATE (h)-[:REL {kind:'other', id:'h0-' + toString(i)}]->(:Item {id:'h0-' + toString(i)})",
			map[string]interface{}{"from": int64(from), "to": int64(from + 1000)})
	}
	run("MATCH (h:Hub {key:'h0'}) CREATE (h)-[:REL {kind:'same', id:'h0-same'}]->(:Item {id:'h0-same'})", nil)
	pre := "MATCH (h:Hub {key:'h0'}) "
	forms := []struct{ name, q string }{
		{"B-plain-where", pre + "OPTIONAL MATCH (h)-[r:REL]-(o:Item) WHERE r.kind = 'same' RETURN count(o)"},
		{"C-inline-property", pre + "OPTIONAL MATCH (h)-[r:REL {kind: 'same'}]-(o:Item) RETURN count(o)"},
		{"E-no-far-node", pre + "OPTIONAL MATCH (h)-[r:REL]-() WHERE r.kind = 'same' RETURN count(r)"},
		{"F-every-far-node", pre + "MATCH (h)-[r:REL]-(o:Item) RETURN count(o)"},
		{"G-every-rel-no-far-node", pre + "MATCH (h)-[r:REL]-() RETURN count(r)"},
	}
	for _, form := range forms {
		var times []time.Duration
		var rows interface{}
		for rep := 0; rep < 5; rep++ {
			result, elapsed := run(form.q, nil)
			times = append(times, elapsed)
			rows = result.Rows
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		profiled, _ := run("PROFILE "+form.q, nil)
		t.Logf("RCBFAR FORM %s degree=%d rows=%v median_ms=%.3f", form.name, degree, rows, float64(times[len(times)/2].Microseconds())/1000)
		t.Logf("RCBFAR PLAN %s metadata=%s", form.name, rcbMetadata(profiled, 6000))
	}
}
