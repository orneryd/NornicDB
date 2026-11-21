package server

// Verifier of hunt leads L-H6-001 (CREATE INDEX on a label property) and
// L-H6-002 (UNWIND ... CREATE), test-side only, not for merge.
//
// TestVerifyH6Steps builds the frozen H6 graph (huntH6Build) at each scale and
// times the steps of the two statements:
//
//   - T06 by direct calls on the same store, the steps the DDL owner runs
//     (pkg/cypher/schema.go addPropertyIndex / backfillCompositeIndex): read the
//     label's stored nodes, build the entries map, declare the index, fill it,
//     drop it. VERIFY_H6_STEP lines.
//   - T06 through Bolt (the user path), with the server goroutines' stacks
//     sampled every 50 ms, and a reference linear read over the same nodes
//     (MATCH (n:G) RETURN count(n.s)). VERIFY_H6_PROFILE lines.
//   - T26 through Bolt with list sizes 1000, 16000 and 64000 on every store
//     scale, so list size and store size are separated, sampled the same way.
//
// Every timed call also reports the Go runtime's GC CPU, GC cycles and live
// heap around it (runtime/metrics), so GC growth is separated from the work.
//
// Run: VERIFY_H6=1 [VERIFY_H6_SCALES=1,16,64]
//      go test ./pkg/server -run '^TestVerifyH6Steps$' -count=1 -timeout=60m -v

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var verifyH6MetricNames = []string{
	"/cpu/classes/gc/total:cpu-seconds",
	"/cpu/classes/total:cpu-seconds",
	"/gc/cycles/total:gc-cycles",
	"/gc/heap/live:bytes",
	"/gc/heap/allocs:bytes",
}

func verifyH6Metrics() map[string]float64 {
	samples := make([]metrics.Sample, len(verifyH6MetricNames))
	for i, name := range verifyH6MetricNames {
		samples[i].Name = name
	}
	metrics.Read(samples)
	out := map[string]float64{}
	for _, s := range samples {
		switch s.Value.Kind() {
		case metrics.KindFloat64:
			out[s.Name] = s.Value.Float64()
		case metrics.KindUint64:
			out[s.Name] = float64(s.Value.Uint64())
		}
	}
	return out
}

func verifyH6RuntimeDelta(before, after map[string]float64) map[string]any {
	return map[string]any{
		"gc_cpu_s":        after["/cpu/classes/gc/total:cpu-seconds"] - before["/cpu/classes/gc/total:cpu-seconds"],
		"total_cpu_s":     after["/cpu/classes/total:cpu-seconds"] - before["/cpu/classes/total:cpu-seconds"],
		"gc_cycles":       after["/gc/cycles/total:gc-cycles"] - before["/gc/cycles/total:gc-cycles"],
		"alloc_mib":       (after["/gc/heap/allocs:bytes"] - before["/gc/heap/allocs:bytes"]) / (1 << 20),
		"live_before_mib": before["/gc/heap/live:bytes"] / (1 << 20),
		"live_after_mib":  after["/gc/heap/live:bytes"] / (1 << 20),
	}
}

// verifyH6Frames parses one goroutine block of runtime.Stack output into
// (function, file:line) pairs, innermost first; "created by" lines are skipped.
func verifyH6Frames(block string) (status string, funcs, places []string) {
	lines := strings.Split(block, "\n")
	if len(lines) == 0 {
		return "", nil, nil
	}
	if open, end := strings.Index(lines[0], "["), strings.Index(lines[0], "]"); open >= 0 && end > open {
		status = lines[0][open+1 : end]
		if comma := strings.Index(status, ","); comma >= 0 {
			status = status[:comma]
		}
	}
	for i := 1; i+1 < len(lines); i++ {
		line := lines[i]
		if line == "" || strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "created by ") {
			continue
		}
		fn := line
		if open := strings.LastIndex(line, "("); open > 0 && strings.HasSuffix(line, ")") {
			fn = line[:open]
		}
		place := strings.TrimPrefix(lines[i+1], "\t")
		if sp := strings.Index(place, " "); sp >= 0 {
			place = place[:sp]
		}
		funcs = append(funcs, strings.TrimPrefix(fn, "github.com/orneryd/nornicdb/"))
		places = append(places, filepath.Base(place))
	}
	return status, funcs, places
}

type verifyH6Count struct {
	Frame string `json:"frame"`
	N     int    `json:"n"`
}

func verifyH6Top(m map[string]int, k int) []verifyH6Count {
	out := make([]verifyH6Count, 0, len(m))
	for f, n := range m {
		out = append(out, verifyH6Count{f, n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].N > out[j].N || out[i].N == out[j].N && out[i].Frame < out[j].Frame })
	if len(out) > k {
		out = out[:k]
	}
	return out
}

const verifyH6Marker = "pkg/cypher.(*StorageExecutor)"

// verifyH6Sampled runs call while sampling every goroutine's stack each 50 ms.
// Goroutines inside the Cypher executor are counted by innermost frame (self)
// and by every nornicdb or badger function on the stack (inclusive); other
// running or runnable goroutines are counted by their innermost non-runtime
// frame (background).
func verifyH6Sampled(t *testing.T, record map[string]any, call func() error) {
	t.Helper()
	done := make(chan error, 1)
	before := verifyH6Metrics()
	start := time.Now()
	go func() { done <- call() }()
	self, incl, background, states := map[string]int{}, map[string]int{}, map[string]int{}, map[string]int{}
	samples := 0
	buf := make([]byte, 16<<20)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var err error
loop:
	for {
		select {
		case err = <-done:
			break loop
		case <-tick.C:
			samples++
			n := runtime.Stack(buf, true)
			for _, block := range strings.Split(string(buf[:n]), "\n\n") {
				if strings.Contains(block, "verifyH6Sampled") {
					continue
				}
				status, funcs, places := verifyH6Frames(block)
				if len(funcs) == 0 {
					continue
				}
				if strings.Contains(block, verifyH6Marker) {
					states[status]++
					self[funcs[0]+" "+places[0]]++
					seen := map[string]bool{}
					for _, fn := range funcs {
						if (strings.HasPrefix(fn, "pkg/") || strings.Contains(fn, "badger")) && !seen[fn] {
							seen[fn] = true
							incl[fn]++
						}
					}
					continue
				}
				if status != "running" && status != "runnable" && status != "syscall" {
					continue
				}
				for i, fn := range funcs {
					if !strings.HasPrefix(fn, "runtime.") || i == len(funcs)-1 {
						background[fn+" "+places[i]]++
						break
					}
				}
			}
		}
	}
	elapsed := time.Since(start)
	after := verifyH6Metrics()
	record["elapsed_ms"] = float64(elapsed.Microseconds()) / 1000
	record["runtime"] = verifyH6RuntimeDelta(before, after)
	record["samples"] = samples
	record["executor_states"] = states
	record["self_top"] = verifyH6Top(self, 12)
	record["inclusive_top"] = verifyH6Top(incl, 25)
	record["background_top"] = verifyH6Top(background, 8)
	if err != nil {
		record["error"] = err.Error()
	}
	data, _ := json.Marshal(record)
	fmt.Printf("VERIFY_H6_PROFILE %s\n", data)
}

func verifyH6Bolt(h *huntServer, query string, params map[string]any) func() error {
	return func() error {
		if outcome := h.Run(routeBoltAuto, query, params, 30*time.Minute); outcome.Failed() {
			return fmt.Errorf("%s: %s", outcome.Code, outcome.Message)
		}
		return nil
	}
}

func TestVerifyH6Steps(t *testing.T) {
	if os.Getenv("VERIFY_H6") == "" {
		t.Skip("set VERIFY_H6=1")
	}
	scales := []int{1, 16, 64}
	if v := os.Getenv("VERIFY_H6_SCALES"); v != "" {
		scales = nil
		for _, s := range strings.Split(v, ",") {
			n, _ := strconv.Atoi(s)
			scales = append(scales, n)
		}
	}
	worker, _ := os.Hostname()
	fmt.Printf("VERIFY_H6_ENV worker=%s gomaxprocs=%d numcpu=%d go=%s\n", worker, runtime.GOMAXPROCS(0), runtime.NumCPU(), runtime.Version())
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "verify-h6-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	ctx := context.Background()
	var unique int64
	for _, f := range scales {
		dir := filepath.Join(base, fmt.Sprintf("x%d", f))
		h := huntOpen(t, dir, huntOptions{QueryCache: true})
		buildStart := time.Now()
		huntH6Build(t, h, f)
		fmt.Printf("VERIFY_H6_BUILD scale=%d nodes=%d seconds=%.1f\n", f, huntH6BaseNodes*f, time.Since(buildStart).Seconds())
		engine := h.db.GetStorage()
		schema := engine.GetSchema()

		// T06 by direct steps (the DDL owner's own sequence).
		for rep := 0; rep < 3; rep++ {
			before := verifyH6Metrics()
			t0 := time.Now()
			nodes, err := storage.GetStoredNodesByLabel(ctx, engine, "G")
			t1 := time.Now()
			if err != nil {
				t.Fatalf("read nodes: %v", err)
			}
			entries := make(map[storage.NodeID]map[string]interface{}, len(nodes))
			for _, node := range nodes {
				if node == nil || node.Properties == nil {
					continue
				}
				entries[storage.EnsureNodeIDDatabasePrefixForEngine(engine, node.ID)] = node.Properties
			}
			t2 := time.Now()
			name := fmt.Sprintf("verify_h6_direct_%d_%d", f, rep)
			if err := schema.AddCompositeIndex(name, "G", []string{"s"}); err != nil {
				t.Fatalf("declare: %v", err)
			}
			t3 := time.Now()
			if err := schema.BackfillCompositeIndex(ctx, name, entries); err != nil {
				t.Fatalf("fill: %v", err)
			}
			t4 := time.Now()
			if err := schema.DropIndex(name); err != nil {
				t.Fatalf("drop: %v", err)
			}
			t5 := time.Now()
			after := verifyH6Metrics()
			ms := func(a, b time.Time) float64 { return float64(b.Sub(a).Microseconds()) / 1000 }
			data, _ := json.Marshal(map[string]any{"template": "T06", "scale": f, "rep": rep, "nodes": len(nodes), "entries": len(entries),
				"read_ms": ms(t0, t1), "map_ms": ms(t1, t2), "declare_ms": ms(t2, t3), "fill_ms": ms(t3, t4), "drop_ms": ms(t4, t5),
				"total_ms": ms(t0, t5), "runtime": verifyH6RuntimeDelta(before, after), "worker": worker})
			fmt.Printf("VERIFY_H6_STEP %s\n", data)
			nodes, entries = nil, nil
		}

		// T06 through Bolt, sampled; then the reference linear read.
		for rep := 0; rep < 4; rep++ {
			unique++
			name := fmt.Sprintf("hunt_h6_v%d", unique)
			q := fmt.Sprintf("CREATE INDEX %s FOR (n:G) ON (n.s)", name)
			verifyH6Sampled(t, map[string]any{"template": "T06", "scale": f, "rep": rep, "query": q, "worker": worker}, verifyH6Bolt(h, q, nil))
			_ = h.Run(routeBoltAuto, "DROP INDEX "+name+" IF EXISTS", nil, 10*time.Minute)
		}
		for rep := 0; rep < 3; rep++ {
			q := "MATCH (n:G) RETURN count(n.s) AS c"
			verifyH6Sampled(t, map[string]any{"template": "SCAN", "scale": f, "rep": rep, "query": q, "worker": worker}, verifyH6Bolt(h, q, nil))
		}

		// T26 through Bolt: list size crossed with store size.
		created := 0
		for _, m := range []int{1000, 16000, 64000} {
			for rep := 0; rep < 4; rep++ {
				unique++
				verifyH6Sampled(t, map[string]any{"template": "T26", "scale": f, "m": m, "rep": rep, "w2_before": created, "worker": worker},
					verifyH6Bolt(h, "UNWIND range(1, $m) AS i CREATE (:W2 {id: i, run: $r})", map[string]any{"m": int64(m), "r": unique}))
				created += m
			}
		}
		h.Close(t)
		_ = os.RemoveAll(dir)
	}
}

// TestVerifyH6CacheProof (L-H6-001 root cause by run): the same graph at each
// scale, opened once with the default Badger node cache cap (10,000 entries,
// pkg/storage/badger.go:135) and once with VERIFY_H6_CACHE_BIG entries. Times
// T06 as the harness does (one warm-up, three timed, drop after each), the
// label read alone, and three first-run full reads with distinct statement
// texts (no result-cache hit). VERIFY_H6_CACHE lines.
//
// Run: VERIFY_H6=1 [VERIFY_H6_SCALES=1,16,64] [VERIFY_H6_CACHE_BIG=1000000]
//      go test ./pkg/server -run '^TestVerifyH6CacheProof$' -count=1 -timeout=90m -v
func TestVerifyH6CacheProof(t *testing.T) {
	if os.Getenv("VERIFY_H6") == "" {
		t.Skip("set VERIFY_H6=1")
	}
	scales := []int{1, 16, 64}
	if v := os.Getenv("VERIFY_H6_SCALES"); v != "" {
		scales = nil
		for _, s := range strings.Split(v, ",") {
			n, _ := strconv.Atoi(s)
			scales = append(scales, n)
		}
	}
	big := huntEnvInt("VERIFY_H6_CACHE_BIG", 1000000)
	worker, _ := os.Hostname()
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "verify-h6-cache-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	ctx := context.Background()
	var unique int64
	for _, cache := range []int{0, big} {
		for _, f := range scales {
			dir := filepath.Join(base, fmt.Sprintf("c%d-x%d", cache, f))
			h := huntOpen(t, dir, huntOptions{QueryCache: true, NodeCacheMaxEntries: cache})
			buildStart := time.Now()
			huntH6Build(t, h, f)
			build := time.Since(buildStart).Seconds()
			emit := func(step string, rep int, elapsed time.Duration, extra map[string]any) {
				rec := map[string]any{"cache": cache, "scale": f, "step": step, "rep": rep, "ms": float64(elapsed.Microseconds()) / 1000,
					"us_per_node": float64(elapsed.Nanoseconds()) / 1000 / float64(huntH6BaseNodes*f), "build_s": build, "worker": worker}
				for k, v := range extra {
					rec[k] = v
				}
				data, _ := json.Marshal(rec)
				fmt.Printf("VERIFY_H6_CACHE %s\n", data)
			}
			for rep := 0; rep < 4; rep++ {
				unique++
				name := fmt.Sprintf("hunt_h6_c%d", unique)
				start := time.Now()
				outcome := h.Run(routeBoltAuto, fmt.Sprintf("CREATE INDEX %s FOR (n:G) ON (n.s)", name), nil, 30*time.Minute)
				elapsed := time.Since(start)
				_ = h.Run(routeBoltAuto, "DROP INDEX "+name+" IF EXISTS", nil, 10*time.Minute)
				emit("T06", rep, elapsed, map[string]any{"code": outcome.Code})
			}
			for rep := 0; rep < 3; rep++ {
				start := time.Now()
				nodes, err := storage.GetStoredNodesByLabel(ctx, h.db.GetStorage(), "G")
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				emit("label_read", rep, time.Since(start), map[string]any{"nodes": len(nodes)})
			}
			for rep := 0; rep < 3; rep++ {
				start := time.Now()
				outcome := h.Run(routeBoltAuto, fmt.Sprintf("MATCH (n:G) RETURN count(n.s) AS c%d", rep), nil, 30*time.Minute)
				emit("SCAN", rep, time.Since(start), map[string]any{"code": outcome.Code})
			}
			h.Close(t)
			_ = os.RemoveAll(dir)
		}
	}
}
