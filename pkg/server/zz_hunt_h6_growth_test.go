package server

// Hunt ground H6: growth. The 30 most used operation shapes of the corpus
// (sig/handoff/v152-verification/hunt/H6-shapes-ranked.tsv, ranks 1-30), each
// as one template, at 4 sizes (1, 4, 16 and 64 times a base) along the axis
// that grows for that shape: store size, degree, parameter (list) size or
// statement length. Plus a deadline check and a transaction memory limit check
// at the same 4 sizes.
//
// Oracle:
//   - "constant" templates (the result does not grow with the axis: indexed
//     lookups, counts, LIMITed reads, single writes, catalog procedures): the
//     median time at 64x must be at most 4 times the median at 1x;
//   - "linear" templates (the work grows with the axis): at most 2 times
//     proportional, i.e. the median at 64x at most 128 times the median at 1x;
//   - deadline: a statement in a transaction with a 2 s timeout ends (with an
//     error) within 4 s;
//   - memory limit: with db.memory.transaction.max = 64 MiB a statement that
//     builds a large list either fails with a memory error, or succeeds while
//     the process heap grows by less than 4 times the limit.
//
// Measurements: 2 warm-up runs, then 7 timed runs; the median is reported
// (HUNT_H6_MEASURE lines). Timings from a shared host: H6 runs in the quiet
// window and a lead names the worker; a lead is confirmed only by a paired
// rerun.
//
// Run: [HUNT_H6_SCALES=1,4,16,64] [HUNT_H6_ONLY=T03,T13]
//      go test ./pkg/server -run '^TestHuntH6Growth$' -count=1 -timeout=4h -v

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

const (
	huntH6Seed      = 20261120
	huntH6BaseNodes = 5000
	huntH6BaseList  = 1000
	huntH6BaseTerms = 20
	huntH6BaseHub   = 256
)

type huntH6Template struct {
	ID, Rank, Shape, Axis, Class string
	Query                        func(f int, rnd *rand.Rand) (string, map[string]any)
}

func huntH6List(f int) []any {
	list := make([]any, huntH6BaseList*f)
	for i := range list {
		list[i] = int64(i)
	}
	return list
}

func huntH6Templates() []huntH6Template {
	id := func(f int, rnd *rand.Rand) int64 { return int64(rnd.Intn(huntH6BaseNodes * f)) }
	list := func(q string) func(int, *rand.Rand) (string, map[string]any) {
		return func(f int, _ *rand.Rand) (string, map[string]any) { return q, map[string]any{"list": huntH6List(f)} }
	}
	byID := func(q string) func(int, *rand.Rand) (string, map[string]any) {
		return func(f int, rnd *rand.Rand) (string, map[string]any) { return q, map[string]any{"id": id(f, rnd)} }
	}
	fixed := func(q string) func(int, *rand.Rand) (string, map[string]any) {
		return func(int, *rand.Rand) (string, map[string]any) { return q, nil }
	}
	var unique int64
	return []huntH6Template{
		{"T01", "1", "RETURN", "list", "linear", list("RETURN reduce(s = 0, x IN $list | s + x) AS s")},
		{"T02", "2", "RETURN WHERE", "list", "linear", list("RETURN [x IN $list WHERE x % 7 = 0 | x][0..5] AS v")},
		{"T03", "3", "MATCH RETURN [rel]", "store", "constant", byID("MATCH (a:G {id: $id})-[:GR]->(b) RETURN b.id")},
		{"T04", "4", "WITH RETURN", "list", "linear", list("WITH $list AS l RETURN size(l) AS n")},
		{"T05", "5", "MATCH WITH ORDER BY LIMIT RETURN", "store", "linear", fixed("MATCH (n:G) WITH n ORDER BY n.k DESC LIMIT 10 RETURN n.id")},
		{"T06", "6", "DDL", "store", "linear", func(f int, _ *rand.Rand) (string, map[string]any) {
			n := atomic.AddInt64(&unique, 1)
			return fmt.Sprintf("CREATE INDEX hunt_h6_%d FOR (n:G) ON (n.s)", n), nil
		}},
		{"T07", "7", "MATCH RETURN", "store", "constant", byID("MATCH (n:G {id: $id}) RETURN n")},
		{"T08", "8", "CREATE [propmap]", "store", "constant", func(int, *rand.Rand) (string, map[string]any) {
			return "CREATE (:W {id: $i, s: 'w'})", map[string]any{"i": atomic.AddInt64(&unique, 1)}
		}},
		{"T09", "9", "CALL", "store", "constant", fixed("CALL db.labels()")},
		{"T10", "10", "CALL RETURN", "store", "constant", fixed("CALL db.relationshipTypes() YIELD relationshipType RETURN count(*) AS c")},
		{"T11", "11", "MATCH WHERE RETURN [rel]", "store", "constant", byID("MATCH (a:G)-[r:GR]->(b) WHERE a.id = $id AND r.w > 0.5 RETURN b.id")},
		{"T12", "12", "UNWIND WITH RETURN WHERE [agg]", "list", "linear", list("UNWIND $list AS x WITH x WHERE x % 2 = 0 RETURN count(x) AS c")},
		{"T13", "13", "MATCH RETURN [agg]", "store", "constant", fixed("MATCH (n:G) RETURN count(n) AS c")},
		{"T14", "14", "CREATE", "store", "constant", fixed("CREATE (:W)")},
		{"T15", "15", "MATCH WHERE RETURN", "store", "constant", byID("MATCH (n:G) WHERE n.id = $id RETURN n.k")},
		{"T16", "16", "WITH UNWIND WITH WHERE ... [agg]", "list", "linear", list("WITH $list AS l UNWIND l AS x WITH x WHERE x > 0 RETURN sum(x) AS s")},
		{"T17", "17", "UNWIND WITH ORDER BY LIMIT RETURN", "list", "linear", list("UNWIND $list AS x WITH x ORDER BY x DESC LIMIT 5 RETURN x")},
		{"T18", "18", "WITH RETURN WHERE", "statement-length", "linear", func(f int, _ *rand.Rand) (string, map[string]any) {
			terms := make([]string, huntH6BaseTerms*f)
			for i := range terms {
				terms[i] = "v = " + strconv.Itoa(i)
			}
			return "WITH $v AS v RETURN (" + strings.Join(terms, " OR ") + ") AS hit", map[string]any{"v": int64(-1)}
		}},
		{"T19", "19", "MATCH WITH ORDER BY RETURN [agg]", "store", "linear", fixed("MATCH (n:G) WITH n.k % 10 AS g, count(*) AS c ORDER BY g RETURN g, c")},
		{"T20", "20", "WITH ORDER BY RETURN", "list", "linear", list("UNWIND $list AS x WITH x ORDER BY x RETURN collect(x)[0] AS first")},
		{"T21", "21", "RETURN [agg]", "list", "linear", list("UNWIND $list AS x RETURN max(x) AS m")},
		{"T22", "22", "UNWIND RETURN", "list", "linear", list("UNWIND $list AS x RETURN x")},
		{"T23", "23", "MATCH RETURN [rel,agg]", "degree", "linear", fixed("MATCH (h:Hub {id: 0})-[:HR]->() RETURN count(*) AS c")},
		{"T24", "24", "ADMIN", "store", "constant", fixed("SHOW INDEXES YIELD name RETURN count(*) AS c")},
		{"T25", "25", "MATCH RETURN ORDER BY", "store", "constant", fixed("MATCH (n:G) WHERE n.id < 100 RETURN n.id ORDER BY n.id")},
		{"T26", "26", "UNWIND CREATE [propmap]", "list", "linear", func(f int, _ *rand.Rand) (string, map[string]any) {
			return "UNWIND range(1, $m) AS i CREATE (:W2 {id: i, run: $r})", map[string]any{"m": int64(huntH6BaseList * f), "r": atomic.AddInt64(&unique, 1)}
		}},
		{"T27", "27", "UNWIND WITH UNWIND WITH WHERE ... [agg]", "list", "linear", list("UNWIND $list AS x UNWIND [x, x + 1] AS y WITH y WHERE y % 3 = 0 RETURN count(*) AS c")},
		{"T28", "28", "CREATE [rel]", "store", "constant", func(f int, rnd *rand.Rand) (string, map[string]any) {
			return "MATCH (a:G {id: $a}), (b:G {id: $b}) CREATE (a)-[:GN]->(b)", map[string]any{"a": id(f, rnd), "b": id(f, rnd)}
		}},
		{"T29", "29", "CREATE [rel,propmap]", "store", "constant", func(int, *rand.Rand) (string, map[string]any) {
			n := atomic.AddInt64(&unique, 1)
			return "CREATE (:X {id: $i})-[:XR {w: 1}]->(:X {id: $j})", map[string]any{"i": n * 2, "j": n*2 + 1}
		}},
		{"T30", "30", "WITH MATCH RETURN", "store", "constant", byID("WITH $id AS id MATCH (n:G {id: id}) RETURN n.k")},
	}
}

// huntH6Build creates the graph for scale f: G nodes with a unique id, a
// random k and a string, three GR relationships each, and one Hub with
// huntH6BaseHub*f relationships.
func huntH6Build(t *testing.T, h *huntServer, f int) {
	n := huntH6BaseNodes * f
	must := func(q string, p map[string]any) {
		if outcome := h.Run(routeBoltAuto, q, p, 30*time.Minute); outcome.Failed() {
			t.Fatalf("build %s: %s %s", q, outcome.Code, outcome.Message)
		}
	}
	must("CREATE CONSTRAINT hunt_g_id IF NOT EXISTS FOR (n:G) REQUIRE n.id IS UNIQUE", nil)
	for from := 0; from < n; from += 5000 {
		must("UNWIND range($from, $to) AS i CREATE (:G {id: i, k: (i * 7919) % 100003, s: 's' + toString(i)})", map[string]any{"from": int64(from), "to": int64(min(from+4999, n-1))})
	}
	for from := 0; from < n; from += 2000 {
		must("UNWIND range($from, $to) AS i MATCH (a:G {id: i}) UNWIND [1, 2, 3] AS j MATCH (b:G {id: (i * 31 + j * 7907) % $n}) CREATE (a)-[:GR {w: toFloat((i + j) % 100) / 100.0}]->(b)",
			map[string]any{"from": int64(from), "to": int64(min(from+1999, n-1)), "n": int64(n)})
	}
	must("CREATE (:Hub {id: 0})", nil)
	hub := huntH6BaseHub * f
	for from := 0; from < hub; from += 5000 {
		must("MATCH (h:Hub {id: 0}) UNWIND range($from, $to) AS i CREATE (h)-[:HR]->(:Leaf {i: i})", map[string]any{"from": int64(from), "to": int64(min(from+4999, hub-1))})
	}
}

type huntH6Measure struct {
	MedianMS, P90MS float64
	Rows            int
	Code            string
}

func huntH6Time(h *huntServer, tpl huntH6Template, f int, rnd *rand.Rand) huntH6Measure {
	var samples []float64
	var last differential.Outcome
	runs := 9
	if tpl.ID == "T06" {
		runs = 4
	}
	for i := 0; i < runs; i++ {
		q, p := tpl.Query(f, rnd)
		start := time.Now()
		last = h.Run(routeBoltAuto, q, p, 10*time.Minute)
		elapsed := time.Since(start)
		if tpl.ID == "T06" && !last.Failed() {
			name := strings.Fields(q)[2]
			_ = h.Run(routeBoltAuto, "DROP INDEX "+name+" IF EXISTS", nil, 10*time.Minute)
		}
		if i >= 2 || (tpl.ID == "T06" && i >= 1) {
			samples = append(samples, float64(elapsed.Microseconds())/1000)
		}
		if last.Failed() {
			break
		}
	}
	sort.Float64s(samples)
	m := huntH6Measure{Rows: len(last.Rows), Code: last.Code}
	if len(samples) > 0 {
		m.MedianMS = samples[len(samples)/2]
		m.P90MS = samples[(len(samples)*9)/10]
	}
	return m
}

func TestHuntH6Growth(t *testing.T) {
	if os.Getenv("HUNT_H6") == "" {
		t.Skip("set HUNT_H6=1 to run the growth ground")
	}
	scales := []int{1, 4, 16, 64}
	if v := os.Getenv("HUNT_H6_SCALES"); v != "" {
		scales = nil
		for _, s := range strings.Split(v, ",") {
			n, _ := strconv.Atoi(s)
			scales = append(scales, n)
		}
	}
	only := map[string]bool{}
	for _, id := range strings.Split(os.Getenv("HUNT_H6_ONLY"), ",") {
		if id != "" {
			only[id] = true
		}
	}
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h6-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)
	worker, _ := os.Hostname()
	results := map[string]map[int]huntH6Measure{}
	templates := huntH6Templates()
	leads := 0
	lead := func(verdict string, detail map[string]any) {
		leads++
		entry := map[string]any{"ground": "H6", "worker": worker, "seed": huntH6Seed, "verdict": verdict}
		for k, v := range detail {
			entry[k] = v
		}
		huntLead(t, entry)
	}
	for _, f := range scales {
		h := huntOpen(t, filepath.Join(base, fmt.Sprintf("x%d", f)), huntOptions{QueryCache: true})
		buildStart := time.Now()
		huntH6Build(t, h, f)
		fmt.Printf("HUNT_H6_BUILD scale=%d nodes=%d seconds=%.1f\n", f, huntH6BaseNodes*f, time.Since(buildStart).Seconds())
		rnd := rand.New(rand.NewSource(huntH6Seed + int64(f)))
		for _, tpl := range templates {
			if len(only) > 0 && !only[tpl.ID] {
				continue
			}
			m := huntH6Time(h, tpl, f, rnd)
			if results[tpl.ID] == nil {
				results[tpl.ID] = map[int]huntH6Measure{}
			}
			results[tpl.ID][f] = m
			fmt.Printf("HUNT_H6_MEASURE %s scale=%d axis=%s class=%s median_ms=%.3f p90_ms=%.3f rows=%d code=%s worker=%s\n", tpl.ID, f, tpl.Axis, tpl.Class, m.MedianMS, m.P90MS, m.Rows, m.Code, worker)
			if m.Code != "" {
				lead("valid statement failed at this size", map[string]any{"template": tpl.ID, "scale": f, "code": m.Code})
			}
		}
		// deadline: a quadratic statement in a transaction with a 2 s timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		start := time.Now()
		var derr error
		if tx, err := session.BeginTransaction(ctx, neo4jdriver.WithTxTimeout(2*time.Second)); err != nil {
			derr = err
		} else {
			var result neo4jdriver.ResultWithContext
			if result, derr = tx.Run(ctx, "MATCH (a:G), (b:G) WHERE a.k + b.k = -1 RETURN count(*) AS c", nil); derr == nil {
				_, derr = result.Consume(ctx)
			}
			_ = tx.Rollback(context.Background())
		}
		elapsed := time.Since(start)
		_ = session.Close(context.Background())
		cancel()
		fmt.Printf("HUNT_H6_DEADLINE scale=%d seconds=%.2f err=%v\n", f, elapsed.Seconds(), derr)
		if elapsed > 4*time.Second {
			lead("transaction timeout of 2 s not honoured", map[string]any{"scale": f, "seconds": elapsed.Seconds(), "error": fmt.Sprint(derr)})
		}
		h.Close(t)
		_ = os.RemoveAll(filepath.Join(base, fmt.Sprintf("x%d", f)))

		// memory limit: 64 MiB per transaction
		const limit = 64 << 20
		mh := huntOpen(t, filepath.Join(base, fmt.Sprintf("mem%d", f)), huntOptions{QueryCache: true, TxMemoryMax: limit})
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		var peak uint64
		var wg sync.WaitGroup
		done := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case <-time.After(50 * time.Millisecond):
					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					if ms.HeapAlloc > atomic.LoadUint64(&peak) {
						atomic.StoreUint64(&peak, ms.HeapAlloc)
					}
				}
			}
		}()
		outcome := mh.Run(routeBoltAuto, "UNWIND range(1, $n) AS i WITH collect('item-' + toString(i) + '-padding-padding-padding') AS l RETURN size(l) AS c", map[string]any{"n": int64(250000 * f)}, 10*time.Minute)
		close(done)
		wg.Wait()
		growth := int64(atomic.LoadUint64(&peak)) - int64(before.HeapAlloc)
		fmt.Printf("HUNT_H6_MEMLIMIT scale=%d code=%s heap_growth_mib=%.1f\n", f, outcome.Code, float64(growth)/(1<<20))
		switch {
		case outcome.Failed() && !strings.Contains(outcome.Code, "OutOfMemory") && !strings.Contains(outcome.Code, "Memory"):
			lead("memory-limited statement failed with another error", map[string]any{"scale": f, "code": outcome.Code, "message": outcome.Message})
		case !outcome.Failed() && growth > 4*limit:
			lead("transaction memory limit not respected", map[string]any{"scale": f, "heapGrowthMiB": growth >> 20, "limitMiB": limit >> 20})
		}
		mh.Close(t)
		_ = os.RemoveAll(filepath.Join(base, fmt.Sprintf("mem%d", f)))
	}
	if len(scales) >= 2 {
		lo, hi := scales[0], scales[len(scales)-1]
		factor := float64(hi) / float64(lo)
		for _, tpl := range templates {
			r := results[tpl.ID]
			if r == nil || r[lo].MedianMS <= 0 || r[hi].MedianMS <= 0 {
				continue
			}
			ratio := r[hi].MedianMS / r[lo].MedianMS
			allowed := 4.0
			if tpl.Class == "linear" {
				allowed = 2 * factor
			}
			if ratio > allowed {
				lead("cost grows faster than the template's class", map[string]any{"template": tpl.ID, "shape": tpl.Shape, "axis": tpl.Axis, "class": tpl.Class,
					"medians_ms": fmt.Sprint(r), "ratio": ratio, "allowed": allowed, "from": lo, "to": hi})
			}
		}
	}
	huntCoverage(t, map[string]any{"ground": "H6", "scales": scales, "templates": len(results), "leads": leads, "worker": worker})
}
