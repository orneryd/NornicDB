//go:build report_gaps

package nornicdb_test

// Report gap D10 (v1.5.2 verification campaign, test-side only): during the
// start-up search build of a reopened 60,000-node store (the Q07 / E35-R1
// store, zz_gaps_q07_test.go helpers), the same query vector is sent, again
// and again until the build ends, through three routes:
//   cypher: CALL db.index.vector.queryNodes('doc_vec', 300, $v) (scalar read);
//   full:   search.Service.Search (the full search entry, search.go:5129);
//   qdrant: search.Service.VectorQueryNodesWithOptions (the optioned read the
//           Qdrant API uses).
// For each route it logs how many calls answered and how many refused (with
// the error), and compares every answered top-300 during the build with the
// answer of the same route after the build (same store, nothing written).
// Embeddings off, plain process. Diagnostic: fails only on harness errors.

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
)

type gapD10Answer struct {
	ids []string
	err error
	ms  float64
}

func gapD10Cypher(db *nornicdb.DB, v []float64) gapD10Answer {
	start := time.Now()
	res, err := db.ExecuteCypher(context.Background(), "CALL db.index.vector.queryNodes('doc_vec', 300, $v) YIELD node, score RETURN node.id AS id, score", map[string]any{"v": v})
	a := gapD10Answer{err: err, ms: float64(time.Since(start).Microseconds()) / 1000}
	if err == nil {
		for _, r := range res.Rows {
			a.ids = append(a.ids, fmt.Sprintf("%v@%.6f", r[0], r[1]))
		}
	}
	return a
}

func gapD10Full(svc *search.Service, v32 []float32) gapD10Answer {
	start := time.Now()
	opts := search.DefaultSearchOptions()
	opts.Limit = 300
	resp, err := svc.Search(context.Background(), "invoice payment contract", v32, opts)
	a := gapD10Answer{err: err, ms: float64(time.Since(start).Microseconds()) / 1000}
	if err == nil && resp != nil {
		for _, r := range resp.Results {
			a.ids = append(a.ids, r.ID)
		}
	}
	return a
}

func gapD10Qdrant(svc *search.Service, v32 []float32) gapD10Answer {
	start := time.Now()
	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), v32, search.VectorQuerySpec{
		IndexName: "doc_vec", Label: "Searchable", Property: "embedding", Similarity: "cosine", Limit: 300, ScoreScale: search.VectorScoreSimilarity,
	}, &search.SearchOptions{})
	a := gapD10Answer{err: err, ms: float64(time.Since(start).Microseconds()) / 1000}
	for _, h := range hits {
		a.ids = append(a.ids, fmt.Sprintf("%s@%.6f", h.ID, h.Score))
	}
	return a
}

type gapD10Route struct {
	name                    string
	answered, refused       int
	errs                    map[string]int
	during                  []gapD10Answer
	after                   gapD10Answer
	identical, differing    int
	minOverlap, sizesDiffer int
}

func (r *gapD10Route) add(a gapD10Answer) {
	if a.err != nil {
		r.refused++
		e := a.err.Error()
		if len(e) > 160 {
			e = e[:160]
		}
		r.errs[e]++
		return
	}
	r.answered++
	r.during = append(r.during, a)
}

func gapD10Compare(r *gapD10Route) {
	if r.after.err != nil {
		return
	}
	after := map[string]bool{}
	for _, id := range r.after.ids {
		after[id] = true
	}
	r.minOverlap = -1
	for _, a := range r.during {
		if strings.Join(a.ids, ",") == strings.Join(r.after.ids, ",") {
			r.identical++
			continue
		}
		r.differing++
		if len(a.ids) != len(r.after.ids) {
			r.sizesDiffer++
		}
		ov := 0
		for _, id := range a.ids {
			if after[id] {
				ov++
			}
		}
		if r.minOverlap < 0 || ov < r.minOverlap {
			r.minOverlap = ov
		}
	}
}

func gapD10Median(as []gapD10Answer) float64 {
	if len(as) == 0 {
		return 0
	}
	ms := make([]float64, len(as))
	for i, a := range as {
		ms[i] = a.ms
	}
	sort.Float64s(ms)
	return ms[len(ms)/2]
}

func TestGapD10SearchesDuringBuild(t *testing.T) {
	gapQ07Embeddings = false
	gapQ07Env(t)
	dir := filepath.Join(t.TempDir(), "data")
	gapQ07Seed(t, dir)
	r := rand.New(rand.NewSource(7))
	v := gapQ07Vector(r)
	v32 := make([]float32, len(v))
	for i := range v {
		v32[i] = float32(v[i])
	}
	db, err := nornicdb.Open(dir, gapQ07Config())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecuteCypher(context.Background(), "RETURN 1", nil); err != nil {
		t.Fatal(err)
	}
	svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(60 * time.Second)
	saw := false
	for time.Now().Before(deadline) {
		if svc.BuildInProgress() {
			saw = true
			break
		}
		if svc.IsReady() {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	routes := []*gapD10Route{{name: "cypher", errs: map[string]int{}}, {name: "full", errs: map[string]int{}}, {name: "qdrant", errs: map[string]int{}}}
	start := time.Now()
	rounds := 0
	for svc.BuildInProgress() || !svc.IsReady() {
		if time.Since(start) > 6*time.Minute {
			break
		}
		rounds++
		routes[1].add(gapD10Full(svc, v32))
		routes[2].add(gapD10Qdrant(svc, v32))
		routes[0].add(gapD10Cypher(db, v))
	}
	buildFor := time.Since(start)
	routes[0].after = gapD10Cypher(db, v)
	routes[1].after = gapD10Full(svc, v32)
	routes[2].after = gapD10Qdrant(svc, v32)
	t.Logf("GAPD10 saw_building=%t build_observed_for=%v rounds=%d ready_after=%t", saw, buildFor.Round(time.Millisecond), rounds, svc.IsReady())
	for _, rt := range routes {
		gapD10Compare(rt)
		t.Logf("GAPD10 route=%s during_build: answered=%d refused=%d refusals=%v answered_median_ms=%.1f | after_build: err=%v rows=%d ms=%.1f | answered_during vs after: identical=%d differing=%d (size_differs=%d, min_overlap_of_rows=%d)",
			rt.name, rt.answered, rt.refused, rt.errs, gapD10Median(rt.during), rt.after.err, len(rt.after.ids), rt.after.ms, rt.identical, rt.differing, rt.sizesDiffer, rt.minOverlap)
		if rt.differing > 0 {
			for _, a := range rt.during {
				if strings.Join(a.ids, ",") != strings.Join(r0(rt.after.ids), ",") {
					head := a.ids
					if len(head) > 5 {
						head = head[:5]
					}
					ah := rt.after.ids
					if len(ah) > 5 {
						ah = ah[:5]
					}
					t.Logf("GAPD10 route=%s first differing answer: rows=%d first5=%v ; after first5=%v", rt.name, len(a.ids), head, ah)
					break
				}
			}
		}
	}
}

func r0(s []string) []string { return s }
