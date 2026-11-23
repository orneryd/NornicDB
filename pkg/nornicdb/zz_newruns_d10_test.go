//go:build report_gaps && report_newruns

package nornicdb_test

// Report new run for D10 (v1.5.2 verification campaign, test-side only).
// TestGapD10SearchesDuringBuild found that every Cypher answer during the
// build differed from the after-build answer (as few as 286 of 300 rows
// shared), while TestGapD10RowsDuringBuild (a new $nonce per call, so no
// result cache) found 300/300 against the exact top-300. This test repeats
// the first test's Cypher route (same store, same fixed query vector, same
// statement text) with the query-result cache on and off, and compares each
// answer three ways: whole answer (id@score, as the first test did), ids
// only, and against the exact top-300 computed from the seeded vectors.
// It also records how many distinct answers were seen during the build and
// how far each returned score is from the exact cosine.
// Diagnostic: fails only on harness errors.

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

type nrD10Row struct {
	id    string
	score float64
}

func nrD10Ask(db *nornicdb.DB, v []float64) ([]nrD10Row, error, float64) {
	start := time.Now()
	res, err := db.ExecuteCypher(context.Background(), "CALL db.index.vector.queryNodes('doc_vec', 300, $v) YIELD node, score RETURN node.id AS id, score", map[string]any{"v": v})
	ms := float64(time.Since(start).Microseconds()) / 1000
	if err != nil {
		return nil, err, ms
	}
	rows := make([]nrD10Row, 0, len(res.Rows))
	for _, r := range res.Rows {
		s, _ := r[1].(float64)
		if f, ok := r[1].(float32); ok {
			s = float64(f)
		}
		rows = append(rows, nrD10Row{fmt.Sprint(r[0]), s})
	}
	return rows, nil, ms
}

func nrD10Key(rows []nrD10Row) string {
	p := make([]string, len(rows))
	for i, r := range rows {
		p[i] = fmt.Sprintf("%s@%.6f", r.id, r.score)
	}
	return strings.Join(p, ",")
}

func nrD10Run(t *testing.T, cache bool) {
	gapQ07Embeddings = false
	gapQ07Env(t)
	if !cache {
		t.Setenv("NORNICDB_QUERY_CACHE_ENABLED", "false")
	}
	dir := filepath.Join(t.TempDir(), "data")
	gapQ07Seed(t, dir)

	// Exact ground truth from the seed's generator (as TestGapD10RowsDuringBuild).
	gen := rand.New(rand.NewSource(11))
	stored := map[string][]float64{}
	for i := 0; i < gapQ07Nodes; i++ {
		_ = gapQ07Text(gen, gapQ07TextWords)
		stored[fmt.Sprintf("s%d", i)] = gapQ07Vector(gen)
	}
	cos := func(a, b []float64) float64 {
		var d, na, nb float64
		for i := range a {
			d += a[i] * b[i]
			na += a[i] * a[i]
			nb += b[i] * b[i]
		}
		if na == 0 || nb == 0 {
			return 0
		}
		return d / (math.Sqrt(na) * math.Sqrt(nb))
	}
	// The same fixed query vector as TestGapD10SearchesDuringBuild.
	v := gapQ07Vector(rand.New(rand.NewSource(7)))
	type sc struct {
		id string
		s  float64
	}
	all := make([]sc, 0, len(stored))
	for id, sv := range stored {
		all = append(all, sc{id, cos(v, sv)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].s > all[j].s })
	exact := map[string]bool{}
	for _, x := range all[:300] {
		exact[x.id] = true
	}
	t.Logf("NRD10 cache=%t exact: score#1=%.6f score#300=%.6f score#301=%.6f", cache, all[0].s, all[299].s, all[300].s)

	recall := func(rows []nrD10Row) int {
		n := 0
		for _, r := range rows {
			if exact[r.id] {
				n++
			}
		}
		return n
	}
	maxScoreDev := func(rows []nrD10Row) float64 {
		m := 0.0
		for _, r := range rows {
			if sv, ok := stored[r.id]; ok {
				if d := math.Abs(r.score - cos(v, sv)); d > m {
					m = d
				}
			}
		}
		return m
	}
	idOverlap := func(a, b []nrD10Row) int {
		in := map[string]bool{}
		for _, r := range b {
			in[r.id] = true
		}
		n := 0
		for _, r := range a {
			if in[r.id] {
				n++
			}
		}
		return n
	}
	pairOverlap := func(a, b []nrD10Row) int {
		in := map[string]bool{}
		for _, r := range b {
			in[fmt.Sprintf("%s@%.6f", r.id, r.score)] = true
		}
		n := 0
		for _, r := range a {
			if in[fmt.Sprintf("%s@%.6f", r.id, r.score)] {
				n++
			}
		}
		return n
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
	start := time.Now()
	answered := 0
	var ms []float64
	rowsOf := map[string][]nrD10Row{}
	errs := map[string]int{}
	distinct := map[string]int{}
	firstSeen := map[string]time.Duration{}
	for svc.BuildInProgress() || !svc.IsReady() {
		if time.Since(start) > 6*time.Minute {
			break
		}
		rows, err, callMs := nrD10Ask(db, v)
		if err != nil {
			e := err.Error()
			if len(e) > 160 {
				e = e[:160]
			}
			errs[e]++
			continue
		}
		k := nrD10Key(rows)
		if _, ok := firstSeen[k]; !ok {
			firstSeen[k] = time.Since(start)
		}
		distinct[k]++
		if _, ok := rowsOf[k]; !ok {
			rowsOf[k] = rows
		}
		answered++
		ms = append(ms, callMs)
	}
	buildFor := time.Since(start)
	after1, err1, ms1 := nrD10Ask(db, v)
	after2, err2, ms2 := nrD10Ask(db, v)
	if err1 != nil || err2 != nil {
		t.Fatalf("after build: %v / %v", err1, err2)
	}
	sort.Float64s(ms)
	med := 0.0
	if len(ms) > 0 {
		med = ms[len(ms)/2]
	}
	t.Logf("NRD10 cache=%t saw_building=%t build_observed_for=%v answered_during=%d refused_during=%d refusals=%v median_ms_during=%.2f distinct_answers_during=%d after1_ms=%.1f after2_ms=%.1f",
		cache, saw, buildFor.Round(time.Millisecond), answered, func() int {
			n := 0
			for _, c := range errs {
				n += c
			}
			return n
		}(), errs, med, len(distinct), ms1, ms2)
	t.Logf("NRD10 cache=%t after1: rows=%d exact_found=%d/300 max_score_dev=%.2e | after2 identical_to_after1=%t", cache, len(after1), recall(after1), maxScoreDev(after1), nrD10Key(after1) == nrD10Key(after2))

	identical, minPair, minID, minRecall, maxDev := 0, -1, -1, -1, 0.0
	afterKey := nrD10Key(after1)
	for k, rows := range rowsOf {
		if k == afterKey {
			identical += distinct[k]
		}
		if p := pairOverlap(rows, after1); minPair < 0 || p < minPair {
			minPair = p
		}
		if o := idOverlap(rows, after1); minID < 0 || o < minID {
			minID = o
		}
		if r := recall(rows); minRecall < 0 || r < minRecall {
			minRecall = r
		}
		if d := maxScoreDev(rows); d > maxDev {
			maxDev = d
		}
	}
	t.Logf("NRD10 cache=%t during vs after1: identical=%d of %d; min_shared_id@score=%d; min_shared_ids=%d; min_exact_found=%d/300; max_score_dev_during=%.2e",
		cache, identical, answered, minPair, minID, minRecall, maxDev)
	// Each distinct during-build answer, in the order first seen.
	type dk struct {
		k  string
		at time.Duration
	}
	var ks []dk
	for k, at := range firstSeen {
		ks = append(ks, dk{k, at})
	}
	sort.Slice(ks, func(i, j int) bool { return ks[i].at < ks[j].at })
	for i, x := range ks {
		if i >= 10 {
			t.Logf("NRD10 cache=%t ... %d more distinct answers", cache, len(ks)-10)
			break
		}
		rows := rowsOf[x.k]
		t.Logf("NRD10 cache=%t distinct#%d first_at=%v count=%d rows=%d shared_id@score_with_after1=%d shared_ids_with_after1=%d exact_found=%d/300 max_score_dev=%.2e",
			cache, i+1, x.at.Round(time.Millisecond), distinct[x.k], len(rows), pairOverlap(rows, after1), idOverlap(rows, after1), recall(rows), maxScoreDev(rows))
		if i == 0 {
			n := 5
			if len(rows) < n {
				n = len(rows)
			}
			m := 5
			if len(after1) < m {
				m = len(after1)
			}
			t.Logf("NRD10 cache=%t distinct#1 first5=%v ; after1 first5=%v", cache, rows[:n], after1[:m])
		}
	}
}

func TestNewRunsD10CacheOn(t *testing.T)  { nrD10Run(t, true) }
func TestNewRunsD10CacheOff(t *testing.T) { nrD10Run(t, false) }
