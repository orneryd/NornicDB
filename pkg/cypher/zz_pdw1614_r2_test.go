package cypher

// Scratch investigation (pd-w161-4), round 2, not for merge.
//  - Observation 2: the links evaluation in an explicit transaction after K
//    commits landed between its BEGIN and the statement, with no write running
//    while the statement runs; the same statement auto-commit for comparison.
//  - The count-store node-label read: its cost after many commits, and after
//    reopening the engine.
// Logged with the prefix PDW1614R2.

import (
	"context"
	"fmt"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// writeOne runs one import-shaped writer transaction (the same statements
// startWriters runs) on the calling goroutine.
func (f *pdw1614Fixture) writeOne(t *testing.T, label string, seq int, shared bool) {
	t.Helper()
	s := f.sizes
	id := fmt.Sprintf("w-%s-%05d", label, seq)
	version := id + ":v"
	source := pdw1614Source(id+":s", id, version, s.sourceChars, s.sections, s.views, s.pages, s.emailTexts, "et-"+id, fmt.Sprintf("et-%04d", seq%s.docs))
	entities, links := []any{}, []any{}
	for k := 0; k < 10; k++ {
		entities = append(entities, map[string]any{"id": fmt.Sprintf("%s:e%02d", id, k), "document_id": id, "current": true})
	}
	for m := 0; m < 40; m++ {
		to := fmt.Sprintf("%s:e%02d", id, (m+1)%10)
		if shared && m%2 == 0 {
			to = fmt.Sprintf("shared-%03d", (seq*13+m)%s.shared)
		}
		links = append(links, pdw1614Link(fmt.Sprintf("%s:l%02d", id, m), fmt.Sprintf("%s:e%02d", id, m%10), to, id))
	}
	ctx := context.Background()
	exec := NewStorageExecutor(f.store)
	steps := []struct {
		q string
		p map[string]any
	}{
		{"BEGIN", nil},
		{pdw1614CreateDocuments, map[string]any{"docs": []any{map[string]any{"id": id, "version": version, "family": "fam-" + id,
			"source": source, "collection": fmt.Sprintf("coll-%d", seq%10), "emails": source["email_own_keys"]}}}},
		{pdw1614CreateEntities, map[string]any{"entities": entities}},
		{pdw1614CreateLinks, map[string]any{"links": links}},
		{"MATCH (d:PDDocument {id: $id}) SET d.summary_status = 'writing', d.touched = $seq", map[string]any{"id": fmt.Sprintf("doc-%04d", (seq*31)%s.docs), "seq": int64(seq)}},
		{"COMMIT", nil},
	}
	for _, step := range steps {
		if _, err := exec.Execute(ctx, step.q, step.p); err != nil {
			t.Fatalf("writer %s step %.40q: %v", id, step.q, err)
		}
	}
}

func pdw1614StorageCum(w pdw1614Window, n int) string {
	return pdw1614Top(w.cum, w.stmtNs, n, func(name string) bool {
		return strings.Contains(name, "pkg/storage") || strings.Contains(name, "dgraph-io/badger")
	})
}

func (f *pdw1614Fixture) applyEvaluationInputs(t *testing.T) {
	t.Helper()
	params := f.evaluateParams()
	result := pdw1614Exec(t, f.store, f.evaluate, params)
	rows, _ := result.Rows[0][0].([]interface{})
	applied := []any{}
	for _, row := range rows {
		m, _ := row.(map[string]interface{})
		applied = append(applied, map[string]any{"from": m["from"], "link": m["link"], "inputs": m["inputs"]})
	}
	pdw1614Batches(t, f.store, "UNWIND $rows AS row MATCH (s:PDGraphNode {id: row.from})-[r:PD_RELATION]->() WHERE r.id = row.link "+
		"SET r.eff_applied = row.inputs RETURN count(r) AS applied", "rows", applied, 500)
	t.Logf("PDW1614R2 applied inputs of %d links", len(applied))
}

// evaluateAfterCommits begins a transaction, lets k writer commits land (no
// write runs afterwards), then runs the evaluation in that transaction.
func (f *pdw1614Fixture) evaluateAfterCommits(t *testing.T, label string, k int, shared bool) {
	t.Helper()
	params := f.evaluateParams()
	var run pdw1614Run
	var exec *StorageExecutor
	ctx := pprof.WithLabels(context.Background(), pprof.Labels("pdw", "stmt"))
	exec = NewStorageExecutor(f.store)
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		t.Fatal(err)
	}
	// The transaction's first read, so its snapshot is in use before the commits.
	if _, err := exec.Execute(ctx, "MATCH (d:PDDocument {id: 'doc-0000'}) RETURN d.id", nil); err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	for i := 0; i < k; i++ {
		f.writeOne(t, label, i, shared)
	}
	wrote := time.Since(began)
	w := pdw1614Measure(t, func() {
		pprof.Do(context.Background(), pprof.Labels("pdw", "stmt"), func(ctx context.Context) {
			start := time.Now()
			result, err := exec.Execute(ctx, f.evaluate, params)
			run.wall, run.err = time.Since(start), err
			if result != nil && len(result.Rows) > 0 {
				run.summary = pdw1614RowSummary(result)
			}
		})
	})
	if run.err != nil {
		t.Fatalf("EVALUATE after %d commits: %v", k, run.err)
	}
	if _, err := exec.Execute(context.Background(), "COMMIT", nil); err != nil {
		t.Fatalf("commit: %v", err)
	}
	name := fmt.Sprintf("evaluate/tx-after-%d-commits shared=%v", k, shared)
	t.Logf("PDW1614R2 run %s commitsTook=%v wall=%v %s", name, wrote.Round(time.Millisecond), run.wall.Round(time.Millisecond), run.summary)
	w.log(t, name, 1, false)
	t.Logf("PDW1614R2 window %s storage-cum{%s}", name, pdw1614StorageCum(w, 40))
	t.Logf("PDW1614R2 window %s flat{%s}", name, pdw1614Top(w.flat, w.stmtNs, 20, nil))

	auto := pdw1614Measure(t, func() {
		run = pdw1614RunStatement(f.store, f.evaluate, params, false)
	})
	t.Logf("PDW1614R2 run evaluate/auto-commit-after-%d-commits wall=%v %s err=%v", k, run.wall.Round(time.Millisecond), run.summary, run.err)
	auto.log(t, fmt.Sprintf("evaluate/auto-commit-after-%d-commits", k), 1, false)
	tx := pdw1614Measure(t, func() {
		run = pdw1614RunStatement(f.store, f.evaluate, params, true)
	})
	t.Logf("PDW1614R2 run evaluate/fresh-tx-after-%d-commits wall=%v %s err=%v", k, run.wall.Round(time.Millisecond), run.summary, run.err)
	tx.log(t, fmt.Sprintf("evaluate/fresh-tx-after-%d-commits", k), 1, false)
	t.Logf("PDW1614R2 window evaluate/fresh-tx-after-%d-commits storage-cum{%s}", k, pdw1614StorageCum(tx, 25))
}

func TestPDW1614EvaluateSnapshotAge(t *testing.T) {
	f := pdw1614Build(t)
	f.applyEvaluationInputs(t)
	f.evaluateAfterCommits(t, "a", 0, false)
	f.evaluateAfterCommits(t, "b", 300, false)
	f.evaluateAfterCommits(t, "c", 1500, false)
	f.evaluateAfterCommits(t, "d", 1500, true)
}

func (f *pdw1614Fixture) countReadProfile(t *testing.T, name string) {
	t.Helper()
	reader := f.store.(interface {
		ReadCountStore(context.Context, storage.CountStoreReadRequest) (storage.CountStoreRead, error)
	})
	request := storage.CountStoreReadRequest{Kind: storage.CountStoreNodesByLabel, Name: "PDGraphNode"}
	const calls = 2000
	var answer storage.CountStoreRead
	w := pdw1614Measure(t, func() {
		pprof.Do(context.Background(), pprof.Labels("pdw", "stmt"), func(ctx context.Context) {
			for i := 0; i < calls; i++ {
				answer, _ = reader.ReadCountStore(ctx, request)
			}
		})
	})
	t.Logf("PDW1614R2 count-read %s nodes:PDGraphNode per-call=%v count=%d", name, (w.wall / calls).Round(time.Microsecond), answer.Count)
	t.Logf("PDW1614R2 count-read %s storage-cum{%s}", name, pdw1614StorageCum(w, 25))
	t.Logf("PDW1614R2 count-read %s flat{%s}", name, pdw1614Top(w.flat, w.stmtNs, 15, nil))
}

func TestPDW1614CountReadAfterCommits(t *testing.T) {
	f := pdw1614Build(t)
	f.countReadProfile(t, "fixture")
	f.reopen()
	f.countReadProfile(t, "fixture-reopened")
	for i := 0; i < 3000; i++ {
		f.writeOne(t, "n", i, false)
	}
	f.countReadProfile(t, "after-3000-commits")
	f.reopen()
	f.countReadProfile(t, "after-3000-commits-reopened")
	if compactor, ok := any(f.engine).(interface{ RunGC() error }); ok {
		t.Logf("PDW1614R2 engine RunGC err=%v", compactor.RunGC())
		f.countReadProfile(t, "after-RunGC")
	}
}
