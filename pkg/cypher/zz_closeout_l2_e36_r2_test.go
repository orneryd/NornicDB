//go:build closeout_l2_e36_r2

package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E36 Reproduction 2: an indexed node read of 12,000 nodes with no pending
// writes takes 3.9-6.7 times as long between BEGIN and ROLLBACK as it does
// auto-commit (median of 7, the same rows).
//
// The parser is the process's (NORNICDB_PARSER, read at init); the lab runs
// this file once per parser.
//
// Threshold: the explicit/auto-commit ratio of the medians. The entry's
// defect is 3.9-6.7 at N = 12,000 (3.1-4.1 in its paired rerun); the same
// read with the transaction costing nothing extra is about 1. A ratio above
// 2.0 for either read is the defect: more than 1.5x below the lowest defect
// figure and twice the fixed one. Both modes run in this process on the same
// store, alternating per round, so machine speed cancels out.

const (
	l2E36R2N         = 12000
	l2E36R2Other     = 256
	l2E36R2Rounds    = 7
	l2E36R2MaxRatio  = 2.0
	l2E36R2BatchSize = 2000
)

type l2E36R2Fixture struct {
	base *storage.BadgerEngine
	ns   *storage.NamespacedEngine
	exec *StorageExecutor
	ctx  context.Context
}

func l2E36R2Open(t *testing.T) *l2E36R2Fixture {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir()}))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "db")
	f := &l2E36R2Fixture{base: base, ns: ns, exec: NewStorageExecutor(ns, QueryCachePolicy{MaxEntries: 0, TTL: 0}), ctx: WithTransactionControl(context.Background())}
	f.mustExec(t, "CREATE CONSTRAINT txread_id IF NOT EXISTS FOR (m:Item) REQUIRE m.id IS UNIQUE", nil)
	f.mustExec(t, "CREATE INDEX txread_document IF NOT EXISTS FOR (m:Item) ON (m.document_id)", nil)
	summary := strings.Repeat("s", 200)
	total := l2E36R2N + l2E36R2Other
	for start := 0; start < total; start += l2E36R2BatchSize {
		var batch []interface{}
		for i := start; i < total && i < start+l2E36R2BatchSize; i++ {
			doc := "doc-1"
			if i >= l2E36R2N {
				doc = "other-doc"
			}
			batch = append(batch, map[string]interface{}{
				"id":          fmt.Sprintf("ent-%06d", i),
				"document_id": doc,
				"reading_id":  "r1",
				"name":        fmt.Sprintf("n%d", i),
				"summary":     summary,
			})
		}
		f.mustExec(t, "UNWIND $nodes AS node CREATE (m:Item:Entity) SET m = node", map[string]interface{}{"nodes": batch})
	}
	return f
}

func (f *l2E36R2Fixture) mustExec(t *testing.T, query string, params map[string]interface{}) *ExecuteResult {
	t.Helper()
	res, err := f.exec.Execute(f.ctx, query, params)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return res
}

func l2E36R2Int(v interface{}) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return -1
}

// check verifies one read's rows: the count read returns N, the nodes read
// returns N rows.
func l2E36R2Check(t *testing.T, name string, res *ExecuteResult) {
	t.Helper()
	if name == "count" {
		if len(res.Rows) != 1 || l2E36R2Int(res.Rows[0][0]) != l2E36R2N {
			t.Fatalf("count read returned %v, want %d", res.Rows, l2E36R2N)
		}
		return
	}
	if len(res.Rows) != l2E36R2N {
		t.Fatalf("nodes read returned %d rows, want %d", len(res.Rows), l2E36R2N)
	}
}

func l2E36R2Median(values []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[len(sorted)/2]
}

// measure times query auto-commit and between BEGIN and ROLLBACK (only the
// read timed), alternating, l2E36R2Rounds times each, after one untimed run.
func (f *l2E36R2Fixture) measure(t *testing.T, name, query string, params map[string]interface{}) (auto, explicit []time.Duration) {
	t.Helper()
	l2E36R2Check(t, name, f.mustExec(t, query, params))
	for i := 0; i < l2E36R2Rounds; i++ {
		started := time.Now()
		res := f.mustExec(t, query, params)
		auto = append(auto, time.Since(started))
		l2E36R2Check(t, name, res)

		f.mustExec(t, "BEGIN", nil)
		started = time.Now()
		res = f.mustExec(t, query, params)
		explicit = append(explicit, time.Since(started))
		f.mustExec(t, "ROLLBACK", nil)
		l2E36R2Check(t, name, res)
	}
	return auto, explicit
}

var l2E36R2Reads = []struct{ name, query string }{
	{"count", "MATCH (m:Item:Entity {document_id:$document_id}) RETURN count(m)"},
	{"nodes", "MATCH (m:Item:Entity {document_id:$document_id}) RETURN m"},
}

func TestL2_e36_r2_Repro(t *testing.T) {
	f := l2E36R2Open(t)
	params := map[string]interface{}{"document_id": "doc-1"}
	defect := false
	var decisive []string
	for _, read := range l2E36R2Reads {
		auto, explicit := f.measure(t, read.name, read.query, params)
		am, em := l2E36R2Median(auto), l2E36R2Median(explicit)
		ratio := float64(em) / float64(am)
		t.Logf("%s: auto-commit %v median %v; explicit %v median %v; ratio %.2f", read.name, auto, am, explicit, em, ratio)
		decisive = append(decisive, fmt.Sprintf("%s=%.1f/%.1fms(ratio=%.2f)", read.name, float64(am.Microseconds())/1000, float64(em.Microseconds())/1000, ratio))
		if ratio > l2E36R2MaxRatio {
			defect = true
			t.Errorf("%s read: explicit transaction median %v is %.2f times the auto-commit median %v (bound %.1f)", read.name, em, ratio, am, l2E36R2MaxRatio)
		}
	}
	// Four count reads in one transaction (the entry: the cost does not fall
	// with repetition). Logged only.
	f.mustExec(t, "BEGIN", nil)
	var repeated []time.Duration
	for i := 0; i < 4; i++ {
		started := time.Now()
		res := f.mustExec(t, l2E36R2Reads[0].query, params)
		repeated = append(repeated, time.Since(started))
		l2E36R2Check(t, "count", res)
	}
	f.mustExec(t, "ROLLBACK", nil)
	t.Logf("four count reads in one transaction: %v", repeated)
	verdict := "OK"
	if defect {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E36-R2 %s N=%d %s inTx4=%v", verdict, l2E36R2N, strings.Join(decisive, " "), repeated)
}

// TestL2_e36_r2_Control: the same store and harness with a read that touches
// no node bodies (`RETURN 1`) shows no explicit-transaction overhead beyond
// the bound, and the transaction sees the committed rows (count = N).
func TestL2_e36_r2_Control(t *testing.T) {
	f := l2E36R2Open(t)
	auto, explicit := []time.Duration{}, []time.Duration{}
	for i := 0; i < l2E36R2Rounds; i++ {
		started := time.Now()
		f.mustExec(t, "UNWIND range(1, 20000) AS x WITH x WHERE x < 0 RETURN count(*) AS c", nil)
		auto = append(auto, time.Since(started))
		f.mustExec(t, "BEGIN", nil)
		started = time.Now()
		f.mustExec(t, "UNWIND range(1, 20000) AS x WITH x WHERE x < 0 RETURN count(*) AS c", nil)
		explicit = append(explicit, time.Since(started))
		f.mustExec(t, "ROLLBACK", nil)
	}
	am, em := l2E36R2Median(auto), l2E36R2Median(explicit)
	ratio := float64(em) / float64(am)
	t.Logf("control (no node reads): auto median %v explicit median %v ratio %.2f", am, em, ratio)
	// A tiny statement's absolute time is noisy; only a large overhead fails.
	if ratio > 3*l2E36R2MaxRatio && em-am > 20*time.Millisecond {
		t.Errorf("control: explicit transaction overhead without node reads: ratio %.2f", ratio)
	}
	f.mustExec(t, "BEGIN", nil)
	l2E36R2Check(t, "count", f.mustExec(t, l2E36R2Reads[0].query, map[string]interface{}{"document_id": "doc-1"}))
	f.mustExec(t, "ROLLBACK", nil)
}

// TestL2_e36_r2_Cause compares, below the executor, the point read the
// entry's profile names (transactionStorageWrapper.GetQueryNode ->
// BadgerTransaction.GetQueryNode -> getCommittedNodeReadLocked,
// pkg/storage/badger_transaction_graph_reads.go:1161 at the pin) with the
// auto-commit point read (NamespacedEngine.GetQueryNode) for the same 12,000
// nodes. Logged only: a transaction read that is several times the engine
// read places the cost in the transaction's committed-node read.
func TestL2_e36_r2_Cause(t *testing.T) {
	f := l2E36R2Open(t)
	nodes, err := f.ns.GetNodesByLabel(context.Background(), "Item")
	if err != nil {
		t.Fatalf("nodes: %v", err)
	}
	var ids []storage.NodeID
	for _, n := range nodes {
		if n.Properties["document_id"] == "doc-1" {
			ids = append(ids, n.ID)
		}
	}
	t.Logf("ids: %d", len(ids))
	ctx := context.Background()
	var engine, inTx, inTxSecond []time.Duration
	for round := 0; round < 3; round++ {
		started := time.Now()
		for _, id := range ids {
			if _, err := f.ns.GetQueryNode(ctx, id); err != nil {
				t.Fatalf("engine read %s: %v", id, err)
			}
		}
		engine = append(engine, time.Since(started))

		tx, err := f.base.BeginTransaction(ctx, storage.OrdinaryBegin)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		if err := tx.SetNamespace("db"); err != nil {
			t.Fatalf("set namespace: %v", err)
		}
		for pass := 0; pass < 2; pass++ {
			started = time.Now()
			for _, id := range ids {
				if _, err := tx.GetQueryNode(ctx, f.ns.PrefixNodeID(id)); err != nil {
					t.Fatalf("tx read %s: %v", id, err)
				}
			}
			if pass == 0 {
				inTx = append(inTx, time.Since(started))
			} else {
				inTxSecond = append(inTxSecond, time.Since(started))
			}
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("rollback: %v", err)
		}
	}
	em, tm, tm2 := l2E36R2Median(engine), l2E36R2Median(inTx), l2E36R2Median(inTxSecond)
	t.Logf("L2CAUSE E36-R2 GetQueryNode x%d: engine median %v; transaction median %v (second pass %v); ratio %.2f",
		len(ids), em, tm, tm2, float64(tm)/float64(em))
}
