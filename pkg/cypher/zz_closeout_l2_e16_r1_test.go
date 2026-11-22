//go:build closeout_l2_e16_r1

package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// E16 Reproduction 1 (Cypher): inside an older explicit transaction, the
// relationship a peer committed after the transaction's snapshot is counted
// by count(r) but not listed, and a MERGE that matches/creates the
// relationship returns null endpoints.
//
// The parser is the process's (NORNICDB_PARSER, read by pkg/config at init);
// the lab's "--parser both" runs this file once per parser.

type l2E16R1Fixture struct {
	ctx  context.Context
	peer *StorageExecutor
	tx   *StorageExecutor
}

func l2E16R1NewFixture(t *testing.T) *l2E16R1Fixture {
	t.Helper()
	base, err := testfixture.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	ns := storage.NewNamespacedEngine(base, "nornic")
	off := QueryCachePolicy{MaxEntries: 0, TTL: 0}
	return &l2E16R1Fixture{
		ctx:  context.Background(),
		peer: NewStorageExecutor(ns, off),
		tx:   NewStorageExecutor(ns, off),
	}
}

func (f *l2E16R1Fixture) run(t *testing.T, exec *StorageExecutor, query string) *ExecuteResult {
	t.Helper()
	result, err := exec.Execute(f.ctx, query, nil)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return result
}

func l2E16R1Int(v interface{}) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case float64:
		return int64(x), true
	}
	return 0, false
}

// l2E16R1Setup runs steps 2-4: the peer's setup, T's BEGIN and snapshot pin,
// and the peer's committed relationship. withOld adds (c)-[:OLD]->(d) before
// T begins (the entry's "Other listings" setup).
func l2E16R1Setup(t *testing.T, f *l2E16R1Fixture, withOld bool) {
	t.Helper()
	// The entry's first setup statement only advances the namespace's commit
	// sequence; its odd literal is kept, and a refusal is logged, not fatal.
	if _, err := f.peer.Execute(f.ctx, "CREATE (:Audit {amount: 1.0_0})", nil); err != nil {
		t.Logf("setup CREATE (:Audit {amount: 1.0_0}) refused (kept going): %v", err)
		f.run(t, f.peer, "CREATE (:Audit {amount: 1.0})")
	}
	f.run(t, f.peer, "CREATE (:P {id:'a'}),(:P {id:'b'})")
	if withOld {
		f.run(t, f.peer, "CREATE (:P {id:'c'})-[:OLD]->(:P {id:'d'})")
	}
	f.run(t, f.tx, "BEGIN")
	t.Cleanup(func() { _, _ = f.tx.Execute(context.Background(), "ROLLBACK", nil) })
	f.run(t, f.tx, "MATCH (n:P) RETURN count(n)")
	f.run(t, f.peer, "MATCH (a:P {id:'a'}),(b:P {id:'b'}) CREATE (a)-[:LINK]->(b)")
}

func TestL2_e16_r1_Repro(t *testing.T) {
	f := l2E16R1NewFixture(t)
	l2E16R1Setup(t, f, false)

	// Step 5.
	countRes := f.run(t, f.tx, "MATCH ()-[r]->() RETURN count(r)")
	var count int64 = -1
	if len(countRes.Rows) == 1 && len(countRes.Rows[0]) == 1 {
		count, _ = l2E16R1Int(countRes.Rows[0][0])
	}
	listRes := f.run(t, f.tx, "MATCH ()-[r]->() RETURN r")
	rows := len(listRes.Rows)

	// Step 6.
	mergeRes := f.run(t, f.tx, "MATCH (a:P {id:'a'}),(b:P {id:'b'}) SET a += {} WITH a,b MERGE (a)-[r:LINK]->(b) RETURN elementId(r), startNode(r).id, endNode(r).id")
	var elementID, startID, endID interface{} = "<no row>", "<no row>", "<no row>"
	if len(mergeRes.Rows) >= 1 && len(mergeRes.Rows[0]) == 3 {
		elementID, startID, endID = mergeRes.Rows[0][0], mergeRes.Rows[0][1], mergeRes.Rows[0][2]
	}

	// Step 7.
	f.run(t, f.tx, "ROLLBACK")

	countAgrees := count == int64(rows)
	mergeOK := len(mergeRes.Rows) == 1 && startID == "a" && endID == "b"
	verdict := "OK"
	if !countAgrees || !mergeOK {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E16-R1 %s parser=%s step5_count=%d step5_rows=%d step6_rows=%d step6=[%v %v %v]",
		verdict, config.GetParserType(), count, rows, len(mergeRes.Rows), elementID, startID, endID)
	if !countAgrees {
		t.Errorf("step 5: count(r)=%d but MATCH ()-[r]->() RETURN r returned %d rows", count, rows)
	}
	if !mergeOK {
		t.Errorf("step 6: MERGE returned %d rows, startNode(r).id=%v endNode(r).id=%v; want one row with 'a','b'", len(mergeRes.Rows), startID, endID)
	}
}

// Control: the same reads with no concurrent commit after T's snapshot (the
// relationship exists before BEGIN) agree: count = rows, endpoints a and b.
func TestL2_e16_r1_Control(t *testing.T) {
	f := l2E16R1NewFixture(t)
	f.run(t, f.peer, "CREATE (:P {id:'a'}),(:P {id:'b'})")
	f.run(t, f.peer, "MATCH (a:P {id:'a'}),(b:P {id:'b'}) CREATE (a)-[:LINK]->(b)")
	f.run(t, f.tx, "BEGIN")
	t.Cleanup(func() { _, _ = f.tx.Execute(context.Background(), "ROLLBACK", nil) })
	f.run(t, f.tx, "MATCH (n:P) RETURN count(n)")
	countRes := f.run(t, f.tx, "MATCH ()-[r]->() RETURN count(r)")
	count, _ := l2E16R1Int(countRes.Rows[0][0])
	rows := len(f.run(t, f.tx, "MATCH ()-[r]->() RETURN r").Rows)
	mergeRes := f.run(t, f.tx, "MATCH (a:P {id:'a'}),(b:P {id:'b'}) SET a += {} WITH a,b MERGE (a)-[r:LINK]->(b) RETURN elementId(r), startNode(r).id, endNode(r).id")
	f.run(t, f.tx, "ROLLBACK")
	t.Logf("control parser=%s count=%d rows=%d merge=%v", config.GetParserType(), count, rows, mergeRes.Rows)
	if count != 1 || rows != 1 {
		t.Errorf("control: count=%d rows=%d, want 1 and 1", count, rows)
	}
	if len(mergeRes.Rows) != 1 || mergeRes.Rows[0][1] != "a" || mergeRes.Rows[0][2] != "b" {
		t.Errorf("control: MERGE rows %v, want one row with endpoints a,b", mergeRes.Rows)
	}
}

// Cause: the entry's "Other listings" matrix. Inside T, after the peer's
// commit, every listing is logged next to the counter-answered counts
// (transactionStorageWrapper.EdgeCount / EdgeCountByType / ReadCountStore),
// plus a by-ID read of the peer's relationship. It logs only; it fails on
// harness errors.
func TestL2_e16_r1_Cause(t *testing.T) {
	f := l2E16R1NewFixture(t)
	l2E16R1Setup(t, f, true)
	peerID := f.run(t, f.peer, "MATCH ()-[r:LINK]->() RETURN elementId(r)")
	var linkID interface{}
	if len(peerID.Rows) == 1 {
		linkID = peerID.Rows[0][0]
	}
	bind := "MATCH (a:P {id:'a'}), (b:P {id:'b'}) "
	queries := []string{
		"MATCH ()-[r]->() RETURN count(r)",
		"MATCH ()-[r:LINK]->() RETURN count(r)",
		"MATCH ()-[r]->() RETURN type(r)",
		"MATCH ()-[r:LINK]->() RETURN type(r)",
		"MATCH ()-[r]->() RETURN type(r), count(*)",
		bind + "MATCH (a)-[r]->(x) RETURN count(r)",
		bind + "MATCH (a)-[r:LINK]->(x) RETURN x.id",
		bind + "MATCH (b)<-[r]-(x) RETURN x.id",
		bind + "MATCH (a)-[r]->(b) RETURN count(r)",
		bind + "RETURN EXISTS {(a)-->(b)}",
		bind + "RETURN EXISTS {(a)-->()}",
		bind + "RETURN COUNT {(a)-->()}",
		bind + "RETURN size([(a)-->(x) | x.id])",
		bind + "OPTIONAL MATCH (a)-[r]->(x) RETURN x.id",
		"MATCH ()-[r]->() WHERE type(r)='LINK' RETURN count(*)",
		"MATCH ()-[r]-() RETURN count(r)",
		"CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType",
	}
	if linkID != nil {
		queries = append(queries, fmt.Sprintf("MATCH ()-[r]->() WHERE elementId(r) = '%v' RETURN type(r)", linkID))
	}
	t.Logf("cause parser=%s peer LINK elementId=%v", config.GetParserType(), linkID)
	for _, q := range queries {
		res, err := f.tx.Execute(f.ctx, q, nil)
		if err != nil {
			t.Logf("cause in-T %q -> error %v", q, err)
			continue
		}
		t.Logf("cause in-T %q -> rows %v", q, res.Rows)
	}
	f.run(t, f.tx, "ROLLBACK")
	outside := f.run(t, f.peer, "MATCH ()-[r]->() RETURN type(r) ORDER BY type(r)")
	t.Logf("cause outside-T types %v", outside.Rows)
}
