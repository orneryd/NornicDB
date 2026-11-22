package storage_test

// Untracked probe (earlier-issues batch 1, report row L484) through Cypher:
// one of two stored relationships has an undecodable body. Shows what the
// full-text relationship index, a relationship count and a relationship scan
// report to the user.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestEI1CorruptRelationshipRowPublic(t *testing.T) {
	b, err := storage.NewBadgerEngineInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ex := cypher.NewStorageExecutor(storage.NewNamespacedEngine(b, "nornic"))
	ctx := t.Context()
	run := func(q string) {
		r, err := ex.Execute(ctx, q, nil)
		var rows any
		if r != nil {
			rows = r.Rows
		}
		t.Logf("%s -> rows=%v err=%v", q, rows, err)
	}
	if _, err := ex.Execute(ctx, "CREATE (a:EI {name:'a'})-[:T {text:'alpha', name:'good'}]->(b:EI {name:'b'}), (b)-[:T {text:'alpha', name:'bad'}]->(a)", nil); err != nil {
		t.Fatal(err)
	}
	r, err := ex.Execute(ctx, "MATCH ()-[r:T {name:'bad'}]->() RETURN elementId(r) AS id, id(r) AS iid", nil)
	if err != nil || len(r.Rows) != 1 {
		t.Fatalf("find bad edge: %v %v", r, err)
	}
	t.Logf("bad edge ids: %v", r.Rows)
	// Find the stored relationship row whose key ends with the bad edge's ID.
	var target []byte
	all, _ := b.AllEdges(ctx)
	for _, e := range all {
		if e.Properties["name"] == "bad" {
			target = []byte(e.ID)
		}
	}
	if target == nil {
		t.Fatal("bad edge not found in engine")
	}
	if err := storage.EI1CorruptEdgeRowForProbe(b, storage.EdgeID(target), []byte{0xff, 0x00, 0x13, 0x37, 0x42}); err != nil {
		t.Fatal(err)
	}
	run("MATCH ()-[r:T]->() RETURN count(r) AS c")
	run("MATCH ()-[r]->() RETURN r.name AS name ORDER BY name")
	run("MATCH (a {name:'b'})-[r]->() RETURN r.name AS name")
	run("CREATE FULLTEXT INDEX ei1_ft FOR ()-[r:T]-() ON EACH [r.text]")
	run("CALL db.awaitIndexes(30)")
	r, err = ex.Execute(ctx, "CALL db.index.fulltext.queryRelationships('ei1_ft','alpha') YIELD relationship RETURN relationship.name AS name", nil)
	var rows any
	if r != nil {
		rows = r.Rows
	}
	t.Logf("fulltext query -> rows=%v err=%v", rows, err)
	if err == nil && r != nil && len(r.Rows) == 1 {
		t.Errorf("L484 EXISTS: full-text relationship query returned %v with no error while one of two stored relationships is unreadable", r.Rows)
	}
}
