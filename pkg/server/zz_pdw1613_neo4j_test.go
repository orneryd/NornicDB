package server

// Scratch investigation (pd-w161-3), not for merge: Neo4j 5.26.30 reference
// answers for the two observations, run against the lab's reference server
// (--ref neo-ref). Everything created here is removed at the end.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

type pdw1613NeoOutcome struct {
	rows []map[string]any
	err  error
}

func pdw1613NeoRun(ctx context.Context, tx neo4jdriver.ExplicitTransaction, stmt string, params map[string]any) pdw1613NeoOutcome {
	result, err := tx.Run(ctx, stmt, params)
	if err != nil {
		return pdw1613NeoOutcome{err: err}
	}
	records, err := result.Collect(ctx)
	if err != nil {
		return pdw1613NeoOutcome{err: err}
	}
	rows := make([]map[string]any, 0, len(records))
	for _, record := range records {
		rows = append(rows, record.AsMap())
	}
	return pdw1613NeoOutcome{rows: rows}
}

func pdw1613ReadFile(t *testing.T, name string) string {
	t.Helper()
	text, err := os.ReadFile("../bolt/testdata/zz_pdw1613/" + name + ".cypher")
	require.NoError(t, err)
	return string(text)
}

func TestPDW1613Neo4jReference(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("NORNICDB_NEO4J_REFERENCE_URI is required (--ref neo-ref)")
	}
	ctx := t.Context()
	driver, err := neo4jdriver.NewDriverWithContext(uri, neo4jdriver.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(context.Background()) })
	auto := func(stmt string, params map[string]any) {
		t.Helper()
		result, err := neo4jdriver.ExecuteQuery(ctx, driver, stmt, params, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("neo4j"))
		if err != nil {
			t.Logf("PDW1613NEO auto statement=%.120q error=%v", stmt, err)
			return
		}
		rows := make([]map[string]any, 0, len(result.Records))
		for _, record := range result.Records {
			rows = append(rows, record.AsMap())
		}
		t.Logf("PDW1613NEO auto statement=%.120q rows=%v", stmt, rows)
	}
	begin := func() (neo4jdriver.SessionWithContext, neo4jdriver.ExplicitTransaction) {
		t.Helper()
		session := driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "neo4j", AccessMode: neo4jdriver.AccessModeWrite})
		tx, err := session.BeginTransaction(ctx)
		require.NoError(t, err)
		return session, tx
	}
	cleanup := func() {
		auto("MATCH (n) WHERE any(l IN labels(n) WHERE l STARTS WITH 'PD') DETACH DELETE n", nil)
	}
	auto("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition", nil)
	auto("SHOW FUNCTIONS YIELD name WHERE name IN ['apoc.coll.toSet', 'apoc.map.fromPairs', 'apoc.convert.toJson', 'apoc.coll.sort'] RETURN collect(name) AS apoc", nil)
	cleanup()

	// overlap runs firstStmt in tx1 (both txs have read d first), then
	// secondStmt in tx2 off this goroutine, waits until tx2 is blocked (or
	// done), commits tx1 and collects tx2.
	overlap := func(name, firstStmt string, firstParams map[string]any, secondStmt string, secondParams map[string]any, later string) {
		t.Helper()
		s1, tx1 := begin()
		defer s1.Close(ctx)
		s2, tx2 := begin()
		defer s2.Close(ctx)
		t.Logf("PDW1613NEO %s tx1 read=%v", name, pdw1613NeoRun(ctx, tx1, "MATCH (d:PDDocument) RETURN d.id AS id", nil))
		t.Logf("PDW1613NEO %s tx2 read=%v", name, pdw1613NeoRun(ctx, tx2, "MATCH (d:PDDocument) RETURN d.id AS id", nil))
		first := pdw1613NeoRun(ctx, tx1, firstStmt, firstParams)
		t.Logf("PDW1613NEO %s tx1 statement rows=%d err=%v", name, len(first.rows), first.err)
		done := make(chan pdw1613NeoOutcome, 1)
		go func() { done <- pdw1613NeoRun(ctx, tx2, secondStmt, secondParams) }()
		blocked := false
		var got pdw1613NeoOutcome
		select {
		case got = <-done:
		case <-time.After(1500 * time.Millisecond):
			blocked = true
		}
		commit1 := tx1.Commit(ctx)
		if blocked {
			got = <-done
		}
		var laterRows pdw1613NeoOutcome
		if got.err == nil && later != "" {
			laterRows = pdw1613NeoRun(ctx, tx2, later, nil)
		}
		var commit2 error
		if got.err == nil {
			commit2 = tx2.Commit(ctx)
		} else {
			_ = tx2.Rollback(ctx)
		}
		t.Logf("PDW1613NEO %s tx2-blocked-before-tx1-commit=%v tx1-commit=%v tx2 rows=%v err=%v later=%v tx2-commit=%v", name, blocked, commit1, got.rows, got.err, laterRows, commit2)
	}

	// Observation 1, decomposed shape, with both lock forms.
	for _, lock := range []string{"SET %[1]s += {}", "SET %[1]s._lock = true REMOVE %[1]s._lock"} {
		lockOf := func(v string) string { return fmt.Sprintf(lock, v) }
		exclusion := "CREATE (c:PDCorrection {id: 'x', status: 'active', operation: 'exclude_link', targets: ['d', 'thing']}) " +
			"WITH c UNWIND c.targets AS t MATCH (n:PDGraphNode {id: t}) CREATE (c)-[:CORRECTS]->(n) " +
			"WITH DISTINCT c UNWIND c.targets AS lt WITH lt ORDER BY lt MATCH (k:PDGraphNode {id: lt}) " + lockOf("k") + " RETURN count(k) AS locked"
		link := "MATCH (a:PDGraphNode {id: 'd'}) MATCH (b:PDGraphNode {id: 'thing'}) " +
			lockOf("a") + " " + strings.Replace(lockOf("b"), "SET ", "SET ", 1) + " " +
			"CREATE (a)-[r:PD_RELATION {id: 'l', kind: 'involves'}]->(b) " +
			"WITH r, startNode(r) AS s " +
			"OPTIONAL MATCH (s)<-[:CORRECTS]-(c:PDCorrection) " +
			"WITH r, collect(c) AS cs " +
			"WITH r, cs, size(cs) AS hop1, [c IN cs | c.id] AS ids, [c IN cs | c.status] AS statuses " +
			"OPTIONAL MATCH (k:PDCorrection {id: 'x'}) " +
			"WITH r, cs, hop1, ids, statuses, count(k) AS byKey " +
			"CALL (cs) { UNWIND cs AS c2 OPTIONAL MATCH (c2)-[:CORRECTS]->(n) RETURN collect(n.id) AS named } " +
			"WITH r, hop1, ids, statuses, byKey, named, [c IN cs | size([(c)-[:CORRECTS]->(m) | m.id])] AS namedByComprehension " +
			"SET r.usable = NOT ('d' IN named AND 'thing' IN named) " +
			"RETURN hop1, ids, statuses, byKey, named, namedByComprehension, r.usable AS usable"
		cleanup()
		auto("CREATE (:PDGraphNode:PDDocument {id: 'd'}), (:PDGraphNode:PDEntity {id: 'thing'})", nil)
		overlap("obs1 lock="+lockOf("x"), exclusion, nil, link, nil, "")
		auto("MATCH ()-[r:PD_RELATION {id: 'l'}]->() RETURN r.usable AS usable", nil)
	}

	// Observation 1, the application's statements, both orders.
	writeLink, recordCorrection := pdw1613ReadFile(t, "WRITE_LINK"), pdw1613ReadFile(t, "RECORD_CORRECTION")
	linkParams := map[string]any{"from_id": "d", "to_id": "thing", "expected_revision": int64(0),
		"link": map[string]any{"id": "l", "kind": "involves", "origin": "user"}}
	exclusionParams := map[string]any{"correction": map[string]any{"id": "x", "operation": "exclude_link", "targets": []any{"d", "thing"},
		"link_kind": "involves", "scope": []any{"all"}, "author": "probe", "basis": "probe", "reason": "probe"}}
	seed := "CREATE (:PDGraphNode:PDDocument {id:'d'}), (:PDGraphNode:PDEntity {id:'thing',document_id:'other'})"
	effective := "MATCH ()-[r:PD_RELATION {id:'l'}]->() RETURN r.eff_usable AS usable, r.eff_reason AS reason"
	for _, linkFirst := range []bool{true, false} {
		a, ap, b, bp := writeLink, linkParams, recordCorrection, exclusionParams
		if !linkFirst {
			a, ap, b, bp = b, bp, a, ap
		}
		cleanup()
		auto(seed, nil)
		auto(a, ap)
		auto(b, bp)
		t.Logf("PDW1613NEO obs1-app linkFirst=%v sequential:", linkFirst)
		auto(effective, nil)
		cleanup()
		auto(seed, nil)
		overlap(fmt.Sprintf("obs1-app linkFirst=%v", linkFirst), a, ap, b, bp, "")
		t.Logf("PDW1613NEO obs1-app linkFirst=%v stored:", linkFirst)
		auto(effective, nil)
	}

	// Observation 2: a SET of a non-key property against a MERGE writer of the
	// same node by its UNIQUE key.
	cleanup()
	auto("CREATE CONSTRAINT pdw1613_doc IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE", nil)
	auto("CREATE (:PDDocument {id: 'admission-document'})", nil)
	overlap("obs2",
		"MATCH (d:PDDocument) SET d.admission_count = coalesce(d.admission_count, 0) + 1", nil,
		"MERGE (d:PDDocument {id: 'admission-document'}) SET d.registrations = coalesce(d.registrations, 0) + 1 RETURN d.id AS id", nil, "")
	auto("MATCH (d:PDDocument) RETURN d.admission_count AS admission_count, d.registrations AS registrations", nil)
	cleanup()
	auto("DROP CONSTRAINT pdw1613_doc IF EXISTS", nil)

	// Side finding: a MERGE (with SET) of a UNIQUE key another transaction
	// created after this one began and read.
	cleanup()
	auto("CREATE CONSTRAINT pdw1613_owed IF NOT EXISTS FOR (n:PDOwed) REQUIRE n.id IS UNIQUE", nil)
	auto("CREATE (:PDGraphNode:PDDocument {id: 'd'})", nil)
	owedMerge := "MERGE (o:PDOwed {id: 'holdings:d'}) ON CREATE SET o.n = 0 SET o.n = o.n + 1 RETURN o.n AS n"
	overlap("side-simple observed-read-first", owedMerge, nil, owedMerge, nil, "")
	auto("MATCH (o:PDOwed) RETURN count(o) AS count, max(o.n) AS n", nil)
	cleanup()
	auto("DROP CONSTRAINT pdw1613_owed IF EXISTS", nil)
}
