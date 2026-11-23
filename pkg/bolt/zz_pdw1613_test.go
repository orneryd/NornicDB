package bolt

// Scratch investigation (pd-w161-3), not for merge. Two observations:
//  1. a statement that creates relationship r, takes write locks and then
//     reads startNode(r)<-[:CORRECTS]-(c)-[:CORRECTS]->() after a peer
//     committed c while the statement waited for the lock;
//  2. an auto-commit SET of a non-key property on a node with a UNIQUE key
//     reported DeadlockDetected "... would wait for this transaction itself".
// Every deciding value is logged with the prefix PDW1613.

import (
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func pdw1613Server(t *testing.T) (int, <-chan struct{}) {
	t.Helper()
	store := nor213BoltStore(t, false)
	_, port := startBoltIntegrationServer(t, store, boltIntegrationExplicitTx)
	waits := fixtureowner.ObserveHookSignal(t, storage.SetUniqueKeyLockWaitHookForTesting, 256)
	return port, waits
}

type pdw1613Outcome struct {
	rows [][]any
	err  error
}

// pdw1613WaitThenCommit runs stmt on waiter off the test goroutine, waits
// until it waits for a lock (or finishes), commits holder, and returns the
// waiter's outcome, the holder's commit outcome and whether it waited.
func pdw1613WaitThenCommit(t *testing.T, waits <-chan struct{}, holder, waiter net.Conn, stmt string, params map[string]any) (pdw1613Outcome, error, bool) {
	t.Helper()
	nor213DrainEntityWriteWaits(waits)
	done := make(chan pdw1613Outcome, 1)
	go func() {
		rows, err := runBoltQueryRows(t, waiter, stmt, params)
		done <- pdw1613Outcome{rows, err}
	}()
	waited := false
	var early *pdw1613Outcome
	select {
	case <-waits:
		waited = true
	case got := <-done:
		early = &got
	case <-time.After(nor213WireWaitLimit):
		t.Fatalf("the waiter neither waited nor finished")
	}
	commitErr := nor213BoltCommitResult(t, holder)
	if early != nil {
		return *early, commitErr, waited
	}
	select {
	case got := <-done:
		return got, commitErr, waited
	case <-time.After(nor213WireWaitLimit):
		_ = waiter.Close()
		t.Fatalf("the waiter did not resume after the holder's commit (%v)", commitErr)
	}
	return pdw1613Outcome{}, commitErr, waited
}

// ---- Observation 1: decomposed shape ----

const pdw1613Exclusion = "CREATE (c:PDCorrection {id: 'x', status: 'active', operation: 'exclude_link', targets: ['d', 'thing']}) " +
	"WITH c UNWIND c.targets AS t MATCH (n:PDGraphNode {id: t}) CREATE (c)-[:CORRECTS]->(n) " +
	"WITH DISTINCT c UNWIND c.targets AS lt WITH lt ORDER BY lt MATCH (k:PDGraphNode {id: lt}) SET k += {} RETURN count(k)"

// %s is an optional statement part run between the first and second hop.
const pdw1613LinkReads = "MATCH (a:PDGraphNode {id: 'd'}) MATCH (b:PDGraphNode {id: 'thing'}) " +
	"SET a += {}, b += {} " +
	"CREATE (a)-[r:PD_RELATION {id: 'l', kind: 'involves'}]->(b) " +
	"WITH r, startNode(r) AS s " +
	"OPTIONAL MATCH (s)<-[:CORRECTS]-(c:PDCorrection) " +
	"WITH r, collect(c) AS cs " +
	"%s" +
	"WITH r, cs, size(cs) AS hop1, [c IN cs | c.id] AS ids, [c IN cs | c.status] AS statuses " +
	"OPTIONAL MATCH (k:PDCorrection {id: 'x'}) " +
	"WITH r, cs, hop1, ids, statuses, count(k) AS byKey " +
	"CALL (cs) { UNWIND cs AS c2 OPTIONAL MATCH (c2)-[:CORRECTS]->(n) RETURN collect(n.id) AS named } " +
	"WITH r, hop1, ids, statuses, byKey, named, [c IN cs | size([(c)-[:CORRECTS]->(m) | m.id])] AS namedByComprehension " +
	"SET r.usable = NOT ('d' IN named AND 'thing' IN named) " +
	"RETURN hop1, ids, statuses, byKey, named, namedByComprehension, r.usable"

const pdw1613LaterReads = "MATCH (s:PDGraphNode {id: 'd'}) OPTIONAL MATCH (s)<-[:CORRECTS]-(c:PDCorrection) " +
	"WITH collect(c) AS cs " +
	"OPTIONAL MATCH (k:PDCorrection {id: 'x'}) WITH cs, count(k) AS byKey " +
	"CALL (cs) { UNWIND cs AS c2 OPTIONAL MATCH (c2)-[:CORRECTS]->(n) RETURN collect(n.id) AS named } " +
	"RETURN size(cs), [c IN cs | c.status], byKey, named"

func pdw1613Sorted(v any) any {
	list, ok := v.([]any)
	if !ok {
		return v
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, fmt.Sprint(item))
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

func TestPDW1613LockedStatementReadsPeerCorrection(t *testing.T) {
	variants := []struct{ name, between string }{
		{"plain", ""},
		{"correction-locked", "CALL (cs) { UNWIND cs AS lc SET lc += {} RETURN count(*) AS lockedCorrections } "},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			port, waits := pdw1613Server(t)
			setup := openBoltTestConn(t, port)
			runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT pdg IF NOT EXISTS FOR (n:PDGraphNode) REQUIRE n.id IS UNIQUE", nil)
			runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT pdc IF NOT EXISTS FOR (n:PDCorrection) REQUIRE n.id IS UNIQUE", nil)
			runBoltQueryWithParams(t, setup, "CREATE (:PDGraphNode:PDDocument {id: 'd'}), (:PDGraphNode:PDEntity {id: 'thing'})", nil)

			linker := openBoltTestConn(t, port)
			nor213BoltBegin(t, linker)
			require.Len(t, runBoltQueryWithParams(t, linker, "MATCH (d:PDDocument {id: 'd'}) RETURN d.id", nil), 1)
			excluder := openBoltTestConn(t, port)
			nor213BoltBegin(t, excluder)
			require.Len(t, runBoltQueryWithParams(t, excluder, "MATCH (d:PDDocument {id: 'd'}) RETURN d.id", nil), 1)
			t.Logf("PDW1613 obs1 %s exclusion rows=%v", variant.name, runBoltQueryWithParams(t, excluder, pdw1613Exclusion, nil))

			got, commitErr, waited := pdw1613WaitThenCommit(t, waits, excluder, linker, fmt.Sprintf(pdw1613LinkReads, variant.between), nil)
			t.Logf("PDW1613 obs1 %s link-statement waited=%v exclusionCommit=%v err=%v rows(hop1, ids, statuses, byKey, named, namedByComprehension, usable)=%v",
				variant.name, waited, commitErr, got.err, got.rows)
			require.NoError(t, commitErr)
			require.NoError(t, got.err)
			later := runBoltQueryWithParams(t, linker, pdw1613LaterReads, nil)
			t.Logf("PDW1613 obs1 %s later-statement-same-tx rows(hop1, statuses, byKey, named)=%v", variant.name, later)
			linkCommit := nor213BoltCommitResult(t, linker)
			t.Logf("PDW1613 obs1 %s linkCommit=%v", variant.name, linkCommit)
			after := runBoltQueryWithParams(t, setup, "MATCH ()-[r:PD_RELATION {id: 'l'}]->() RETURN r.usable", nil)
			fresh := runBoltQueryWithParams(t, setup, pdw1613LaterReads, nil)
			t.Logf("PDW1613 obs1 %s after-both-commits stored usable=%v fresh reads=%v", variant.name, after, fresh)

			require.NoError(t, linkCommit)
			require.Len(t, got.rows, 1)
			row := got.rows[0]
			require.Equal(t, []any{int64(1), "x", "active", int64(1), "d,thing", "2", false},
				[]any{row[0], pdw1613Sorted(row[1]), pdw1613Sorted(row[2]), row[3], pdw1613Sorted(row[4]), pdw1613Sorted(row[5]), row[6]},
				"%s: the locked statement sees the committed correction and both its targets (Neo4j read committed)", variant.name)
			require.Equal(t, [][]any{{false}}, after, "%s: stored as in sequence", variant.name)
		})
	}
}

// ---- Observation 1: the exact application statements ----

func pdw1613Statement(t *testing.T, name string) string {
	t.Helper()
	text, err := os.ReadFile("testdata/zz_pdw1613/" + name + ".cypher")
	require.NoError(t, err)
	return string(text)
}

var pdw1613Uniques = [][2]string{
	{"PDGraphNode", "id"}, {"PDIdentityDecision", "id"}, {"PDDocument", "id"}, {"PDVersion", "id"}, {"PDSearchSource", "id"},
	{"PDOriginal", "id"}, {"PDUploadOccurrence", "id"}, {"PDCollection", "id"}, {"PDDefinition", "id"}, {"PDCorrection", "id"},
	{"PDCorrectionRevision", "id"}, {"PDConflict", "id"}, {"PDReading", "id"}, {"PDHeld", "thing_id"}, {"PDRelevance", "id"},
	{"PDOcrReport", "id"}, {"PDUse", "id"}, {"PDEffectiveConflict", "id"}, {"PDOwed", "id"}, {"PDHoldingsParty", "id"},
}

func pdw1613ApplicationSchema(t *testing.T, conn net.Conn) {
	t.Helper()
	for i, u := range pdw1613Uniques {
		runBoltQueryWithParams(t, conn, fmt.Sprintf("CREATE CONSTRAINT pdw_u%d IF NOT EXISTS FOR (n:%s) REQUIRE n.%s IS UNIQUE", i, u[0], u[1]), nil)
	}
	runBoltQueryWithParams(t, conn, "CREATE CONSTRAINT pdw_rel IF NOT EXISTS FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE", nil)
	runBoltQueryWithParams(t, conn, "CREATE INDEX pdw_i1 IF NOT EXISTS FOR (n:PDCorrection) ON (n.status)", nil)
	runBoltQueryWithParams(t, conn, "CREATE INDEX pdw_i2 IF NOT EXISTS FOR (n:PDGraphNode) ON (n.document_id)", nil)
	runBoltQueryWithParams(t, conn, "CREATE INDEX pdw_i3 IF NOT EXISTS FOR (n:PDVersion) ON (n.document_id)", nil)
	runBoltQueryWithParams(t, conn, "CREATE INDEX pdw_i4 IF NOT EXISTS FOR (n:PDDocument) ON (n.current_version_id)", nil)
}

const pdw1613ApplicationSeed = "CREATE (:PDGraphNode:PDDocument {id:'d'}), (:PDGraphNode:PDEntity {id:'thing',document_id:'other'})"
const pdw1613Effective = "MATCH ()-[r:PD_RELATION {id:'l'}]->() RETURN r.eff_usable AS usable, r.eff_reason AS reason"

func pdw1613LinkParams() map[string]any {
	return map[string]any{"from_id": "d", "to_id": "thing", "expected_revision": int64(0),
		"link": map[string]any{"id": "l", "kind": "involves", "origin": "user"}}
}

func pdw1613ExclusionParams() map[string]any {
	return map[string]any{"correction": map[string]any{"id": "x", "operation": "exclude_link", "targets": []any{"d", "thing"},
		"link_kind": "involves", "scope": []any{"all"}, "author": "probe", "basis": "probe", "reason": "probe"}}
}

func TestPDW1613ApplicationLinkAgainstExclusion(t *testing.T) {
	link, correction := pdw1613Statement(t, "WRITE_LINK"), pdw1613Statement(t, "RECORD_CORRECTION")
	for _, linkFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("link-first-%v", linkFirst), func(t *testing.T) {
			port, waits := pdw1613Server(t)
			setup := openBoltTestConn(t, port)
			pdw1613ApplicationSchema(t, setup)
			type edit struct {
				name   string
				text   string
				params map[string]any
			}
			edits := []edit{{"link", link, pdw1613LinkParams()}, {"exclusion", correction, pdw1613ExclusionParams()}}
			if !linkFirst {
				edits[0], edits[1] = edits[1], edits[0]
			}
			// The two edits one after the other.
			runBoltQueryWithParams(t, setup, pdw1613ApplicationSeed, nil)
			for _, e := range edits {
				require.Len(t, runBoltQueryWithParams(t, setup, e.text, e.params), 1, "sequential %s", e.name)
			}
			sequential := runBoltQueryWithParams(t, setup, pdw1613Effective, nil)
			runBoltQueryWithParams(t, setup, "MATCH (n) DETACH DELETE n", nil)
			runBoltQueryWithParams(t, setup, pdw1613ApplicationSeed, nil)

			first, second := openBoltTestConn(t, port), openBoltTestConn(t, port)
			for _, c := range []net.Conn{first, second} {
				nor213BoltBegin(t, c)
				require.Len(t, runBoltQueryWithParams(t, c, "MATCH (d:PDDocument {id:'d'}) RETURN d.id AS id", nil), 1)
			}
			firstRows := runBoltQueryWithParams(t, first, edits[0].text, edits[0].params)
			got, firstCommit, waited := pdw1613WaitThenCommit(t, waits, first, second, edits[1].text, edits[1].params)
			secondCommit := error(nil)
			if got.err == nil {
				secondCommit = nor213BoltCommitResult(t, second)
			}
			stored := runBoltQueryWithParams(t, setup, pdw1613Effective, nil)
			t.Logf("PDW1613 obs1-app linkFirst=%v first=%s rows=%d firstCommit=%v second=%s waited=%v err=%v rows=%d secondCommit=%v stored=%v sequential=%v",
				linkFirst, edits[0].name, len(firstRows), firstCommit, edits[1].name, waited, got.err, len(got.rows), secondCommit, stored, sequential)
			if firstCommit == nil && got.err == nil && secondCommit == nil {
				require.Equal(t, sequential, stored, "both committed: as in sequence (link first %v)", linkFirst)
			}
		})
	}
}

// ---- Observation 2 ----

const pdw1613AutoSet = "MATCH (d:PDDocument) SET d.admission_count = coalesce(d.admission_count, 0) + 1"
const pdw1613MergeWrite = "MERGE (d:PDDocument {id: 'admission-document'}) SET d.registrations = coalesce(d.registrations, 0) + 1 RETURN d.id"

// TestPDW1613SetAgainstMergeWriterDeadlock: transaction A sets a non-key
// property of the document (it takes the node's write lock in the
// statement); transaction R then MERGEs the document by its UNIQUE key and
// writes it, so it holds the key and waits for A's node lock; A commits.
func TestPDW1613SetAgainstMergeWriterDeadlock(t *testing.T) {
	for _, constrained := range []bool{true, false} {
		t.Run(fmt.Sprintf("unique-key-%v", constrained), func(t *testing.T) {
			port, waits := pdw1613Server(t)
			setup := openBoltTestConn(t, port)
			if constrained {
				runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT pdd IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE", nil)
			}
			runBoltQueryWithParams(t, setup, "CREATE (:PDDocument {id: 'admission-document'})", nil)
			a, r := openBoltTestConn(t, port), openBoltTestConn(t, port)
			nor213BoltBegin(t, a)
			runBoltQueryWithParams(t, a, pdw1613AutoSet, nil)
			nor213BoltBegin(t, r)
			got, aCommit, waited := pdw1613WaitThenCommit(t, waits, a, r, pdw1613MergeWrite, nil)
			rCommit := error(nil)
			if got.err == nil {
				rCommit = nor213BoltCommitResult(t, r)
			}
			final := runBoltQueryWithParams(t, setup, "MATCH (d:PDDocument) RETURN d.admission_count, d.registrations", nil)
			t.Logf("PDW1613 obs2 unique-key=%v merge-waited=%v A-commit=%v R-statement-err=%v R-commit=%v final=%v", constrained, waited, aCommit, got.err, rCommit, final)
			require.NoError(t, aCommit, "A's commit")
			require.NoError(t, got.err, "R's MERGE")
			require.NoError(t, rCommit, "R's commit")
			require.Equal(t, [][]any{{int64(1), int64(1)}}, final)
		})
	}
}

// TestPDW1613AutoCommitSetRacesMergeWriter: the observation's shape. One
// connection repeats the auto-commit SET, another repeats an explicit
// transaction that MERGEs the document by key and writes it.
func TestPDW1613AutoCommitSetRacesMergeWriter(t *testing.T) {
	port, _ := pdw1613Server(t)
	setup := openBoltTestConn(t, port)
	runBoltQueryWithParams(t, setup, "CREATE CONSTRAINT pdd IF NOT EXISTS FOR (n:PDDocument) REQUIRE n.id IS UNIQUE", nil)
	runBoltQueryWithParams(t, setup, "CREATE (:PDDocument {id: 'admission-document'})", nil)
	const rounds = 300
	auto, merger := openBoltTestConn(t, port), openBoltTestConn(t, port)
	var wg sync.WaitGroup
	var mu sync.Mutex
	failures := map[string]int{}
	note := func(side string, err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		key := side + ": " + err.Error()
		if failures[key] == 0 {
			t.Logf("PDW1613 obs2-race first %s", key)
		}
		failures[key]++
	}
	reset := func(conn net.Conn) {
		if err := SendReset(t, conn); err == nil {
			_ = ReadSuccess(t, conn)
		}
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			_, err := runBoltQueryRows(t, auto, pdw1613AutoSet, nil)
			note("auto-commit SET", err)
			if err != nil {
				reset(auto)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if err := SendBegin(t, merger, nil); err != nil {
				note("merge BEGIN", err)
				return
			}
			if err := ReadSuccess(t, merger); err != nil {
				note("merge BEGIN", err)
				return
			}
			_, err := runBoltQueryRows(t, merger, pdw1613MergeWrite, nil)
			if err == nil {
				time.Sleep(time.Millisecond)
				err = nor213BoltCommitResult(t, merger)
				note("merge COMMIT", err)
			} else {
				note("merge statement", err)
			}
			if err != nil {
				reset(merger)
			}
		}
	}()
	wg.Wait()
	final := runBoltQueryWithParams(t, setup, "MATCH (d:PDDocument) RETURN d.admission_count, d.registrations", nil)
	t.Logf("PDW1613 obs2-race rounds=%d failures=%v final=%v", rounds, failures, final)
	require.Empty(t, failures, "no deadlock between a key-less SET and a MERGE writer (Neo4j: none)")
}

// ---- Side finding: link first, then the exclusion fails with Outdated ----

const pdw1613NodeDump = "MATCH (n) RETURN elementId(n), labels(n), properties(n) ORDER BY elementId(n)"

// pdw1613LinkFirstOutdated runs the exact link statement in tx1 and the exact
// exclusion in tx2 (both began and read d first), commits tx1 while tx2
// waits, and logs tx2's outcome with the graph before and after tx1.
func pdw1613LinkFirstOutdated(t *testing.T, tag string) error {
	t.Helper()
	link, correction := pdw1613Statement(t, "WRITE_LINK"), pdw1613Statement(t, "RECORD_CORRECTION")
	port, waits := pdw1613Server(t)
	setup := openBoltTestConn(t, port)
	pdw1613ApplicationSchema(t, setup)
	runBoltQueryWithParams(t, setup, pdw1613ApplicationSeed, nil)
	t.Logf("PDW1613 side %s nodes-before=%v", tag, runBoltQueryWithParams(t, setup, pdw1613NodeDump, nil))
	linker, excluder := openBoltTestConn(t, port), openBoltTestConn(t, port)
	for _, c := range []net.Conn{linker, excluder} {
		nor213BoltBegin(t, c)
		require.Len(t, runBoltQueryWithParams(t, c, "MATCH (d:PDDocument {id:'d'}) RETURN d.id AS id", nil), 1)
	}
	require.Len(t, runBoltQueryWithParams(t, linker, link, pdw1613LinkParams()), 1)
	got, linkCommit, waited := pdw1613WaitThenCommit(t, waits, linker, excluder, correction, pdw1613ExclusionParams())
	t.Logf("PDW1613 side %s nodes-after-link-commit=%v", tag, runBoltQueryWithParams(t, setup, pdw1613NodeDump, nil))
	commit := error(nil)
	if got.err == nil {
		commit = nor213BoltCommitResult(t, excluder)
	}
	t.Logf("PDW1613 side %s linkCommit=%v exclusion-waited=%v exclusion-err=%v exclusion-commit=%v stored=%v",
		tag, linkCommit, waited, got.err, commit, runBoltQueryWithParams(t, setup, pdw1613Effective, nil))
	require.NoError(t, linkCommit)
	if got.err != nil {
		return got.err
	}
	return commit
}

func TestPDW1613LinkFirstExclusionOutdated(t *testing.T) {
	err := pdw1613LinkFirstOutdated(t, "plain")
	require.NoError(t, err, "the exclusion, begun before the link committed, waits for it and commits (Neo4j)")
}
