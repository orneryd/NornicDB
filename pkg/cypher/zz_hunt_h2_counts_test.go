package cypher

// H2 variant of 0bddcdec1 "Keep transaction relationship reads and counts on
// one view" (units H2-0470 EdgeCount, H2-0471 EdgeCountByType, H2-0474
// NodeCount, H2-0475 NodeCountByLabel, H2-0476 ReadCountStore, H2-0477
// countStoreDelta, H2-0654 BadgerEngine.ReadCountStore).
//
// The fix's own assertion (transaction_count_served_test.go:
// require.Equal(t, rows, count, "the count must count the rows this
// transaction reads")) applied to the sibling count shapes and write kinds the
// regression test does not cover: total node count, end-label, incoming,
// undirected, multi-type, multi-label, count(*), DISTINCT, own deletes, own
// label changes, peer detach-delete, and the server storage stack (WAL) as
// well as the in-memory fixture. Oracle: count == number of listing rows,
// read by the same session at the same point.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type h2CountShape struct {
	unit    string
	count   string
	listing string
}

func h2CountShapes() []h2CountShape {
	return []h2CountShape{
		{"H2-0475", `MATCH (n:A) RETURN count(n)`, `MATCH (n:A) RETURN n`},
		{"H2-0475", `MATCH (n:A) RETURN count(*)`, `MATCH (n:A) RETURN n`},
		{"H2-0475", `MATCH (n:A) RETURN count(DISTINCT n)`, `MATCH (n:A) RETURN DISTINCT n`},
		{"H2-0475", `MATCH (n:B) RETURN count(n)`, `MATCH (n:B) RETURN n`},
		{"H2-0475", `MATCH (n:A:B) RETURN count(n)`, `MATCH (n:A:B) RETURN n`},
		{"H2-0474", `MATCH (n) RETURN count(n)`, `MATCH (n) RETURN n`},
		{"H2-0474", `MATCH (n) RETURN count(*)`, `MATCH (n) RETURN n`},
		{"H2-0471", `MATCH ()-[r:T]->() RETURN count(r)`, `MATCH ()-[r:T]->() RETURN r`},
		{"H2-0471", `MATCH ()-[r:T]->() RETURN count(*)`, `MATCH ()-[r:T]->() RETURN r`},
		{"H2-0471", `MATCH ()<-[r:T]-() RETURN count(r)`, `MATCH ()<-[r:T]-() RETURN r`},
		{"H2-0471", `MATCH ()-[r:T]-() RETURN count(r)`, `MATCH ()-[r:T]-() RETURN r`},
		{"H2-0471", `MATCH ()-[r:T|U]->() RETURN count(r)`, `MATCH ()-[r:T|U]->() RETURN r`},
		{"H2-0470", `MATCH ()-[r]->() RETURN count(r)`, `MATCH ()-[r]->() RETURN r`},
		{"H2-0470", `MATCH ()-[r]-() RETURN count(r)`, `MATCH ()-[r]-() RETURN r`},
		{"H2-0476", `MATCH (a:A)-[r:T]->() RETURN count(r)`, `MATCH (a:A)-[r:T]->() RETURN r`},
		{"H2-0476", `MATCH ()-[r:T]->(b:B) RETURN count(r)`, `MATCH ()-[r:T]->(b:B) RETURN r`},
		{"H2-0476", `MATCH (a:A)<-[r:U]-() RETURN count(r)`, `MATCH (a:A)<-[r:U]-() RETURN r`},
		{"H2-0476", `MATCH (a:A)-[r]->() RETURN count(r)`, `MATCH (a:A)-[r]->() RETURN r`},
		{"H2-0476", `MATCH (b:B)<-[r:T]-() RETURN count(r)`, `MATCH (b:B)<-[r:T]-() RETURN r`},
	}
}

// h2CountScenario is one point at which the counts are read.
type h2CountScenario struct {
	name string
	// tx opens an explicit transaction on the reading session.
	tx bool
	// own runs on the reading session (inside the transaction when tx).
	own []string
	// peer runs on another session and commits before the counts are read.
	peer []string
	// commitThenRead commits the transaction before reading (autocommit read).
	commitThenRead bool
}

func h2CountScenarios() []h2CountScenario {
	return []h2CountScenario{
		{name: "autocommit"},
		{name: "tx-clean", tx: true},
		{name: "tx-own-create", tx: true, own: []string{`CREATE (:A {k: 'own'})-[:T]->(:B {k: 'ownb'})`}},
		{name: "tx-own-rel-delete", tx: true, own: []string{`MATCH (:A {k: 'a1'})-[r:T]->(:B {k: 'b1'}) DELETE r`}},
		{name: "tx-own-detach-delete", tx: true, own: []string{`MATCH (n:A {k: 'a1'}) DETACH DELETE n`}},
		{name: "tx-own-add-label", tx: true, own: []string{`MATCH (n:B {k: 'b1'}) SET n:A`}},
		{name: "tx-own-remove-label", tx: true, own: []string{`MATCH (n:A {k: 'a2'}) REMOVE n:A`}},
		{name: "tx-own-create-then-delete", tx: true, own: []string{`CREATE (:A {k: 'tmp'})-[:T]->(:B {k: 'tmpb'})`, `MATCH (n {k: 'tmp'}) DETACH DELETE n`}},
		{name: "tx-own-rel-create-existing", tx: true, own: []string{`MATCH (a:A {k: 'a2'}), (b:B {k: 'b1'}) CREATE (a)-[:T]->(b)`}},
		{name: "tx-peer-create", tx: true, peer: []string{`CREATE (:A {k: 'peer'})-[:T]->(:B {k: 'peerb'})`}},
		{name: "tx-peer-detach-delete", tx: true, peer: []string{`MATCH (n:A {k: 'a1'}) DETACH DELETE n`}},
		{name: "tx-peer-add-label", tx: true, peer: []string{`MATCH (n:B {k: 'b1'}) SET n:A`}},
		{name: "tx-own-and-peer", tx: true, own: []string{`CREATE (:A {k: 'own'})-[:U]->(:B {k: 'ownb'})`}, peer: []string{`CREATE (:B {k: 'peer'})-[:U]->(:A {k: 'peera'})`}},
		{name: "after-commit-own-detach-delete", tx: true, own: []string{`MATCH (n:A {k: 'a1'}) DETACH DELETE n`}, commitThenRead: true},
		{name: "after-commit-own-label-change", tx: true, own: []string{`MATCH (n:B {k: 'b1'}) SET n:A`, `MATCH (n:A {k: 'a2'}) REMOVE n:A`}, commitThenRead: true},
		{name: "autocommit-own-detach-delete", own: []string{`MATCH (n:A {k: 'a1'}) DETACH DELETE n`}},
		{name: "autocommit-own-label-change", own: []string{`MATCH (n:B {k: 'b1'}) SET n:A`, `MATCH (n:A {k: 'a2'}) REMOVE n:A`}},
	}
}

var h2CountSeed = []string{
	`CREATE (:A {k: 'a1'})-[:T]->(:B {k: 'b1'})`,
	`CREATE (:A {k: 'a2'})`,
	`MATCH (a:A {k: 'a2'}) CREATE (:B {k: 'b2'})-[:U]->(a)`,
	`CREATE (s:A:B {k: 'ab'}) CREATE (s)-[:T]->(s)`,
}

func TestHuntH2TransactionCountShapes(t *testing.T) {
	cases, leads := 0, 0
	forEachParser(t, func(t *testing.T) {
		for _, store := range h2Stores() {
			for _, scenario := range h2CountScenarios() {
				t.Run(store.name+"/"+scenario.name, func(t *testing.T) {
					engine := store.open(t)
					ctx := context.Background()
					peer := NewStorageExecutor(engine)
					reader := NewStorageExecutor(engine)
					for _, statement := range h2CountSeed {
						_, err := peer.Execute(ctx, statement, nil)
						require.NoError(t, err, statement)
					}
					open := false
					if scenario.tx {
						_, err := reader.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						open = true
						t.Cleanup(func() {
							if open {
								_, _ = reader.Execute(context.Background(), "ROLLBACK", nil)
							}
						})
					}
					for _, statement := range scenario.own {
						if _, err := reader.Execute(ctx, statement, nil); err != nil {
							leads++
							h2Lead(map[string]any{"harness": "counts", "kind": "own-write-error", "store": store.name, "scenario": scenario.name, "statement": statement, "error": err.Error()})
							return
						}
					}
					for _, statement := range scenario.peer {
						if _, err := peer.Execute(ctx, statement, nil); err != nil {
							leads++
							h2Lead(map[string]any{"harness": "counts", "kind": "peer-write-error", "store": store.name, "scenario": scenario.name, "statement": statement, "error": err.Error()})
							return
						}
					}
					if scenario.commitThenRead {
						_, err := reader.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
						open = false
					}
					for _, shape := range h2CountShapes() {
						cases++
						countRows, countErr := h2Exec(reader, ctx, shape.count, nil)
						listRows, listErr := h2Exec(reader, ctx, shape.listing, nil)
						lead := map[string]any{"harness": "counts", "unit": shape.unit, "store": store.name, "scenario": scenario.name, "seed": h2CountSeed, "own": scenario.own, "peer": scenario.peer, "count_statement": shape.count, "listing": shape.listing}
						if countErr != nil || listErr != nil {
							lead["kind"] = "error"
							if countErr != nil {
								lead["count_error"] = countErr.Error()
							}
							if listErr != nil {
								lead["listing_error"] = listErr.Error()
							}
							leads++
							h2Lead(lead)
							continue
						}
						var count any
						if len(countRows) == 1 && len(countRows[0]) == 1 {
							count = countRows[0][0]
						}
						got, ok := count.(int64)
						if !ok || got != int64(len(listRows)) {
							lead["kind"] = "count-disagrees-with-rows"
							lead["count"] = count
							lead["rows"] = len(listRows)
							leads++
							h2Lead(lead)
						}
					}
					if open {
						_, err := reader.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
						open = false
						// After the commit a fresh session must agree too.
						fresh := NewStorageExecutor(engine)
						for _, shape := range h2CountShapes() {
							cases++
							countRows, countErr := h2Exec(fresh, ctx, shape.count, nil)
							listRows, listErr := h2Exec(fresh, ctx, shape.listing, nil)
							if countErr != nil || listErr != nil {
								leads++
								h2Lead(map[string]any{"harness": "counts", "kind": "error-after-commit", "unit": shape.unit, "store": store.name, "scenario": scenario.name, "count_statement": shape.count, "count_error": errString(countErr), "listing_error": errString(listErr)})
								continue
							}
							var count any
							if len(countRows) == 1 && len(countRows[0]) == 1 {
								count = countRows[0][0]
							}
							if got, ok := count.(int64); !ok || got != int64(len(listRows)) {
								leads++
								h2Lead(map[string]any{"harness": "counts", "kind": "count-disagrees-with-rows-after-commit", "unit": shape.unit, "store": store.name, "scenario": scenario.name, "seed": h2CountSeed, "own": scenario.own, "peer": scenario.peer, "count_statement": shape.count, "listing": shape.listing, "count": count, "rows": len(listRows)})
							}
						}
					}
				})
			}
		}
	})
	h2Coverage(map[string]any{"harness": "counts", "units": []string{"H2-0470", "H2-0471", "H2-0474", "H2-0475", "H2-0476", "H2-0477", "H2-0654"}, "cases": cases, "leads": leads})
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
