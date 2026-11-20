package cypher

// H2 variant of eced6a242 "Preserve CALL imports in FOREACH and transactional
// subqueries" (units of pkg/cypher/result_columns_compile.go and
// result_columns_membership.go). The fix's own assertion
// (call_import_foreach_transactions_test.go: returned rows and stored state)
// applied to the sibling body shapes: other write clauses that use the
// import, FOREACH under CALL ... IN TRANSACTIONS, nested scoped CALLs, several
// imports, UNWIND bodies, CALL (*) in transactions, and WITH * plus an extra
// item. Oracle: rows and stored state equal the Cypher semantics written in
// each case (the same values the regression test asserts for its shapes).

import (
	"context"
	"reflect"
	"testing"
)

type h2CallImportCase struct {
	name       string
	setup      []string
	query      string
	wantRows   [][]interface{}
	stored     string
	wantStored [][]interface{}
}

func h2CallImportCases() []h2CallImportCase {
	one := func(v int64) [][]interface{} { return [][]interface{}{{v}} }
	return []h2CallImportCase{
		{name: "foreach-in-scoped-call-in-transactions", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } IN TRANSACTIONS RETURN n.v AS v",
			wantRows: one(2), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(2)},
		{name: "foreach-in-wildcard-scoped-call-in-transactions", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (*) { FOREACH (i IN [1,2] | SET n.v = i) } IN TRANSACTIONS OF 1 ROW RETURN n.v AS v",
			wantRows: one(2), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(2)},
		{name: "foreach-in-nested-scoped-call", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { CALL (n) { FOREACH (i IN [1,2] | SET n.v = i) } } RETURN n.v AS v",
			wantRows: one(2), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(2)},
		{name: "foreach-two-imports", setup: []string{"CREATE (:ZZForeach {v:0}), (:ZZOther {v:0})"},
			query: "MATCH (n:ZZForeach), (m:ZZOther) CALL (n, m) { FOREACH (i IN [1,2] | SET n.v = i, m.v = i * 10) } RETURN n.v + m.v AS v",
			wantRows: one(22), stored: "MATCH (n:ZZForeach), (m:ZZOther) RETURN n.v + m.v AS v", wantStored: one(22)},
		{name: "foreach-creates-rel-from-import", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | CREATE (n)-[:ZZR {v:i}]->(:ZZOther {v:i})) } RETURN n.v AS v",
			wantRows: one(0), stored: "MATCH (:ZZForeach)-[r:ZZR]->(:ZZOther) RETURN count(r) AS c", wantStored: one(2)},
		{name: "foreach-merge-rel-from-import", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,1,2] | MERGE (n)-[:ZZR]->(:ZZOther {v:i})) } RETURN n.v AS v",
			wantRows: one(0), stored: "MATCH (:ZZForeach)-[:ZZR]->(o:ZZOther) RETURN count(o) AS c", wantStored: one(2)},
		{name: "foreach-set-label-on-import", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1] | SET n:ZZMarked) } RETURN n.v AS v",
			wantRows: one(0), stored: "MATCH (n:ZZMarked) RETURN count(n) AS c", wantStored: one(1)},
		{name: "foreach-remove-property-on-import", setup: []string{"CREATE (:ZZForeach {v:0, w:1})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1] | REMOVE n.w) } RETURN n.v AS v",
			wantRows: one(0), stored: "MATCH (n:ZZForeach) RETURN count(n.w) AS c", wantStored: one(0)},
		{name: "foreach-set-map-on-import", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [1,2] | SET n += {v: i}) } RETURN n.v AS v",
			wantRows: one(2), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(2)},
		{name: "foreach-case-list-on-import", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (_ IN CASE WHEN n.v = 0 THEN [1] ELSE [] END | SET n.v = 5) } RETURN n.v AS v",
			wantRows: one(5), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(5)},
		{name: "foreach-list-from-import", setup: []string{"CREATE (:ZZForeach {v:0, xs:[3,4]})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN n.xs | SET n.v = n.v + i) } RETURN n.v AS v",
			wantRows: one(7), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(7)},
		{name: "unwind-set-in-scoped-call", setup: []string{"CREATE (:ZZForeach {v:0})"},
			query: "MATCH (n:ZZForeach) CALL (n) { UNWIND [1,2] AS i SET n.v = i } RETURN n.v AS v",
			wantRows: one(2), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(2)},
		{name: "foreach-after-match-in-body", setup: []string{"CREATE (:ZZForeach {v:0}), (:ZZOther {v:9})"},
			query: "MATCH (n:ZZForeach) CALL (n) { MATCH (o:ZZOther) FOREACH (i IN [o.v] | SET n.v = i) } RETURN n.v AS v",
			wantRows: one(9), stored: "MATCH (n:ZZForeach) RETURN n.v AS v", wantStored: one(9)},
		{name: "foreach-per-row-two-nodes", setup: []string{"CREATE (:ZZForeach {v:1}), (:ZZForeach {v:2})"},
			query: "MATCH (n:ZZForeach) CALL (n) { FOREACH (i IN [10] | SET n.v = n.v + i) } RETURN n.v AS v ORDER BY v",
			wantRows: [][]interface{}{{int64(11)}, {int64(12)}}, stored: "MATCH (n:ZZForeach) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(11)}, {int64(12)}}},
		{name: "wildcard-scope-call-in-transactions", query: "UNWIND [1,2] AS x CALL (*) { CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "scope-clause-call-in-transactions", query: "UNWIND [1,2] AS x CALL (x) { CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "wildcard-import-in-transactions-two-vars", query: "UNWIND [1,2] AS x WITH x, x * 10 AS y CALL { WITH * CREATE (:ZZB {v:x + y}) RETURN y AS value } IN TRANSACTIONS OF 1 ROW RETURN value ORDER BY value",
			wantRows: [][]interface{}{{int64(10)}, {int64(20)}}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(11)}, {int64(22)}}},
		{name: "wildcard-import-in-transactions-batch-of-2", query: "UNWIND [1,2,3] AS x CALL { WITH * CREATE (:ZZB {v:x}) RETURN x AS value } IN TRANSACTIONS OF 2 ROWS RETURN value ORDER BY value",
			wantRows: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}}},
		{name: "wildcard-import-in-transactions-no-return", query: "UNWIND [1,2] AS x CALL { WITH * CREATE (:ZZB {v:x}) } IN TRANSACTIONS OF 1 ROW",
			wantRows: [][]interface{}{}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "wildcard-import-in-transactions-merge", query: "UNWIND [1,1,2] AS x CALL { WITH * MERGE (:ZZB {v:x}) } IN TRANSACTIONS OF 1 ROW",
			wantRows: [][]interface{}{}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}}},
		{name: "wildcard-import-in-transactions-node", setup: []string{"CREATE (:ZZForeach {v:1}), (:ZZForeach {v:2})"},
			query: "MATCH (n:ZZForeach) CALL { WITH * SET n.v = n.v * 100 } IN TRANSACTIONS OF 1 ROW",
			wantRows: [][]interface{}{}, stored: "MATCH (n:ZZForeach) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(100)}, {int64(200)}}},
		{name: "wildcard-import-in-transactions-foreach", query: "UNWIND [1,2] AS x CALL { WITH * FOREACH (i IN [x] | CREATE (:ZZB {v:i})) } IN TRANSACTIONS OF 1 ROW",
			wantRows: [][]interface{}{}, stored: "MATCH (n:ZZB) RETURN n.v AS v ORDER BY v", wantStored: [][]interface{}{{int64(1)}, {int64(2)}}},
	}
}

func TestHuntH2CallImportSiblings(t *testing.T) {
	cases, leads := 0, 0
	forEachParser(t, func(t *testing.T) {
		for _, tc := range h2CallImportCases() {
			t.Run(tc.name, func(t *testing.T) {
				cases++
				exec := newCallScopeExecutor(t)
				ctx := context.Background()
				for _, statement := range tc.setup {
					if _, err := exec.Execute(ctx, statement, nil); err != nil {
						t.Fatalf("setup %q: %v", statement, err)
					}
				}
				lead := map[string]any{"harness": "call-import", "unit": "eced6a242", "case": tc.name, "setup": tc.setup, "query": tc.query}
				result, err := exec.Execute(ctx, tc.query, nil)
				if err != nil {
					lead["kind"] = "error"
					lead["error"] = err.Error()
					leads++
					h2Lead(lead)
					return
				}
				rows := result.Rows
				if rows == nil {
					rows = [][]interface{}{}
				}
				stored := refreshedReadRows(t, exec, t.Context(), tc.stored)
				if stored == nil {
					stored = [][]interface{}{}
				}
				if !reflect.DeepEqual(rows, tc.wantRows) || !reflect.DeepEqual(stored, tc.wantStored) {
					lead["kind"] = "wrong-result"
					lead["rows"], lead["want_rows"] = rows, tc.wantRows
					lead["stored_query"], lead["stored"], lead["want_stored"] = tc.stored, stored, tc.wantStored
					leads++
					h2Lead(lead)
				}
			})
		}
	})
	h2Coverage(map[string]any{"harness": "call-import", "commit": "eced6a242", "cases": cases, "leads": leads})
}
