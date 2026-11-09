package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A CALL subquery body concludes as Neo4j 5.26.30 requires: every body (each
// UNION branch, at any depth, old-style and scoped, EXPLAIN included) ends
// with RETURN, FINISH, an update clause, a unit subquery call or a procedure
// call. A body ending with a CALL that returns
// columns, or with MATCH, WITH or UNWIND, is a SyntaxError ("Query cannot
// conclude with CALL"); expression subqueries are not held to it. Each
// verdict is Neo4j's (but the marked extension), in auto-commit and in an
// explicit transaction.
func TestQueryConclusionMatchesNeo4j(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		query    string
		rejected bool
	}{
		{"EXPLAIN UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN seed", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN 1 AS a", true},
		{"CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"EXPLAIN CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } RETURN hidden } RETURN *", false},
		{"UNWIND [] AS seed CALL { CALL { CREATE (x) } } RETURN *", false},
		{"UNWIND [] AS seed CALL { RETURN 1 AS a } RETURN *", false},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN hidden", true},
		{"UNWIND [] AS seed CALL (*) { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL () { CALL () { RETURN 'inner' AS hidden } } RETURN *", true},
		{"WITH 1 AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { MATCH (n) CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { UNWIND [1] AS x CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { CREATE (x) } } RETURN seed", false},
		{"UNWIND [] AS seed CALL { CREATE (x) } RETURN *", false},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } WITH * RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } } RETURN count(*) AS c", true},
		{"UNWIND [] AS seed CALL { CALL { CALL { RETURN 1 AS z } } } RETURN *", true},
		{"UNWIND [] AS seed CALL { CALL { RETURN 'inner' AS hidden } UNION CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"UNWIND [] AS seed CALL { WITH seed CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"MATCH (n) CALL { CALL { RETURN 'inner' AS hidden } } RETURN *", true},
		{"MATCH (n)", true},
		{"UNWIND [1] AS x", true},
		{"WITH 1 AS x", true},
		// NornicDB extension: a statement may end with a CALL that returns
		// its columns (Neo4j: "Query cannot conclude with CALL").
		{"CALL { RETURN 1 AS a }", false},
		// NornicDB extension: a statement may end with a CALL that returns
		// its columns (Neo4j: "Query cannot conclude with CALL").
		{"UNWIND [] AS s CALL { RETURN 1 AS a }", false},
		{"UNWIND [] AS s CALL { MATCH (n) } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { WITH 1 AS x } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { UNWIND [1] AS x } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL db.labels() YIELD label } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { FINISH } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { MATCH (n) SET n.x = 1 } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { FOREACH (x IN [1] | CREATE ()) } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { MATCH (n) DELETE n } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { MERGE (n:Z) } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { MATCH (n) REMOVE n.x } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { OPTIONAL MATCH (n) } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { CREATE () } } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { CALL { RETURN 1 AS b } RETURN b } RETURN *", false},
		{"UNWIND [] AS s CALL { RETURN 1 AS a UNION CALL { RETURN 2 AS a } } RETURN a", true},
		{"UNWIND [] AS s CALL { MATCH (n) WHERE n.x = 1 } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL (s) { CALL { RETURN 1 AS b } } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { RETURN 1 AS b } } IN TRANSACTIONS RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { CALL { CREATE () } } } RETURN 1 AS a", false},
		{"UNWIND [] AS s WITH s WHERE EXISTS { CALL { RETURN 1 AS b } } RETURN 1 AS a", false},
		{"UNWIND [] AS s RETURN COUNT { CALL { RETURN 1 AS b } } AS a", false},
		{"UNWIND [] AS s CALL { CALL { RETURN 1 AS b } FINISH } RETURN 1 AS a", false},
		{"UNWIND [] AS s CALL { CALL { RETURN 1 AS b } CREATE () } RETURN 1 AS a", false},
		// Round-5 review: a CALL body behind FINISH, behind an escaped alias,
		// or inside EXISTS / COUNT concludes by the same rule.
		{"UNWIND [] AS s CALL { CALL { CALL { RETURN 1 AS b } } FINISH } RETURN 1 AS a", true},
		{"EXPLAIN UNWIND [] AS s CALL { CALL { CALL { RETURN 1 AS b } } FINISH } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { MATCH (n) } FINISH } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { WITH 1 AS b } FINISH } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { UNWIND [1] AS b } FINISH } RETURN 1 AS a", true},
		{"UNWIND [] AS s CALL { CALL { CALL { RETURN 1 AS b } } RETURN 1 AS `FINISH` } RETURN 1 AS a", true},
		{"UNWIND [] AS s RETURN EXISTS { CALL { CALL { RETURN 1 AS b } } } AS a", true},
		{"UNWIND [] AS s RETURN COUNT { CALL { CALL { RETURN 1 AS b } } } AS a", true},
	} {
		for _, mode := range []string{"auto", "tx"} {
			t.Run(mode+"/"+tc.query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "query_conclusion"))
				if mode == "tx" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				}
				_, err := exec.Execute(ctx, tc.query, nil)
				if tc.rejected {
					requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
					return
				}
				require.NoError(t, err)
			})
		}
	}
}

// An EXISTS, COUNT or COLLECT body holds no update clause, at any depth (in
// a CALL body nested in it, behind FINISH, in a WHERE, under EXPLAIN): each
// is Neo4j 5.26.30's SyntaxError and message, in auto-commit and in an
// explicit transaction. A read-only body is accepted.
func TestExpressionSubqueryUpdatesMatchNeo4j(t *testing.T) {
	ctx := context.Background()
	const exists, count, collect = "An Exists Expression cannot contain any updates", "A Count Expression cannot contain any updates", "A Collect Expression cannot contain any updates"
	for _, tc := range []struct {
		query   string
		message string
	}{
		{"UNWIND [] AS s RETURN EXISTS { CREATE () } AS a", exists},
		{"UNWIND [] AS s RETURN EXISTS { CALL { CREATE () } } AS a", exists},
		{"UNWIND [] AS s RETURN EXISTS { CALL { CALL { CREATE () } } } AS a", exists},
		{"UNWIND [] AS s RETURN COUNT { CREATE () } AS a", count},
		{"UNWIND [] AS s RETURN COUNT { CALL { CREATE () } } AS a", count},
		{"UNWIND [] AS s RETURN COUNT { MATCH (n) SET n.x = 1 } AS a", count},
		{"UNWIND [] AS s RETURN COLLECT { CREATE () RETURN 1 AS b } AS a", collect},
		{"UNWIND [] AS s RETURN COLLECT { CALL { CREATE () } RETURN 1 AS b } AS a", collect},
		{"UNWIND [] AS s RETURN COUNT { FOREACH (x IN [1] | CREATE ()) } AS a", count},
		{"UNWIND [] AS s RETURN EXISTS { MATCH (n) DELETE n } AS a", exists},
		{"UNWIND [] AS s RETURN COUNT { CALL { MATCH (n) REMOVE n.x } } AS a", count},
		{"UNWIND [] AS s RETURN COUNT { MERGE (:Z) } AS a", count},
		{"UNWIND [] AS s MATCH (n) WHERE EXISTS { CALL { CREATE () } } RETURN n", exists},
		{"UNWIND [] AS s MATCH (n) WHERE COUNT { CALL { CALL { MERGE (:Z) } } } > 0 RETURN n", count},
		{"EXPLAIN UNWIND [] AS s RETURN COUNT { CALL { CREATE () } } AS a", count},
		{"UNWIND [] AS s RETURN COUNT { CALL { CREATE () } FINISH } AS a", count},
		{"UNWIND [] AS s RETURN EXISTS { CALL { CREATE () } FINISH } AS a", exists},
		{"UNWIND [] AS s RETURN COUNT { CALL { RETURN 1 AS c } } AS a", ""},
		{"UNWIND [] AS s RETURN EXISTS { CALL { RETURN 1 AS c } } AS a", ""},
	} {
		for _, mode := range []string{"auto", "tx"} {
			t.Run(mode+"/"+tc.query, func(t *testing.T) {
				exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "query_conclusion"))
				if mode == "tx" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
				}
				_, err := exec.Execute(ctx, tc.query, nil)
				if tc.message == "" {
					require.NoError(t, err)
					return
				}
				requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
				require.Contains(t, err.Error(), tc.message)
			})
		}
	}
}
