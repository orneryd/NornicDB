package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The result summary counts what a statement wrote, as Neo4j 5.26.30 does:
// CREATE and MERGE count the properties they set and the labels they add
// (#651), and writes inside a correlated CALL subquery count for the
// statement (#650).
func TestResultCountersMatchNeo4j(t *testing.T) {
	type counters struct{ nodes, rels, props, labels, deleted int }
	cases := []struct {
		stmt string
		want counters
	}{
		{"CREATE (:X {a: 1, b: 2})", counters{nodes: 1, props: 2, labels: 1}},
		{"CREATE (:X:Y)", counters{nodes: 1, labels: 2}},
		{"CREATE (a:X {a: 1})-[:R {w: 1}]->(b:Y {b: 2})", counters{nodes: 2, rels: 1, props: 3, labels: 2}},
		{"MERGE (:X {a: 1})", counters{nodes: 1, props: 1, labels: 1}},
		{"MERGE (m:X {a: 1}) ON CREATE SET m.b = 2", counters{nodes: 1, props: 2, labels: 1}},
		{"UNWIND [1, 2] AS i CREATE (:X {i: i})", counters{nodes: 2, props: 2, labels: 2}},
		{"MATCH (n:T {id: 1}) CREATE (n)-[:R {w: 1}]->(:Y {b: 2})", counters{nodes: 1, rels: 1, props: 2, labels: 1}},
		{"MATCH (n:T {id: 1}) SET n:L", counters{labels: 1}},
		{"MATCH (n:T {id: 1}) SET n.q = 1", counters{props: 1}},
		{"MATCH (n:T {id: 1}) CALL (n) { CREATE (:X {from: n.id}) } RETURN n.id AS id", counters{nodes: 1, props: 1, labels: 1}},
		{"MATCH (n:T {id: 1}) CALL { WITH n CREATE (:X {from: n.id}) } RETURN n.id AS id", counters{nodes: 1, props: 1, labels: 1}},
		{"MATCH (n:T {id: 1}) CALL (n) { CREATE (n)-[:R]->(:X) } RETURN n.id AS id", counters{nodes: 1, rels: 1, labels: 1}},
		{"MATCH (n:T {id: 1}) CALL { WITH n MERGE (m:M {id: n.id}) } RETURN n.id AS id", counters{nodes: 1, props: 1, labels: 1}},
		{"MATCH (n:T {id: 1}) CALL (n) { SET n.y = 2 } RETURN n.id AS id", counters{props: 1}},
		{"UNWIND [1, 2] AS i CALL { WITH i CREATE (:X {i: i}) } RETURN count(*) AS c", counters{nodes: 2, props: 2, labels: 2}},
		{"MATCH (n:T {id: 1}) CALL (n) { DETACH DELETE n } RETURN count(*) AS c", counters{deleted: 1}},
		{"MERGE (m:T {id: 1}) ON MATCH SET m.b = 2", counters{props: 1}},
		{"MERGE (m:T {id: 1}) ON MATCH SET m.b = 2, m:L", counters{props: 1, labels: 1}},
		{"MERGE (m:T {id: 2}) SET m.b = 2", counters{nodes: 1, props: 2, labels: 1}},
		{"MERGE (m:X {a: 2}) ON CREATE SET m.a = 3", counters{nodes: 1, props: 2, labels: 1}},
		{"MATCH (n:T {id: 1}) WITH n MERGE (m:T {id: 1}) ON MATCH SET m.b = 3 RETURN m.b AS b", counters{props: 1}},
		{"MERGE (a:T {id: 1}) ON MATCH SET a.b = 1 MERGE (b:X {k: 1}) ON CREATE SET b.c = 2", counters{nodes: 1, props: 3, labels: 1}},
		{"MERGE (a:T {id: 1}) MERGE (b:X {k: 1}) MERGE (a)-[:R]->(b)", counters{nodes: 1, rels: 1, props: 1, labels: 1}},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:T {id: 1, x: 5})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					var got counters
					if res.Stats != nil {
						got = counters{res.Stats.NodesCreated, res.Stats.RelationshipsCreated, res.Stats.PropertiesSet, res.Stats.LabelsAdded, res.Stats.NodesDeleted}
					}
					assert.Equal(t, tc.want, got)
				})
			}
		}
	}
}

// UNWIND ... CALL { } runs the subquery once per unwound value, as Neo4j
// 5.26.30 does: its writes happen, a unit subquery keeps the outer row, a
// returning subquery joins its rows onto it, and IN TRANSACTIONS commits it
// in batches. It used to skip the subquery and return only the unwound
// values.
func TestUnwindCallSubqueryRunsPerRow(t *testing.T) {
	cases := []struct {
		stmt   string
		rows   [][]interface{}
		stored []interface{}
	}{
		{"UNWIND [1, 2] AS i CALL { WITH i CREATE (:X {i: i}) } RETURN count(*) AS c",
			[][]interface{}{{int64(2)}}, []interface{}{int64(1), int64(2)}},
		{"UNWIND [1, 2] AS i CALL (i) { CREATE (x:X {i: i}) RETURN x.i * 10 AS t } RETURN i, t",
			[][]interface{}{{int64(1), int64(10)}, {int64(2), int64(20)}}, []interface{}{int64(1), int64(2)}},
		{"UNWIND [1, 2] AS i CALL (i) { RETURN i * 2 AS d } CALL (d) { RETURN d + 1 AS e } RETURN i, d, e",
			[][]interface{}{{int64(1), int64(2), int64(3)}, {int64(2), int64(4), int64(5)}}, []interface{}{}},
		{"UNWIND [] AS i CALL (i) { CREATE (:X {i: i}) } RETURN count(*) AS c",
			[][]interface{}{{int64(0)}}, []interface{}{}},
		// A subquery that imports nothing but writes runs once per row.
		{"UNWIND [1, 2, 3] AS i CALL { CREATE (:X) } RETURN count(*) AS c",
			[][]interface{}{{int64(3)}}, []interface{}{nil, nil, nil}},
		// A statement ending with the subquery returns no rows.
		{"UNWIND [1, 2] AS i CALL (i) { CREATE (:X {i: i}) }",
			[][]interface{}{}, []interface{}{int64(1), int64(2)}},
		{"UNWIND [1, 2, 3] AS i CALL (i) { CREATE (:X {i: i}) } IN TRANSACTIONS OF 2 ROWS",
			[][]interface{}{}, []interface{}{int64(1), int64(2), int64(3)}},
		{"UNWIND [1, 2, 3] AS i CALL (i) { CREATE (x:X {i: i}) RETURN x.i AS v } IN TRANSACTIONS RETURN count(v) AS c",
			[][]interface{}{{int64(3)}}, []interface{}{int64(1), int64(2), int64(3)}},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"server stack": newSetRouteServerStackExecutor,
	}
	for stack, build := range stacks {
		for _, tc := range cases {
			t.Run(stack+"/"+tc.stmt, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				res, err := exec.Execute(ctx, tc.stmt, nil)
				require.NoError(t, err)
				assert.Equal(t, tc.rows, res.Rows)
				stored, err := exec.Execute(ctx, "MATCH (x:X) RETURN x.i AS i ORDER BY i", nil)
				require.NoError(t, err)
				got := []interface{}{}
				for _, row := range stored.Rows {
					got = append(got, row[0])
				}
				assert.Equal(t, tc.stored, got)
			})
		}
	}
}

// The UNWIND batch fast paths count what they write as Neo4j 5.26.30 does:
// created entities with their properties and labels, and what their SET /
// ON CREATE SET / ON MATCH SET changed (#651).
func TestUnwindBatchCountersMatchNeo4j(t *testing.T) {
	type counters struct{ nodes, rels, props, labels int }
	cases := []struct {
		stmt     string
		params   map[string]interface{}
		want     counters
		fastPath func(HotPathTrace) bool
	}{
		{"UNWIND [1, 2] AS i MERGE (a:X {k: i}) MERGE (b:Y {k: i}) MERGE (a)-[:R]->(b)", nil,
			counters{nodes: 4, rels: 2, props: 4, labels: 4}, func(h HotPathTrace) bool { return h.UnwindMergeChainBatch }},
		{"UNWIND [1, 2] AS i MERGE (a:X {k: i}) ON CREATE SET a.c = i MERGE (b:Y {k: i}) MERGE (a)-[r:R]->(b) SET r.w = i", nil,
			counters{nodes: 4, rels: 2, props: 8, labels: 4}, func(h HotPathTrace) bool { return h.UnwindMergeChainBatch }},
		{"UNWIND [1, 2] AS i MERGE (a:T {id: 1}) ON MATCH SET a.b = i", nil,
			counters{props: 2}, func(h HotPathTrace) bool { return h.UnwindMergeChainBatch }},
		{"UNWIND $rows AS row MATCH (t:T {id: row.t}) MATCH (m:M {k: row.m}) CREATE (x:X {i: row.i}) CREATE (t)-[:R {i: row.i}]->(x) CREATE (x)-[:S]->(m)",
			map[string]interface{}{"rows": []interface{}{
				map[string]interface{}{"t": int64(1), "m": int64(1), "i": int64(1)},
				map[string]interface{}{"t": int64(1), "m": int64(1), "i": int64(2)},
			}},
			counters{nodes: 2, rels: 4, props: 4, labels: 2}, func(h HotPathTrace) bool { return h.UnwindMultiMatchCreateBatch }},
	}
	for _, tc := range cases {
		t.Run(tc.stmt, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			// The multi-MATCH CREATE batch needs a property index per MATCH.
			for _, setup := range []string{
				"CREATE INDEX t_id FOR (n:T) ON (n.id)",
				"CREATE INDEX m_k FOR (n:M) ON (n.k)",
				"CREATE (:T {id: 1, x: 5}), (:M {k: 1})",
			} {
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
			}
			res, err := exec.Execute(ctx, tc.stmt, tc.params)
			require.NoError(t, err)
			assert.True(t, tc.fastPath(exec.LastHotPathTrace()), "expected the batch fast path")
			got := counters{res.Stats.NodesCreated, res.Stats.RelationshipsCreated, res.Stats.PropertiesSet, res.Stats.LabelsAdded}
			assert.Equal(t, tc.want, got)
		})
	}
}
