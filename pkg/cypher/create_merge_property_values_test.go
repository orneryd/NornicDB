package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CREATE and MERGE store property values under the same rule as SET: a map
// or a nested list is a TypeError and nothing is written, on every route
// including the UNWIND batch creators (#583, following b968c6a2). A map in a
// MATCH pattern is still a valid predicate. Expected results are Neo4j 5.26's.
func TestCreateMergeRejectNonPropertyValues(t *testing.T) {
	rows := func(row map[string]interface{}) map[string]interface{} {
		return map[string]interface{}{"rows": []interface{}{row}}
	}
	cases := []struct {
		stmt   string
		params map[string]interface{}
		want   [][]interface{} // nil: TypeError
		nodes  int64
		rels   int64
	}{
		{stmt: "CREATE (n:T {m: {a: 1}}) RETURN n.m AS m", nodes: 2},
		{stmt: "CREATE (n:T {l: [[1], [2]]}) RETURN n.l AS l", nodes: 2},
		{stmt: "CREATE (n:T {m: $p}) RETURN n.m AS m", params: map[string]interface{}{"p": map[string]interface{}{"a": int64(1)}}, nodes: 2},
		{stmt: "MERGE (n:T {id: 1, m: {a: 1}}) RETURN n.m AS m", nodes: 2},
		{stmt: "UNWIND [{a: 1}] AS row CREATE (n:T {m: row}) RETURN n.m AS m", nodes: 2},
		{stmt: "CREATE (:T)-[r:R {m: {a: 1}}]->(:T) RETURN r.m AS m", nodes: 2},
		{stmt: "CREATE (n:T) SET n.m = {a: 1} RETURN n.m AS m", nodes: 2},
		{stmt: "UNWIND $rows AS row MERGE (n:T {id: row.id}) SET n.m = row.m RETURN count(n) AS c",
			params: rows(map[string]interface{}{"id": int64(1), "m": map[string]interface{}{"a": int64(1)}}), nodes: 2},
		{stmt: "UNWIND $rows AS row MERGE (n:T {id: row.id, m: row.m}) RETURN count(n) AS c",
			params: rows(map[string]interface{}{"id": int64(1), "m": map[string]interface{}{"a": int64(1)}}), nodes: 2},
		{stmt: "UNWIND $rows AS row MATCH (a:A {id: row.a}) CREATE (a)-[:R {m: row.m}]->(:B {m: row.m}) RETURN count(*) AS c",
			params: rows(map[string]interface{}{"a": int64(1), "m": map[string]interface{}{"a": int64(1)}}), nodes: 2},
		{stmt: "UNWIND $rows AS row MATCH (a:A {id: row.a}), (b:B2 {id: row.b}) MERGE (a)-[r:R {k: row.k}]->(b) SET r.m = row.m RETURN count(r) AS c",
			params: rows(map[string]interface{}{"a": int64(1), "b": int64(2), "k": int64(1), "m": map[string]interface{}{"a": int64(1)}}), nodes: 2},
		{stmt: "MATCH (a:A), (b:B2) MERGE (a)-[r:R {m: {a: 1}}]->(b) RETURN count(r) AS c", nodes: 2},
		{stmt: "MATCH (n:T {m: {a: 1}}) RETURN count(n) AS c", want: [][]interface{}{{int64(0)}}, nodes: 2},
		{stmt: "CREATE (n:T {l: [1, 2], s: ['a']}) RETURN n.l AS l", want: [][]interface{}{{[]interface{}{int64(1), int64(2)}}}, nodes: 3},
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
					_, err := exec.Execute(ctx, "CREATE (:A {id: 1}), (:B2 {id: 2})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, tc.params)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, cerr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, cerr)
						}
					}
					if tc.want == nil {
						require.Error(t, err)
						assert.Contains(t, err.Error(), "TypeError")
					} else {
						require.NoError(t, err)
						assert.Equal(t, tc.want, res.Rows)
					}
					nodes, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
					require.NoError(t, err)
					rels, err := exec.Execute(ctx, "MATCH ()-[r]->() RETURN count(r) AS c", nil)
					require.NoError(t, err)
					assert.Equal(t, tc.nodes, nodes.Rows[0][0], "nodes")
					assert.Equal(t, tc.rels, rels.Rows[0][0], "relationships")
				})
			}
		}
	}
}
