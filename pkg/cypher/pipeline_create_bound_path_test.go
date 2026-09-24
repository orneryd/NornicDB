package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A pipeline CREATE (or MERGE) whose pattern reuses a bound variable and names
// a path writes the pattern once and binds the path for later clauses;
// nodes() / relationships() read it like any other path. Expected rows and
// graph sizes are Neo4j 5.26's.
func TestPipelineCreateBoundPathWritesOnce(t *testing.T) {
	cases := []struct {
		seed  string
		stmt  string
		want  [][]interface{}
		nodes int64
		rels  int64
	}{
		{"CREATE (:T {id: 1})", "MATCH (a:T) CREATE p = (a)-[:R]->(b:U) RETURN length(p) AS l, nodes(p)[0].id AS s, labels(nodes(p)[1]) AS e",
			[][]interface{}{{int64(1), int64(1), []interface{}{"U"}}}, 2, 1},
		{"CREATE (:T {id: 1})", "MATCH (a:T), (c:T) CREATE p = (a)-[:R]->(c) RETURN length(p) AS l",
			[][]interface{}{{int64(1)}}, 1, 1},
		{"CREATE (:T {id: 1})", "MATCH (a:T) WITH a CREATE p = (a)-[:R]->(:U) RETURN length(p) AS l",
			[][]interface{}{{int64(1)}}, 2, 1},
		{"CREATE (:T {id: 1}), (:T {id: 2})", "MATCH (a:T) CREATE p = (a)-[:R]->(:U) RETURN a.id AS id, length(p) AS l ORDER BY id",
			[][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}, 4, 2},
		{"CREATE (:T {id: 1})", "MATCH (a:T) CREATE p = (a)-[:R]->(b:U)-[:S]->(c:V) RETURN length(p) AS l, size(relationships(p)) AS r",
			[][]interface{}{{int64(2), int64(2)}}, 3, 2},
		{"CREATE (:T {id: 1})", "MATCH (a:T) CREATE p = (a)-[:R]->(b:U) SET b.x = 1 RETURN length(p) AS l, b.x AS x",
			[][]interface{}{{int64(1), int64(1)}}, 2, 1},
		{"CREATE (:T {id: 1})", "UNWIND [1, 2] AS i MATCH (a:T) CREATE p = (a)-[:R {i: i}]->(:U) RETURN i, length(p) AS l ORDER BY i",
			[][]interface{}{{int64(1), int64(1)}, {int64(2), int64(1)}}, 3, 2},
		{"CREATE (:T {id: 1})", "MATCH (a:T) CREATE p = (a)-[:R]->(b:U) RETURN type(relationships(p)[0]) AS t, size(nodes(p)) AS n",
			[][]interface{}{{"R", int64(2)}}, 2, 1},
		{"CREATE (:T {id: 1})", "MATCH (a:T) MERGE p = (a)-[:R]->(b:U) RETURN size(relationships(p)) AS r, type(relationships(p)[0]) AS t",
			[][]interface{}{{int64(1), "R"}}, 2, 1},
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
					_, err := exec.Execute(ctx, tc.seed, nil)
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
					assert.Equal(t, tc.want, normalizeSetRouteRows(res.Rows))
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
