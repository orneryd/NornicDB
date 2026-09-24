package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An expression that cannot be evaluated is a syntax error and never becomes
// its own text as a value (#514), and a CREATE property value may not read a
// node or relationship created by the same path pattern. Expected results are
// Neo4j 5.26's; failed statements leave the graph unchanged.
func TestUnparseableExpressionsAreSyntaxErrors(t *testing.T) {
	cases := []struct {
		stmt    string
		wantErr string
		want    [][]interface{}
		nodes   int64
	}{
		{stmt: "CREATE (a:T {name:'x'})-[:R]->(b:U {name: a.name + '!'}) RETURN b.name", wantErr: "The Node variable 'a' is referencing a Node that is created in the same CREATE clause", nodes: 1},
		{stmt: "CREATE (a:T)-[r:R {x: 1}]->(b:U {y: r.x})", wantErr: "The Relationship variable 'r' is referencing a Relationship that is created in the same CREATE clause", nodes: 1},
		{stmt: "MATCH (a:T) CREATE (a)-[:R]->(b:U)-[:S]->(c {n: b.x})", wantErr: "The Node variable 'b' is referencing a Node", nodes: 1},
		{stmt: "CREATE (a:T {name: a.name})", wantErr: "The Node variable 'a' is referencing a Node", nodes: 1},
		{stmt: "CREATE (a:T)-[:R]->(b:U {m: reduce(s = 0, a IN range(1, 2) | s + a)}) RETURN b.m AS m", want: [][]interface{}{{int64(3)}}, nodes: 3},
		{stmt: "CREATE (a:T {name:'x'}), (b:U {name: a.name + '!'}) RETURN b.name AS n", want: [][]interface{}{{"x!"}}, nodes: 3},
		{stmt: "CREATE (a:T {name:'x'}), (a)-[:R]->(b:U {name: a.name}) RETURN b.name AS n", want: [][]interface{}{{"x"}}, nodes: 3},
		{stmt: "MATCH (a:T) CREATE (a)-[:R]->(b:U {name: a.name}) RETURN b.name AS n", want: [][]interface{}{{"t"}}, nodes: 2},
		{stmt: "MATCH (n:T) RETURN n.name 'x'", nodes: 1},
		{stmt: "MATCH (n:T) RETURN (n.name =~ 'T.*')'T.*'", nodes: 1},
		{stmt: "MATCH (n:T) RETURN n.name 5", nodes: 1},
		{stmt: "MATCH (n:T) RETURN n.name n.id", nodes: 1},
		{stmt: "MATCH (n:T) RETURN 5 'x'", nodes: 1},
		{stmt: "MATCH (n:T) RETURN [1] 'x'", nodes: 1},
		{stmt: "MATCH (n:T) RETURN toUpper(n.name) 'x'", nodes: 1},
		{stmt: "MATCH (n:T) RETURN 'a' 'b'", nodes: 1},
		{stmt: "RETURN 1 2", nodes: 1},
		{stmt: "MATCH (n:T) WITH n.name 'x' AS y RETURN y", nodes: 1},
		{stmt: "MATCH (n:T) RETURN n.name AS x ORDER BY n.name 'x'", nodes: 1},
		{stmt: "MATCH (n:T) WHERE n.name 'x' RETURN n.name", nodes: 1},
		{stmt: "MATCH (n:T) WHERE n.name = 't' 'x' RETURN n.name", nodes: 1},
		{stmt: "MATCH (n:T) SET n.x = n.name 'x' RETURN n.x", nodes: 1},
		{stmt: "MATCH (n:T) RETURN n.name CONTAINS 't' AS c, n.name STARTS WITH 't' AS s, CASE WHEN n.name = 't' THEN 'a' ELSE 'b' END AS k", want: [][]interface{}{{true, true, "a"}}, nodes: 1},
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
					_, err := exec.Execute(ctx, "CREATE (:T {name: 't'})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, nil)
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
						assert.Contains(t, err.Error(), tc.wantErr)
						assert.Contains(t, err.Error(), "SyntaxError")
					} else {
						require.NoError(t, err)
						assert.Equal(t, tc.want, res.Rows)
					}
					count, err := exec.Execute(ctx, "MATCH (n) RETURN count(n) AS c", nil)
					require.NoError(t, err)
					assert.Equal(t, tc.nodes, count.Rows[0][0], "nodes")
				})
			}
		}
	}
}
