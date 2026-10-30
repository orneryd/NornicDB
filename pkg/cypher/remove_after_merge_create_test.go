package cypher

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// REMOVE after MERGE (with SET, ON CREATE SET, ON MATCH SET, before SET) and
// after CREATE runs as a row clause, as after MATCH (#585), with MERGE actions
// applied by the same row operators. Expected rows,
// stored node labels / keys and relationship keys are Neo4j 5.26's.
func TestRemoveAfterMergeAndCreate(t *testing.T) {
	cases := []struct {
		setup  []string
		stmt   string
		want   [][]interface{}
		stored []string
		rels   []string
	}{
		{nil, "MERGE (n:T {id: 1}) SET n.x = 1 REMOVE n.a RETURN n.c AS c", [][]interface{}{{int64(3)}}, []string{"[T] [c id x]"}, nil},
		{nil, "MERGE (n:T {id: 1}) ON CREATE SET n.x = 1 REMOVE n.a RETURN n.c AS c", [][]interface{}{{int64(3)}}, []string{"[T] [c id]"}, nil},
		{nil, "MERGE (n:T {id: 9}) ON CREATE SET n.x = 1 REMOVE n.x RETURN n.id AS id", [][]interface{}{{int64(9)}}, []string{"[T] [a c id]", "[T] [id]"}, nil},
		{nil, "MERGE (n:T {id: 1}) ON MATCH SET n.x = 1 REMOVE n.a RETURN n.c AS c", [][]interface{}{{int64(3)}}, []string{"[T] [c id x]"}, nil},
		{nil, "MERGE (n:T {id: 1}) REMOVE n.a SET n.x = 1 RETURN n.c AS c", [][]interface{}{{int64(3)}}, []string{"[T] [c id x]"}, nil},
		{nil, "MATCH (m:T {id: 1}) MERGE (n:U {id: 1}) ON CREATE SET n.x = 1 REMOVE n.x RETURN n.id AS id", [][]interface{}{{int64(1)}}, []string{"[T] [a c id]", "[U] [id]"}, nil},
		{nil, "CREATE (n:U {a: 1, b: 2}) REMOVE n.a RETURN n.b AS b", [][]interface{}{{int64(2)}}, []string{"[T] [a c id]", "[U] [b]"}, nil},
		{nil, "CREATE (n:U {a: 1}) SET n.b = 2 REMOVE n.a RETURN n.b AS b", [][]interface{}{{int64(2)}}, []string{"[T] [a c id]", "[U] [b]"}, nil},
		{[]string{"CREATE (:M {k: 1, a: 1}), (:M {k: 1, a: 2})"}, "MERGE (n:M {k: 1}) ON MATCH SET n.hit = true REMOVE n.a RETURN count(n) AS c",
			[][]interface{}{{int64(2)}}, []string{"[M] [hit k]", "[M] [hit k]", "[T] [a c id]"}, nil},
		{[]string{"CREATE (:A {id: 1})-[:R {w: 1, z: 9}]->(:B {id: 2})"}, "MATCH (a:A), (b:B) MERGE (a)-[r:R]->(b) ON MATCH SET r.m = 1 REMOVE r.z RETURN r.w AS w",
			[][]interface{}{{int64(1)}}, []string{"[A] [id]", "[B] [id]", "[T] [a c id]"}, []string{"[m w]"}},
		{nil, "MERGE (n:T {id: 1}) ON CREATE SET n.new = 1 ON MATCH SET n.old = 1 REMOVE n.c RETURN n.old AS o",
			[][]interface{}{{int64(1)}}, []string{"[T] [a id old]"}, nil},
		{nil, "UNWIND [1, 2] AS i MERGE (n:W {id: i}) ON CREATE SET n.x = i SET n.y = 1 REMOVE n.x RETURN count(n) AS c",
			[][]interface{}{{int64(2)}}, []string{"[T] [a c id]", "[W] [id y]", "[W] [id y]"}, nil},
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
					for _, q := range append([]string{"CREATE (:T {id: 1, a: 1, c: 3})"}, tc.setup...) {
						_, err := exec.Execute(ctx, q, nil)
						require.NoError(t, err, q)
					}
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, tc.stmt, nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "COMMIT", nil)
						require.NoError(t, err)
					}
					assert.Equal(t, tc.want, res.Rows)
					assert.Equal(t, tc.stored, storedLabelsAndKeys(t, exec, "MATCH (n) RETURN labels(n) AS l, keys(n) AS k"), "stored nodes")
					assert.Equal(t, tc.rels, storedLabelsAndKeys(t, exec, "MATCH ()-[r]->() RETURN keys(r) AS k"), "stored relationships")
				})
			}
		}
	}
}

// storedLabelsAndKeys renders each row of a labels / keys query as sorted
// "[labels] [keys]" text, sorted, for order-independent comparison.
func storedLabelsAndKeys(t *testing.T, exec *StorageExecutor, query string) []string {
	t.Helper()
	res, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err)
	var out []string
	for _, row := range res.Rows {
		parts := make([]string, 0, len(row))
		for _, value := range row {
			items := make([]string, 0)
			switch list := value.(type) {
			case []interface{}:
				for _, item := range list {
					items = append(items, fmt.Sprint(item))
				}
			case []string:
				items = append(items, list...)
			}
			sort.Strings(items)
			parts = append(parts, fmt.Sprint(items))
		}
		out = append(out, strings.Join(parts, " "))
	}
	sort.Strings(out)
	return out
}
