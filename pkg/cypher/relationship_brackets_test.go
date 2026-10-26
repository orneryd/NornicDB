package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A '[' in a node's property map (a list, or a string containing '[') is not
// a relationship bracket on any route: CREATE / MERGE shape checks, variable
// scoping, the MERGE relationship parser and OPTIONAL MATCH endpoints (#617).
// Expected rows and graph sizes are Neo4j 5.26's.
func TestRelationshipBracketsIgnoreNodeMapBrackets(t *testing.T) {
	const seed = "CREATE (:A {id: 1, tags: ['x'], s: 'a<-b'})-[:R {w: 1}]->(:B {id: 2})"
	cases := []struct {
		seed  string
		stmt  string
		want  [][]interface{}
		nodes int64
		rels  int64
	}{
		{"CREATE (:X)", "CREATE (a:T {n: [1, 2]})-[:R]->(b:U) RETURN a.n AS n", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}, 3, 1},
		{"CREATE (:X)", "CREATE (a:T {n: ['x']})<-[:R]-(b:U) RETURN a.n AS n", [][]interface{}{{[]interface{}{"x"}}}, 3, 1},
		{"CREATE (:X)", "CREATE (a:T {n: [1]})-[:R]->(b:U)-[:S]->(c:V) RETURN a.n AS n", [][]interface{}{{[]interface{}{int64(1)}}}, 4, 2},
		{"CREATE (:X)", "MATCH (x:X) CREATE (a:T {n: [1, 2]})-[:R]->(x) RETURN a.n AS n", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}, 2, 1},
		{"CREATE (:X)", "MERGE (a:T {n: [1, 2]})-[:R]->(b:U) RETURN a.n AS n", [][]interface{}{{[]interface{}{int64(1), int64(2)}}}, 3, 1},
		{"CREATE (:X)", "UNWIND [1] AS i CREATE (a:T {n: [i]})-[:R]->(b:U) RETURN a.n AS n", [][]interface{}{{[]interface{}{int64(1)}}}, 3, 1},
		{"CREATE (:X)", "CREATE (a:T {s: 'x[1]'})-[:R]->(b:U) RETURN a.s AS s", [][]interface{}{{"x[1]"}}, 3, 1},
		{seed, "MATCH (a:A) OPTIONAL MATCH (a {tags: ['x']})-[r:R]->(b) RETURN b.id AS id, r.w AS w", [][]interface{}{{int64(2), int64(1)}}, 2, 1},
		{seed, "MATCH (a:A) OPTIONAL MATCH (a {s: 'a<-b'})-[r:R]->(b) RETURN b.id AS id", [][]interface{}{{int64(2)}}, 2, 1},
		{seed, "MATCH (b:B) OPTIONAL MATCH (b)<-[r:R]-(a {tags: ['x']}) RETURN a.id AS id", [][]interface{}{{int64(1)}}, 2, 1},
		{seed, "MATCH (a:A) OPTIONAL MATCH (a)-->(b) RETURN b.id AS id", [][]interface{}{{int64(2)}}, 2, 1},
		{seed, "MATCH (a:A) OPTIONAL MATCH (a)<--(b) RETURN b.id AS id", [][]interface{}{{nil}}, 2, 1},
		{seed, "MATCH (a {tags: ['x']})-[r:R]->(b) RETURN b.id AS id", [][]interface{}{{int64(2)}}, 2, 1},
		{seed, "MATCH (b:B) MERGE (a:A {tags: ['y']})-[:R]->(b) RETURN a.tags AS t", [][]interface{}{{[]interface{}{"y"}}}, 3, 2},
		{seed, "MERGE (a:A {tags: ['y']}) RETURN a.tags AS t", [][]interface{}{{[]interface{}{"y"}}}, 3, 1},
		{seed, "UNWIND [['y']] AS t MERGE (a:A {tags: t}) RETURN a.tags AS t", [][]interface{}{{[]interface{}{"y"}}}, 3, 1},
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
					assert.Equal(t, tc.want, res.Rows)
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

func BenchmarkRelationshipBracketLookup(b *testing.B) {
	for _, text := range []string{
		"(a)-[r:T]->(b)",
		"(a {items: [1,2], text: 'a-[not relationship]'})-[`r``]x`:T]->(b)",
		strings.Repeat("(a {items: [1,2]})-->(b), ", 64) + "(x)-[r:T]->(y)",
	} {
		b.Run(text[:13], func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				open, close := readRelationshipBracket(originalBraceView(text), 0, requireClosedRelationship)
				if open < 0 || close <= open {
					b.Fatal("expected a closed relationship bracket")
				}
			}
		})
	}
}

func TestRelationshipBracketLookupPolicies(t *testing.T) {
	for _, tc := range []struct {
		text              string
		from, open, close int
	}{
		{"(a)-->(b)", 0, -1, -1},
		{"(a)-[r:T]->(b)", 0, 4, 8},
		{"(a)-[r:T]->(b)-[s:S]->(c)", 10, 15, 19},
		{"(a)-[r:T->(b)", 0, 4, -1},
		{"(a)-[`r ]x`]->(b)", 0, 4, 11},
		{"(a)-[`r``]x`]->(b)", 0, 4, 12},
		{"(a {ids: [1,2][i]})", 0, -1, -1},
		{"(a {k: COUNT { (x)-[r]->() }})", 0, -1, -1},
		{"(a) /* -[r]-> */ -->(b)", 0, -1, -1},
	} {
		for _, policy := range []relationshipBracketPolicy{requireClosedRelationship, reportUnclosedRelationship} {
			open, close := readRelationshipBracket(originalBraceView(tc.text), tc.from, policy)
			wantOpen := tc.open
			if tc.close < 0 && policy == requireClosedRelationship {
				wantOpen = -1
			}
			require.Equal(t, []int{wantOpen, tc.close}, []int{open, close}, "%q policy=%d", tc.text, policy)
		}
	}
}
