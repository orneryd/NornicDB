package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Backtick-quoted labels are read by the same label scanner on every route:
// CREATE, MERGE, MATCH (plain, fast paths, traversal), OPTIONAL MATCH, WHERE
// label tests, REMOVE and UNWIND ... MATCH. Expected rows and stored labels
// are Neo4j 5.26's.
func TestQuotedPatternLabelsOnEveryRoute(t *testing.T) {
	cases := []struct {
		setup  []string
		stmt   string
		want   [][]interface{}
		stored [][]interface{}
	}{
		{nil, "CREATE (n:`A B`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A B"}}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:T:`A B` {id: 2}) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A B", "T"}}}, [][]interface{}{{[]interface{}{"A B", "T"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:`Q``L`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"Q`L"}}}, [][]interface{}{{[]interface{}{"Q`L"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:`A:B`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A:B"}}}, [][]interface{}{{[]interface{}{"A:B"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:`A{B`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A{B"}}}, [][]interface{}{{[]interface{}{"A{B"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:`A(B`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A(B"}}}, [][]interface{}{{[]interface{}{"A(B"}}, {[]interface{}{"T"}}}},
		{nil, "CREATE (n:`it's`) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"it's"}}}, [][]interface{}{{[]interface{}{"it's"}}, {[]interface{}{"T"}}}},
		{[]string{"CREATE (:`it's` {id: 5})"}, "MATCH (n:`it's`) RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{nil, "CREATE (a:T)-[:R]->(b:`A B`) RETURN labels(b) AS l",
			[][]interface{}{{[]interface{}{"A B"}}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}, {[]interface{}{"T"}}}},
		{nil, "MERGE (n:`A B` {id: 1}) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A B"}}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}}},
		{nil, "MATCH (a:T) CREATE (b:`A B`) RETURN labels(b) AS l",
			[][]interface{}{{[]interface{}{"A B"}}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}}},
		{nil, "UNWIND [1] AS i CREATE (n:`A B` {id: i}) RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"A B"}}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}}},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (n:`A B`) RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (n:`A B`) RETURN count(n) AS c",
			[][]interface{}{{int64(1)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (n) WHERE n:`A B` RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (n) WITH n WHERE n:`A B` RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{[]string{"CREATE (:`A B`:T {id: 5})"}, "MATCH (n:T) REMOVE n:`A B` RETURN labels(n) AS l",
			[][]interface{}{{[]interface{}{"T"}}, {[]interface{}{"T"}}}, [][]interface{}{{[]interface{}{"T"}}, {[]interface{}{"T"}}}},
		{[]string{"CREATE (:`A B` {id: 5})"}, "OPTIONAL MATCH (n:`A B`) RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MERGE (n:`A B` {id: 5}) RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, [][]interface{}{{[]interface{}{"A B"}}, {[]interface{}{"T"}}}},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (a:T), (b:`A B`) MERGE (a)-[:R]->(b) RETURN b.id AS id",
			[][]interface{}{{int64(5)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})-[:R]->(:T {id: 6})"}, "MATCH (a:`A B`)-[:R]->(b) RETURN b.id AS id",
			[][]interface{}{{int64(6)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "MATCH (n:`A B` {id: 5}) SET n.x = 1 RETURN n.x AS x",
			[][]interface{}{{int64(1)}}, nil},
		{[]string{"CREATE (:`A B` {id: 5})"}, "UNWIND [5] AS i MATCH (n:`A B` {id: i}) RETURN n.id AS id",
			[][]interface{}{{int64(5)}}, nil},
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
					for _, q := range append([]string{"CREATE (:T {id: 1})"}, tc.setup...) {
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
					assert.ElementsMatch(t, tc.want, normalizeSetRouteRows(res.Rows))
					if tc.stored != nil {
						stored, err := exec.Execute(ctx, "MATCH (n) RETURN labels(n) AS l", nil)
						require.NoError(t, err)
						assert.ElementsMatch(t, tc.stored, normalizeSetRouteRows(stored.Rows), "stored labels")
					}
				})
			}
		}
	}
}
