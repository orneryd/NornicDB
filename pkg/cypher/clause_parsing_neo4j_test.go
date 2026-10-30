package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Clause parsing matches Neo4j 5.26.30 for UNION branches with SKIP / LIMIT
// (#571), backtick-quoted names and word operators in ORDER BY (#573), and a
// WHERE after WITH … ORDER BY.
func TestClauseParsingMatchesNeo4j(t *testing.T) {
	cases := []struct {
		stmt    string
		columns []string
		rows    [][]interface{}
	}{
		{"MATCH (n:P) RETURN n.id AS id ORDER BY id LIMIT 2 UNION ALL MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP 4",
			[]string{"id"}, [][]interface{}{{int64(1)}, {int64(2)}, {int64(5)}}},
		{"MATCH (n:P) RETURN n.id AS id ORDER BY id LIMIT 1 + 1 UNION ALL MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP 4",
			[]string{"id"}, [][]interface{}{{int64(1)}, {int64(2)}, {int64(5)}}},
		{"MATCH (n:P) RETURN n.id AS id ORDER BY id SKIP 1 LIMIT 1 UNION ALL MATCH (n:P) RETURN n.id AS id ORDER BY id DESC SKIP 1 LIMIT 1 UNION ALL MATCH (n:T) RETURN n.id AS id ORDER BY id LIMIT 1",
			[]string{"id"}, [][]interface{}{{int64(2)}, {int64(4)}, {int64(1)}}},
		{"RETURN 1 AS x LIMIT 1 UNION RETURN 2 AS x", []string{"x"}, [][]interface{}{{int64(1)}, {int64(2)}}},
		{"MATCH (n:P) RETURN DISTINCT n.g AS `the g` ORDER BY `the g`", []string{"the g"}, [][]interface{}{{int64(0)}, {int64(1)}}},
		{"MATCH (n:P) WITH DISTINCT n.g AS `the g` ORDER BY `the g` RETURN collect(`the g`) AS l", []string{"l"}, [][]interface{}{{[]interface{}{int64(0), int64(1)}}}},
		{"UNWIND [1] AS x WITH x AS `x y` WITH `x y` RETURN `x y` AS v", []string{"v"}, [][]interface{}{{int64(1)}}},
		{"MATCH (n:T) WITH n.name AS `x` WITH `x` RETURN `x` ORDER BY `x`", []string{"x"}, [][]interface{}{{"Ann"}, {"Tom"}}},
		{"UNWIND [1, 2] AS `x` RETURN `x` ORDER BY `x` DESC", []string{"x"}, [][]interface{}{{int64(2)}, {int64(1)}}},
		{"MATCH (n:T) WITH n ORDER BY n.name STARTS WITH 'T' RETURN n.id AS id", []string{"id"}, [][]interface{}{{int64(2)}, {int64(1)}}},
		{"MATCH (n:T) WITH n ORDER BY n.name IS NULL, n.id DESC RETURN n.id AS id", []string{"id"}, [][]interface{}{{int64(2)}, {int64(1)}}},
		{"MATCH (n:T) WITH n ORDER BY size(n.name) > 2 AND n.name CONTAINS 'o' DESC RETURN n.id AS id", []string{"id"}, [][]interface{}{{int64(1)}, {int64(2)}}},
		{"MATCH (n:T) RETURN DISTINCT n.id AS id ORDER BY id IS NULL, id DESC", []string{"id"}, [][]interface{}{{int64(2)}, {int64(1)}}},
		{"MATCH (n:T) WITH n ORDER BY n.name STARTS WITH 'T' WHERE n.id > 0 RETURN n.id AS id", []string{"id"}, [][]interface{}{{int64(2)}, {int64(1)}}},
		{"MATCH (n:T) WITH n, n.name AS nm ORDER BY nm CONTAINS 'n' WHERE n.id > 0 RETURN n.id AS id", []string{"id"}, [][]interface{}{{int64(1)}, {int64(2)}}},
	}
	for _, tc := range cases {
		t.Run(tc.stmt, func(t *testing.T) {
			exec, _ := newTestExecutor(t)
			ctx := context.Background()
			for _, setup := range []string{
				"UNWIND range(1, 5) AS i CREATE (:P {id: i, g: i % 2})",
				"CREATE (:T {id: 1, name: 'Tom'}), (:T {id: 2, name: 'Ann'})",
			} {
				_, err := exec.Execute(ctx, setup, nil)
				require.NoError(t, err)
			}
			// Repeated: a sort that doesn't apply leaves the order unspecified.
			for run := 0; run < 5; run++ {
				res, err := exec.Execute(ctx, tc.stmt, nil)
				require.NoError(t, err)
				require.Equal(t, tc.columns, res.Columns)
				require.Equal(t, tc.rows, res.Rows)
			}
		})
	}
}

// TestBacktickQuotedPatternVariablesAreBound: a backtick-quoted node,
// relationship or path variable bound by MATCH is in scope for later
// clauses, as a plain one is (Neo4j's results).
func TestBacktickQuotedPatternVariablesAreBound(t *testing.T) {
	exec, _ := newTestExecutor(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:BQ {name: 'a', v: 5})-[:R {w: 1}]->(:BQ {name: 'b', v: 7})", nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		query   string
		columns []string
		rows    [][]interface{}
	}{
		{"MATCH (`n n`:BQ) RETURN `n n`.name AS v ORDER BY v", []string{"v"}, [][]interface{}{{"a"}, {"b"}}},
		{"MATCH (`n n`:BQ) WHERE `n n`.v = 5 RETURN count(*) AS c", []string{"c"}, [][]interface{}{{int64(1)}}},
		{"MATCH (`n n`:BQ)-[`r r`:R]->(`m m`) RETURN `r r`.w AS w, `m m`.name AS b", []string{"w", "b"}, [][]interface{}{{int64(1), "b"}}},
		{"MATCH `p p` = (:BQ)-[:R]->(:BQ) RETURN length(`p p`) AS l", []string{"l"}, [][]interface{}{{int64(1)}}},
		{"MATCH (`n n`:BQ) WITH `n n` WHERE `n n`.v > 6 RETURN `n n`.name AS v", []string{"v"}, [][]interface{}{{"b"}}},
	} {
		result, err := exec.Execute(ctx, tc.query, nil)
		require.NoError(t, err, tc.query)
		require.Equal(t, tc.columns, result.Columns, tc.query)
		require.Equal(t, tc.rows, result.Rows, tc.query)
	}
	result, err := exec.Execute(ctx, "MATCH (`n n`:BQ {name: 'a'}) RETURN `n n`", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"n n"}, result.Columns)
	require.Len(t, result.Rows, 1)
}
