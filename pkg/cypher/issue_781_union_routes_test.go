package cypher

// Regression coverage for NornicDB #781: on the server's storage stack, an
// auto-commit node-only CREATE takes the async fast path, which ran before the
// top-level UNION routing. A UNION whose first branch is a CREATE then ran
// that branch for the whole statement: mismatched columns wrote the node and
// returned the rest of the text as a column name, and CREATE … FINISH UNION
// branches wrote an extra unlabeled node. Expected results are Neo4j 5.26.30's.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIssue781UnionRoutesOnEveryStack(t *testing.T) {
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"async stack": newAsyncStackTestExecutor,
	}
	cases := []struct {
		query   string
		wantErr string
		rows    [][]interface{}
		graph   [][]interface{}
	}{
		{query: "CREATE (:U) RETURN 1 AS x UNION RETURN 2 AS y", wantErr: "UNION queries must return the same columns"},
		{query: "CREATE (:U) RETURN 1 AS x UNION ALL RETURN 2 AS y", wantErr: "UNION queries must return the same columns"},
		{query: "CREATE (:U) FINISH UNION ALL CREATE (:V) FINISH", graph: [][]interface{}{{"U", int64(1)}, {"V", int64(1)}}},
		{query: "CREATE (:U) FINISH UNION CREATE (:V) FINISH", graph: [][]interface{}{{"U", int64(1)}, {"V", int64(1)}}},
		{query: "CREATE (:U) RETURN 1 AS x UNION ALL CREATE (:V) RETURN 2 AS x", rows: [][]interface{}{{int64(1)}, {int64(2)}}, graph: [][]interface{}{{"U", int64(1)}, {"V", int64(1)}}},
		{query: "CREATE (:U) FINISH", graph: [][]interface{}{{"U", int64(1)}}},
		{query: "CREATE (:U {n: 1}) RETURN 1 AS x", rows: [][]interface{}{{int64(1)}}, graph: [][]interface{}{{"U", int64(1)}}},
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			t.Run(stack+"/"+mode, func(t *testing.T) {
				exec := build(t)
				ctx := context.Background()
				for _, tc := range cases {
					_, err := exec.Execute(ctx, "MATCH (n) DETACH DELETE n", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					result, err := exec.Execute(ctx, tc.query, nil)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, commitErr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, commitErr, tc.query)
						}
					}
					if tc.wantErr != "" {
						require.ErrorContains(t, err, tc.wantErr, tc.query)
					} else {
						require.NoError(t, err, tc.query)
						if tc.rows != nil {
							require.ElementsMatch(t, tc.rows, result.Rows, tc.query)
						}
					}
					graph, err := exec.Execute(ctx, "MATCH (n) RETURN labels(n)[0] AS l, count(*) AS c ORDER BY l", nil)
					require.NoError(t, err)
					want := tc.graph
					if want == nil {
						want = [][]interface{}{}
					}
					require.Equal(t, want, graph.Rows, tc.query)
				}
			})
		}
	}
}
