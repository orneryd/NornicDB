package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every keyword scanner treats the WITH of STARTS WITH / ENDS WITH as part of
// the operator and a WITH after a variable named starts / ends as a clause
// (#596).
func TestKeywordScannersSkipOperatorWith(t *testing.T) {
	cases := []struct {
		query string
		want  int // index of the WITH clause, -1 if none
	}{
		{"CALL db.labels() YIELD label WHERE label STARTS WITH 'T' RETURN label", -1},
		{"MATCH (n) WHERE n.name ENDS  WITH 'x' WITH n RETURN n", 38},
		{"MATCH (n) WHERE n.name starts\nwith 'x' RETURN n", -1},
		{"MATCH (n) WHERE f(n) STARTS WITH 'x' RETURN n", -1},
		{"UNWIND [1] AS starts WITH starts RETURN starts", 21},
		{"UNWIND [1] AS ends WITH ends RETURN ends", 19},
		{"MATCH (starts) WITH starts RETURN starts", 15},
		{"MATCH (n) WITH n, n.x AS a WHERE n.starts WITH n RETURN n", 10},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, findKeywordIndex(tc.query, "WITH"), "findKeywordIndex %q", tc.query)
		assert.Equal(t, tc.want, findKeywordIndexInContext(tc.query, "WITH"), "findKeywordIndexInContext %q", tc.query)
		positions := findAllTopLevelPipelineKeywordPositions(tc.query, "WITH")
		first := -1
		if len(positions) > 0 {
			first = positions[0]
		}
		assert.Equal(t, tc.want, first, "findAllTopLevelPipelineKeywordPositions %q", tc.query)
	}
	// A later clause WITH after a property key named starts is still found.
	query := "MATCH (n) WITH n, n.x AS a WHERE n.starts WITH n RETURN n"
	assert.Equal(t, 42, findKeywordIndexFromAfter(query, "WITH", 11))
}

func findKeywordIndexFromAfter(s, keyword string, from int) int {
	return keywordIndexFrom(s, keyword, from, defaultKeywordScanOpts())
}

func TestYieldWhereStringOperators(t *testing.T) {
	setup := []string{"CREATE (:T {id: 1}), (:V {id: 2})"}
	cases := []struct {
		name string
		stmt string
		want [][]interface{}
	}{
		{name: "STARTS WITH", stmt: "CALL db.labels() YIELD label WHERE label STARTS WITH 'T' RETURN label", want: [][]interface{}{{"T"}}},
		{name: "ENDS WITH", stmt: "CALL db.labels() YIELD label WHERE label ENDS WITH 'V' RETURN label", want: [][]interface{}{{"V"}}},
		{name: "AND STARTS WITH", stmt: "CALL db.labels() YIELD label WHERE size(label) = 1 AND label STARTS WITH 'V' RETURN label", want: [][]interface{}{{"V"}}},
		{name: "STARTS WITH then WITH clause", stmt: "CALL db.labels() YIELD label WHERE label STARTS WITH 'T' WITH label AS l RETURN l", want: [][]interface{}{{"T"}}},
		{name: "YIELD alias named starts", stmt: "CALL db.labels() YIELD label AS starts WITH starts WHERE starts ENDS WITH 'T' RETURN starts", want: [][]interface{}{{"T"}}},
		{name: "UNWIND variable named starts", stmt: "UNWIND ['a', 'b'] AS starts WITH starts WHERE starts STARTS WITH 'a' RETURN starts", want: [][]interface{}{{"a"}}},
		{name: "UNWIND variable named ends", stmt: "UNWIND [1] AS ends WITH ends RETURN ends", want: [][]interface{}{{int64(1)}}},
	}
	stacks := map[string]func(t *testing.T) *StorageExecutor{
		"memory": func(t *testing.T) *StorageExecutor {
			exec, _ := newTestExecutor(t)
			return exec
		},
		"async stack": newAsyncStackTestExecutor,
	}
	for stack, build := range stacks {
		for _, mode := range []string{"auto-commit", "explicit transaction"} {
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.name, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					for _, q := range setup {
						_, err := exec.Execute(ctx, q, nil)
						require.NoError(t, err)
					}
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
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
					require.NoError(t, err, tc.stmt)
					assert.Equal(t, tc.want, res.Rows, tc.stmt)
				})
			}
		}
	}
}
