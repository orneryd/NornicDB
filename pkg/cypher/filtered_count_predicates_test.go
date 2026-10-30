package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MATCH (n:Label) WHERE … RETURN count(n) counts what the same WHERE returns
// as rows, on every route (#624, #535): parameters in the fallback predicate
// row, constant expressions / hex literals in compiled comparisons and IN
// lists, and numbers compared by value (1 = 1.0, float parameters as HTTP
// sends them). Expected counts are Neo4j 5.26's.
func TestFilteredCountMatchesRowFilter(t *testing.T) {
	cases := []struct {
		where  string
		params map[string]interface{}
		want   int64
	}{
		{"$q = 1", map[string]interface{}{"q": int64(1)}, 4},
		{"$q = 2", map[string]interface{}{"q": int64(1)}, 0},
		{"$q = 1 AND n.id IN [1, 2]", map[string]interface{}{"q": int64(1)}, 2},
		{"n.id IN [1, 2] AND $q = 1", map[string]interface{}{"q": int64(1)}, 2},
		{"$flag", map[string]interface{}{"flag": true}, 4},
		{"$q IS NOT NULL AND n.id < 3", map[string]interface{}{"q": int64(1)}, 2},
		{"n.id IN [1.0, 2.0]", nil, 2},
		{"n.id IN $p", map[string]interface{}{"p": []interface{}{int64(1), int64(2)}}, 2},
		{"n.id IN $p", map[string]interface{}{"p": []interface{}{float64(1), float64(2)}}, 2},
		{"n.name IN ['a' + 'b']", nil, 1},
		{"n.v IN [8 * 2]", nil, 1},
		{"n.v IN [0x10]", nil, 1},
		{"n.v = 0x10", nil, 1},
		{"n.name = 'a' + 'b'", nil, 1},
		{"n.name STARTS WITH 'a' + ''", nil, 2},
		{"n.id = 1.0", nil, 1},
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
				t.Run(stack+"/"+mode+"/"+tc.where, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:T {id: 1, name: 'a', ok: true}), (:T {id: 2, name: 'b', ok: false}), (:T {id: 3, name: 'c'}), (:T {name: 'ab', v: 16})", nil)
					require.NoError(t, err)
					if mode == "explicit transaction" {
						_, err = exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) }()
					}
					count, err := exec.Execute(ctx, "MATCH (n:T) WHERE "+tc.where+" RETURN count(n) AS c", tc.params)
					require.NoError(t, err)
					assert.Equal(t, [][]interface{}{{tc.want}}, count.Rows, "count")
					rows, err := exec.Execute(ctx, "MATCH (n:T) WHERE "+tc.where+" RETURN n.name AS name", tc.params)
					require.NoError(t, err)
					assert.Len(t, rows.Rows, int(tc.want), "rows")
				})
			}
		}
	}
}
