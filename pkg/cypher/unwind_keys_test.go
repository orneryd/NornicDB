package cypher

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// keys() works in a statement that starts with UNWIND, on every UNWIND route:
// the row pipeline, map rows, SET / CREATE / WHERE expressions, nested UNWIND
// and the MERGE batch shapes, and the characters "keys(" in a string literal
// are just text (#587). Expected values are Neo4j 5's; key lists are compared
// sorted because key order is unspecified.
func TestUnwindStatementsEvaluateKeys(t *testing.T) {
	setup := []string{"CREATE (:T {id: 1, a: 1, c: 3})"}
	rows := map[string]interface{}{"rows": []interface{}{
		map[string]interface{}{"id": "m1", "a": int64(1), "b": int64(2)},
	}}
	cases := []struct {
		name   string
		stmt   string
		params map[string]interface{}
		want   [][]interface{}
		check  string
		wantCk [][]interface{}
	}{
		{name: "match row keys", stmt: "UNWIND [1] AS i MATCH (n:T {id: i}) RETURN keys(n) AS k",
			want: [][]interface{}{{[]interface{}{"a", "c", "id"}}}},
		{name: "map row keys", stmt: "UNWIND [{a: 1, b: 2}] AS m RETURN keys(m) AS k",
			want: [][]interface{}{{[]interface{}{"a", "b"}}}},
		{name: "keys( in a string literal", stmt: "UNWIND [1, 2] AS i RETURN i, 'keys(x)' AS s",
			want: [][]interface{}{{int64(1), "keys(x)"}, {int64(2), "keys(x)"}}},
		{name: "keys in WHERE", stmt: "UNWIND $rows AS row WITH row WHERE 'a' IN keys(row) RETURN row.a AS a", params: rows,
			want: [][]interface{}{{int64(1)}}},
		{name: "UNWIND over keys", stmt: "UNWIND keys({a: 1}) AS k RETURN k",
			want: [][]interface{}{{"a"}}},
		{name: "nested UNWIND over keys", stmt: "UNWIND [{x: 1, y: 2}] AS m UNWIND keys(m) AS k RETURN k ORDER BY k",
			want: [][]interface{}{{"x"}, {"y"}}},
		{name: "keys as a SET value", stmt: "UNWIND $rows AS row MATCH (n:T {id: 1}) SET n.ks = keys(row)", params: rows,
			check: "MATCH (n:T {id: 1}) RETURN n.ks AS ks", wantCk: [][]interface{}{{[]interface{}{"a", "b", "id"}}}},
		{name: "keys in a CREATE property", stmt: "UNWIND $rows AS row CREATE (n:C {ks: keys(row)}) RETURN n.ks AS ks", params: rows,
			want: [][]interface{}{{[]interface{}{"a", "b", "id"}}}},
		{name: "keys size in a MERGE SET", stmt: "UNWIND $rows AS row MERGE (n:M {id: row.id}) SET n.k = size(keys(row))", params: rows,
			check: "MATCH (n:M) RETURN n.k AS k", wantCk: [][]interface{}{{int64(3)}}},
		{name: "MERGE then RETURN keys", stmt: "UNWIND $rows AS row MERGE (n:M {id: row.id}) SET n += row RETURN keys(n) AS k", params: rows,
			want: [][]interface{}{{[]interface{}{"a", "b", "id"}}}},
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
					res, err := exec.Execute(ctx, tc.stmt, tc.params)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, cerr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, cerr)
						}
					}
					require.NoError(t, err, tc.stmt)
					if tc.want != nil {
						assert.Equal(t, tc.want, sortStringLists(res.Rows), tc.stmt)
					}
					if tc.check != "" {
						chk, err := exec.Execute(ctx, tc.check, nil)
						require.NoError(t, err)
						assert.Equal(t, tc.wantCk, sortStringLists(chk.Rows), tc.check)
					}
				})
			}
		}
	}
}

// sortStringLists sorts every list of strings in the rows (keys() order is
// unspecified) and normalizes []string to []interface{}.
func sortStringLists(rows [][]interface{}) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		out[i] = make([]interface{}, len(row))
		for j, v := range row {
			var strs []string
			switch list := v.(type) {
			case []string:
				strs = append(strs, list...)
			case []interface{}:
				for _, item := range list {
					s, ok := item.(string)
					if !ok {
						strs = nil
						break
					}
					strs = append(strs, s)
				}
				if len(strs) != len(list) {
					out[i][j] = v
					continue
				}
			default:
				out[i][j] = v
				continue
			}
			sort.Strings(strs)
			sorted := make([]interface{}, len(strs))
			for k, s := range strs {
				sorted[k] = s
			}
			out[i][j] = sorted
		}
	}
	return out
}
