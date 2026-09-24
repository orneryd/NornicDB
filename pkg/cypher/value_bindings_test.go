package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Non-entity variables (WITH / UNWIND values, comprehension and reduce
// variables) are values to every function and operator, on every route, and a
// real node is never mistaken for one (#588). Expected values are Neo4j 5's.
func TestNonEntityVariablesAreValues(t *testing.T) {
	setup := []string{"CREATE (:T {id: 1}), (:V {value: 5})"}
	cases := []struct {
		name    string
		stmt    string
		want    [][]interface{}
		wantErr bool
	}{
		{name: "keys of a WITH map in SET", stmt: "WITH {a: 1, b: 2} AS m MATCH (n:T) SET n.k = keys(m) RETURN n.k AS k",
			want: [][]interface{}{{[]interface{}{"a", "b"}}}},
		{name: "size of keys in SET", stmt: "WITH {a: 1, b: 2} AS m MATCH (n:T) SET n.k = size(keys(m)) RETURN n.k AS k",
			want: [][]interface{}{{int64(2)}}},
		{name: "keys after WITH n, map", stmt: "MATCH (n:T) WITH n, {a: 1} AS m SET n.p = keys(m) RETURN n.p AS p",
			want: [][]interface{}{{[]interface{}{"a"}}}},
		{name: "keys in MERGE SET", stmt: "WITH {a: 1, b: 2} AS m MERGE (n:T {id: 1}) SET n.k = keys(m) RETURN n.k AS k",
			want: [][]interface{}{{[]interface{}{"a", "b"}}}},
		{name: "properties of a map in SET", stmt: "WITH {a: 1, b: 2} AS m MATCH (n:T) SET n.p = properties(m).a RETURN n.p AS p",
			want: [][]interface{}{{int64(1)}}},
		{name: "labels of a map is a type error", stmt: "WITH {a: 1} AS m MATCH (n:T) SET n.l = labels(m) RETURN n.l AS l",
			wantErr: true},
		{name: "map with a value key keeps the map", stmt: "WITH {value: 3} AS m MATCH (n:T) SET n.k = keys(m), n.v = m.value RETURN n.k AS k, n.v AS v",
			want: [][]interface{}{{[]interface{}{"value"}, int64(3)}}},
		{name: "comprehension over maps in SET", stmt: "MATCH (n:T) SET n.c = [x IN [{a: 1, b: 2}] | size(keys(x))] RETURN n.c AS c",
			want: [][]interface{}{{[]interface{}{int64(2)}}}},
		{name: "comprehension over a map with a value key", stmt: "MATCH (n:T) SET n.c = [x IN [{value: 3}] | x.value] RETURN n.c AS c",
			want: [][]interface{}{{[]interface{}{int64(3)}}}},
		{name: "reduce over maps in SET", stmt: "MATCH (n:T) SET n.r = reduce(s = 0, x IN [{a: 1}, {a: 2}] | s + x.a) RETURN n.r AS r",
			want: [][]interface{}{{int64(3)}}},
		{name: "real node whose only property is value", stmt: "MATCH (v:V), (n:T) SET n.k = keys(v), n.l = labels(v) RETURN n.k AS k, n.l AS l",
			want: [][]interface{}{{[]interface{}{"value"}, []interface{}{"V"}}}},
		{name: "real node carried by WITH", stmt: "MATCH (v:V) WITH v AS w MATCH (n:T) SET n.l = labels(w), n.v = w.value RETURN n.l AS l, n.v AS v",
			want: [][]interface{}{{[]interface{}{"V"}, int64(5)}}},
		{name: "YIELD value in WHERE", stmt: "CALL db.labels() YIELD label WHERE label = 'T' RETURN label",
			want: [][]interface{}{{"T"}}},
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
					if tc.wantErr {
						assert.Error(t, err, tc.stmt)
						return
					}
					require.NoError(t, err, tc.stmt)
					assert.Equal(t, tc.want, normalizeSetRouteRows(res.Rows), tc.stmt)
				})
			}
		}
	}
}
