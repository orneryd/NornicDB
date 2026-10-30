package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Operator evaluation matches Neo4j 5.26.30 on every route (#462, #610):
// + with a null operand is null (never the text '<nil>…'), INTEGER
// arithmetic is exact with an overflow error, and =~ matches the whole string
// in projections, WHERE, CASE and SET alike.
func TestOperatorEvaluationMatchesNeo4j(t *testing.T) {
	type tc struct {
		stmt    string
		want    [][]interface{}
		wantErr string
	}
	cases := []tc{
		{stmt: "MATCH (:O {id: 2})-[h:HAS]->(:I) SET h.q = h.missing + 1 RETURN h.q AS q", want: [][]interface{}{{nil}}},
		{stmt: "MATCH (o:O) SET o.q = o.missing + 1 RETURN o.q AS q", want: [][]interface{}{{nil}}},
		{stmt: "MATCH (o:O) SET o.t = 'x' + o.missing RETURN o.t AS t", want: [][]interface{}{{nil}}},
		{stmt: "MATCH (o:O) SET o.l = [1] + o.missing RETURN o.l AS l", want: [][]interface{}{{nil}}},
		{stmt: "MATCH (o:O) SET o.s = 0 + head([]) RETURN o.s AS s", want: [][]interface{}{{nil}}},
		{stmt: "MATCH (o:O) RETURN o.missing + 1 AS q, 1 + o.missing AS r", want: [][]interface{}{{nil, nil}}},
		{stmt: "RETURN null + 1 AS a, 1 + null AS b, null + 'x' AS c, [1] + null AS d", want: [][]interface{}{{nil, nil, nil, nil}}},
		{stmt: "RETURN 'a' + 1 AS a, 1 + 'a' AS b, 'a' + 1.5 AS c, 'a' + [1] AS d, [1] + 2 AS e", want: [][]interface{}{{"a1", "1a", "a1.5", []interface{}{"a", int64(1)}, []interface{}{int64(1), int64(2)}}}},
		{stmt: "MATCH (:O)-[h:HAS]->(:I) SET h.n = h.n + 1 RETURN h.n AS n", want: [][]interface{}{{int64(6)}}},
		{stmt: "RETURN 9007199254740993 + 1 AS next", want: [][]interface{}{{int64(9007199254740994)}}},
		{stmt: "WITH 9007199254740993 AS b RETURN b + 1 AS next, b - 1 AS prev, b * 1 AS same, b / 1 AS div, b % 10 AS mod", want: [][]interface{}{{int64(9007199254740994), int64(9007199254740992), int64(9007199254740993), int64(9007199254740993), int64(3)}}},
		{stmt: "MATCH (o:O) SET o.big = 9007199254740993 + 2 RETURN o.big AS b", want: [][]interface{}{{int64(9007199254740995)}}},
		{stmt: "RETURN 5 / 2 AS a, -5 / 2 AS b, -5 % 3 AS c, 2 + 3.0 AS d", want: [][]interface{}{{int64(2), int64(-2), int64(-2), float64(5)}}},
		{stmt: "RETURN 9223372036854775807 + 1 AS o", wantErr: "long overflow"},
		{stmt: "RETURN 4611686018427387904 * 2 AS o", wantErr: "long overflow"},
		{stmt: "WITH 9223372036854775807 AS m RETURN m + 1 AS o", wantErr: "long overflow"},
		{stmt: "RETURN 'Tom' =~ 'T.*' AS a, 'Ann' =~ 'T.*' AS b", want: [][]interface{}{{true, false}}},
		{stmt: "MATCH (n:T) RETURN n.id AS id, n.name =~ 'T.*' AS m ORDER BY id", want: [][]interface{}{{int64(1), true}, {int64(2), false}}},
		{stmt: "UNWIND ['Tom', 'Ann'] AS x RETURN x, x =~ 'T.*' AS m", want: [][]interface{}{{"Tom", true}, {"Ann", false}}},
		{stmt: "MATCH (n:T) RETURN n.id AS id ORDER BY n.name =~ 'T.*'", want: [][]interface{}{{int64(2)}, {int64(1)}}},
		{stmt: "MATCH (n:T) WITH n, n.name =~ 'T.*' AS m ORDER BY m RETURN n.id AS id, m", want: [][]interface{}{{int64(2), false}, {int64(1), true}}},
		{stmt: "MATCH (n:T) WHERE n.name =~ 'T.*' RETURN n.id AS id", want: [][]interface{}{{int64(1)}}},
		{stmt: "MATCH (n:T) WHERE n.name =~ 'o' RETURN n.id AS id", want: [][]interface{}{}},
		{stmt: "MATCH (n:T) SET n.m = n.name =~ 'T.*' RETURN n.m AS m ORDER BY n.id", want: [][]interface{}{{true}, {false}}},
		{stmt: "RETURN 'Tom' =~ 'o' AS partial, 'Tom' =~ '.*o.*' AS full, null =~ 'T' AS n1, 'T' =~ null AS n2", want: [][]interface{}{{false, true, nil, nil}}},
		{stmt: "RETURN 'ab' =~ '[' AS v", wantErr: "Invalid Regex"},
		{stmt: "WITH 1 AS x RETURN x =~ '1' AS v", wantErr: "Type mismatch: expected String"},
		{stmt: "RETURN 'a' + true AS v", wantErr: "Type mismatch"},
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
			for _, c := range cases {
				t.Run(stack+"/"+mode+"/"+c.stmt, func(t *testing.T) {
					exec := build(t)
					ctx := context.Background()
					for _, setup := range []string{
						"CREATE (:O {id: 2})-[:HAS {n: 5}]->(:I {sku: 'a1'})",
						"CREATE (:T {id: 1, name: 'Tom'}), (:T {id: 2, name: 'Ann'})",
					} {
						_, err := exec.Execute(ctx, setup, nil)
						require.NoError(t, err)
					}
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
					}
					res, err := exec.Execute(ctx, c.stmt, nil)
					if mode == "explicit transaction" {
						if err != nil {
							_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						} else {
							_, cerr := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, cerr)
						}
					}
					if c.wantErr != "" {
						require.Error(t, err)
						assert.Contains(t, err.Error(), c.wantErr)
						return
					}
					require.NoError(t, err)
					assert.Equal(t, c.want, res.Rows)
				})
			}
		}
	}
}
