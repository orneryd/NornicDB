package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// size() takes a String or a List: size(null) is null, and any other value is
// Neo4j's "Type mismatch: expected String or List<T> but was <type>"
// SyntaxError on every route, never null (#600, #601). Expectations are Neo4j
// 5.26.30's for the same statements.
func TestSizeRejectsNonStringNonListArguments(t *testing.T) {
	mismatch := []struct{ stmt, typ string }{
		{"WITH {a: 1} AS m RETURN size(properties(m)) AS s", "Map"},
		{"MATCH (o:O) RETURN size(o) AS s", "Node"},
		{"MATCH (o:O) RETURN size(properties(o)) IS NULL AS isnull", "Map"},
		{"MATCH (o:O) SET o.s = size(properties(o)) RETURN o.s AS s", "Map"},
		{"RETURN size({a: 1}) AS s", "Map"},
		{"RETURN size(1) AS s", "Integer"},
		{"RETURN size(1.5) AS s", "Float"},
		{"RETURN size(true) AS s", "Boolean"},
		{"MATCH (o:O) WHERE size(properties(o)) > 0 RETURN o.id AS id", "Map"},
		{"MATCH (o:O) WHERE size(o) > 0 RETURN o.id AS id", "Node"},
		{"MATCH (o:O) WITH o WHERE size(properties(o)) > 0 RETURN o.id AS id", "Map"},
		{"MATCH (o:O) WITH o, size(o) AS s RETURN s", "Node"},
		{"MATCH (o:O)-[r]->() RETURN size(r) AS s", "Relationship"},
		{"MATCH p = (o:O)-->() RETURN size(p) AS s", "Path"},
		{"UNWIND [{a: 1}] AS m RETURN size(m) AS s", "Map"},
	}
	valid := []struct {
		stmt string
		want [][]interface{}
	}{
		{"RETURN size(null) AS s", [][]interface{}{{nil}}},
		{"WITH null AS x RETURN size(x) AS s", [][]interface{}{{nil}}},
		{"RETURN head(null) AS h, last(null) AS l, tail(null) AS t, reverse(null) AS r", [][]interface{}{{nil, nil, nil, nil}}},
		{"RETURN size('abc') AS s, size([1, 2]) AS l", [][]interface{}{{int64(3), int64(2)}}},
		{"UNWIND ['ab'] AS m RETURN size(m) AS s", [][]interface{}{{int64(2)}}},
		{"MATCH (o:O) RETURN size(keys(o)) AS s", [][]interface{}{{int64(1)}}},
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
			exec := build(t)
			ctx := context.Background()
			_, err := exec.Execute(ctx, "CREATE (:O {id: 2})-[:R]->(:X)", nil)
			require.NoError(t, err)
			run := func(stmt string) (*ExecuteResult, error) {
				if mode == "explicit transaction" {
					_, err := exec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					res, err := exec.Execute(ctx, stmt, nil)
					if err != nil {
						_, _ = exec.Execute(ctx, "ROLLBACK", nil)
						return nil, err
					}
					_, err = exec.Execute(ctx, "COMMIT", nil)
					return res, err
				}
				return exec.Execute(ctx, stmt, nil)
			}
			for _, tc := range mismatch {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					_, err := run(tc.stmt)
					require.Error(t, err)
					assert.Contains(t, err.Error(), "Neo.ClientError.Statement.SyntaxError")
					assert.Contains(t, err.Error(), "Type mismatch: expected String or List<T> but was "+tc.typ)
				})
			}
			for _, tc := range valid {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					res, err := run(tc.stmt)
					require.NoError(t, err)
					assert.Equal(t, tc.want, res.Rows)
				})
			}
			stored, err := exec.Execute(ctx, "MATCH (o:O) RETURN o.s AS s", nil)
			require.NoError(t, err)
			assert.Equal(t, [][]interface{}{{nil}}, stored.Rows, "the failed SET wrote nothing")
		}
	}
}
