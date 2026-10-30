package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// EXISTS { } is a boolean value in RETURN and WITH, evaluated per row like the
// WHERE predicate (#626). Expected rows are Neo4j 5.26.30's for the same
// statements.
func TestExistsSubqueryInProjectionIsEvaluatedPerRow(t *testing.T) {
	flags := [][]interface{}{{"a", true}, {"b", false}, {"c", false}}
	negated := [][]interface{}{{"a", false}, {"b", true}, {"c", true}}
	cases := []struct {
		stmt string
		want [][]interface{}
	}{
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->() } AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, (EXISTS { MATCH (i)-->() }) AS e ORDER BY id", flags},
		{"MATCH (i:W) WITH i, EXISTS { MATCH (i)-->() } AS e RETURN i.id AS id, e ORDER BY id", flags},
		{"MATCH (i:W) WITH i, (EXISTS { MATCH (i)-->() }) AS e RETURN i.id AS id, e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { (i)-->() } AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->(x) WHERE x.id = 'b' } AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, NOT (EXISTS { MATCH (i)-->() }) AS e ORDER BY id", negated},
		{"MATCH (i:W) RETURN i.id AS id, NOT EXISTS { MATCH (i)-->() } AS e ORDER BY id", negated},
		{"MATCH (i:W) WITH i, NOT (EXISTS { MATCH (i)-->() }) AS e RETURN i.id AS id, e ORDER BY id", negated},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->() } = false AS e ORDER BY id", negated},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->() } AND true AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->() } OR false AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, coalesce(EXISTS { MATCH (i)-->() }, 5) AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, toString(EXISTS { MATCH (i)-->() }) AS e ORDER BY id",
			[][]interface{}{{"a", "true"}, {"b", "false"}, {"c", "false"}}},
		{"MATCH (i:W) RETURN i.id AS id, {e: EXISTS { MATCH (i)-->() }} AS e ORDER BY id",
			[][]interface{}{{"a", map[string]interface{}{"e": true}}, {"b", map[string]interface{}{"e": false}}, {"c", map[string]interface{}{"e": false}}}},
		{"MATCH (i:W) RETURN i.id AS id, [x IN [1] WHERE EXISTS { MATCH (i)-->() } | x] AS e ORDER BY id",
			[][]interface{}{{"a", []interface{}{int64(1)}}, {"b", []interface{}{}}, {"c", []interface{}{}}}},
		{"MATCH (i:W) RETURN i.id AS id, CASE WHEN EXISTS { MATCH (i)-->() } THEN 1 ELSE 0 END AS e ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
		{"MATCH (i:W) UNWIND [1] AS x RETURN i.id AS id, EXISTS { MATCH (i)-->() } AS e ORDER BY id", flags},
		{"MATCH (i:W) RETURN i.id AS id, EXISTS { MATCH (i)-->() } AS e ORDER BY e DESC, id", flags},
		{"MATCH (i:W) RETURN EXISTS { MATCH (i)-->() } AS e, count(*) AS c ORDER BY e",
			[][]interface{}{{false, int64(2)}, {true, int64(1)}}},
		{"MATCH (i:W) WITH i, EXISTS { MATCH (i)-->() } AS e SET i.owner = e RETURN i.id AS id, i.owner AS o ORDER BY id", flags},
		{"MATCH (i:W) WHERE EXISTS { MATCH (i)-->() } RETURN i.id AS id", [][]interface{}{{"a"}}},
		{"MATCH (i:W) RETURN i.id AS id, COUNT { MATCH (i)-->() } AS e ORDER BY id",
			[][]interface{}{{"a", int64(1)}, {"b", int64(0)}, {"c", int64(0)}}},
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
			_, err := exec.Execute(ctx, "CREATE (:W {id: 'a'}), (:W {id: 'b'}), (:W {id: 'c'})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "MATCH (a:W {id: 'a'}), (b:W {id: 'b'}) CREATE (a)-[:USES]->(b)", nil)
			require.NoError(t, err)
			for _, tc := range cases {
				t.Run(stack+"/"+mode+"/"+tc.stmt, func(t *testing.T) {
					if mode == "explicit transaction" {
						_, err := exec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() {
							_, err := exec.Execute(ctx, "COMMIT", nil)
							require.NoError(t, err)
						}()
					}
					res, err := exec.Execute(ctx, tc.stmt, nil)
					require.NoError(t, err)
					assert.Equal(t, tc.want, res.Rows)
				})
			}
		}
	}
}
