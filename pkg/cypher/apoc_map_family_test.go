package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Policies come from APOC Core 5.26.30 Maps.java and common Util.java,
// rather than the implementation being tested. "null" is a null result;
// "error" is ProcedureCallFailed. SET of either must preserve properties.
func TestAPOCMapNullFamily(t *testing.T) {
	testAPOCMapNullFamily(t, false)
}

func testAPOCMapNullFamily(t *testing.T, plugin bool) {
	cases := []struct {
		expression, policy string
		want               interface{}
	}{
		{"apoc.convert.fromJsonMap(null)", "null", nil},
		{"apoc.map.get(null,'x',{})", "error", nil},
		{"apoc.map.get(null,'x',1)", "error", nil},
		{"apoc.map.get(null,'x',null)", "error", nil},
		{"apoc.map.fromPairs(null)", "error", nil},
		{"apoc.map.fromLists(null,[])", "error", nil},
		{"apoc.map.fromLists([],null)", "error", nil},
		{"apoc.map.fromValues(null)", "error", nil},
		{"apoc.map.merge(null,{a:1})", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.merge({a:1},null)", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.merge(null,null)", "map", map[string]interface{}{}},
		{"apoc.map.mergeList(null)", "error", nil},
		{"apoc.map.mergeList([null])", "error", nil},
		{"apoc.map.setKey(null,'a',1)", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.setEntry(null,'a',1)", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.setPairs(null,[['a',1]])", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.setLists(null,['a'],[1])", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.setValues(null,['a',1])", "map", map[string]interface{}{"a": int64(1)}},
		{"apoc.map.setPairs({a:1},null)", "error", nil},
		{"apoc.map.setLists({a:1},null,[])", "error", nil},
		{"apoc.map.setLists({a:1},[],null)", "error", nil},
		{"apoc.map.setValues({a:1},null)", "error", nil},
		{"apoc.map.removeKey(null,'a')", "error", nil},
		{"apoc.map.removeKeys(null,[])", "error", nil},
		{"apoc.map.removeKeys({a:1},null)", "error", nil},
		{"apoc.map.clean(null,[],[])", "error", nil},
		{"apoc.map.clean({a:1},null,[])", "error", nil},
		{"apoc.map.clean({a:1},[],null)", "error", nil},
		{"apoc.map.subMap(null,[])", "null", nil},
		{"apoc.map.subMap({a:1},null)", "null", nil},
		{"apoc.map.flatten(null,'.')", "error", nil},
		{"apoc.map.unflatten(null,'.')", "error", nil},
		{"apoc.map.groupBy(null,'id')", "error", nil},
		{"apoc.map.groupByMulti(null,['id'])", "error", nil},
		{"apoc.map.updateTree(null,'id',[])", "error", nil},
		{"apoc.map.updateTree({a:1},'id',null)", "error", nil},
		// NornicDB extension, applying the same null-safe map contract.
		{"apoc.map.dropNullValues(null)", "null", nil},
		{"apoc.map.fromPairs([null])", "error", nil},
	}
	t.Run(fmt.Sprintf("bundled=%v", plugin), func(t *testing.T) {
		exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_map_family"))
		for _, tc := range cases {
			name := strings.Split(tc.expression, "(")[0]
			_, builtIn := builtInCypherFunctions[strings.ToLower(name)]
			if !plugin && !builtIn {
				continue
			}
			for _, parameter := range []bool{false, true} {
				expression := tc.expression
				if parameter {
					expression = strings.ReplaceAll(expression, "null", "$nil")
				}
				t.Run(expression, func(t *testing.T) {
					ctx := context.Background()
					params := map[string]interface{}{"nil": nil}
					result, err := exec.Execute(ctx, "RETURN "+expression, params)
					if tc.policy == "error" {
						requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.want, result.Rows[0][0])
					}
					_, err = exec.Execute(ctx, "MATCH (n:MapFamily) DETACH DELETE n", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "CREATE (:MapFamily {id:1, keep:2})", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, "MATCH (n:MapFamily) SET n = "+expression, params)
					after, readErr := exec.Execute(ctx, "MATCH (n:MapFamily) RETURN properties(n)", nil)
					require.NoError(t, readErr)
					if tc.policy == "map" {
						require.NoError(t, err)
						require.Equal(t, tc.want, after.Rows[0][0])
					} else {
						require.Error(t, err)
						require.Equal(t, map[string]interface{}{"id": int64(1), "keep": int64(2)}, after.Rows[0][0])
					}
				})
			}
		}
	})
}
