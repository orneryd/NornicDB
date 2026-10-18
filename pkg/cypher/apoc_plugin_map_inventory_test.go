package cypher

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Probe every statically map-returning scalar export, not just apoc.map.
// No-argument reports have no null input; procedures are excluded by the
// actual bundled export list. Scalar-null arguments fail typed dispatch.
func TestAPOCMapReturnNullInventory(t *testing.T) {
	functions := bundledAPOCPluginFunctions(t)
	withPluginFunctions(t, functions)
	var names []string
	for name, handler := range functions {
		typ := reflect.TypeOf(handler)
		if typ.Kind() == reflect.Func && typ.NumOut() > 0 && typ.Out(0).Kind() == reflect.Map {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_inventory"))
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			typ := reflect.TypeOf(functions[name])
			if typ.NumIn() == 0 {
				t.Logf("MAP POLICY %s no null input", name)
				return
			}
			for _, primaryOnly := range []bool{false, true} {
				args := make([]string, typ.NumIn())
				for i := range args {
					args[i] = "null"
					if i > 0 && primaryOnly {
						switch typ.In(i).Kind() {
						case reflect.String:
							args[i] = "'id'"
						case reflect.Slice:
							args[i] = "[]"
						case reflect.Map:
							args[i] = "{}"
						case reflect.Bool:
							args[i] = "false"
						case reflect.Int, reflect.Int64, reflect.Float64, reflect.Interface:
							args[i] = "1"
						}
					}
				}
				expression := name + "(" + strings.Join(args, ",") + ")"
				result, err := exec.Execute(context.Background(), "RETURN "+expression, nil)
				t.Logf("MAP POLICY %s result=%v error=%v", name, result, err)
				allowEmpty := name == "apoc.coll.frequenciesAsMap" || name == "apoc.date.fields"
				allowConstructor := primaryOnly && (name == "apoc.map.setEntry" || name == "apoc.map.setPairs" || name == "apoc.map.setLists" || name == "apoc.map.setValues")
				if err == nil {
					if allowEmpty {
						require.Empty(t, result.Rows[0][0])
					} else if allowConstructor {
						require.IsType(t, map[string]interface{}{}, result.Rows[0][0])
					} else {
						require.Nil(t, result.Rows[0][0], "null must not become an empty/report map")
					}
				}
				_, clearErr := exec.Execute(context.Background(), "MATCH (n:Inventory) DETACH DELETE n", nil)
				require.NoError(t, clearErr)
				_, createErr := exec.Execute(context.Background(), "CREATE (:Inventory {keep:1})", nil)
				require.NoError(t, createErr)
				_, setErr := exec.Execute(context.Background(), "MATCH (n:Inventory) SET n = "+expression, nil)
				after, readErr := exec.Execute(context.Background(), "MATCH (n:Inventory) RETURN properties(n)", nil)
				require.NoError(t, readErr)
				t.Logf("MAP SET %s stored=%v error=%v", name, after.Rows, setErr)
				if allowEmpty {
					require.NoError(t, setErr)
					require.Empty(t, after.Rows[0][0])
				} else if allowConstructor {
					require.NoError(t, setErr)
					require.Equal(t, result.Rows[0][0], after.Rows[0][0])
				} else {
					require.Error(t, setErr)
					require.Equal(t, map[string]interface{}{"keep": int64(1)}, after.Rows[0][0])
				}
				_, deleteErr := exec.Execute(context.Background(), "MATCH (n:Inventory) DETACH DELETE n", nil)
				require.NoError(t, deleteErr)
			}
		})
	}
}
