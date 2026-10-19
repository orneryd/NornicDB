package cypher

import (
	"context"
	"fmt"
	"testing"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPluginNestedNullConsumers(t *testing.T) {
	type namedMap map[string]interface{}
	type namedList []interface{}
	nulls := map[string]interface{}{
		"map":      map[string]interface{}(nil),
		"namedMap": namedMap(nil),
		"otherMap": map[interface{}]interface{}(nil),
		"node":     (*storage.Node)(nil),
		"edge":     (*storage.Edge)(nil),
		"time":     (*time.Time)(nil),
		"remote":   storage.RemoteEntityValue(nil),
		"path":     (*PathResult)(nil),
	}
	handlers := map[string]interface{}{}
	for name, value := range nulls {
		value := value
		handlers["nested."+name] = func() interface{} { return map[string]interface{}{"a": value} }
	}
	handlers["nested.reflected"] = func(context.Context) (namedMap, error) {
		return namedMap{"a": map[string]interface{}(nil)}, nil
	}
	handlers["nested.tree"] = func() map[string]interface{} {
		return map[string]interface{}{"m": namedMap{"a": (*storage.Edge)(nil)}, "l": namedList{map[string]interface{}(nil), []interface{}{(*storage.Node)(nil)}}}
	}
	handlers["nested.list"] = func() []interface{} { return []interface{}{map[string]interface{}(nil)} }
	handlers["nested.typedList"] = func() []*storage.Node { return []*storage.Node{nil} }
	handlers["nested.pairs"] = func() []interface{} { return []interface{}{[]interface{}{"a", (*storage.Node)(nil)}} }
	handlers["nested.empty"] = func() map[string]interface{} {
		return map[string]interface{}{"m": map[string]interface{}{}, "l": []interface{}(nil), "bytes": []byte{0, 128, 255}}
	}
	withPluginFunctions(t, handlers)
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nested_null"))
	for name := range handlers {
		if name == "nested.tree" || name == "nested.list" || name == "nested.typedList" || name == "nested.empty" || name == "nested.pairs" {
			continue
		}
		t.Run(name, func(t *testing.T) {
			for round := 0; round < 3; round++ {
				result, err := exec.Execute(ctx, "RETURN "+name+"() AS v", nil)
				require.NoError(t, err)
				require.Equal(t, map[string]interface{}{"a": nil}, result.Rows[0][0])
			}
			for _, query := range []string{
				"RETURN apoc.map.clean(%s(), [], [])",
				"RETURN apoc.map.clean(apoc.map.removeKeys(%s(), ['absent']), [], [])",
				"RETURN apoc.map.clean(apoc.map.merge({}, %s()), [], [])",
				"RETURN apoc.map.clean(apoc.map.setKey({}, 'a', %s().a), [], [])",
			} {
				result, err := exec.Execute(ctx, fmt.Sprintf(query, name), nil)
				require.NoError(t, err)
				require.Equal(t, map[string]interface{}{}, result.Rows[0][0], query)
			}
			for _, algorithm := range []string{"md5", "sha1", "sha256"} {
				result, err := exec.Execute(ctx, fmt.Sprintf("RETURN apoc.util.%s([%s()]), apoc.util.%s([{a:null}])", algorithm, name, algorithm), nil)
				require.NoError(t, err)
				require.Equal(t, result.Rows[0][1], result.Rows[0][0])
			}
			for _, mutation := range []struct {
				query string
				want  map[string]interface{}
			}{
				{"MATCH (n:NestedNull) SET n += apoc.map.clean(%s(), [], [])", map[string]interface{}{"a": int64(1), "keep": int64(2)}},
				{"MATCH (n:NestedNull) SET n = apoc.map.clean(%s(), [], [])", map[string]interface{}{}},
				{"MATCH (n:NestedNull) SET n += %s()", map[string]interface{}{"keep": int64(2)}},
				{"MATCH (n:NestedNull) SET n = %s()", map[string]interface{}{}},
			} {
				_, err := exec.Execute(ctx, "CREATE (:NestedNull {a:1, keep:2})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, fmt.Sprintf(mutation.query, name), nil)
				require.NoError(t, err)
				result, err := exec.Execute(ctx, "MATCH (n:NestedNull) RETURN properties(n)", nil)
				require.NoError(t, err)
				require.Equal(t, mutation.want, result.Rows[0][0])
				_, err = exec.Execute(ctx, "MATCH (n:NestedNull) DETACH DELETE n", nil)
				require.NoError(t, err)
			}
			for _, clause := range []string{"CREATE", "MERGE"} {
				_, err := exec.Execute(ctx, clause+" (:NestedNull {a:"+name+"().a, keep:2})", nil)
				if clause == "CREATE" {
					require.NoError(t, err)
					result, err := exec.Execute(ctx, "MATCH (n:NestedNull) RETURN properties(n)", nil)
					require.NoError(t, err)
					require.Equal(t, map[string]interface{}{"keep": int64(2)}, result.Rows[0][0])
				} else {
					require.Error(t, err, "MERGE cannot match a null property")
					result, err := exec.Execute(ctx, "MATCH (n:NestedNull) RETURN count(n)", nil)
					require.NoError(t, err)
					require.Equal(t, int64(0), result.Rows[0][0])
				}
				_, err = exec.Execute(ctx, "MATCH (n:NestedNull) DETACH DELETE n", nil)
				require.NoError(t, err)
			}
		})
	}
	for _, pair := range [][2]string{
		{"nested.tree()", "{m:{a:null},l:[null,[null]]}"},
		{"nested.list()", "[null]"}, {"nested.typedList()", "[null]"},
		{"apoc.map.fromPairs(nested.pairs())", "{a:null}"},
		{"apoc.map.fromLists(['a'],nested.list())", "{a:null}"},
		{"apoc.map.removeKeys(nested.tree(), ['absent'], {recursive:true})", "{m:{a:null},l:[null,[null]]}"},
	} {
		for _, algorithm := range []string{"md5", "sha1", "sha256"} {
			result, err := exec.Execute(ctx, fmt.Sprintf("RETURN apoc.util.%s([%s]), apoc.util.%s([%s])", algorithm, pair[0], algorithm, pair[1]), nil)
			require.NoError(t, err, pair[0])
			require.Equal(t, result.Rows[0][1], result.Rows[0][0], pair[0])
		}
	}
	result, err := exec.Execute(ctx, "RETURN nested.empty().m IS NULL, nested.empty().l IS NULL, size(nested.empty().l), nested.empty().bytes", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{false, false, int64(0), []byte{0, 128, 255}}, result.Rows[0])
	for _, clause := range []string{"CREATE (:NestedNull {a:nested.list()})", "MERGE (:NestedNull {a:nested.list()})", "CREATE (n:NestedNull {keep:2}) SET n.a = nested.list()"} {
		_, err := exec.Execute(ctx, clause, nil)
		require.Error(t, err, "lists containing null cannot be stored: %s", clause)
		result, err := exec.Execute(ctx, "MATCH (n:NestedNull) RETURN count(n)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(0), result.Rows[0][0], "a failed write must leave no node: %s", clause)
	}
}

func BenchmarkPluginResultBoundary(b *testing.B) {
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"scalar", int64(42)},
		{"canonical", map[string]interface{}{"a": int64(1), "list": []interface{}{nil, "value"}}},
		{"nested_null", map[string]interface{}{"a": map[string]interface{}(nil), "list": []interface{}{(*storage.Node)(nil)}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			handler := func() interface{} { return tc.value }
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := callPluginHandler(handler, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestRegisteredNestedNullResults(t *testing.T) {
	cypherfn.Register("registeredNestedNull", func(cypherfn.Context, []string) (interface{}, error) {
		return map[string]interface{}{"a": (*storage.Node)(nil)}, nil
	})
	value, found, err := cypherfn.EvaluateFunction("registeredNestedNull", nil, cypherfn.Context{Eval: func(string) (interface{}, error) { return nil, nil }})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, map[string]interface{}{"a": nil}, value)
	ClearUserProcedures()
	t.Cleanup(ClearUserProcedures)
	require.NoError(t, RegisterUserProcedure(ProcedureSpec{
		Name: "nested.procedure", Mode: ProcedureModeRead,
		Signature: "nested.procedure() :: (value :: MAP)", Returns: []ProcedureColumn{{Name: "value", Type: "MAP"}},
	}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
		return &ExecuteResult{Columns: []string{"value"}, Rows: [][]interface{}{{map[string]interface{}{"a": (*storage.Edge)(nil)}}}}, nil
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "registered_nested_null"))
	for _, query := range []string{
		"CALL nested.procedure() YIELD value RETURN apoc.map.clean(value, [], [])",
		"UNWIND [1,2] AS x CALL nested.procedure() YIELD value RETURN apoc.map.clean(value, [], [])",
	} {
		result, err := exec.Execute(context.Background(), query, nil)
		require.NoError(t, err)
		for _, row := range result.Rows {
			require.Equal(t, map[string]interface{}{}, row[0], query)
		}
	}
}
