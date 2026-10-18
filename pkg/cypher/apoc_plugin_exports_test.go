package cypher

import (
	"context"
	"sort"
	"strings"
	"testing"

	apocpkg "github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// bundledAPOCPluginFunctions are the handlers the bundled APOC plugin
// (apoc.so) exports, installed as a loaded plugin's.
func bundledAPOCPluginFunctions(t *testing.T) map[string]interface{} {
	t.Helper()
	require.NoError(t, apocpkg.Initialize(apocstorage.NewInMemoryStorage(), nil))
	functions := make(map[string]interface{})
	for _, name := range apocpkg.PluginFunctionNames() {
		info, ok := apocpkg.GetInfo(name)
		require.True(t, ok, name)
		functions[name] = info.Handler
	}
	return functions
}

func TestBundledAPOCDigestNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testApocUtilHashRecursiveNulls(t)
}

func TestBundledAPOCMapNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testAPOCMapNullFamily(t, true)
}

func TestBundledAPOCMapGetNullError(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_get_error"))
	// Maps.java#get dereferences map.containsKey when the default is null,
	// and map.getOrDefault otherwise. A supplied map default cannot make a
	// null source map valid. Match the existing owner's null-map failure.
	for _, value := range []string{"null", "{}", "1"} {
		_, err := exec.Execute(context.Background(), "RETURN apoc.map.get(null,'x',"+value+")", nil)
		requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		require.ErrorContains(t, err, "apoc.map.get")
		require.ErrorContains(t, err, "`map` argument is null")
	}
	result, err := exec.Execute(context.Background(), "RETURN apoc.map.get({},'x',{}), apoc.map.get({x:{keep:1}},'x',{})", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{}, result.Rows[0][0])
	require.Equal(t, map[string]interface{}{"keep": int64(1)}, result.Rows[0][1])
}

// The bundled APOC plugin's list of built-in functions is exactly the
// catalog's apoc.* functions, and the plugin exports none of them, so a
// built-in answers its calls with or without the plugin.
func TestBundledAPOCPluginLeavesBuiltInFunctions(t *testing.T) {
	var catalog []string
	for name := range builtInCypherFunctions {
		if strings.HasPrefix(name, "apoc.") {
			catalog = append(catalog, name)
		}
	}
	sort.Strings(catalog)
	listed := apocpkg.BuiltInFunctionNames()
	sort.Strings(listed)
	require.Equal(t, catalog, listed, "apoc/plugin_exports.go builtInFunctionNames must list the catalog's apoc.* functions")

	exported := bundledAPOCPluginFunctions(t)
	require.NotEmpty(t, exported)
	for name := range exported {
		_, builtIn := builtInCypherFunctions[lowerASCII(name)]
		require.False(t, builtIn, "the plugin exports built-in %s", name)
	}
}

// With the bundled plugin loaded, apoc.map.removeKeys / removeKey of a null
// map still fail the call, so SET n = … leaves the node's properties as
// they were (the plugin's handler returned {}, which removed them all).
func TestBundledAPOCPluginNullMapKeepsProperties(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "apoc_plugin_null_map"))
	ctx := context.Background()
	nullMap := map[string]interface{}{"m": nil}
	for _, tc := range []struct {
		query  string
		params map[string]interface{}
	}{
		{"MATCH (n:PluginNullMap) SET n = apoc.map.removeKeys(null, ['x'])", nil},
		{"MATCH (n:PluginNullMap) SET n = apoc.map.removeKeys($m, ['x'])", nullMap},
		{"MATCH (n:PluginNullMap) SET n = apoc.map.removeKey($m, 'x')", nullMap},
		{"MERGE (n:PluginNullMap {id: 1}) ON MATCH SET n = apoc.map.removeKeys($m, ['x'])", nullMap},
	} {
		t.Run(tc.query, func(t *testing.T) {
			_, err := exec.Execute(ctx, "MATCH (n:PluginNullMap) DETACH DELETE n", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CREATE (:PluginNullMap {id: 1, a: 1, b: 2})", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, tc.query, tc.params)
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
			result, err := exec.Execute(ctx, "MATCH (n:PluginNullMap) RETURN properties(n)", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{map[string]interface{}{"id": int64(1), "a": int64(1), "b": int64(2)}}}, result.Rows)
		})
	}

	// The built-in answers a map; the plugin still answers its own functions.
	result, err := exec.Execute(ctx, "RETURN apoc.map.removeKeys({a: 1, b: {a: 2, c: 3}}, ['a'], {recursive: true}), apoc.map.removeKeys({a: 1, b: 2}, ['a']), apoc.text.camelCase('hello world')", nil)
	require.NoError(t, err)
	require.Equal(t, map[string]interface{}{"b": map[string]interface{}{"c": int64(3)}}, result.Rows[0][0])
	require.Equal(t, map[string]interface{}{"b": int64(2)}, result.Rows[0][1])
	require.Equal(t, "helloWorld", result.Rows[0][2])
}
