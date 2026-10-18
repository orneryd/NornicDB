package cypher

import (
	"context"
	"testing"

	apocpkg "github.com/orneryd/nornicdb/apoc"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Use the exact export owner shared with the bundled plugin wrapper.
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

func TestBundledAPOCMapNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testAPOCMapNullFamily(t, true)
}

func TestBundledAPOCDigestNullFamily(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	testApocUtilHashRecursiveNulls(t)
}

func TestBundledAPOCMapGetNullError(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "map_get_error"))
	// Maps.java#get dereferences containsKey for a null default and
	// getOrDefault otherwise; neither accepts a null source map.
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
