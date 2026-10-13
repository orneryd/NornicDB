package nornicdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// SHOW FUNCTIONS lists each registered plugin function with a signature from
// its handler's Go type (#1168).
func TestPluginFunctionsAreListedWithTheirGoSignature(t *testing.T) {
	require.Equal(t, "zz.f(arg1 :: STRING, arg2 :: INTEGER, arg3 :: FLOAT, arg4 :: BOOLEAN) :: LIST<STRING>",
		pluginFunctionSignature("zz.f", func(string, int64, float64, bool) []string { return nil }))
	require.Equal(t, "zz.g(arg1 :: MAP, arg2 :: LIST<ANY>, arg3 :: ANY) :: MAP",
		pluginFunctionSignature("zz.g", func(map[string]interface{}, []interface{}, interface{}) map[string]interface{} { return nil }))
	require.Equal(t, "zz.h(arg1 :: LIST<FLOAT>) :: ANY", pluginFunctionSignature("zz.h", func(...float64) {}))
	require.Equal(t, "zz.i() :: ANY", pluginFunctionSignature("zz.i", "not a function"))

	pluginsMu.Lock()
	previous := pluginFunctions
	pluginFunctions = map[string]PluginFunction{
		"zz.text.camelCase": {Name: "zz.text.camelCase", Handler: func(string) string { return "" }, Description: "Camel-cases text", Category: "text"},
	}
	pluginsMu.Unlock()
	t.Cleanup(func() {
		pluginsMu.Lock()
		pluginFunctions = previous
		pluginsMu.Unlock()
	})
	infos := pluginFunctionInfos()
	require.Len(t, infos, 1)
	require.Equal(t, "zz.text.camelCase", infos[0].Name)
	require.Equal(t, "text", infos[0].Category)
	require.Equal(t, "Camel-cases text", infos[0].Description)
	require.Equal(t, "zz.text.camelCase(arg1 :: STRING) :: STRING", infos[0].Signature)
}
