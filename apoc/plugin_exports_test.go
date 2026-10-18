package apoc

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
	"github.com/stretchr/testify/require"
)

// The plugin exports the registered functions without APOC's procedures and
// without the functions the query engine has built in, in any letter case.
func TestPluginFunctionNames(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	exported := make(map[string]bool)
	for _, name := range PluginFunctionNames() {
		exported[name] = true
	}
	require.True(t, exported["apoc.math.sigmoid"])
	require.True(t, exported["apoc.text.camelCase"])
	require.False(t, exported["apoc.cypher.doIt"], "a procedure")
	require.False(t, exported["apoc.map.removeKeys"], "a built-in")
	require.False(t, exported["apoc.coll.sum"], "a built-in")
	excluded := 0
	for _, name := range List() {
		if isProcedureName(name) || IsBuiltInFunction(name) {
			excluded++
		}
	}
	require.Positive(t, excluded)
	procedures, native := 0, 0
	for _, name := range List() {
		if isProcedureName(name) {
			procedures++
		} else if IsBuiltInFunction(name) {
			native++
		}
	}
	t.Logf("catalog=%d procedure-only=%d scalar=%d native-owner=%d native-registered=%d exported=%d", len(List()), procedures, len(List())-procedures, len(BuiltInFunctionNames()), native, len(exported))
	require.Len(t, exported, len(List())-excluded)

	require.True(t, IsBuiltInFunction("apoc.map.removeKeys"))
	require.True(t, IsBuiltInFunction("APOC.MAP.REMOVEKEYS"))
	require.False(t, IsBuiltInFunction("apoc.text.camelCase"))
	require.Contains(t, BuiltInFunctionNames(), "apoc.map.removekeys")
	require.Len(t, BuiltInFunctionNames(), 52)
}
