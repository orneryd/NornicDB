package apoc

import (
	"github.com/orneryd/nornicdb/apoc/registry"
	"reflect"
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
	require.NotContains(t, List(), "apoc.algo.pageRank", "retired graph-free Go stub is not registered")
	require.False(t, exported["apoc.algo.pageRank"], "native CALL owns PageRank; no scalar stub")
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

// Capture effective admission, not source declaration annotations. This does
// not invoke exporters, loaders or other handlers to infer their behavior.
func TestPluginExportReadObservationInventory(t *testing.T) {
	require.NoError(t, Initialize(storage.NewInMemoryStorage(), nil))
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	counts := make(map[string]int)
	names := PluginFunctionNames()
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		require.False(t, seen[name], "effective export names must be unique")
		seen[name] = true
		info, ok := GetInfo(name)
		require.True(t, ok, name)
		require.NotNil(t, info.Handler, name)
		signature := info.Handler.Signature()
		require.NotNil(t, signature, name)
		policy := info.Handler.ReadObservation()
		require.True(t, policy.Valid(), name)
		domain := "value"
		errors := 0
		for output := 0; output < signature.NumOut(); output++ {
			if signature.Out(output).Implements(errorType) {
				errors++
			}
		}
		switch {
		case errors > 0:
			domain = "checked-error"
		case signature.NumOut() > 1:
			domain = "metadata-only-tuple"
			require.Equal(t, registry.OpaqueReads, policy, "tuple descriptors cannot inherit declaration trust: %s", name)
		case signature.NumOut() == 0:
			domain = "void"
		case signature.Out(0).Kind() == reflect.Func:
			domain = "capability-result"
		}
		observation := "opaque"
		if policy == registry.CanonicalReads {
			observation = "canonical"
		}
		counts[observation+"/"+domain]++
		t.Logf("export=%s symbol=%s observation=%s result_domain=%s signature=%s", name, info.Handler.Symbol(), observation, domain, signature)
	}
	require.NotEmpty(t, names)
	t.Logf("effective export admission totals: exports=%d groups=%v", len(names), counts)
}
