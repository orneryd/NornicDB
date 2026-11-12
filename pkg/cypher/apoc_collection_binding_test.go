package cypher

import (
	"testing"

	apoccoll "github.com/orneryd/nornicdb/apoc/coll"
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

func TestBundledCollectionsKeepCanonicalValueSemantics(t *testing.T) {
	functions := map[string]interface{}{
		"apoc.coll.contains":    apoccoll.Contains,
		"apoc.coll.containsAll": apoccoll.ContainsAll,
		"apoc.coll.containsAny": apoccoll.ContainsAny,
		"apoc.coll.indexOf":     apoccoll.IndexOf,
		"apoc.coll.occurrences": apoccoll.Occurrences,
	}
	handlers := make(map[string]*PluginFunctionHandler, len(functions))
	for name, function := range functions {
		handler, err := registry.Compile(function, registry.CanonicalReads)
		require.NoError(t, err)
		handlers[name] = handler
	}
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
		if handler, found := handlers[name]; found {
			return handler, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	forEachParser(t, func(t *testing.T) {
		exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0})
		for query, want := range map[string]interface{}{
			"RETURN apoc.coll.contains([1],1.0)":          true,
			"RETURN apoc.coll.contains([[1]],[1.0])":      true,
			"RETURN apoc.coll.contains([null],null)":      false,
			"RETURN apoc.coll.containsAll([1],[1.0])":     true,
			"RETURN apoc.coll.containsAny([1],[1.0])":     true,
			"RETURN apoc.coll.indexOf([1,2],2.0)":         int64(1),
			"RETURN apoc.coll.occurrences([1,1.0,'1'],1)": int64(2),
		} {
			result, err := exec.Execute(t.Context(), query, nil)
			require.NoError(t, err, query)
			require.Equal(t, [][]interface{}{{want}}, result.Rows, query)
		}
	})
	// An explicitly supplied third-party implementation still overrides the
	// name. Only the bundled representation-based implementation is retired
	// from the Cypher value-comparison path.
	custom, err := registry.Compile(func([]interface{}, interface{}) bool { return false }, registry.OpaqueReads)
	require.NoError(t, err)
	handlers["apoc.coll.contains"] = custom
	exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0})
	result, err := exec.Execute(t.Context(), "RETURN apoc.coll.contains([1],1)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{false}}, result.Rows)
}
