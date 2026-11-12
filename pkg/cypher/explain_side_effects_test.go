package cypher

import (
	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExplainNeverEvaluatesPatternFunctions(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		calls := 0
		handler, err := registry.Compile(func() string { calls++; return "Ada" }, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "plan.probe" {
				return handler, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		exec := NewStorageExecutor(newTestMemoryEngine(t), QueryCachePolicy{MaxEntries: 0})
		_, err = exec.Execute(t.Context(), "CREATE INDEX plan_person_name FOR (n:Person) ON (n.name)", nil)
		require.NoError(t, err)
		result, err := exec.Execute(t.Context(), "EXPLAIN MATCH (n:Person {name:plan.probe()}) RETURN n", nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
		require.Zero(t, calls, "EXPLAIN plans expressions without invoking configured functions")
	})
}
