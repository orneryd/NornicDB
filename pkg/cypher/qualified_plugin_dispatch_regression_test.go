package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompatDottedPluginFailureUsesItsOwnHandler(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	handlers := map[string]interface{}{
		"apoc.bitwise.and":       func(a, b int64) int64 { return a & b },
		"apoc.search.range":      func(string, string, int64, int64) []interface{} { return nil },
		"apoc.periodic.truncate": func(map[string]interface{}) map[string]interface{} { return nil },
		"apoc.path.shortestPath": func(interface{}, interface{}) []interface{} { return nil },
	}
	PluginFunctionLookup = func(name string) (interface{}, bool) { handler, ok := handlers[name]; return handler, ok }
	for _, query := range []string{
		"RETURN apoc.bitwise.and(5,3)",
		"RETURN apoc.search.range('Person','age',18,65)",
		"RETURN apoc.periodic.truncate({})",
		"RETURN apoc.path.shortestPath(null,null)",
	} {
		t.Run(query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_audit"))
			_, err := executor.Execute(context.Background(), query, nil)
			require.ErrorContains(t, err, "unsupported function signature")
		})
	}
}

func TestQualifiedPluginNamesKeepTheirHandler(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		switch name {
		case "custom.range", "custom.truncate", "custom.shortestPath":
			return func(value string) string { return "plugin:" + value }, true
		}
		return nil, false
	}
	for _, name := range []string{"custom.range", "custom.truncate", "custom.shortestPath"} {
		t.Run(name, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "qualified_plugin"))
			result, err := executor.Execute(context.Background(), "RETURN "+name+"('ok') AS value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{"plugin:ok"}}, result.Rows)
		})
	}
}

func TestUnregisteredQualifiedFunctionsStayUnknown(t *testing.T) {
	previous := PluginFunctionLookup
	PluginFunctionLookup = nil
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for _, query := range []string{
		"RETURN apoc.bitwise.and(5,3)",
		"RETURN apoc.search.range('Person','age',18,65)",
		"RETURN apoc.periodic.truncate({})",
		"RETURN apoc.path.shortestPath(null,null)",
	} {
		t.Run(query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unknown_qualified"))
			_, err := executor.Execute(context.Background(), query, nil)
			require.Error(t, err)
			require.Contains(t, lowerASCII(err.Error()), "unknown function")
		})
	}
}
