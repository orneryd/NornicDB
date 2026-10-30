package cypher

// Author: Codex NOR-184 lane: migrate fixtures to admitted callable publication.

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCompatDottedPluginFailureUsesItsOwnHandler(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	// These typed signatures are supported. Deliberate handler errors prove
	// dispatch does not swallow a failure or fall through to another built-in.
	calls := make(map[string]int)
	ownError := func(name string) error { calls[name]++; return fmt.Errorf("own-handler: %s", name) }
	handlers := map[string]interface{}{
		"apoc.bitwise.and": func(a, b int64) (int64, error) {
			require.EqualValues(t, 5, a)
			require.EqualValues(t, 3, b)
			return 0, ownError("apoc.bitwise.and")
		},
		"apoc.search.range": func(label, key string, low, high int64) ([]interface{}, error) {
			require.Equal(t, "Person", label)
			require.Equal(t, "age", key)
			require.EqualValues(t, 18, low)
			require.EqualValues(t, 65, high)
			return nil, ownError("apoc.search.range")
		},
		"apoc.periodic.truncate": func(config map[string]interface{}) (map[string]interface{}, error) {
			require.Empty(t, config)
			return nil, ownError("apoc.periodic.truncate")
		},
		"apoc.path.shortestPath": func(start, end interface{}) ([]interface{}, error) {
			require.Nil(t, start)
			require.Nil(t, end)
			return nil, ownError("apoc.path.shortestPath")
		},
	}
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) { handler, ok := handlers[name]; return handler, ok })
	for _, test := range []struct{ query, name string }{
		{"RETURN apoc.bitwise.and(5,3)", "apoc.bitwise.and"},
		{"RETURN apoc.search.range('Person','age',18,65)", "apoc.search.range"},
		{"RETURN apoc.periodic.truncate({})", "apoc.periodic.truncate"},
		{"RETURN apoc.path.shortestPath(null,null)", "apoc.path.shortestPath"},
	} {
		t.Run(test.query, func(t *testing.T) {
			executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugin_audit"))
			_, err := executor.Execute(context.Background(), test.query, nil)
			require.ErrorContains(t, err, "own-handler: "+test.name)
		})
	}
	for name := range handlers {
		require.Equal(t, 1, calls[name], name)
	}
}

func TestQualifiedPluginNamesKeepTheirHandler(t *testing.T) {
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	PluginFunctionLookup = checkedTestLookup(func(name string) (interface{}, bool) {
		switch name {
		case "custom.range", "custom.truncate", "custom.shortestPath":
			return func(value string) string { return "plugin:" + value }, true
		}
		return nil, false
	})
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
