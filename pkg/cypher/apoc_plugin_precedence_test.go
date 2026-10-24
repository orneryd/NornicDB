package cypher

import (
	"context"
	"strings"
	"testing"

	apocalgo "github.com/orneryd/nornicdb/apoc/algo"
	apocmap "github.com/orneryd/nornicdb/apoc/map"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The names both the query engine and the bundled APOC plugin know run one
// implementation each, the way the container image wires the plugin
// (nornicdb.GetPluginFunction behind PluginFunctionLookup):
//
//   - apoc.map.setPairs/setLists/subMap/updateTree have no engine
//     implementation: without the plugin they are unknown functions, and with
//     it the plugin's function answers (the engine owns only their null
//     contract around it).
//   - apoc.algo.pageRank is the engine's procedure (CALL) with the plugin
//     loaded or not; the plugin's is a scalar function of the same name, a
//     different call.
func TestBundledAPOCPluginNamesRunOneImplementation(t *testing.T) {
	plugin := map[string]interface{}{
		"apoc.map.setPairs":   apocmap.SetPairs,
		"apoc.map.setLists":   apocmap.SetLists,
		"apoc.map.subMap":     apocmap.SubMap,
		"apoc.map.updateTree": apocmap.UpdateTree,
		"apoc.algo.pageRank":  apocalgo.PageRank,
	}
	calls := map[string]string{
		"apoc.map.setPairs":   "RETURN apoc.map.setPairs({}, [['a', 1]]) AS v",
		"apoc.map.setLists":   "RETURN apoc.map.setLists({}, ['a'], [1]) AS v",
		"apoc.map.subMap":     "RETURN apoc.map.subMap({a: 1, b: 2}, ['a']) AS v",
		"apoc.map.updateTree": "RETURN apoc.map.updateTree({a: 1}, 'a', [['b', 2]]) AS v",
	}
	previous := PluginFunctionLookup
	t.Cleanup(func() { PluginFunctionLookup = previous })
	executor := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "plugins"), 0, 0)
	_, err := executor.Execute(context.Background(), "CREATE (:Rank {id: 1})-[:R]->(:Rank {id: 2})", nil)
	require.NoError(t, err)

	PluginFunctionLookup = nil
	for name, query := range calls {
		_, err := executor.Execute(context.Background(), query, nil)
		require.Error(t, err, "%s has no engine implementation", name)
		require.True(t, strings.Contains(strings.ToLower(err.Error()), "unknown function"), "%s: %v", name, err)
	}
	withoutPlugin, err := executor.Execute(context.Background(), "MATCH (n:Rank) WITH collect(n) AS nodes CALL apoc.algo.pageRank(nodes) YIELD node, score RETURN count(*) AS n", nil)
	require.NoError(t, err)

	pluginCalls := map[string]int{}
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		handler, ok := plugin[name]
		if ok {
			pluginCalls[name]++
		}
		return handler, ok
	}
	for name, query := range calls {
		result, err := executor.Execute(context.Background(), query, nil)
		require.NoError(t, err, name)
		require.Len(t, result.Rows, 1, name)
		require.NotZero(t, pluginCalls[name], "%s runs the plugin's function", name)
	}
	withPlugin, err := executor.Execute(context.Background(), "MATCH (n:Rank) WITH collect(n) AS nodes CALL apoc.algo.pageRank(nodes) YIELD node, score RETURN count(*) AS n", nil)
	require.NoError(t, err)
	require.Equal(t, withoutPlugin.Rows, withPlugin.Rows, "CALL apoc.algo.pageRank is the engine's procedure with the plugin loaded")
	require.Zero(t, pluginCalls["apoc.algo.pageRank"], "the procedure call never reaches the plugin's function")
}
