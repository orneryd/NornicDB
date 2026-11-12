package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

func TestProfileOpaqueCallbackCannotCertifyItsNestedReadTotal(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		graph := newLargeEmbeddingGraph(t, 1)
		handler, err := registry.Compile(func(ctx context.Context) (string, error) {
			node, err := graph.store.GetNode(ctx, "doc-0")
			if err != nil {
				return "", err
			}
			return node.Properties["title"].(string), nil
		}, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "profile.opaque" {
				return handler, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}
		result := graph.run(t, "PROFILE MATCH (n:Doc {k:0}) RETURN profile.opaque() AS title", nil)
		require.Equal(t, [][]interface{}{{"doc 0"}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.Positive(t, plan.TotalDBHits, "the MATCH's instrumented reads remain available")
		require.False(t, plan.TotalDBHitsObserved, "one observed inner read never proves an opaque callback's complete access count")
		require.NotEmpty(t, plan.Unobserved)
		require.Contains(t, formatPlan(plan), "Total DB Hits: unobserved")
		view := Neo4jPlanMap(plan, true)
		args := view["args"].(map[string]interface{})
		require.Equal(t, false, args["TotalDBHitsObserved"])
	})
}

func TestProfileConstantConfirmsMeasuredZero(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	result, err := exec.Execute(t.Context(), "PROFILE RETURN 1", nil)
	require.NoError(t, err)
	plan := executionPlanFromResult(t, result)
	require.True(t, plan.TotalDBHitsObserved)
	require.Zero(t, plan.TotalDBHits)
	require.Empty(t, plan.Unobserved)
}
