package cypher

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRetrievalWhereReusesChainSyntaxWithFreshStatementBindings(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "chain_plan")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(t.Context(), `CREATE (:Document)-[:HAS_VERSION]->(:Version {id:'v1'})-[:HAS_SOURCE]->(:SearchSource {id:'s1',content:'shared source text'}), (:Document)-[:HAS_VERSION]->(:Version {id:'v2'})-[:HAS_SOURCE]->(:SearchSource {id:'s2',content:'shared source text'})`, nil)
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	require.NoError(t, service.BuildIndexes(t.Context()))
	exec.SetSearchService(service)
	var builds atomic.Int64
	hook := func(kind pipelineClauseAnalysis, text string) {
		if kind == clauseAnalysisBoundChain && strings.Contains(text, "HAS_SOURCE") {
			builds.Add(1)
		}
	}
	old := pipelineClausePlanBuildHook.Swap(&hook)
	t.Cleanup(func() { pipelineClausePlanBuildHook.Store(old) })
	for index, version := range []string{"v1", "v2"} {
		result, err := exec.Execute(t.Context(), "CALL db.retrieve($request) YIELD node RETURN node.id AS id", map[string]any{
			"version": version,
			"request": map[string]any{"query": "shared source text", "limit": int64(10), "where": `EXISTS { (node)<-[:HAS_SOURCE]-(v:Version)<-[:HAS_VERSION]-(:Document) WHERE v.id = $version }`},
		})
		require.NoError(t, err)
		require.Equal(t, [][]any{{"s" + version[1:]}}, result.Rows)
		require.Equal(t, int64(index+1), builds.Load(), "one immutable chain parse per public request, not per candidate")
	}
}

func TestBoundChainSyntaxPlanDoesNotRetainParameterValues(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "chain_parameters")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(t.Context(), `CREATE (:Start {id:'seed'})-[:R {kind:'a'}]->(:End)`, nil)
	require.NoError(t, err)
	result, err := exec.Execute(t.Context(), "MATCH (n:Start) RETURN n", nil)
	require.NoError(t, err)
	seed := result.Rows[0][0].(*storage.Node)
	planned := withPipelineClausePlans(context.Background())
	for _, test := range []struct {
		kind  string
		count int64
	}{{"a", 1}, {"b", 0}, {"a", 1}} {
		ctx := withParams(planned, map[string]any{"kind": test.kind})
		count, handled := exec.boundChainCount(ctx, `(n)-[:R {kind:$kind}]->(:End)`, map[string]any{"n": seed}, false)
		require.True(t, handled)
		require.Equal(t, test.count, count)
	}
	t.Run("concurrent_value_free_readers", func(t *testing.T) {
		ctx := withPipelineClausePlans(t.Context())
		body := `(n)-[:R]->(:End)`
		var builds atomic.Int64
		hook := func(kind pipelineClauseAnalysis, text string) {
			if kind == clauseAnalysisBoundChain && text == body {
				builds.Add(1)
			}
		}
		old := pipelineClausePlanBuildHook.Swap(&hook)
		t.Cleanup(func() { pipelineClausePlanBuildHook.Store(old) })
		type outcome struct {
			count   int64
			handled bool
		}
		completed := make(chan outcome, 16)
		for range 16 {
			go func() {
				count, handled := exec.boundChainCount(ctx, body, map[string]any{"n": seed}, false)
				completed <- outcome{count, handled}
			}()
		}
		for range 16 {
			result := <-completed
			require.True(t, result.handled)
			require.Equal(t, int64(1), result.count)
		}
		require.Equal(t, int64(1), builds.Load())
	})

}
