package cypher

// db.rerank forwards the caller's candidates to the configured reranker through
// search.Service, and that provider is an arbitrary implementation whose
// database access PROFILE cannot see. This file holds the public statement to
// the actual boundary: the provider invocation is reported when it really
// happens, and a provider nobody enabled is not reported as one. The reviewed
// handler reports that actual provider boundary; custom replacements keep
// their opaque registration policy.

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// profiledDbRerankStatement calls the public rerank procedure under PROFILE. One
// candidate mentions a bridge, so a provider that ranks it first returns a
// distinct score verdict instead of the flat-score fallback.
const profiledDbRerankStatement = "CALL db.rerank({query: 'zephyrine', candidates: [{id: 'a', content: 'zephyrine bridge', score: 0.2}, {id: 'b', content: 'zephyrine notes', score: 0.9}]}) YIELD id, original_rank, new_rank RETURN id, original_rank, new_rank"

func TestProfileDbRerankReportsOnlyTheActualRerankProviderCall(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		newExecutor := func(t *testing.T) (*StorageExecutor, *search.Service) {
			t.Helper()
			engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "profile_rerank_provider")
			service := search.NewService(engine, search.DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			exec := NewStorageExecutor(engine)
			exec.SetSearchService(service)
			return exec, service
		}

		t.Run("configured provider is invoked", func(t *testing.T) {
			calls := 0
			exec, service := newExecutor(t)
			service.SetReranker(bridgeFirstReranker{calls: &calls, available: true})

			result, err := exec.Execute(t.Context(), "PROFILE "+profiledDbRerankStatement, nil)
			require.NoError(t, err)
			require.Equal(t, 1, calls, "the provider was invoked once")
			require.Len(t, result.Rows, 2)

			plan := executionPlanFromResult(t, result)
			require.Contains(t, plan.Unobserved, "database reads: rerank provider",
				"the provider actually called is the statement's unknown read")
			require.NotContains(t, plan.Unobserved, "database reads: procedure callback: db.rerank", "the reviewed handler reports its actual provider boundary")
		})

		t.Run("disabled provider is not invoked", func(t *testing.T) {
			exec, service := newExecutor(t)
			service.SetReranker(search.NewCrossEncoder(nil))

			result, err := exec.Execute(t.Context(), "PROFILE "+profiledDbRerankStatement, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2, "the candidates keep their original order")

			plan := executionPlanFromResult(t, result)
			require.NotContains(t, plan.Unobserved, "database reads: rerank provider",
				"a provider nobody enabled was not invoked")
			require.Empty(t, plan.Unobserved)
			require.True(t, plan.TotalDBHitsObserved)
			require.Zero(t, plan.TotalDBHits)
		})

		t.Run("no provider configured", func(t *testing.T) {
			exec, _ := newExecutor(t)

			result, err := exec.Execute(t.Context(), "PROFILE "+profiledDbRerankStatement, nil)
			require.NoError(t, err)
			require.Len(t, result.Rows, 2, "the candidates keep their original order")

			plan := executionPlanFromResult(t, result)
			require.NotContains(t, plan.Unobserved, "database reads: rerank provider",
				"no provider was configured, so none was invoked")
			require.Empty(t, plan.Unobserved)
			require.True(t, plan.TotalDBHitsObserved)
			require.Zero(t, plan.TotalDBHits)
		})
	})
}
