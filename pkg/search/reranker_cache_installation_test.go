package search

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRerankerInstallationInvalidatesCachedResults(t *testing.T) {
	service := newHybridRerankBudgetService(t, 6)
	service.SetReranker(nil)
	options := DefaultSearchOptions()
	options.RerankEnabled = true
	options.RerankTopK = 6
	options.Limit = 6
	query := "library transcript"
	embedding := []float32{1, 0}
	before, err := service.Search(t.Context(), query, embedding, options)
	require.NoError(t, err)
	require.Len(t, before.Results, 6)
	require.NotContains(t, before.SearchMethod, "+rerank")
	provider := &countingReranker{testReranker: testReranker{enabled: true}}
	revision := service.IndexRevision()
	service.SetReranker(provider)
	after, err := service.Search(t.Context(), query, embedding, options)
	require.NoError(t, err)
	require.Contains(t, after.SearchMethod, "+rerank")
	require.Equal(t, 1, provider.calls, "a cached pre-installation response cannot hide the new provider")
	require.Greater(t, service.IndexRevision(), revision, "both service and Cypher caches observe provider installation")
	sameRevision := service.IndexRevision()
	service.SetReranker(provider)
	cached, err := service.Search(t.Context(), query, embedding, options)
	require.NoError(t, err)
	require.Equal(t, after.SearchMethod, cached.SearchMethod)
	require.Equal(t, 1, provider.calls, "unchanged provider preserves useful cached results")
	require.Equal(t, sameRevision, service.IndexRevision())
	service.SetReranker(nil)
	removed, err := service.Search(t.Context(), query, embedding, options)
	require.NoError(t, err)
	require.NotContains(t, removed.SearchMethod, "+rerank")
}

// The public interface permits value providers containing slices or maps.
// Installation must not require their dynamic values to support Go equality.
func TestRerankerInstallationAcceptsNonComparableProvider(t *testing.T) {
	service := newHybridRerankBudgetService(t, 6)
	provider := struct {
		Reranker
		Choices []string
	}{Reranker: &testReranker{enabled: true}, Choices: []string{"rank"}}
	service.SetReranker(provider)
	require.NotPanics(t, func() { service.SetReranker(provider) })
	require.True(t, service.RerankerAvailable(t.Context()))
	service.SetReranker(nil)
	require.False(t, service.RerankerAvailable(t.Context()))
}
