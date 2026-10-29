package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// MinRRFScore's cut is counted: of 50 results from one ranking, the default
// 0.01 keeps the first 40 (1/(60+40)) and drops 10 (#1092).
func TestFuseRRFCountingReportsMinimumScoreDrops(t *testing.T) {
	service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
	vectorResults := make([]indexResult, 50)
	for index := range vectorResults {
		vectorResults[index] = indexResult{ID: fmt.Sprintf("node-%02d", index+1), Score: 1}
	}
	results, dropped := service.fuseRRFCounting(vectorResults, nil, DefaultSearchOptions())
	require.Len(t, results, 40)
	require.Equal(t, 10, dropped)

	keep := DefaultSearchOptions()
	keep.MinRRFScore = 0
	results, dropped = service.fuseRRFCounting(vectorResults, nil, keep)
	require.Len(t, results, 50)
	require.Zero(t, dropped)
}

// A search whose matches come from one ranking only (the query text matches
// no indexed word) returns that ranking's first 40 under the default
// MinRRFScore, and a page cut short by it says so with fallback_reason
// min_rrf_score (#1092). A page the cut didn't shorten, and MinRRFScore 0,
// report nothing.
func TestSearchReportsAPageCutShortByMinRRFScore(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine, 2, nil)
	for index := 0; index < 60; index++ {
		node := &storage.Node{
			ID:     storage.NodeID(fmt.Sprintf("doc-%02d", index)),
			Labels: []string{"Document"},
			Properties: map[string]any{
				"content":   "alpha beta",
				"embedding": []float32{1, float32(index) * 0.001},
			},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	search := func(limit int, minRRFScore float64) *SearchResponse {
		opts := DefaultSearchOptions()
		opts.Limit = limit
		opts.MinRRFScore = minRRFScore
		response, err := service.Search(context.Background(), "zzqx", []float32{1, 0}, opts)
		require.NoError(t, err)
		return response
	}

	cut := search(50, 0.01)
	require.Len(t, cut.Results, 40)
	require.True(t, cut.FallbackTriggered)
	require.Equal(t, SearchFallbackMinRRFScore, cut.FallbackReason)
	require.Equal(t, map[string]string{"X-NornicDB-Search-Fallback-Reason": "min_rrf_score"}, cut.ResponseHeaders())

	full := search(30, 0.01)
	require.Len(t, full.Results, 30)
	require.False(t, full.FallbackTriggered)
	require.Equal(t, SearchFallbackNone, full.FallbackReason)

	kept := search(50, 0)
	require.Len(t, kept.Results, 50)
	require.False(t, kept.FallbackTriggered)
	require.Equal(t, SearchFallbackNone, kept.FallbackReason)
}
