package search

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// windowRecordingReranker records every provider input and answers in that
// window's order, or reversed when reverse is set, so the caller can prove
// which candidates one window held and where the provider put them.
type windowRecordingReranker struct {
	reverse bool

	mu    sync.Mutex
	calls [][]string
}

func (r *windowRecordingReranker) Name() string                     { return "window_recording_reranker" }
func (r *windowRecordingReranker) Enabled() bool                    { return true }
func (r *windowRecordingReranker) IsAvailable(context.Context) bool { return true }

func (r *windowRecordingReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	submitted := make([]string, len(candidates))
	for index := range candidates {
		submitted[index] = candidates[index].ID
	}
	r.mu.Lock()
	r.calls = append(r.calls, submitted)
	r.mu.Unlock()

	results := make([]RerankResult, len(candidates))
	for rank := range candidates {
		at := rank
		if r.reverse {
			at = len(candidates) - 1 - rank
		}
		score := 1 - 0.1*float64(rank)
		results[rank] = RerankResult{
			ID:           candidates[at].ID,
			Content:      candidates[at].Content,
			OriginalRank: at + 1,
			NewRank:      rank + 1,
			CrossScore:   score,
			FinalScore:   score,
		}
	}
	return results, nil
}

// recorded returns every provider input in call order.
func (r *windowRecordingReranker) recorded() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := make([][]string, len(r.calls))
	for index := range r.calls {
		calls[index] = append([]string(nil), r.calls[index]...)
	}
	return calls
}

// continuationFusedIDs is the canonical fused order a ranked continuation
// reranks: the same search with Stage-2 reranking off.
func continuationFusedIDs(t *testing.T, service *Service, options *SearchOptions, embedding []float32) []string {
	t.Helper()
	plain := *options
	plain.RerankEnabled = false
	response, err := service.Search(context.Background(), "library transcript", embedding, &plain)
	require.NoError(t, err)
	ids := make([]string, len(response.Results))
	for index := range response.Results {
		ids[index] = response.Results[index].ID
	}
	return ids
}

func continuationResultIDs(results []SearchResult) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = results[index].ID
	}
	return ids
}

func continuationResultPhases(results []SearchResult) []string {
	phases := make([]string, len(results))
	for index := range results {
		phases[index] = results[index].Phase
	}
	return phases
}

// Replaces TestSearchTextContinuationRerankTopKCapsRankedResults, which
// asserted the old top-K output cap. TopK bounds one provider window, not the
// ranked population: the continuation reranks successive windows in fused
// order, each candidate reaches the provider once, and the terminal page
// covers every hit.
func TestSearchTextContinuationRerankWindowsCoverRankedResults(t *testing.T) {
	embedding := []float32{1, 0}
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			service := newHybridRerankBudgetService(t, 3)
			service.SetSearchResultCachePolicy(0, 0)
			reranker := &windowRecordingReranker{reverse: true}
			service.SetReranker(reranker)

			options := DefaultSearchOptions()
			options.Limit = 10
			options.RerankEnabled = true
			options.RerankTopK = 2
			fused := continuationFusedIDs(t, service, options, embedding)
			require.Len(t, fused, 3)

			request := SearchContinuationRequest{Owner: "windows", Database: "nornic", Mode: mode, N: 1}
			if mode == SearchContinuationRankedThenID {
				request.N = 10
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "library transcript", options, request,
				nil,
				func(context.Context, string) ([]float32, error) { return embedding, nil },
				service.Search,
				ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			ids := continuationResultIDs(page.Results)
			for page.HasMore {
				request.QID = page.QID
				request.N = 10
				page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				ids = append(ids, continuationResultIDs(page.Results)...)
			}

			// Windows follow the fused order and hold at most TopK candidates;
			// the provider's order holds inside each window.
			require.Equal(t, [][]string{{fused[0], fused[1]}, {fused[2]}}, reranker.recorded())
			require.Equal(t, []string{fused[1], fused[0], fused[2]}, ids)

			require.Equal(t, "rrf_hybrid+rerank", page.SearchMethod)
			require.False(t, page.FallbackTriggered)
			require.False(t, page.HasMore)
			require.True(t, page.RankedPoolExhausted)
			require.True(t, page.CollectionExhausted)
			require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
			require.NotNil(t, page.Total)
			require.Equal(t, uint64(3), *page.Total)
			require.Equal(t, 3, page.RankedCount)
			if mode == SearchContinuationRankedThenID {
				require.NotNil(t, page.EligibleCount)
				require.Equal(t, 3, *page.EligibleCount)
			}

			// Each candidate reached the provider once, not once per depth.
			submitted := map[string]int{}
			for _, call := range reranker.recorded() {
				for _, id := range call {
					submitted[id]++
				}
			}
			for _, id := range fused {
				require.Equal(t, 1, submitted[id], "provider input for %s", id)
			}
		})
	}
}

// Replaces TestSearchTextContinuationRerankCapsRankedPoolAtTopK, which
// asserted that ranked continuation stopped at TopK of a 30-candidate pool.
func TestSearchTextContinuationRerankWindowsCoverRankedPool(t *testing.T) {
	embedding := []float32{1, 0}
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			service := newHybridRerankBudgetService(t, 30)
			service.SetSearchResultCachePolicy(0, 0)
			reranker := &windowRecordingReranker{}
			service.SetReranker(reranker)

			options := DefaultSearchOptions()
			options.Limit = 1
			options.RerankEnabled = true
			options.RerankTopK = 10
			options.MaxCandidateLimit = 64
			request := SearchContinuationRequest{Owner: "windows", Database: "nornic", Mode: mode, N: 5}
			if mode == SearchContinuationRankedThenID {
				request.N = 12
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "library transcript", options, request,
				nil,
				func(context.Context, string) ([]float32, error) { return embedding, nil },
				service.Search,
				ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			ids := continuationResultIDs(page.Results)
			for page.HasMore {
				request.QID = page.QID
				request.N = 10
				page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				ids = append(ids, continuationResultIDs(page.Results)...)
			}

			seen := map[string]struct{}{}
			for _, id := range ids {
				require.NotContains(t, seen, id, "duplicate hit")
				seen[id] = struct{}{}
			}
			require.Len(t, ids, 30)
			require.Equal(t, "rrf_hybrid+rerank", page.SearchMethod)
			require.False(t, page.FallbackTriggered)
			require.False(t, page.HasMore)
			require.True(t, page.RankedPoolExhausted)
			require.True(t, page.CollectionExhausted)
			require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
			require.Equal(t, 30, page.RankedCount)
			require.NotNil(t, page.Total)
			require.Equal(t, uint64(30), *page.Total)

			// Every provider input is one bounded window, windows are consumed
			// in fused order, and each candidate is submitted exactly once.
			var submitted []string
			for _, call := range reranker.recorded() {
				require.LessOrEqual(t, len(call), options.RerankTopK)
				submitted = append(submitted, call...)
			}
			require.Equal(t, ids, submitted)
			if mode == SearchContinuationRankedThenID {
				require.NotNil(t, page.EligibleCount)
				require.Equal(t, 30, *page.EligibleCount)
			}
		})
	}
}

// Caller ceilings still own the stream: maxResults ends it without claiming
// collection exhaustion, and rankedLimit still fixes the ranked_then_id prefix
// while windows fill it.
func TestSearchTextContinuationRerankWindowsRespectCallerCeilings(t *testing.T) {
	embedding := []float32{1, 0}

	t.Run("max_results", func(t *testing.T) {
		service := newHybridRerankBudgetService(t, 30)
		service.SetSearchResultCachePolicy(0, 0)
		reranker := &windowRecordingReranker{}
		service.SetReranker(reranker)

		options := DefaultSearchOptions()
		options.Limit = 1
		options.RerankEnabled = true
		options.RerankTopK = 10
		request := SearchContinuationRequest{Owner: "windows-max", Database: "nornic", Mode: SearchContinuationRanked, N: 5, MaxResults: 12}
		page, err := service.SearchTextContinuation(
			context.Background(), "library transcript", options, request,
			nil,
			func(context.Context, string) ([]float32, error) { return embedding, nil },
			service.Search,
			ChunkedSearchErrorPolicy{},
		)
		require.NoError(t, err)
		ids := continuationResultIDs(page.Results)
		for page.HasMore {
			request.QID = page.QID
			request.N = 10
			page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			ids = append(ids, continuationResultIDs(page.Results)...)
		}
		require.Len(t, ids, 12)
		require.False(t, page.HasMore)
		require.Equal(t, SearchContinuationMaxResultsComplete, page.Completion)
		require.False(t, page.RankedPoolExhausted)
		require.False(t, page.CollectionExhausted)
	})

	t.Run("ranked_limit", func(t *testing.T) {
		service := newHybridRerankBudgetService(t, 30)
		service.SetSearchResultCachePolicy(0, 0)
		reranker := &windowRecordingReranker{}
		service.SetReranker(reranker)

		options := DefaultSearchOptions()
		options.Limit = 1
		options.RerankEnabled = true
		options.RerankTopK = 3
		request := SearchContinuationRequest{Owner: "windows-ranked", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 12, RankedLimit: 5}
		page, err := service.SearchTextContinuation(
			context.Background(), "library transcript", options, request,
			nil,
			func(context.Context, string) ([]float32, error) { return embedding, nil },
			service.Search,
			ChunkedSearchErrorPolicy{},
		)
		require.NoError(t, err)
		ids := continuationResultIDs(page.Results)
		phases := continuationResultPhases(page.Results)
		for page.HasMore {
			request.QID = page.QID
			request.N = 12
			page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			ids = append(ids, continuationResultIDs(page.Results)...)
			phases = append(phases, continuationResultPhases(page.Results)...)
		}

		require.Len(t, ids, 30)
		require.Equal(t, 5, page.RankedCount)
		require.NotNil(t, page.EligibleCount)
		require.Equal(t, 30, *page.EligibleCount)
		require.True(t, page.CollectionExhausted)
		require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
		require.Len(t, phases, 30)
		for index, phase := range phases {
			if index < 5 {
				require.Equal(t, SearchContinuationRankedPhase, phase)
				continue
			}
			require.Equal(t, SearchContinuationCatalogPhase, phase)
		}
		// The ranked prefix is still RankedLimit long, in TopK windows.
		var submitted []string
		for _, call := range reranker.recorded() {
			require.LessOrEqual(t, len(call), options.RerankTopK)
			submitted = append(submitted, call...)
		}
		require.Len(t, submitted, 5)
		require.Equal(t, ids[:5], submitted)
	})
}

// A window's request minimum still decides which rows are hits: excluded rows
// do not consume a page slot, the total counts only rows above the threshold,
// and a window the provider saw one candidate of keeps that candidate's
// provider score, so the minimum governs it too.
func TestSearchTextContinuationRerankWindowsHonorMinScore(t *testing.T) {
	service := newHybridRerankBudgetService(t, 7)
	service.SetSearchResultCachePolicy(0, 0)
	reranker := &windowRecordingReranker{}
	service.SetReranker(reranker)

	embedding := []float32{1, 0}
	options := DefaultSearchOptions()
	options.Limit = 10
	options.RerankEnabled = true
	options.RerankTopK = 3
	options.RerankMinScore = 0.85
	fused := continuationFusedIDs(t, service, options, embedding)
	require.Len(t, fused, 7)

	request := SearchContinuationRequest{Owner: "windows-min", Database: "nornic", Mode: SearchContinuationRanked, N: 20}
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options, request,
		nil,
		func(context.Context, string) ([]float32, error) { return embedding, nil },
		service.Search,
		ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)

	// Each full window is scored 1.0, 0.9, 0.8 by position and the trailing
	// one-candidate window 1.0, so only the 0.8 rows fall below 0.85.
	want := []string{fused[0], fused[1], fused[3], fused[4], fused[6]}
	require.Equal(t, want, continuationResultIDs(page.Results))
	require.Equal(t, [][]string{{fused[0], fused[1], fused[2]}, {fused[3], fused[4], fused[5]}, {fused[6]}}, reranker.recorded())

	require.False(t, page.HasMore)
	require.True(t, page.RankedPoolExhausted)
	require.True(t, page.CollectionExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
	require.Equal(t, 5, page.RankedCount)
	require.NotNil(t, page.Total)
	require.Equal(t, uint64(5), *page.Total)
}
