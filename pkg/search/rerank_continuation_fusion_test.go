package search

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// scriptedWindowReranker records every provider input and answers with what
// the test's script returns, so a test can control the provider's subset,
// filtering, failure or order without a real cross-encoder.
type scriptedWindowReranker struct {
	script func(candidates []RerankCandidate) ([]RerankResult, error)

	mu    sync.Mutex
	calls [][]string
}

func (r *scriptedWindowReranker) Name() string                     { return "scripted_window_reranker" }
func (r *scriptedWindowReranker) Enabled() bool                    { return true }
func (r *scriptedWindowReranker) IsAvailable(context.Context) bool { return true }

func (r *scriptedWindowReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	submitted := make([]string, len(candidates))
	for index := range candidates {
		submitted[index] = candidates[index].ID
	}
	r.mu.Lock()
	r.calls = append(r.calls, submitted)
	r.mu.Unlock()
	return r.script(candidates)
}

func (r *scriptedWindowReranker) recorded() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	calls := make([][]string, len(r.calls))
	for index := range r.calls {
		calls[index] = append([]string(nil), r.calls[index]...)
	}
	return calls
}

// firstOnlyRerankResult is the window's first candidate with a score, a valid
// unique subset of its provider input.
func firstOnlyRerankResult(candidates []RerankCandidate) ([]RerankResult, error) {
	return []RerankResult{{
		ID:           candidates[0].ID,
		OriginalRank: 1,
		CrossScore:   0.9,
		FinalScore:   0.9,
	}}, nil
}

// multiChunkRerankService holds three documents: one the first query chunk
// matches, one the second matches, and one both match, so the chunked owner's
// outer RRF has to merge two lists that overlap.
func multiChunkRerankService(t *testing.T) *Service {
	t.Helper()
	engine := newNamespacedEngine(t)
	service := NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	service.SetSearchResultCachePolicy(0, 0)
	for _, node := range []*storage.Node{
		{
			ID: storage.NodeID("alpha"), Labels: []string{"Document"},
			Properties:      map[string]any{"content": "alpha only"},
			ChunkEmbeddings: [][]float32{{1, 0}},
		},
		{
			ID: storage.NodeID("beta"), Labels: []string{"Document"},
			Properties:      map[string]any{"content": "beta only"},
			ChunkEmbeddings: [][]float32{{0, 1}},
		},
		{
			ID: storage.NodeID("both"), Labels: []string{"Document"},
			Properties:      map[string]any{"content": "alpha and beta together"},
			ChunkEmbeddings: [][]float32{{0.7071, 0.7071}},
		},
	} {
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
	return service
}

// multiChunkSplit is the two-chunk split of "alpha beta".
func multiChunkSplit(context.Context, string) ([]string, error) {
	return []string{"alpha", "beta"}, nil
}

// multiChunkEmbed embeds each split chunk in its own direction.
func multiChunkEmbed(_ context.Context, chunk string) ([]float32, error) {
	if chunk == "beta" {
		return []float32{0, 1}, nil
	}
	return []float32{1, 0}, nil
}

// multiChunkFusedIDs is the fused order a multi-chunk continuation reranks:
// the same chunked search with Stage-2 reranking off.
func multiChunkFusedIDs(t *testing.T, service *Service, options *SearchOptions) []string {
	t.Helper()
	plain := *options
	plain.RerankEnabled = false
	response, err := SearchTextChunks(
		context.Background(), "alpha beta", &plain,
		multiChunkSplit, multiChunkEmbed, service.Search,
	)
	require.NoError(t, err)
	return continuationResultIDs(response.Results)
}

// deeperFusedIDs is the fused order a continuation reranks at depth: the same
// search with Stage-2 reranking off and enough depth to expose every row.
func deeperFusedIDs(t *testing.T, service *Service, options *SearchOptions, embedding []float32) []string {
	t.Helper()
	reference := *options
	reference.Limit = 100
	return continuationFusedIDs(t, service, &reference, embedding)
}

// collectContinuationIDs starts a ranked continuation and pulls it to its end,
// returning every delivered row in delivery order.
func collectContinuationIDs(t *testing.T, service *Service, options *SearchOptions, request SearchContinuationRequest,
	chunkQuery ChunkQueryFunc, embedQuery EmbedQueryFunc, query string,
) ([]string, *SearchContinuationPage) {
	t.Helper()
	page, err := service.SearchTextContinuation(
		context.Background(), query, options, request,
		chunkQuery, embedQuery, service.Search, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	ids := continuationResultIDs(page.Results)
	for page.HasMore {
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		ids = append(ids, continuationResultIDs(page.Results)...)
	}
	return ids, page
}

// A multi-chunk ranked continuation reranks the chunked owner's outer fusion,
// not each chunk's own list: every document is delivered once, in the order
// the provider gave inside each window of the fused prefix, and each candidate
// reaches the provider once per query.
func TestSearchTextContinuationRerankWindowsFollowOuterChunkFusion(t *testing.T) {
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			service := multiChunkRerankService(t)
			reranker := &windowRecordingReranker{reverse: true}
			service.SetReranker(reranker)

			options := DefaultSearchOptions()
			options.Limit = 10
			options.RerankEnabled = true
			options.RerankTopK = 2
			options.MinRRFScore = 0
			fused := multiChunkFusedIDs(t, service, options)
			require.Len(t, fused, 3)

			ids, page := collectContinuationIDs(t, service, options,
				SearchContinuationRequest{Owner: "chunk-windows", Database: "nornic", Mode: mode, N: 10},
				multiChunkSplit, multiChunkEmbed, "alpha beta")

			// Windows are the outer fused order, TopK-sized, and the provider's
			// order holds inside each window.
			require.Equal(t, [][]string{{fused[0], fused[1]}, {fused[2]}}, reranker.recorded())
			require.Equal(t, []string{fused[1], fused[0], fused[2]}, ids)

			// The chunks' overlapping hit is fused, not repeated.
			seen := make(map[string]struct{}, len(ids))
			for _, id := range ids {
				require.NotContains(t, seen, id, "duplicate hit")
				seen[id] = struct{}{}
			}
			require.Len(t, seen, len(fused))

			// One provider input per candidate, keyed by the original query: a
			// per-chunk rerank would have submitted every candidate twice.
			submitted := make(map[string]int, len(fused))
			for _, call := range reranker.recorded() {
				for _, id := range call {
					submitted[id]++
				}
			}
			for _, id := range fused {
				require.Equal(t, 1, submitted[id], "provider input for %s", id)
			}

			require.Equal(t, "chunked_rrf_hybrid+rerank", page.SearchMethod)
			require.False(t, page.FallbackTriggered)
			require.False(t, page.HasMore)
			require.True(t, page.RankedPoolExhausted)
			require.True(t, page.CollectionExhausted)
			require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
			require.Equal(t, 3, page.RankedCount)
			require.Equal(t, 2, page.QueryChunksSearched)
			require.Equal(t, 2, page.QueryChunksTotal)
		})
	}
}

// A window is the batch its candidates were first admitted in. A deeper pull
// adds new windows for the newly retrieved rows instead of re-forming an
// accepted one over a longer prefix, so an admitted window keeps both its
// membership and the provider order it produced.
func TestSearchTextContinuationRerankKeepsFirstAdmittedWindows(t *testing.T) {
	service := newHybridRerankBudgetService(t, 4)
	service.SetSearchResultCachePolicy(0, 0)
	reranker := &windowRecordingReranker{reverse: true}
	service.SetReranker(reranker)

	embedding := []float32{1, 0}
	options := DefaultSearchOptions()
	options.Limit = 2
	options.RerankEnabled = true
	options.RerankTopK = 3
	fused := deeperFusedIDs(t, service, options, embedding)
	require.Len(t, fused, 4)

	ids, page := collectContinuationIDs(t, service, options,
		SearchContinuationRequest{Owner: "first-windows", Database: "nornic", Mode: SearchContinuationRanked, N: 1},
		nil, func(context.Context, string) ([]float32, error) { return embedding, nil }, "library transcript")

	// The first pull admitted [fused0, fused1] and ranked them against each
	// other; the deeper pull admitted [fused2, fused3] as their own window.
	require.Equal(t, [][]string{{fused[0], fused[1]}, {fused[2], fused[3]}}, reranker.recorded())
	require.Equal(t, []string{fused[1], fused[0], fused[3], fused[2]}, ids)
	require.False(t, page.HasMore)
	require.True(t, page.RankedPoolExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
}

// Every accepted window keeps its canonical outcome: a provider subset that
// omits rows completes them in fused order, provider filtering keeps the
// excluded row out, and a failed batch keeps reporting rerank_failed - all
// without submitting the same window again on a deeper pull.
func TestSearchTextContinuationRerankRetainsWindowOutcomes(t *testing.T) {
	testCases := []struct {
		name   string
		script func(candidates []RerankCandidate) ([]RerankResult, error)
		// want is the delivery order; wantCalls is one provider input per
		// window, in admission order.
		want      func(fused []string) []string
		wantCalls func(fused []string) [][]string
		// wantReason is the retained fallback reason every page reports.
		wantReason      SearchFallbackReason
		wantMethod      string
		wantTriggered   bool
		wantProviderHit int
	}{
		{
			name:   "subset omitted",
			script: firstOnlyRerankResult,
			want: func(fused []string) []string {
				return fused
			},
			wantCalls: func(fused []string) [][]string {
				return [][]string{{fused[0], fused[1]}, {fused[2], fused[3]}}
			},
			wantMethod:      "rrf_hybrid+rerank",
			wantProviderHit: 2,
		},
		{
			name: "provider filtered",
			script: func(candidates []RerankCandidate) ([]RerankResult, error) {
				results := make([]RerankResult, len(candidates))
				for index, candidate := range candidates {
					score := 1 - 0.1*float64(index)
					results[index] = RerankResult{
						ID: candidate.ID, OriginalRank: index + 1,
						CrossScore: score, FinalScore: score, Filtered: index == 0,
					}
				}
				return results, nil
			},
			want: func(fused []string) []string {
				return []string{fused[1], fused[3]}
			},
			wantCalls: func(fused []string) [][]string {
				return [][]string{{fused[0], fused[1]}, {fused[2], fused[3]}}
			},
			wantMethod:      "rrf_hybrid+rerank",
			wantProviderHit: 2,
		},
		{
			name: "provider failed",
			script: func([]RerankCandidate) ([]RerankResult, error) {
				return nil, errors.New("reranker unavailable")
			},
			want: func(fused []string) []string {
				return fused
			},
			wantCalls: func(fused []string) [][]string {
				return [][]string{{fused[0], fused[1]}, {fused[2], fused[3]}}
			},
			wantReason:      SearchFallbackRerankFailed,
			wantMethod:      "rrf_hybrid",
			wantTriggered:   true,
			wantProviderHit: 2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			service := newHybridRerankBudgetService(t, 4)
			service.SetSearchResultCachePolicy(0, 0)
			reranker := &scriptedWindowReranker{script: testCase.script}
			service.SetReranker(reranker)

			embedding := []float32{1, 0}
			options := DefaultSearchOptions()
			options.Limit = 2
			options.RerankEnabled = true
			options.RerankTopK = 3
			fused := deeperFusedIDs(t, service, options, embedding)
			require.Len(t, fused, 4)

			ids, page := collectContinuationIDs(t, service, options,
				SearchContinuationRequest{Owner: "retained-" + testCase.name, Database: "nornic", Mode: SearchContinuationRanked, N: 1},
				nil, func(context.Context, string) ([]float32, error) { return embedding, nil }, "library transcript")

			require.Equal(t, testCase.want(fused), ids)
			// The deeper pulls rebuilt the admitted windows from their retained
			// outcomes: each window reached the provider once.
			require.Equal(t, testCase.wantCalls(fused), reranker.recorded())
			require.Len(t, reranker.recorded(), testCase.wantProviderHit)
			require.Equal(t, testCase.wantMethod, page.SearchMethod)
			require.Equal(t, testCase.wantTriggered, page.FallbackTriggered)
			require.Equal(t, testCase.wantReason, page.FallbackReason)
			require.False(t, page.HasMore)
			require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
		})
	}
}

// A window whose every candidate already has a retained "no provider result"
// decision is completed in its own order, not rejected as an empty provider
// response; a window whose batch failed keeps reporting rerank_failed without
// repeating the call.
func TestRunRerankCompletesRetainedContinuationWindows(t *testing.T) {
	service := NewService(newNamespacedEngine(t), 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	candidates := []RerankCandidate{
		{ID: "alpha", Score: 0.9},
		{ID: "beta", Score: 0.8},
	}

	t.Run("omitted subset", func(t *testing.T) {
		memo := newRerankMemo()
		for _, candidate := range candidates {
			memo.putUnranked("query", candidate, false)
		}
		reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
		outcome := service.runRerank(context.Background(), reranker, "query", candidates, rerankRunOptions{
			providerLimit:      len(candidates),
			memo:               memo,
			continuationWindow: true,
			memoizedCandidates: len(candidates),
		})
		require.Empty(t, reranker.recorded())
		require.Equal(t, rerankMethodApplied, outcome.Method)
		require.Equal(t, SearchFallbackNone, outcome.FallbackReason)
		require.Equal(t, []string{"alpha", "beta"}, rerankResultIDs(outcome.Results))
	})

	t.Run("failed batch", func(t *testing.T) {
		memo := newRerankMemo()
		for _, candidate := range candidates {
			memo.putUnranked("query", candidate, true)
		}
		reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
		outcome := service.runRerank(context.Background(), reranker, "query", candidates, rerankRunOptions{
			providerLimit:      len(candidates),
			memo:               memo,
			continuationWindow: true,
			memoizedCandidates: len(candidates),
		})
		require.Empty(t, reranker.recorded())
		require.Equal(t, rerankMethodOriginal, outcome.Method)
		require.Equal(t, SearchFallbackRerankFailed, outcome.FallbackReason)
		require.Equal(t, []string{"alpha", "beta"}, rerankResultIDs(outcome.Results))
	})

	t.Run("mixed retained outcomes", func(t *testing.T) {
		memo := newRerankMemo()
		memo.putUnranked("query", candidates[0], false)
		memo.putUnranked("query", candidates[1], true)
		reranker := &scriptedWindowReranker{script: firstOnlyRerankResult}
		outcome := service.runRerank(context.Background(), reranker, "query", candidates, rerankRunOptions{
			providerLimit:      len(candidates),
			memo:               memo,
			continuationWindow: true,
			memoizedCandidates: len(candidates),
		})
		require.Empty(t, reranker.recorded())
		require.Equal(t, SearchFallbackRerankFailed, outcome.FallbackReason)
		require.Equal(t, []string{"alpha", "beta"}, rerankResultIDs(outcome.Results))
	})
}

// A single search and db.rerank keep their existing TopK cap: one bounded
// provider window and no more than TopK delivered rows.
func TestRerankOneShotKeepsTopKCap(t *testing.T) {
	service := newHybridRerankBudgetService(t, 5)
	service.SetSearchResultCachePolicy(0, 0)
	reranker := &windowRecordingReranker{}
	service.SetReranker(reranker)

	options := DefaultSearchOptions()
	options.Limit = 10
	options.RerankEnabled = true
	options.RerankTopK = 2
	response, err := service.Search(context.Background(), "library transcript", []float32{1, 0}, options)
	require.NoError(t, err)
	require.Equal(t, "rrf_hybrid+rerank", response.SearchMethod)
	require.Len(t, response.Results, 2)
	require.Len(t, reranker.recorded(), 1)
	require.Len(t, reranker.recorded()[0], 2)

	candidates := make([]RerankCandidate, 5)
	for index := range candidates {
		candidates[index] = RerankCandidate{ID: string(rune('a' + index)), Content: "content", Score: 1}
	}
	results, err := service.RerankCandidates(context.Background(), "query", candidates, options)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Len(t, reranker.recorded(), 2)
	require.Len(t, reranker.recorded()[1], 2)
}

func rerankResultIDs(results []RerankResult) []string {
	ids := make([]string, len(results))
	for index := range results {
		ids[index] = results[index].ID
	}
	return ids
}

// SearchQueryFunc is exported; external callbacks cannot fill sourceNode.
// Their bare hits must use the same metadata reader as service-owned hits.
func TestSearchTextContinuationReranksBareCallbackHits(t *testing.T) {
	service := multiChunkRerankService(t)
	provider := &windowRecordingReranker{reverse: true}
	service.SetReranker(provider)
	options := DefaultSearchOptions()
	options.Limit = 3
	options.RerankEnabled = true
	options.RerankTopK = 2
	querySearch := func(ctx context.Context, query string, embedding []float32, opts *SearchOptions) (*SearchResponse, error) {
		response, err := service.Search(ctx, query, embedding, opts)
		if response != nil {
			for i := range response.Results {
				response.Results[i].sourceNode = nil
			}
		}
		return response, err
	}
	page, err := service.SearchTextContinuation(t.Context(), "alpha beta", options, SearchContinuationRequest{Owner: "bare-callback", Database: "nornic", Mode: SearchContinuationRanked, N: 3}, multiChunkSplit, multiChunkEmbed, querySearch, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 3)
	require.Equal(t, 2, len(provider.recorded()))
	require.False(t, page.HasMore)
}

func TestSearchTextContinuationCancellationStopsLaterRerankWindows(t *testing.T) {
	service := multiChunkRerankService(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	provider := &scriptedWindowReranker{script: func(candidates []RerankCandidate) ([]RerankResult, error) {
		cancel()
		return firstOnlyRerankResult(candidates)
	}}
	service.SetReranker(provider)
	options := DefaultSearchOptions()
	options.Limit = 3
	options.RerankEnabled = true
	options.RerankTopK = 1
	page, err := service.SearchTextContinuation(ctx, "alpha beta", options, SearchContinuationRequest{Owner: "cancel-windows", Database: "nornic", Mode: SearchContinuationRanked, N: 3}, multiChunkSplit, multiChunkEmbed, service.Search, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, page, "a canceled stage cannot publish partial rerank windows")
	require.Len(t, provider.recorded(), 1, "cancellation stops before the next provider window")
}
