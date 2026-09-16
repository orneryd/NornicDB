package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestContinuationShortApproximateBatchDoesNotEndSearch(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 3; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	var depths []int
	search := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		depths = append(depths, opts.Limit)
		count := 1
		if opts.Limit >= 8 {
			count = 3
		}
		results := make([]SearchResult, count)
		for i := range results {
			id := fmt.Sprintf("doc-%d", i)
			results[i] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Results: results}, nil
	}
	request := SearchContinuationRequest{Owner: "alice", N: 2, MaxResults: 3}
	page, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 4}, request, nil, nil, search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 2)
	require.True(t, page.HasMore)
	require.Contains(t, depths, 8)
	request.QID = page.QID
	page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 1)
	require.Equal(t, "doc-2", page.Results[0].ID)
	require.False(t, page.HasMore)
	require.Equal(t, SearchContinuationMaxResultsComplete, page.Completion)
	require.Equal(t, []int{4, 8}, depths)
}

func TestContinuationFilteredBM25EnumeratesBeyondShortFirstBatch(t *testing.T) {
	for _, engine := range []string{"v1", "v2"} {
		t.Run(engine, func(t *testing.T) {
			t.Setenv("NORNICDB_SEARCH_BM25_ENGINE", engine)
			testContinuationFilteredBM25(t)
		})
	}
}

func testContinuationFilteredBM25(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 100; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%03d", i)), Properties: map[string]any{"content": "library searchable transcript"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	opts := DefaultSearchOptions()
	opts.Limit = 100
	all, err := service.Search(ctx, "transcript", nil, opts)
	require.NoError(t, err)
	require.Len(t, all.Results, 100)
	want := []string{searchResultID(all.Results[1]), searchResultID(all.Results[70])}
	for _, id := range want {
		node, err := engine.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		node.Properties["eligible"] = "yes"
		require.NoError(t, engine.UpdateNode(node))
	}
	opts = DefaultSearchOptions()
	opts.Limit = 4
	opts.AdaptiveOverfetch = false
	opts.InitialOverfetchRatio = 1
	opts.Filters = map[string][]string{"eligible": {"yes"}}
	page, err := service.SearchTextContinuation(ctx, "transcript", opts, SearchContinuationRequest{Owner: "alice", N: 2}, nil, nil, service.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	var got []string
	for _, result := range page.Results {
		got = append(got, searchResultID(result))
	}
	require.ElementsMatch(t, want, got)
	require.False(t, page.HasMore)
}

func TestContinuationUnknownExhaustionReachesExplicitBudget(t *testing.T) {
	service := NewService(storage.NewMemoryEngine())
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	var depths []int
	search := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		depths = append(depths, opts.Limit)
		return &SearchResponse{Results: []SearchResult{{ID: "doc"}}}, nil
	}
	page, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 2, MaxCandidateLimit: 16}, SearchContinuationRequest{Owner: "alice", N: 2}, nil, nil, search, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrCapacity)
	require.Nil(t, page, "a resource limit must not return a successful exhausted page")
	require.Equal(t, []int{2, 4, 8, 16}, depths)
}

func TestContinuationCanDeepenBeyondFormerEngineCeiling(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 8_000; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	var depths []int
	search := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		depths = append(depths, opts.Limit)
		results := make([]SearchResult, opts.Limit)
		for index := range results {
			id := fmt.Sprintf("doc-%d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Results: results}, nil
	}
	request := SearchContinuationRequest{Owner: "alice", N: 500, MaxResults: 6_000}
	page, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 4_000, MaxCandidateLimit: 10_000}, request, nil, nil, search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 500)
	require.True(t, page.HasMore)
	for page.HasMore {
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Equal(t, []int{4_000, 8_000}, depths)
	require.False(t, page.HasMore)
}

func TestContinuationShortExpansionDoesNotEndSearch(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 7; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", i))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	search := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		count := 4
		if opts.Limit >= 8 {
			count = 5
		}
		if opts.Limit >= 16 {
			count = 7
		}
		results := make([]SearchResult, count)
		for i := range results {
			id := fmt.Sprintf("doc-%d", i)
			results[i] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Results: results}, nil
	}
	request := SearchContinuationRequest{Owner: "alice", N: 2, MaxResults: 7}
	page, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 4}, request, nil, nil, search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	var ids []string
	for {
		for _, result := range page.Results {
			ids = append(ids, result.ID)
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		replay, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		require.Equal(t, page.Results, replay.Results)
	}
	require.Equal(t, []string{"doc-0", "doc-1", "doc-2", "doc-3", "doc-4", "doc-5", "doc-6"}, ids)
}

func TestContinuationVectorExhaustionRequiresExactCandidateEvidence(t *testing.T) {
	index := NewVectorIndex(2)
	hnsw := NewHNSWIndex(2, DefaultHNSWConfig())
	accelerated := gpu.NewEmbeddingIndex(nil, gpu.DefaultEmbeddingIndexConfig(2))
	t.Cleanup(accelerated.Release)
	require.NoError(t, index.Add("a", []float32{1, 0}))
	require.NoError(t, hnsw.Add("a", []float32{1, 0}))
	require.NoError(t, accelerated.Add("a", []float32{1, 0}))
	for _, tc := range []struct {
		name      string
		generator CandidateGenerator
		exhausted bool
	}{
		{"exact", NewBruteForceCandidateGen(index), true},
		{"GPU generator with supported CPU backend", NewGPUBruteForceCandidateGen(accelerated), true},
		{"fully explored HNSW", NewHNSWCandidateGen(hnsw), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipeline := NewVectorSearchPipeline(tc.generator, NewCPUExactScorer(index))
			results, exhausted, err := pipeline.searchWithExhaustion(context.Background(), []float32{1, 0}, 10, 0.5)
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, tc.exhausted, exhausted)
		})
	}
}

func TestContinuationHNSWShortThresholdBatchNeedsFullCoverage(t *testing.T) {
	index := NewHNSWIndex(2, DefaultHNSWConfig())
	for i := 0; i < 64; i++ {
		angle := float64(i) / 64
		require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), []float32{float32(math.Cos(angle)), float32(math.Sin(angle))}))
	}
	short, exhausted, err := index.searchWithEfExhaustion(context.Background(), []float32{1, 0}, 5, 0.99999, 5)
	require.NoError(t, err)
	require.Len(t, short, 1)
	require.False(t, exhausted, "a short result after threshold filtering is not corpus coverage")
	all, exhausted, err := index.searchWithEfExhaustion(context.Background(), []float32{1, 0}, 128, 0.99999, 128)
	require.NoError(t, err)
	require.Equal(t, short, all)
	require.True(t, exhausted, "the complete pre-filter heap supplies actual coverage evidence")
}

func TestContinuationDefaultHNSWFinishesFinitePopulation(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted=%t", deleted), func(t *testing.T) { testContinuationDefaultHNSWPopulation(t, deleted) })
	}
}

func testContinuationDefaultHNSWPopulation(t *testing.T, deleted bool) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 6; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), ChunkEmbeddings: [][]float32{{1, float32(i) / 10}}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	want := 6
	if deleted {
		_, err := service.Search(context.Background(), "", []float32{1, 0}, &SearchOptions{Limit: 6})
		require.NoError(t, err)
		require.NoError(t, engine.DeleteNode("doc-5"))
		require.NoError(t, service.RemoveNode("doc-5"))
		want = 5
	}
	embedCalls := 0
	embed := func(context.Context, string) ([]float32, error) { embedCalls++; return []float32{1, 0}, nil }
	request := SearchContinuationRequest{Owner: "alice", N: 2}
	page, err := service.SearchTextContinuation(context.Background(), "", &SearchOptions{Limit: 2}, request, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	ids := map[string]bool{}
	for {
		for _, result := range page.Results {
			id := searchResultID(result)
			require.False(t, ids[id], "duplicate %s", id)
			ids[id] = true
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Len(t, ids, want)
	require.Equal(t, 1, embedCalls)
}

type unavailableContinuationCandidates struct{}

func (unavailableContinuationCandidates) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	return nil, errors.New("vector retrieval unavailable")
}

func TestContinuationUnavailableEmbeddingPreservesBM25Fallback(t *testing.T) {
	for _, unavailable := range []string{"error", "empty", "retrieval"} {
		t.Run(unavailable, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			node := &storage.Node{ID: "doc", Properties: map[string]any{"content": "library transcript"}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, service.IndexNode(node))
			if unavailable == "retrieval" {
				service.vectorPipeline = NewVectorSearchPipeline(unavailableContinuationCandidates{}, &IdentityExactScorer{})
			}
			chunk := func(context.Context, string) ([]string, error) { return []string{"library", "transcript"}, nil }
			embed := func(context.Context, string) ([]float32, error) {
				if unavailable == "retrieval" {
					return []float32{1}, nil
				}
				if unavailable == "error" {
					return nil, errors.New("embedding provider unavailable")
				}
				return nil, nil
			}
			page, err := service.SearchTextContinuation(context.Background(), "library transcript", &SearchOptions{Limit: 2}, SearchContinuationRequest{Owner: "alice", N: 2}, chunk, embed, service.Search, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.Len(t, page.Results, 1)
			require.Equal(t, "doc", searchResultID(page.Results[0]))
			require.False(t, page.HasMore)
			switch unavailable {
			case "error":
				require.Equal(t, SearchFallbackQueryEmbeddingFailed, page.FallbackReason)
			case "empty":
				require.Equal(t, SearchFallbackQueryEmbeddingUnavailable, page.FallbackReason)
			case "retrieval":
				require.NotEqual(t, SearchFallbackNone, page.FallbackReason)
			}
			require.Equal(t, page.FallbackReason, page.SearchResponse().FallbackReason)
		})
	}
}

func TestContinuationChunkExhaustionSurvivesFusionAndFallback(t *testing.T) {
	chunk := func(context.Context, string) ([]string, error) { return []string{"one", "two"}, nil }
	embed := func(context.Context, string) ([]float32, error) { return []float32{1}, nil }
	for _, tc := range []struct {
		name  string
		known bool
		empty bool
	}{
		{"known", true, false}, {"unknown", false, false}, {"unknown with fallback", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fallback := &SearchResponse{RetrievalExhausted: true}
			search := func(_ context.Context, _ string, vector []float32, _ *SearchOptions) (*SearchResponse, error) {
				if len(vector) == 0 {
					return fallback, nil
				}
				response := &SearchResponse{RetrievalExhausted: tc.known}
				if !tc.empty {
					response.Results = []SearchResult{{ID: "a"}}
				}
				return response, nil
			}
			response, err := SearchTextChunks(context.Background(), "query", &SearchOptions{Limit: 2}, chunk, embed, search)
			require.NoError(t, err)
			require.Equal(t, tc.known, response.RetrievalExhausted)
			require.True(t, fallback.RetrievalExhausted, "shared callback response must not be mutated")
		})
	}
}

func TestContinuationChunkDepthIsNotCappedAtOneShotLimit(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 600; i++ {
		id := storage.NodeID(fmt.Sprintf("doc-%04d", i))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	embeds := 0
	search := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		results := make([]SearchResult, min(opts.Limit, 600))
		for i := range results {
			id := fmt.Sprintf("doc-%04d", i)
			results[i] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Results: results}, nil
	}
	chunk := func(context.Context, string) ([]string, error) { return []string{"one", "two"}, nil }
	embed := func(context.Context, string) ([]float32, error) { embeds++; return []float32{1}, nil }
	request := SearchContinuationRequest{Owner: "alice", N: 200, MaxResults: 400}
	page, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 200}, request, chunk, embed, search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 200)
	require.True(t, page.HasMore)
	request.QID = page.QID
	page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 200)
	require.False(t, page.HasMore)
	require.Equal(t, 2, embeds)
}
