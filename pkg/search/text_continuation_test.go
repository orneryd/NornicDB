package search

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type blockingContinuationEngine struct {
	storage.Engine
	streamer storage.StreamingEngine
	started  chan struct{}
	resume   chan struct{}
	once     sync.Once
}

type nonStreamingContinuationEngine struct {
	storage.Engine
}

type projectedContinuationEngine struct {
	storage.Engine
	calls      int
	prefixes   []string
	properties []string
}

func (e nonStreamingContinuationEngine) GraphMutationVersion() (uint64, bool) {
	return e.Engine.(storage.GraphMutationVersionProvider).GraphMutationVersion()
}

func (e *projectedContinuationEngine) GraphMutationVersion() (uint64, bool) {
	return e.Engine.(storage.GraphMutationVersionProvider).GraphMutationVersion()
}

func (e *projectedContinuationEngine) StreamNodesByPrefixProjected(ctx context.Context, prefix string, properties []string, visit func(*storage.Node) error) error {
	e.calls++
	e.prefixes = append(e.prefixes, prefix)
	e.properties = append([]string(nil), properties...)
	return storage.StreamNodesWithFallback(ctx, e.Engine, 1000, func(node *storage.Node) error {
		if !strings.HasPrefix(string(node.ID), prefix) {
			return nil
		}
		if properties == nil {
			return visit(node)
		}
		projected := *node
		projected.Properties = make(map[string]any, len(properties))
		for _, property := range properties {
			if value, ok := node.Properties[property]; ok {
				projected.Properties[property] = value
			}
		}
		return visit(&projected)
	})
}

func (e *blockingContinuationEngine) GraphMutationVersion() (uint64, bool) {
	return e.Engine.(storage.GraphMutationVersionProvider).GraphMutationVersion()
}

func (e *blockingContinuationEngine) StreamNodes(ctx context.Context, fn func(*storage.Node) error) error {
	return e.streamer.StreamNodes(ctx, func(node *storage.Node) error {
		e.once.Do(func() {
			close(e.started)
			<-e.resume
		})
		return fn(node)
	})
}

func (e *blockingContinuationEngine) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	return e.streamer.StreamEdges(ctx, fn)
}

func (e *blockingContinuationEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*storage.Node) error) error {
	return e.streamer.StreamNodeChunks(ctx, chunkSize, fn)
}

func TestSearchTextContinuationExpandsWithoutReembedding(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 30; index++ {
		id := storage.NodeID(fmt.Sprintf("node-%03d", index))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	var chunkCalls atomic.Int32
	var embedCalls atomic.Int32
	chunkQuery := func(context.Context, string) ([]string, error) {
		chunkCalls.Add(1)
		return []string{"one", "two"}, nil
	}
	embedQuery := func(_ context.Context, query string) ([]float32, error) {
		embedCalls.Add(1)
		if query == "one" {
			return []float32{1, 0}, nil
		}
		return []float32{0, 1}, nil
	}
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		results := make([]SearchResult, opts.Limit)
		for index := range results {
			id := fmt.Sprintf("node-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(opts.Limit - index)}
		}
		return &SearchResponse{Status: "success", Results: results, SearchMethod: "test"}, nil
	}

	request := SearchContinuationRequest{
		Owner:    "alice",
		Database: "nornic",
		N:        2,
	}
	options := &SearchOptions{Limit: 2}
	first, err := service.SearchTextContinuation(context.Background(), "query", options, request, chunkQuery, embedQuery, searchQuery, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, first.Results, 2)
	require.True(t, first.HasMore)
	require.NotEmpty(t, first.QID)

	request.QID = first.QID
	request.N = 4
	second, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, second.Results, 4)
	require.Equal(t, "node-002", second.Results[0].ID)
	seen := map[string]struct{}{}
	for _, page := range [][]SearchResult{first.Results, second.Results} {
		for _, result := range page {
			_, duplicate := seen[result.ID]
			require.False(t, duplicate, "duplicate result %s", result.ID)
			seen[result.ID] = struct{}{}
		}
	}
	require.Len(t, seen, 6)
	require.Equal(t, int32(1), chunkCalls.Load())
	require.Equal(t, int32(2), embedCalls.Load())
}

func TestSearchTextContinuationReusesRerankMemoAcrossExpansion(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 8; index++ {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%03d", index)), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	var memos []*rerankMemo
	searchQuery := func(ctx context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		memos = append(memos, rerankMemoFromContext(ctx))
		results := make([]SearchResult, opts.Limit)
		for index := range results {
			id := fmt.Sprintf("node-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Status: "success", Results: results, SearchMethod: "test"}, nil
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", N: 2}
	first, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 2}, request, nil, nil, searchQuery, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)

	request.QID = first.QID
	_, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(memos), 2)
	require.NotNil(t, memos[0])
	for _, memo := range memos[1:] {
		require.Same(t, memos[0], memo)
	}
}

func TestSearchTextContinuationMaxResultsCapsInitialPage(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 10; index++ {
		id := storage.NodeID(fmt.Sprintf("node-%03d", index))
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		results := make([]SearchResult, opts.Limit)
		for index := range results {
			id := fmt.Sprintf("node-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id)}
		}
		return &SearchResponse{Status: "success", Results: results, SearchMethod: "test"}, nil
	}

	page, err := service.SearchTextContinuation(
		context.Background(),
		"query",
		&SearchOptions{Limit: 10},
		SearchContinuationRequest{Owner: "alice", Database: "nornic", N: 10, MaxResults: 3},
		nil,
		nil,
		searchQuery,
		ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Len(t, page.Results, 3)
	require.False(t, page.HasMore)
	require.Empty(t, page.QID)
}

func TestSearchTextContinuationIDModeGroupsCompleteFilteredPopulation(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, node := range []*storage.Node{
		{ID: "frame-c", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-b"}},
		{ID: "frame-b", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "frame-a", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "winter", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "winter", "asset_id": "asset-c"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	request := SearchContinuationRequest{
		Owner:    "alice",
		Database: "nornic",
		Mode:     SearchContinuationID,
		GroupBy:  "asset_id",
		N:        1,
	}
	options := &SearchOptions{
		Types:   []string{"frame"},
		Filters: map[string][]string{"collection": {"summer"}},
	}
	first, err := service.SearchTextContinuation(context.Background(), "", options, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, first.Results, 1)
	require.Equal(t, "frame-a", first.Results[0].ID)
	require.Equal(t, "asset-a", first.Results[0].GroupKey)
	require.Equal(t, []string{"frame-a", "frame-b"}, passageIDs(first.Results[0].Passages))
	require.Equal(t, SearchContinuationCatalogPhase, first.Results[0].Phase)
	require.Equal(t, SearchContinuationID, first.Mode)
	require.Equal(t, 2, *first.EligibleCount)
	require.True(t, first.RankedPoolExhausted)
	require.False(t, first.CollectionExhausted)
	require.NotEmpty(t, first.QID)

	request.QID = first.QID
	second, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, second.Results, 1)
	require.Equal(t, "frame-c", second.Results[0].ID)
	require.Equal(t, "asset-b", second.Results[0].GroupKey)
	require.True(t, second.CollectionExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, second.Completion)
	require.Empty(t, second.QID)
}

func TestSearchTextContinuationIDModeProjectedBuildStillHydratesFullResults(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "nornic")
	for _, node := range []*storage.Node{
		{ID: "doc-a", Labels: []string{"Report"}, Properties: map[string]any{"asset_id": "asset-a", "status": "active", "type": "report", "secret": "hydrate-me"}},
		{ID: "doc-b", Labels: []string{"Report"}, Properties: map[string]any{"asset_id": "asset-b", "status": "inactive", "type": "report", "secret": "skip-me"}},
		{ID: "doc-c", Labels: []string{"Other"}, Properties: map[string]any{"asset_id": "asset-c", "status": "active", "type": "other", "secret": "skip-me-too"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	projected := &projectedContinuationEngine{Engine: engine}
	service := NewService(projected)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	options := &SearchOptions{
		Types:   []string{"report"},
		Filters: map[string][]string{"status": {"active"}},
	}
	page, err := service.SearchTextContinuation(
		context.Background(),
		"",
		options,
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 1},
		nil,
		nil,
		nil,
		ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, 1, projected.calls)
	require.Equal(t, []string{""}, projected.prefixes)
	require.ElementsMatch(t, []string{"asset_id", "status", "type"}, projected.properties)
	require.Len(t, page.Results, 1)
	require.Equal(t, "doc-a", page.Results[0].ID)
	require.Equal(t, "hydrate-me", page.Results[0].Properties["secret"])
}

func TestSearchTextContinuationRankedThenIDFreezesPrefixBeforeCatalog(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, node := range []*storage.Node{
		{ID: "frame-a", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "frame-b", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "frame-c", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-b"}},
		{ID: "frame-d", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer", "asset_id": "asset-c"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	var searchCalls atomic.Int32
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		searchCalls.Add(1)
		results := []SearchResult{
			{ID: "frame-b", NodeID: "frame-b", Score: 0.9},
			{ID: "frame-a", NodeID: "frame-a", Score: 0.85},
			{ID: "frame-c", NodeID: "frame-c", Score: 0.8},
		}
		return &SearchResponse{Results: results[:min(opts.Limit, len(results))], SearchMethod: "test"}, nil
	}
	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID,
		GroupBy: "asset_id", RankedLimit: 3, N: 2,
	}
	options := &SearchOptions{Limit: 1, Types: []string{"frame"}, Filters: map[string][]string{"collection": {"summer"}}}
	first, err := service.SearchTextContinuation(
		context.Background(), "query", options, request,
		func(context.Context, string) ([]string, error) { return []string{"query"}, nil },
		func(context.Context, string) ([]float32, error) { return []float32{1}, nil },
		searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"frame-b", "frame-c"}, []string{first.Results[0].ID, first.Results[1].ID})
	require.Equal(t, []string{"frame-b", "frame-a"}, passageIDs(first.Results[0].Passages))
	require.Equal(t, []string{SearchContinuationRankedPhase, SearchContinuationRankedPhase}, []string{first.Results[0].Phase, first.Results[1].Phase})
	require.Equal(t, 2, first.RankedCount)
	require.False(t, first.RankedPoolExhausted)
	require.False(t, first.CollectionExhausted)

	request.QID = first.QID
	last, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, last.Results, 1)
	require.Equal(t, "frame-d", last.Results[0].ID)
	require.Equal(t, "asset-c", last.Results[0].GroupKey)
	require.Equal(t, SearchContinuationCatalogPhase, last.Results[0].Phase)
	require.True(t, last.CollectionExhausted)
	require.Equal(t, int32(1), searchCalls.Load())
}

func TestSearchTextContinuationRankedThenIDUsesBranchLimitWhenRankedLimitOmitted(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	_, err := engine.CreateNode(&storage.Node{ID: "doc-a", Labels: []string{"Document"}, Properties: map[string]any{"content": "alpha"}})
	require.NoError(t, err)

	requestedLimits := []int{}
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		return &SearchResponse{
			Results:      []SearchResult{{ID: "doc-a", NodeID: "doc-a", Score: 1}},
			SearchMethod: "test", RetrievalExhausted: opts.Limit >= 2,
		}, nil
	}
	options := DefaultSearchOptions()
	options.Limit = 1
	page, err := service.SearchTextContinuation(
		context.Background(), "alpha", options,
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 1},
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, requestedLimits)
	require.Len(t, page.Results, 1)
}

func TestSearchTextContinuationRankedThenIDDeepensMultiChunkRankedPrefix(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 101; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%03d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedDepths []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		require.True(t, opts.continuation, "ranked_then_id preparation must deepen multi-chunk retrieval")
		requestedDepths = append(requestedDepths, opts.Limit)
		resultCount := min(opts.Limit, 101)
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(101 - index)}
		}
		return &SearchResponse{
			Results:            results,
			SearchMethod:       "chunk-test",
			RetrievalExhausted: opts.Limit >= 101,
		}, nil
	}

	options := DefaultSearchOptions()
	options.Limit = 1
	options.MaxCandidateLimit = 256
	page, err := service.SearchTextContinuation(
		context.Background(),
		"library transcript",
		options,
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 1},
		func(context.Context, string) ([]string, error) { return []string{"library", "transcript"}, nil },
		func(context.Context, string) ([]float32, error) { return []float32{1}, nil },
		searchQuery,
		ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	maxRequestedDepth := 0
	for _, depth := range requestedDepths {
		maxRequestedDepth = max(maxRequestedDepth, depth)
	}
	require.Equal(t, 128, maxRequestedDepth)
	require.Len(t, requestedDepths, 16, "both chunks should be searched at each preparation depth")
	require.Len(t, page.Results, 1)
	require.Equal(t, "doc-000", page.Results[0].ID)
	require.Equal(t, SearchContinuationRankedPhase, page.Results[0].Phase)
	require.Equal(t, 101, page.RankedCount)
	require.NotNil(t, page.EligibleCount)
	require.Equal(t, 101, *page.EligibleCount)
	require.True(t, page.RankedPoolExhausted)
	require.True(t, page.HasMore)
}

func TestSearchTextContinuationRankedThenIDStopsAtBoundedRankedPrefix(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 101; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%03d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	requestedLimits := []int{}
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := min(opts.Limit, 100)
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(100 - index)}
		}
		return &SearchResponse{
			Results:                results,
			SearchMethod:           "bounded-rerank-test",
			RetrievalExhausted:     false,
			CandidateBudgetReached: opts.Limit > 100,
		}, nil
	}

	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 1,
	}
	options := DefaultSearchOptions()
	options.Limit = 1
	options.MaxCandidateLimit = 1024
	first, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options, request,
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 4, 8, 16, 32, 64, 128}, requestedLimits)
	require.Len(t, first.Results, 1)
	require.Equal(t, "doc-000", first.Results[0].ID)
	require.Equal(t, SearchContinuationRankedPhase, first.Results[0].Phase)
	require.Equal(t, 100, first.RankedCount)
	require.NotNil(t, first.EligibleCount)
	require.Equal(t, 101, *first.EligibleCount)
	require.False(t, first.RankedPoolExhausted, "bounded ranked prefix must not masquerade as full retrieval exhaustion")
	require.False(t, first.CollectionExhausted)
	require.Equal(t, SearchContinuationMoreResults, first.Completion)
	require.True(t, first.HasMore)

	request.QID = first.QID
	request.N = 200
	second, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, second.Results, 100)
	require.Equal(t, "doc-001", second.Results[0].ID)
	require.Equal(t, SearchContinuationRankedPhase, second.Results[0].Phase)
	require.Equal(t, "doc-100", second.Results[len(second.Results)-1].ID)
	require.Equal(t, SearchContinuationCatalogPhase, second.Results[len(second.Results)-1].Phase)
	require.True(t, second.CollectionExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, second.Completion)
	require.False(t, second.HasMore)
}

func TestSearchTextContinuationRankedThenIDShortPrefixCanStillDeepen(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 3; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedLimits []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := 1
		if opts.Limit >= 8 {
			resultCount = 3
		}
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(3 - index)}
		}
		return &SearchResponse{
			Results:            results,
			SearchMethod:       "short-batch-test",
			RetrievalExhausted: opts.Limit >= 16,
		}, nil
	}

	options := DefaultSearchOptions()
	options.Limit = 4
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options,
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 10},
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []int{4, 8, 16}, requestedLimits)
	require.Len(t, page.Results, 3)
	require.Equal(t, 3, page.RankedCount)
	require.True(t, page.RankedPoolExhausted)
	require.NotNil(t, page.EligibleCount)
	require.Equal(t, 3, *page.EligibleCount)
	require.Equal(t, []string{"doc-0", "doc-1", "doc-2"}, []string{page.Results[0].ID, page.Results[1].ID, page.Results[2].ID})
	require.Equal(t, []string{SearchContinuationRankedPhase, SearchContinuationRankedPhase, SearchContinuationRankedPhase}, []string{page.Results[0].Phase, page.Results[1].Phase, page.Results[2].Phase})
}

func TestSearchTextContinuationRankedThenIDStopsAfterRepeatedNonGrowingExpansion(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 80; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%03d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedLimits []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := min(opts.Limit, 64)
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(80 - index)}
		}
		return &SearchResponse{
			Results:            results,
			SearchMethod:       "rrf_hybrid",
			RetrievalExhausted: false,
		}, nil
	}

	options := DefaultSearchOptions()
	options.Limit = 20
	options.MaxCandidateLimit = 4096
	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 5}
	first, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options, request,
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []int{20, 40, 80, 160, 320}, requestedLimits)
	require.Len(t, first.Results, 5)
	require.Equal(t, 64, first.RankedCount)
	require.NotNil(t, first.EligibleCount)
	require.Equal(t, 80, *first.EligibleCount)
	require.False(t, first.RankedPoolExhausted)
	require.False(t, first.CollectionExhausted)
	require.Equal(t, SearchContinuationMoreResults, first.Completion)
	require.True(t, first.HasMore)

	request.QID = first.QID
	request.N = 200
	second, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, second.Results, 75)
	require.Equal(t, 64, second.RankedCount)
	require.NotNil(t, second.EligibleCount)
	require.Equal(t, 80, *second.EligibleCount)
	require.Equal(t, SearchContinuationRankedPhase, second.Results[0].Phase)
	require.Equal(t, SearchContinuationCatalogPhase, second.Results[len(second.Results)-1].Phase)
	require.False(t, second.RankedPoolExhausted)
	require.True(t, second.CollectionExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, second.Completion)
	require.False(t, second.HasMore)
}

func TestSearchTextContinuationRankedStopsAfterRepeatedNonGrowingExpansion(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 80; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%03d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedLimits []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := min(opts.Limit, 64)
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(80 - index)}
		}
		return &SearchResponse{
			Results:            results,
			SearchMethod:       "rrf_hybrid",
			RetrievalExhausted: false,
		}, nil
	}

	options := DefaultSearchOptions()
	options.Limit = 20
	options.MaxCandidateLimit = 4096
	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRanked, N: 5}
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options, request,
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	ids := make([]string, 0, 64)
	for {
		for _, result := range page.Results {
			ids = append(ids, result.ID)
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		request.N = 25
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Equal(t, []int{20, 40, 80, 160, 320}, requestedLimits)
	require.Len(t, ids, 64)
	require.Equal(t, "doc-000", ids[0])
	require.Equal(t, "doc-063", ids[len(ids)-1])
	require.False(t, page.RankedPoolExhausted)
	require.False(t, page.CollectionExhausted)
	require.Equal(t, SearchContinuationCandidateComplete, page.Completion)
	require.False(t, page.HasMore)
}

func TestSearchTextContinuationNonGrowingExpansionGetsSecondChance(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 3; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedLimits []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := 1
		if opts.Limit >= 16 {
			resultCount = 3
		}
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(3 - index)}
		}
		return &SearchResponse{
			Results:            results,
			SearchMethod:       "rrf_hybrid",
			RetrievalExhausted: opts.Limit >= 64,
		}, nil
	}

	options := DefaultSearchOptions()
	options.Limit = 4
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", options,
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRankedThenID, N: 10},
		nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []int{4, 8, 16, 32, 64}, requestedLimits)
	require.Len(t, page.Results, 3)
	require.Equal(t, 3, page.RankedCount)
	require.True(t, page.RankedPoolExhausted)
	require.NotNil(t, page.EligibleCount)
	require.Equal(t, 3, *page.EligibleCount)
}

func TestSearchTextContinuationPlateauInferenceMethods(t *testing.T) {
	tests := []struct {
		method string
		want   bool
	}{
		{method: "rrf_hybrid", want: true},
		{method: "rrf_hybrid+rerank", want: true},
		{method: "chunked_rrf_hybrid", want: true},
		{method: "vector_hnsw", want: true},
		{method: "vector_ivf_hnsw", want: true},
		{method: "vector_ivfpq", want: true},
		{method: "vector_clustered", want: true},
		{method: "fulltext", want: false},
		{method: "vector_brute", want: false},
		{method: "vector_gpu_brute", want: false},
		{method: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			require.Equal(t, tt.want, continuationCanInferCandidateBudget(tt.method))
		})
	}
}

func TestSearchTextContinuationRankedStopsAtDeclaredCandidateBudget(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for index := 0; index < 100; index++ {
		id := storage.NodeID(fmt.Sprintf("doc-%03d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
	}

	var requestedLimits []int
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		requestedLimits = append(requestedLimits, opts.Limit)
		resultCount := min(opts.Limit, 100)
		results := make([]SearchResult, resultCount)
		for index := range results {
			id := fmt.Sprintf("doc-%03d", index)
			results[index] = SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(100 - index)}
		}
		return &SearchResponse{
			Results:                results,
			SearchMethod:           "bounded-ranked-test",
			RetrievalExhausted:     false,
			CandidateBudgetReached: opts.Limit > 100,
		}, nil
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRanked, N: 1}
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", &SearchOptions{Limit: 1, MaxCandidateLimit: 1024},
		request, nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	ids := []string{}
	for {
		for _, result := range page.Results {
			ids = append(ids, result.ID)
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		request.N = 25
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Len(t, ids, 100)
	require.Equal(t, "doc-000", ids[0])
	require.Equal(t, "doc-099", ids[len(ids)-1])
	require.Equal(t, []int{1, 2, 4, 8, 16, 32, 64, 128}, requestedLimits)
	require.Equal(t, SearchContinuationCandidateComplete, page.Completion)
	require.False(t, page.RankedPoolExhausted)

	request.N = 25
	replay, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Equal(t, page.Results, replay.Results)
	require.Equal(t, []int{1, 2, 4, 8, 16, 32, 64, 128}, requestedLimits)
}

func TestSearchTextContinuationRerankBudgetBoundary(t *testing.T) {
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			for _, doc := range []struct {
				id      storage.NodeID
				content string
				vector  []float32
			}{
				{id: "doc-a", content: "library transcript alpha", vector: []float32{1, 0}},
				{id: "doc-b", content: "library transcript beta", vector: []float32{0.8, 0.2}},
				{id: "doc-c", content: "library transcript gamma", vector: []float32{0.6, 0.4}},
			} {
				_, err := engine.CreateNode(&storage.Node{
					ID: doc.id, Labels: []string{"Document"}, Properties: map[string]any{"content": doc.content},
				})
				require.NoError(t, err)
				require.NoError(t, service.vectorIndex.Add(string(doc.id), doc.vector))
			}
			service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
			service.SetReranker(&testReranker{enabled: true})

			options := DefaultSearchOptions()
			options.Limit = 10
			options.RerankEnabled = true
			options.RerankTopK = 2
			request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: mode, N: 1}
			if mode == SearchContinuationRankedThenID {
				request.N = 10
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "library transcript", options, request,
				nil,
				func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil },
				service.Search,
				ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			require.False(t, page.RankedPoolExhausted)
			require.Equal(t, "rrf_hybrid+rerank", page.SearchMethod)

			if mode == SearchContinuationRanked {
				require.Len(t, page.Results, 1)
				require.True(t, page.HasMore)
				request.QID = page.QID
				request.N = 10
				page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
				require.Len(t, page.Results, 1)
				require.False(t, page.HasMore)
				require.Equal(t, SearchContinuationCandidateComplete, page.Completion)
				require.False(t, page.RankedPoolExhausted)
				return
			}

			require.Len(t, page.Results, 3)
			require.Equal(t, 2, page.RankedCount)
			require.NotNil(t, page.EligibleCount)
			require.Equal(t, 3, *page.EligibleCount)
			require.Equal(t, []string{SearchContinuationRankedPhase, SearchContinuationRankedPhase, SearchContinuationCatalogPhase}, []string{
				page.Results[0].Phase, page.Results[1].Phase, page.Results[2].Phase,
			})
			require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
			require.False(t, page.HasMore)
		})
	}
}

func TestSearchTextContinuationRerankBudgetDeepensShallowStartsToTopK(t *testing.T) {
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			service := newHybridRerankBudgetService(t, 30)
			options := DefaultSearchOptions()
			options.Limit = 1
			options.RerankEnabled = true
			options.RerankTopK = 10
			options.MaxCandidateLimit = 64

			request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: mode, N: 5}
			if mode == SearchContinuationRankedThenID {
				request.N = 12
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "library transcript", options, request,
				nil, embed, service.Search, ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			require.Equal(t, "rrf_hybrid+rerank", page.SearchMethod)
			require.False(t, page.RankedPoolExhausted)

			if mode == SearchContinuationRankedThenID {
				require.Equal(t, 10, page.RankedCount)
				require.NotNil(t, page.EligibleCount)
				require.Equal(t, 30, *page.EligibleCount)
				require.Len(t, page.Results, 12)
				require.Equal(t, SearchContinuationRankedPhase, page.Results[0].Phase)
				require.Equal(t, SearchContinuationRankedPhase, page.Results[9].Phase)
				require.Equal(t, SearchContinuationCatalogPhase, page.Results[10].Phase)
				require.True(t, page.HasMore)
				return
			}

			seen := map[string]struct{}{}
			for _, result := range page.Results {
				seen[result.ID] = struct{}{}
			}
			require.Len(t, page.Results, 5)
			require.True(t, page.HasMore)

			request.QID = page.QID
			request.N = 10
			page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			for _, result := range page.Results {
				seen[result.ID] = struct{}{}
			}
			require.Len(t, page.Results, 5)
			require.Len(t, seen, 10)
			require.False(t, page.HasMore)
			require.Equal(t, SearchContinuationCandidateComplete, page.Completion)
			require.False(t, page.RankedPoolExhausted)
		})
	}
}

func TestSearchTextContinuationRankedNaturalExhaustionCompletion(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	const resultCount = 16
	allResults := make([]SearchResult, resultCount)
	for index := range allResults {
		id := storage.NodeID(fmt.Sprintf("doc-%02d", index))
		_, err := engine.CreateNode(&storage.Node{
			ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "library transcript"},
		})
		require.NoError(t, err)
		allResults[index] = SearchResult{ID: string(id), NodeID: id, Score: float64(resultCount - index), Phase: SearchContinuationRankedPhase}
	}

	calls := 0
	searchQuery := func(_ context.Context, _ string, _ []float32, opts *SearchOptions) (*SearchResponse, error) {
		calls++
		limit := min(opts.Limit, len(allResults))
		return &SearchResponse{
			Results:            append([]SearchResult(nil), allResults[:limit]...),
			SearchMethod:       "ranked-natural-exhaustion-test",
			RetrievalExhausted: opts.Limit >= len(allResults),
		}, nil
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationRanked, N: 5}
	page, err := service.SearchTextContinuation(
		context.Background(), "library transcript", &SearchOptions{Limit: 20},
		request, nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)

	pageSizes := []int{len(page.Results)}
	var ids []string
	for {
		for _, result := range page.Results {
			ids = append(ids, result.ID)
			require.Equal(t, SearchContinuationRankedPhase, result.Phase)
		}
		if !page.HasMore {
			break
		}
		require.Equal(t, SearchContinuationMoreResults, page.Completion)
		require.True(t, page.RankedPoolExhausted)
		require.False(t, page.CollectionExhausted)

		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		pageSizes = append(pageSizes, len(page.Results))
	}

	require.Equal(t, []int{5, 5, 5, 1}, pageSizes)
	require.Len(t, ids, resultCount)
	require.Equal(t, "doc-00", ids[0])
	require.Equal(t, "doc-15", ids[len(ids)-1])
	require.Equal(t, 1, calls)
	require.Equal(t, resultCount, page.RankedCount)
	require.True(t, page.RankedPoolExhausted)
	require.True(t, page.CollectionExhausted)
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
}

func TestSearchTextContinuationRankedPullRehydratesAndAuthorizes(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []storage.NodeID{"doc-a", "doc-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}, Properties: map[string]any{"version": "current"}})
		require.NoError(t, err)
	}
	var allowSecond atomic.Bool
	allowSecond.Store(true)
	searchQuery := func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
		return &SearchResponse{Results: []SearchResult{
			{ID: "doc-a", NodeID: "doc-a", Score: 1, Properties: map[string]any{"version": "stale"}},
			{ID: "doc-b", NodeID: "doc-b", Score: .9, Properties: map[string]any{"version": "stale"}},
		}, RetrievalExhausted: true}, nil
	}
	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", N: 1,
		AuthorizeNode: func(node *storage.Node) (bool, error) { return node.ID != "doc-b" || allowSecond.Load(), nil },
	}
	first, err := service.SearchTextContinuation(context.Background(), "query", &SearchOptions{Limit: 2}, request, nil, nil, searchQuery, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Equal(t, "current", first.Results[0].Properties["version"])

	allowSecond.Store(false)
	request.QID = first.QID
	_, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}

func TestSearchTextContinuationIDModeMaxResultsReturnsSortedPrefix(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []storage.NodeID{"doc-c", "doc-a", "doc-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	page, err := service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 2, MaxResults: 2},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"doc-a", "doc-b"}, []string{page.Results[0].ID, page.Results[1].ID})
	require.Equal(t, 3, *page.EligibleCount)
	require.False(t, page.HasMore)
	require.False(t, page.CollectionExhausted)
	require.Equal(t, "max_results_reached", page.Completion)
}

func TestSearchTextContinuationMaxResultsReportsCeilingNotExhaustion(t *testing.T) {
	for _, mode := range []SearchContinuationMode{SearchContinuationID, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			for _, id := range []storage.NodeID{"doc-a", "doc-b", "doc-c"} {
				_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}, Properties: map[string]any{"content": "alpha"}})
				require.NoError(t, err)
			}
			searchQuery := func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
				return &SearchResponse{Results: []SearchResult{
					{ID: "doc-a", NodeID: "doc-a", Score: 3},
					{ID: "doc-b", NodeID: "doc-b", Score: 2},
					{ID: "doc-c", NodeID: "doc-c", Score: 1},
				}, RetrievalExhausted: true}, nil
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "alpha", DefaultSearchOptions(),
				SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: mode, N: 2, MaxResults: 2, RankedLimit: 3},
				nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			require.Len(t, page.Results, 2)
			require.False(t, page.HasMore)
			require.NotNil(t, page.EligibleCount)
			require.Equal(t, 3, *page.EligibleCount)
			require.False(t, page.CollectionExhausted)
			require.Equal(t, "max_results_reached", page.Completion)
		})
	}
}

func TestSearchTextContinuationHydratesCanonicalDisplayFields(t *testing.T) {
	for _, mode := range []SearchContinuationMode{SearchContinuationID, SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			_, err := engine.CreateNode(&storage.Node{
				ID:     "doc-a",
				Labels: []string{"Document"},
				Properties: map[string]any{
					"type":        "transcript",
					"title":       "Known title",
					"description": "Known description",
					"content":     "alpha content",
				},
			})
			require.NoError(t, err)
			searchQuery := func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
				return &SearchResponse{Results: []SearchResult{{ID: "doc-a", NodeID: "doc-a", Score: 1}}, RetrievalExhausted: true}, nil
			}
			page, err := service.SearchTextContinuation(
				context.Background(), "alpha", DefaultSearchOptions(),
				SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: mode, N: 1, RankedLimit: 1},
				nil, nil, searchQuery, ChunkedSearchErrorPolicy{},
			)
			require.NoError(t, err)
			require.Len(t, page.Results, 1)
			require.Equal(t, "transcript", page.Results[0].Type)
			require.Equal(t, "Known title", page.Results[0].Title)
			require.Equal(t, "Known description", page.Results[0].Description)
			require.Equal(t, "alpha content", page.Results[0].ContentPreview)
			require.Equal(t, "transcript", page.Results[0].Properties["type"])
		})
	}
}

func TestSearchTextContinuationPreservesCanonicalResponseMetadata(t *testing.T) {
	for _, mode := range []SearchContinuationMode{SearchContinuationRanked, SearchContinuationRankedThenID} {
		t.Run(string(mode), func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "query"}})
				require.NoError(t, err)
			}
			retrieve := func(context.Context, string, []float32, *SearchOptions) (*SearchResponse, error) {
				return &SearchResponse{Results: []SearchResult{{ID: "a", NodeID: "a"}, {ID: "b", NodeID: "b"}}, Message: "provider outcome", RetrievalExhausted: true}, nil
			}
			request := SearchContinuationRequest{Owner: "alice", Mode: mode, N: 1, RankedLimit: 2}
			page, err := service.SearchTextContinuation(context.Background(), "query", DefaultSearchOptions(), request, nil, nil, retrieve, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.Equal(t, "provider outcome", page.SearchResponse().Message)
			require.Len(t, page.SearchResponse().Results, 1)
			request.QID = page.QID
			page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			require.Equal(t, "provider outcome", page.SearchResponse().Message)
			require.Len(t, page.SearchResponse().Results, 1)
		})
	}
}

func TestSearchTextContinuationIDModeCountsRejectedNodesAgainstScanLimit(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxScannedNodes: 2})
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, node := range []*storage.Node{
		{ID: "winter-a", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "winter"}},
		{ID: "winter-b", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "winter"}},
		{ID: "summer", Labels: []string{"Frame"}, Properties: map[string]any{"collection": "summer"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	_, err := service.SearchTextContinuation(
		context.Background(), "",
		&SearchOptions{Filters: map[string][]string{"collection": {"summer"}}},
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.ErrorIs(t, err, resultstream.ErrCapacity)
}

func TestSearchTextContinuationIDModeFailsInsteadOfTruncatingLogicalMembers(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxMembers: 1})
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, node := range []*storage.Node{
		{ID: "frame-a", Labels: []string{"Frame"}, Properties: map[string]any{"asset_id": "asset-a"}},
		{ID: "frame-b", Labels: []string{"Frame"}, Properties: map[string]any{"asset_id": "asset-b"}},
	} {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}

	_, err := service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.True(t, errors.Is(err, resultstream.ErrCapacity), "expected capacity error, got %v", err)
}

func TestSearchTextContinuationIDModeBoundsGroupedPassageFanout(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxMembers: 1, MaxPassages: 2})
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, id := range []string{"frame-a", "frame-b", "frame-c"} {
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Frame"},
			Properties: map[string]any{"asset_id": "asset-a"},
		})
		require.NoError(t, err)
	}

	_, err := service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.ErrorIs(t, err, resultstream.ErrCapacity)
}

func TestSearchTextContinuationIDModeRejectsBuildWhenAdmissionIsFull(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxConcurrentBuilds: 1})
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	_, _, admitted := service.acquireCompleteContinuationBuild()
	require.True(t, admitted)
	t.Cleanup(func() { service.completeBuilds.Add(-1) })

	_, err := service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.ErrorIs(t, err, resultstream.ErrCapacity)
}

func TestSearchTextContinuationIDModeBoundsRetainedDescriptorBytes(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxBuildBytes: 1})
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	_, err := engine.CreateNode(&storage.Node{ID: "node-a", Labels: []string{"Document"}})
	require.NoError(t, err)

	_, err = service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.ErrorIs(t, err, resultstream.ErrCapacity)
}

func TestSearchTextContinuationIDModeBoundsBuildTime(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxBuildDuration: time.Nanosecond})
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	_, err := engine.CreateNode(&storage.Node{ID: "node-a", Labels: []string{"Document"}})
	require.NoError(t, err)

	_, err = service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.ErrorIs(t, err, resultstream.ErrCapacity)
}

func TestSearchTextContinuationIDModeGroupingIsIndependentOfScanOrder(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	for _, idAndAsset := range [][2]string{
		{"frame-z", "asset-b"},
		{"frame-c", "asset-a"},
		{"frame-y", "asset-b"},
		{"frame-a", "asset-a"},
		{"frame-x", "asset-b"},
		{"frame-b", "asset-a"},
	} {
		_, err := engine.CreateNode(&storage.Node{
			ID: storage.NodeID(idAndAsset[0]), Labels: []string{"Frame"},
			Properties: map[string]any{"asset_id": idAndAsset[1]},
		})
		require.NoError(t, err)
	}

	page, err := service.SearchTextContinuation(
		context.Background(), "", DefaultSearchOptions(),
		SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 10},
		nil, nil, nil, ChunkedSearchErrorPolicy{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"asset-a", "asset-b"}, []string{page.Results[0].GroupKey, page.Results[1].GroupKey})
	require.Equal(t, []string{"frame-a", "frame-b", "frame-c"}, passageIDs(page.Results[0].Passages))
	require.Equal(t, []string{"frame-x", "frame-y", "frame-z"}, passageIDs(page.Results[1].Passages))
}

func TestSearchTextContinuationIDModeRejectsInvalidGroupKeys(t *testing.T) {
	testCases := []struct {
		name       string
		properties map[string]any
	}{
		{name: "missing", properties: map[string]any{}},
		{name: "empty", properties: map[string]any{"asset_id": ""}},
		{name: "non-string", properties: map[string]any{"asset_id": 42}},
		{name: "invalid UTF-8", properties: map[string]any{"asset_id": string([]byte{0xff})}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewService(engine)
			t.Cleanup(func() { require.NoError(t, service.Close()) })
			_, err := engine.CreateNode(&storage.Node{ID: "frame-a", Labels: []string{"Frame"}, Properties: testCase.properties})
			require.NoError(t, err)

			_, err = service.SearchTextContinuation(
				context.Background(), "", DefaultSearchOptions(),
				SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 1},
				nil, nil, nil, ChunkedSearchErrorPolicy{},
			)
			require.ErrorContains(t, err, `group property "asset_id" must be a nonempty UTF-8 string`)
		})
	}
}

func TestSearchTextContinuationIDModeInvalidatesAfterMutation(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"node-a", "node-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1}
	first, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, first.QID)
	_, err = engine.CreateNode(&storage.Node{ID: "node-c", Labels: []string{"Document"}})
	require.NoError(t, err)

	request.QID = first.QID
	_, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}

func TestSearchTextContinuationIDModeRejectsMutationDuringBuild(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	_, err := engine.CreateNode(&storage.Node{ID: "node-a", Labels: []string{"Document"}})
	require.NoError(t, err)
	blocking := &blockingContinuationEngine{
		Engine: engine, streamer: engine,
		started: make(chan struct{}), resume: make(chan struct{}),
	}
	service := NewService(blocking)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	result := make(chan error, 1)
	go func() {
		_, searchErr := service.SearchTextContinuation(
			context.Background(), "", DefaultSearchOptions(),
			SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1},
			nil, nil, nil, ChunkedSearchErrorPolicy{},
		)
		result <- searchErr
	}()
	<-blocking.started
	_, err = engine.CreateNode(&storage.Node{ID: "node-b", Labels: []string{"Document"}})
	require.NoError(t, err)
	close(blocking.resume)
	require.ErrorIs(t, <-result, resultstream.ErrInvalidated)
}

func TestSearchTextContinuationIDModeInvalidatesAfterPolicyChange(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"node-a", "node-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1}
	first, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.NotEmpty(t, first.QID)
	service.SetCompleteContinuationPolicy(CompleteContinuationPolicy{MaxMembers: 10})

	request.QID = first.QID
	_, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}

func TestSearchTextContinuationIDModeAuthorizesBuildAndHydration(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"allowed-a", "allowed-b", "denied"} {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Document"}})
		require.NoError(t, err)
	}

	var allowSecond atomic.Bool
	allowSecond.Store(true)
	authorize := func(node *storage.Node) (bool, error) {
		if node.ID == "denied" {
			return false, nil
		}
		return node.ID != "allowed-b" || allowSecond.Load(), nil
	}
	request := SearchContinuationRequest{
		Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 1,
		AuthorizeNode: authorize,
	}
	first, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Equal(t, "allowed-a", first.Results[0].ID)
	require.Equal(t, 2, *first.EligibleCount)
	require.True(t, first.HasMore)

	allowSecond.Store(false)
	request.QID = first.QID
	_, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.ErrorIs(t, err, resultstream.ErrInvalidated)
}

func TestCatalogContinuationCloseDoesNotWaitForHydration(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	for _, id := range []storage.NodeID{"doc-a", "doc-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Document"}})
		require.NoError(t, err)
	}
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	stream, err := service.newIDContinuationStream(context.Background(), *DefaultSearchOptions(), SearchContinuationRequest{
		Mode: SearchContinuationID,
	})
	require.NoError(t, err)
	catalog := stream.(*catalogContinuationStream)
	started := make(chan struct{})
	release := make(chan struct{})
	catalog.authorizeNode = func(*storage.Node) (bool, error) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return true, nil
	}
	pullDone := make(chan error, 1)
	go func() {
		_, pullErr := catalog.Pull(context.Background(), 0, 1)
		pullDone <- pullErr
	}()
	<-started
	closeDone := make(chan error, 1)
	go func() { closeDone <- catalog.Close() }()
	select {
	case closeErr := <-closeDone:
		require.NoError(t, closeErr)
	case <-time.After(time.Second):
		close(release)
		<-closeDone
		t.Fatal("Close blocked on page hydration")
	}
	close(release)
	require.NoError(t, <-pullDone)
}

func TestSearchTextContinuationIDModeEnumeratesExactlyTwentyThousandMembers(t *testing.T) {
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	nodes := make([]*storage.Node, 20_000)
	for index := range nodes {
		nodes[index] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%05d", index)), Labels: []string{"Document"}}
	}
	for start := 0; start < len(nodes); start += 500 {
		require.NoError(t, engine.BulkCreateNodes(nodes[start:min(start+500, len(nodes))]))
	}

	request := SearchContinuationRequest{Owner: "alice", Database: "nornic", Mode: SearchContinuationID, N: 500}
	seen := make(map[string]struct{}, len(nodes))
	for {
		page, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
		for _, result := range page.Results {
			_, duplicate := seen[result.ID]
			require.False(t, duplicate, "duplicate member %s", result.ID)
			seen[result.ID] = struct{}{}
		}
		if !page.HasMore {
			require.Equal(t, 20_000, *page.EligibleCount)
			break
		}
		request.QID = page.QID
	}
	require.Len(t, seen, 20_000)
}

func passageIDs(passages []SearchPassage) []string {
	ids := make([]string, len(passages))
	for index := range passages {
		ids[index] = passages[index].ID
	}
	return ids
}

func BenchmarkSearchTextContinuationCompleteBuild(b *testing.B) {
	b.Run("ID/20000_members", func(b *testing.B) {
		engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		service := NewService(engine)
		b.Cleanup(func() { _ = service.Close() })
		nodes := make([]*storage.Node, 20_000)
		for index := range nodes {
			nodes[index] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("node-%05d", index)), Labels: []string{"Document"}}
		}
		for start := 0; start < len(nodes); start += 500 {
			if err := engine.BulkCreateNodes(nodes[start:min(start+500, len(nodes))]); err != nil {
				b.Fatal(err)
			}
		}
		request := SearchContinuationRequest{Owner: "benchmark", Database: "nornic", Mode: SearchContinuationID, N: 500}
		b.ReportAllocs()
		b.ReportMetric(float64(len(nodes)), "members/op")
		b.ResetTimer()
		for b.Loop() {
			page, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			if err != nil {
				b.Fatal(err)
			}
			if page.QID != "" {
				request.QID, request.Discard = page.QID, true
				if _, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{}); err != nil {
					b.Fatal(err)
				}
				request.QID, request.Discard = "", false
			}
		}
	})

	b.Run("ID_grouped/1000_passages", func(b *testing.B) {
		engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
		service := NewService(engine)
		b.Cleanup(func() { _ = service.Close() })
		nodes := make([]*storage.Node, 1_000)
		for index := range nodes {
			nodes[index] = &storage.Node{
				ID: storage.NodeID(fmt.Sprintf("frame-%04d", index)), Labels: []string{"Frame"},
				Properties: map[string]any{"asset_id": "asset-a"},
			}
		}
		for start := 0; start < len(nodes); start += 500 {
			if err := engine.BulkCreateNodes(nodes[start:min(start+500, len(nodes))]); err != nil {
				b.Fatal(err)
			}
		}
		request := SearchContinuationRequest{Owner: "benchmark", Database: "nornic", Mode: SearchContinuationID, GroupBy: "asset_id", N: 1}
		b.ReportAllocs()
		b.ReportMetric(float64(len(nodes)), "passages/op")
		b.ResetTimer()
		for b.Loop() {
			if _, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSearchTextContinuationCompleteBuildBadger(b *testing.B) {
	b.Run("ID/2000_members_with_embeddings", func(b *testing.B) {
		base, err := storage.NewBadgerEngine(b.TempDir())
		require.NoError(b, err)
		engine := storage.NewNamespacedEngine(base, "nornic")
		service := NewService(engine)
		b.Cleanup(func() {
			_ = service.Close()
			_ = base.Close()
		})
		nodes := make([]*storage.Node, 2_000)
		for index := range nodes {
			nodes[index] = &storage.Node{
				ID:              storage.NodeID(fmt.Sprintf("node-%05d", index)),
				Labels:          []string{"Document"},
				Properties:      map[string]any{"content": "library transcript", "asset_id": "asset-a"},
				ChunkEmbeddings: [][]float32{benchmarkContinuationEmbedding(index)},
			}
		}
		for start := 0; start < len(nodes); start += 250 {
			require.NoError(b, engine.BulkCreateNodes(nodes[start:min(start+250, len(nodes))]))
		}
		request := SearchContinuationRequest{Owner: "benchmark", Database: "nornic", Mode: SearchContinuationID, N: 500}
		b.ReportAllocs()
		b.ReportMetric(float64(len(nodes)), "nodes/op")
		b.ResetTimer()
		for b.Loop() {
			page, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
			if err != nil {
				b.Fatal(err)
			}
			if page.QID != "" {
				request.QID, request.Discard = page.QID, true
				if _, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{}); err != nil {
					b.Fatal(err)
				}
				request.QID, request.Discard = "", false
			}
		}
	})
}

func benchmarkContinuationEmbedding(seed int) []float32 {
	embedding := make([]float32, 64)
	for index := range embedding {
		embedding[index] = float32((seed+index)%17) / 17
	}
	return embedding
}

func BenchmarkSearchTextContinuationScanPath(b *testing.B) {
	base := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	nodes := make([]*storage.Node, 20_000)
	for index := range nodes {
		nodes[index] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("scan-%05d", index)), Labels: []string{"Document"}}
	}
	for start := 0; start < len(nodes); start += 500 {
		if err := base.BulkCreateNodes(nodes[start:min(start+500, len(nodes))]); err != nil {
			b.Fatal(err)
		}
	}
	benchmarks := []struct {
		name   string
		engine storage.Engine
	}{
		{name: "native_streaming", engine: base},
		{name: "AllNodes_fallback", engine: nonStreamingContinuationEngine{Engine: base}},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			service := NewService(benchmark.engine)
			b.Cleanup(func() { _ = service.Close() })
			request := SearchContinuationRequest{Owner: "benchmark", Database: "nornic", Mode: SearchContinuationID, N: 500}
			b.ReportAllocs()
			b.ReportMetric(float64(len(nodes)), "nodes/op")
			for b.Loop() {
				page, err := service.SearchTextContinuation(context.Background(), "", DefaultSearchOptions(), request, nil, nil, nil, ChunkedSearchErrorPolicy{})
				if err != nil {
					b.Fatal(err)
				}
				if page.QID != "" {
					request.QID, request.Discard = page.QID, true
					if _, err := service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{}); err != nil {
						b.Fatal(err)
					}
					request.QID, request.Discard = "", false
				}
			}
		})
	}
}
