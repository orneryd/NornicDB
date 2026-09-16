package nornicgrpc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func mustCountTestTokens(text string) int {
	tokens, err := countTestTokens(text)
	if err != nil {
		panic(err)
	}
	return tokens
}

func chunkTestText(ctx context.Context, text string) ([]string, error) {
	_ = ctx
	return textchunk.ChunkByTokenCount(text, 512, 50, countTestTokens)
}

type stubSearcher struct {
	lastQuery     string
	lastEmbedding []float32
	lastOpts      *search.SearchOptions

	resp *search.SearchResponse
	err  error
}

type continuationStubSearcher struct {
	stubSearcher
	requests  []search.SearchContinuationRequest
	queries   []string
	responses []*search.SearchContinuationPage
	err       error
}

func (s *continuationStubSearcher) SearchTextContinuation(
	_ context.Context,
	query string,
	_ *search.SearchOptions,
	request search.SearchContinuationRequest,
	_ search.ChunkQueryFunc,
	_ search.EmbedQueryFunc,
	_ search.SearchQueryFunc,
	_ search.ChunkedSearchErrorPolicy,
) (*search.SearchContinuationPage, error) {
	s.queries = append(s.queries, query)
	s.requests = append(s.requests, request)
	if s.err != nil {
		return nil, s.err
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, nil
}

func (s *stubSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	s.lastQuery = query
	s.lastEmbedding = embedding
	s.lastOpts = opts
	return s.resp, s.err
}

type sequenceSearcher struct {
	mu        sync.Mutex
	calls     []sequenceCall
	responses []sequenceResponse
}

type sequenceCall struct {
	query     string
	embedding []float32
	opts      *search.SearchOptions
}

type sequenceResponse struct {
	resp *search.SearchResponse
	err  error
}

func (s *sequenceSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	call := sequenceCall{query: query, embedding: append([]float32(nil), embedding...)}
	if opts != nil {
		copyOpts := *opts
		call.opts = &copyOpts
	}
	s.calls = append(s.calls, call)

	if len(s.responses) == 0 {
		return nil, nil
	}
	next := s.responses[0]
	s.responses = s.responses[1:]
	return next.resp, next.err
}

func TestNewService_DefaultsAndValidation(t *testing.T) {
	svc, err := NewService(Config{}, nil, nil, nil)
	require.Error(t, err)
	require.Nil(t, svc)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	searcher := &stubSearcher{}
	svc, err = NewService(Config{RerankEnabled: true}, nil, nil, searcher)
	require.NoError(t, err)
	require.Equal(t, "nornic", svc.defaultDatabase)
	require.Equal(t, 1000, svc.maxLimit)
	require.True(t, svc.rerankEnabled)
	require.Equal(t, searcher, svc.searcher)
	require.Nil(t, svc.embedQuery)

	svc, err = NewService(Config{DefaultDatabase: "tenant_a", MaxLimit: 25}, nil, nil, searcher)
	require.NoError(t, err)
	require.Equal(t, "tenant_a", svc.defaultDatabase)
	require.Equal(t, 25, svc.maxLimit)
}

func TestService_SearchText_ValidationAndFallback(t *testing.T) {
	t.Run("rejects nil request and empty query", func(t *testing.T) {
		svc, err := NewService(Config{}, nil, nil, &stubSearcher{})
		require.NoError(t, err)

		_, err = svc.SearchText(context.Background(), nil)
		require.Equal(t, codes.InvalidArgument, status.Code(err))

		_, err = svc.SearchText(context.Background(), &gen.SearchTextRequest{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("falls back to bm25 when embeddings unavailable and clamps options", func(t *testing.T) {
		searcher := &stubSearcher{
			resp: &search.SearchResponse{
				SearchMethod:      "bm25",
				FallbackTriggered: true,
				FallbackReason:    search.SearchFallbackQueryEmbeddingFailed,
				Message:           "fallback",
				Results: []search.SearchResult{
					{NodeID: storage.NodeID("nornic:fallback"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "fallback"}, Score: 0.4},
				},
			},
		}
		svc, err := NewService(Config{MaxLimit: 5, RerankEnabled: true}, func(ctx context.Context, query string) ([]float32, error) {
			return nil, errors.New("embedding provider unavailable")
		}, nil, searcher)
		require.NoError(t, err)

		minSim := float32(0.75)
		resp, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{
			Query:         "fallback query",
			Limit:         999,
			Labels:        []string{"Doc"},
			MinSimilarity: &minSim,
		})
		require.NoError(t, err)
		require.Equal(t, "bm25", resp.SearchMethod)
		require.True(t, resp.FallbackTriggered)
		require.Equal(t, "query_embedding_failed", resp.FallbackReason)
		require.Len(t, resp.Hits, 1)
		require.NotNil(t, searcher.lastOpts)
		require.Nil(t, searcher.lastEmbedding)
		require.Equal(t, 5, searcher.lastOpts.Limit)
		require.True(t, searcher.lastOpts.RerankEnabled)
		require.Equal(t, []string{"Doc"}, searcher.lastOpts.Types)
		require.NotNil(t, searcher.lastOpts.MinSimilarity)
		require.InDelta(t, 0.75, *searcher.lastOpts.MinSimilarity, 0.0001)
	})

	t.Run("uses default limit when request limit is non-positive", func(t *testing.T) {
		searcher := &stubSearcher{
			resp: &search.SearchResponse{SearchMethod: "bm25"},
		}
		svc, err := NewService(Config{MaxLimit: 100}, nil, nil, searcher)
		require.NoError(t, err)

		_, err = svc.SearchText(context.Background(), &gen.SearchTextRequest{Query: "default limit", Limit: 0})
		require.NoError(t, err)
		require.NotNil(t, searcher.lastOpts)
		require.Equal(t, 10, searcher.lastOpts.Limit)
	})
}

func TestService_SearchText_ContinuationStartPullAndDiscard(t *testing.T) {
	searcher := &continuationStubSearcher{responses: []*search.SearchContinuationPage{
		{
			Results: []search.SearchResult{{NodeID: "node-1", Score: 0.9}},
			QID:     "qid-1", HasMore: true, Position: 1, Returned: 1,
			SearchMethod: "hybrid",
		},
		{
			Results: []search.SearchResult{{NodeID: "node-2", Score: 0.8}, {NodeID: "node-3", Score: 0.7}},
			QID:     "qid-2", HasMore: true, Position: 3, Returned: 2,
			SearchMethod: "hybrid",
		},
		{Released: true},
	}}
	svc, err := NewService(Config{
		DefaultDatabase:  "nornic",
		OwnerFromContext: func(context.Context) string { return "sub:alice" },
	}, nil, nil, searcher)
	require.NoError(t, err)

	first, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{Query: "alpha", Limit: 1, N: 1})
	require.NoError(t, err)
	require.Equal(t, "qid-1", first.Qid)
	require.True(t, first.HasMore)
	require.Equal(t, uint32(1), first.Returned)
	require.Equal(t, "node-1", first.Hits[0].NodeId)
	require.Equal(t, "sub:alice", searcher.requests[0].Owner)
	require.Equal(t, "nornic", searcher.requests[0].Database)

	second, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{Qid: first.Qid, N: 2})
	require.NoError(t, err)
	require.Equal(t, "qid-2", second.Qid)
	require.Equal(t, uint64(3), second.Position)
	require.Len(t, second.Hits, 2)
	require.Empty(t, searcher.queries[1])
	require.Equal(t, first.Qid, searcher.requests[1].QID)

	released, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{Qid: second.Qid, Discard: true})
	require.NoError(t, err)
	require.True(t, released.Released)
	require.True(t, searcher.requests[2].Discard)
}

func TestService_SearchText_IDContinuationGroupsPassages(t *testing.T) {
	eligibleCount := 2
	searcher := &continuationStubSearcher{responses: []*search.SearchContinuationPage{{
		Results: []search.SearchResult{{
			ID: "frame-a", NodeID: "frame-a", GroupKey: "asset-a", Phase: search.SearchContinuationCatalogPhase,
			Passages: []search.SearchPassage{
				{ID: "frame-a", NodeID: "frame-a", Phase: search.SearchContinuationCatalogPhase},
				{ID: "frame-b", NodeID: "frame-b", GroupKey: "child-b-key", Phase: search.SearchContinuationCatalogPhase},
			},
		}},
		QID: "qid-id", HasMore: true, Returned: 1,
		Mode: search.SearchContinuationID, EligibleCount: &eligibleCount,
		RankedPoolExhausted: true, Completion: search.SearchContinuationMoreResults,
	}}}
	svc, err := NewService(Config{OwnerFromContext: func(context.Context) string { return "sub:alice" }}, nil, nil, searcher)
	require.NoError(t, err)

	rankedLimit := uint64(25)
	response, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{
		Mode: "id", GroupBy: "asset_id", RankedLimit: &rankedLimit, N: 1, Labels: []string{"Frame"},
	})
	require.NoError(t, err)
	require.Equal(t, search.SearchContinuationID, searcher.requests[0].Mode)
	require.Equal(t, "asset_id", searcher.requests[0].GroupBy)
	require.Equal(t, 25, searcher.requests[0].RankedLimit)
	require.Len(t, response.Hits, 1)
	require.Equal(t, "asset-a", response.Hits[0].GroupKey)
	require.Equal(t, search.SearchContinuationCatalogPhase, response.Hits[0].Phase)
	require.Len(t, response.Hits[0].Passages, 2)
	require.Equal(t, "frame-a", response.Hits[0].Passages[0].NodeId)
	require.Equal(t, "frame-b", response.Hits[0].Passages[1].NodeId)
	require.Equal(t, "child-b-key", response.Hits[0].Passages[1].GroupKey)
	require.Equal(t, "id", response.Mode)
	require.NotNil(t, response.EligibleCount)
	require.Equal(t, int64(2), *response.EligibleCount)
	require.True(t, response.RankedPoolExhausted)
	require.Equal(t, search.SearchContinuationMoreResults, response.Completion)
}

func TestService_SearchText_ErrorHandling(t *testing.T) {
	t.Run("maps continuation saturation and gone qids", func(t *testing.T) {
		for _, testCase := range []struct {
			name string
			err  error
			code codes.Code
		}{
			{name: "saturated", err: resultstream.ErrCapacity, code: codes.ResourceExhausted},
			{name: "expired", err: resultstream.ErrExpiredQID, code: codes.NotFound},
			{name: "gone", err: resultstream.ErrGoneQID, code: codes.NotFound},
			{name: "invalid", err: resultstream.ErrInvalidQID, code: codes.InvalidArgument},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				searcher := &continuationStubSearcher{err: testCase.err}
				svc, err := NewService(Config{}, nil, nil, searcher)
				require.NoError(t, err)
				_, err = svc.SearchText(context.Background(), &gen.SearchTextRequest{Qid: "qid", N: 1})
				require.Equal(t, testCase.code, status.Code(err))
			})
		}
	})

	t.Run("returns internal error when fallback search fails", func(t *testing.T) {
		searcher := &stubSearcher{err: fmt.Errorf("search backend failed")}
		svc, err := NewService(Config{}, nil, nil, searcher)
		require.NoError(t, err)

		_, err = svc.SearchText(context.Background(), &gen.SearchTextRequest{Query: "broken"})
		require.Equal(t, codes.Internal, status.Code(err))
		require.Contains(t, err.Error(), "search backend failed")
	})

	t.Run("falls back when vector search fails before bm25 succeeds", func(t *testing.T) {
		searcher := &sequenceSearcher{
			responses: []sequenceResponse{
				{err: fmt.Errorf("vector search failed")},
				{resp: &search.SearchResponse{
					SearchMethod:      "bm25",
					FallbackTriggered: true,
					Message:           "recovered",
					Results:           []search.SearchResult{{NodeID: storage.NodeID("nornic:bm25"), Score: 0.2}},
				}},
			},
		}
		svc, err := NewService(Config{}, func(ctx context.Context, query string) ([]float32, error) {
			return []float32{0.1, 0.2}, nil
		}, nil, searcher)
		require.NoError(t, err)

		resp, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{Query: "single chunk"})
		require.NoError(t, err)
		require.Equal(t, "bm25", resp.SearchMethod)
		require.Len(t, resp.Hits, 1)

		searcher.mu.Lock()
		defer searcher.mu.Unlock()
		require.Len(t, searcher.calls, 2)
		require.Equal(t, []float32{0.1, 0.2}, searcher.calls[0].embedding)
		require.Nil(t, searcher.calls[1].embedding)
	})
}

func TestService_SearchText_EmbedsAndConvertsResults(t *testing.T) {
	st := &stubSearcher{
		resp: &search.SearchResponse{
			SearchMethod:      "rrf_hybrid",
			FallbackTriggered: false,
			Message:           "",
			Results: []search.SearchResult{
				{
					NodeID:     storage.NodeID("nornic:node1"),
					Labels:     []string{"Doc"},
					Properties: map[string]any{"title": "hello"},
					Score:      0.9,
					RRFScore:   0.8,
					VectorRank: 1,
					BM25Rank:   2,
				},
			},
		},
	}

	svc, err := NewService(
		Config{DefaultDatabase: "nornic", MaxLimit: 100},
		func(ctx context.Context, query string) ([]float32, error) {
			require.Equal(t, "database performance", query)
			return []float32{0.1, 0.2}, nil
		},
		nil,
		st,
	)
	require.NoError(t, err)

	resp, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{
		Query:  "database performance",
		Limit:  10,
		Labels: []string{"Doc"},
	})
	require.NoError(t, err)
	require.Equal(t, "rrf_hybrid", resp.SearchMethod)
	require.Len(t, resp.Hits, 1)
	require.Equal(t, "nornic:node1", resp.Hits[0].NodeId)
	require.Equal(t, []string{"Doc"}, resp.Hits[0].Labels)
	require.NotNil(t, resp.Hits[0].Properties)
	require.Equal(t, float32(0.9), resp.Hits[0].Score)
	require.Equal(t, float32(0.8), resp.Hits[0].RrfScore)
	require.Equal(t, int32(1), resp.Hits[0].VectorRank)
	require.Equal(t, int32(2), resp.Hits[0].Bm25Rank)

	require.Equal(t, "database performance", st.lastQuery)
	require.Equal(t, []float32{0.1, 0.2}, st.lastEmbedding)
	require.NotNil(t, st.lastOpts)
	require.Equal(t, 10, st.lastOpts.Limit)
	require.Equal(t, []string{"Doc"}, st.lastOpts.Types)
}

type recordingSearcher struct {
	mu      sync.Mutex
	queries []string
}

func (s *recordingSearcher) Search(ctx context.Context, query string, embedding []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
	s.mu.Lock()
	s.queries = append(s.queries, query)
	s.mu.Unlock()

	// Return deterministic results keyed off which marker appears in this query chunk.
	// Markers are placed far apart so chunking should isolate them.
	var results []search.SearchResult
	switch {
	case strings.Contains(query, "ONE"):
		results = []search.SearchResult{
			{NodeID: storage.NodeID("n2"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "B"}, Score: 0.03, RRFScore: 0.03, VectorRank: 1, BM25Rank: 1},
			{NodeID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "A"}, Score: 0.02, RRFScore: 0.02, VectorRank: 2, BM25Rank: 2},
		}
	case strings.Contains(query, "TWO"):
		results = []search.SearchResult{
			{NodeID: storage.NodeID("n2"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "B"}, Score: 0.03, RRFScore: 0.03, VectorRank: 1, BM25Rank: 1},
			{NodeID: storage.NodeID("n3"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "C"}, Score: 0.02, RRFScore: 0.02, VectorRank: 2, BM25Rank: 2},
		}
	case strings.Contains(query, "THREE"):
		results = []search.SearchResult{
			{NodeID: storage.NodeID("n1"), Labels: []string{"Doc"}, Properties: map[string]any{"title": "A"}, Score: 0.03, RRFScore: 0.03, VectorRank: 1, BM25Rank: 1},
		}
	default:
		results = nil
	}

	return &search.SearchResponse{
		SearchMethod:      "rrf_hybrid",
		FallbackTriggered: false,
		Results:           results,
	}, nil
}

func TestService_SearchText_ChunksLongQueryAndFusesAcrossChunks(t *testing.T) {
	st := &recordingSearcher{}

	var (
		mu       sync.Mutex
		embedMax int
		tokenMax int
		embeds   int
	)

	svc, err := NewService(
		Config{DefaultDatabase: "nornic", MaxLimit: 100},
		func(ctx context.Context, query string) ([]float32, error) {
			mu.Lock()
			embeds++
			if len(query) > embedMax {
				embedMax = len(query)
			}
			if tok := mustCountTestTokens(query); tok > tokenMax {
				tokenMax = tok
			}
			mu.Unlock()

			if mustCountTestTokens(query) > 512 {
				return nil, fmt.Errorf("simulated tokenizer overflow for tokens=%d", mustCountTestTokens(query))
			}
			return []float32{0.1, 0.2}, nil
		},
		chunkTestText,
		st,
	)
	require.NoError(t, err)

	// Load a real large document as the base query text to validate chunking
	// behavior on natural content instead of synthetic repeated characters.
	path := filepath.Join("..", "..", "docs", "features", "gpu-acceleration.md")
	data, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	base := string(data)
	// Keep marker tokens so the stub searcher can return deterministic per-chunk
	// results for fusion assertions.
	longQuery := base + "\nONE\n" + base + "\nTWO\n" + base + "\nTHREE\n" + base
	require.Greater(t, mustCountTestTokens(longQuery), 512)

	resp, err := svc.SearchText(context.Background(), &gen.SearchTextRequest{
		Query: longQuery,
		Limit: 3,
	})
	require.NoError(t, err)
	require.Equal(t, "chunked_rrf_hybrid", resp.SearchMethod)
	require.NotEmpty(t, resp.Hits)
	require.Equal(t, "n2", resp.Hits[0].NodeId, "expected fusion to rank node present in multiple chunks highest")

	mu.Lock()
	gotEmbeds := embeds
	gotEmbedMax := embedMax
	gotTokenMax := tokenMax
	mu.Unlock()
	require.GreaterOrEqual(t, gotEmbeds, 2, "expected multiple chunk embeddings")
	require.Greater(t, gotEmbedMax, 0)
	require.LessOrEqual(t, gotTokenMax, 512, "expected no embedding call on query chunks > 512 tokens")

	st.mu.Lock()
	qs := append([]string(nil), st.queries...)
	st.mu.Unlock()
	require.GreaterOrEqual(t, len(qs), 2, "expected multiple per-chunk searches")
	maxQTokens := 0
	for _, q := range qs {
		tok := mustCountTestTokens(q)
		if tok > maxQTokens {
			maxQTokens = tok
		}
	}
	require.LessOrEqual(t, maxQTokens, 512, "expected no search call on query chunks > 512 tokens")
}
