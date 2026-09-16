package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type stubInferenceManager struct{}
type stubVectorEmbedder struct {
	vec []float32
}

func (s *stubVectorEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return s.vec, nil
}

func (s *stubVectorEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type failingVectorEmbedder struct {
	err error
}

func (s *failingVectorEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	return nil, s.err
}

func (s *failingVectorEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type orthogonalChunkEmbedder struct{}

func (orthogonalChunkEmbedder) Embed(_ context.Context, text string) ([]float32, error) {
	if text == "chunk one" {
		return []float32{1, 0}, nil
	}
	return []float32{0, 1}, nil
}

func (orthogonalChunkEmbedder) ChunkText(string, int, int) ([]string, error) {
	return []string{"chunk one", "chunk two"}, nil
}

func (s *stubInferenceManager) Generate(ctx context.Context, prompt string, params heimdall.GenerateParams) (string, error) {
	return "generated: " + prompt, nil
}

func (s *stubInferenceManager) Chat(ctx context.Context, req heimdall.ChatRequest) (*heimdall.ChatResponse, error) {
	return &heimdall.ChatResponse{
		Model: "stub-model",
		Choices: []heimdall.ChatChoice{
			{
				Message:      &heimdall.ChatMessage{Role: "assistant", Content: "chat-response"},
				FinishReason: "stop",
			},
		},
		Usage: &heimdall.ChatUsage{
			PromptTokens:     3,
			CompletionTokens: 2,
			TotalTokens:      5,
		},
	}, nil
}

func TestCallDbRetrieveAndRerank(t *testing.T) {
	ctx := context.Background()
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)

	_, err := store.CreateNode(&storage.Node{
		ID:         storage.NodeID("doc-1"),
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"content": "alpha retrieval test"},
	})
	require.NoError(t, err)

	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	retrieveRes, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, retrieveRes.Columns)
	assert.Equal(t, "node", retrieveRes.Columns[0])
	require.GreaterOrEqual(t, len(retrieveRes.Rows), 1)

	rretrieveRes, err := exec.Execute(ctx, "CALL db.rretrieve({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, rretrieveRes.Columns)
	assert.Equal(t, "node", rretrieveRes.Columns[0])
	require.GreaterOrEqual(t, len(rretrieveRes.Rows), 1)
}

func TestCallDbRetrieveUsesPerChunkOuterRRF(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(orthogonalChunkEmbedder{})

	for _, node := range []*storage.Node{
		{ID: "repeated", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "repeated"}, ChunkEmbeddings: [][]float32{{1, 0}, {0, 1}}},
		{ID: "averaged", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "averaged"}, ChunkEmbeddings: [][]float32{{0.70710677, 0.70710677}}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	service := search.NewServiceWithDimensions(store, 2)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	result, err := exec.Execute(ctx, "CALL db.retrieve({query: 'complete query', limit: 2})", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 2)
	first, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, storage.NodeID("repeated"), first.ID)
	require.Equal(t, "chunked_rrf_hybrid", result.Rows[0][5])
}

func TestCallDbRetrieveContinuesBeyondInitialLimit(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	for index := 0; index < 5; index++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("doc-%d", index)),
			Labels:     []string{"Document"},
			Properties: map[string]interface{}{"content": "alpha continuation document"},
		})
		require.NoError(t, err)
	}

	service := search.NewService(store)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	first, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 1, n: 1})", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"page"}, first.Columns)
	page := first.Rows[0][0].(map[string]interface{})
	require.True(t, page["has_more"].(bool))
	qid := page["qid"].(string)
	require.NotEmpty(t, qid)

	second, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{"qid": qid, "n": int64(3)},
	})
	require.NoError(t, err)
	page = second.Rows[0][0].(map[string]interface{})
	require.Len(t, page["results"], 3)
}

func TestCallDbRetrieveContinuationBypassesCypherResultCache(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 10, time.Minute)
	service := search.NewService(store)
	exec.SetSearchService(service)

	for index := 0; index < 3; index++ {
		_, err := store.CreateNode(&storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("doc-%d", index)),
			Labels:     []string{"Document"},
			Properties: map[string]interface{}{"content": "cached continuation"},
		})
		require.NoError(t, err)
	}

	startQuery := "CALL db.retrieve({mode: 'id', n: 1})"
	alice := WithAuthenticatedPrincipal(context.Background(), "sub:alice")
	bob := WithAuthenticatedPrincipal(context.Background(), "sub:bob")
	first, err := exec.Execute(alice, startQuery, nil)
	require.NoError(t, err)
	firstPage := first.Rows[0][0].(map[string]interface{})
	firstQID := firstPage["qid"].(string)
	require.NotEmpty(t, firstQID)

	second, err := exec.Execute(bob, startQuery, nil)
	require.NoError(t, err)
	secondPage := second.Rows[0][0].(map[string]interface{})
	require.NotEqual(t, firstQID, secondPage["qid"].(string), "stateful starts must not reuse a cached qid across owners")

	pullParams := map[string]interface{}{
		"request": map[string]interface{}{"qid": firstQID, "n": int64(1)},
	}
	_, err = exec.Execute(alice, "CALL db.retrieve($request)", pullParams)
	require.NoError(t, err)

	discard, err := exec.Execute(alice, "CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{"qid": firstQID, "discard": true},
	})
	require.NoError(t, err)
	discardPage := discard.Rows[0][0].(map[string]interface{})
	require.True(t, discardPage["released"].(bool))

	_, err = exec.Execute(alice, "CALL db.retrieve($request)", pullParams)
	require.ErrorIs(t, err, resultstream.ErrGoneQID, "discarded qids must not be replayed from the query result cache")

	hits, _, _, _, _ := exec.cache.Stats()
	require.Zero(t, hits, "db.retrieve continuation START/PULL/DISCARD must bypass the ordinary Cypher result cache")
}

func TestCallDbRetrieveIDContinuationGroupsAllPassages(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	for _, node := range []*storage.Node{
		{ID: "frame-c", Labels: []string{"Frame"}, Properties: map[string]interface{}{"collection": "summer", "asset_id": "asset-b"}},
		{ID: "frame-b", Labels: []string{"Frame"}, Properties: map[string]interface{}{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "frame-a", Labels: []string{"Frame"}, Properties: map[string]interface{}{"collection": "summer", "asset_id": "asset-a"}},
		{ID: "winter", Labels: []string{"Frame"}, Properties: map[string]interface{}{"collection": "winter", "asset_id": "asset-c"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	first, err := exec.Execute(ctx, `CALL db.retrieve({
		mode: 'id', group_by: 'asset_id', types: ['Frame'],
		filters: {collection: ['summer']}, n: 1
	})`, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"page"}, first.Columns)
	page := first.Rows[0][0].(map[string]interface{})
	require.Equal(t, page["qid"], first.Metadata["durable_qid"])
	require.Equal(t, "id", page["mode"])
	require.Equal(t, int64(2), page["eligible_count"])
	require.True(t, page["ranked_pool_exhausted"].(bool))
	require.False(t, page["collection_exhausted"].(bool))
	results := page["results"].([]interface{})
	require.Len(t, results, 1)
	asset := results[0].(map[string]interface{})
	require.Equal(t, "asset-a", asset["group_key"])
	require.Equal(t, search.SearchContinuationCatalogPhase, asset["phase"])
	passages := asset["passages"].([]interface{})
	require.Len(t, passages, 2)
	require.Equal(t, storage.NodeID("frame-a"), passages[0].(map[string]interface{})["node"].(*storage.Node).ID)
	require.Equal(t, storage.NodeID("frame-b"), passages[1].(map[string]interface{})["node"].(*storage.Node).ID)

	second, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{"qid": page["qid"], "n": int64(1)},
	})
	require.NoError(t, err)
	page = second.Rows[0][0].(map[string]interface{})
	require.True(t, page["collection_exhausted"].(bool))
	require.Equal(t, search.SearchContinuationCollectionComplete, page["completion"])
}

func TestSearchContinuationOwnerUsesOnlyAuthenticatedPrincipal(t *testing.T) {
	require.Empty(t, searchContinuationOwner(WithAuthToken(context.Background(), "Bearer secret")))
	require.Equal(t, "sub:alice", searchContinuationOwner(WithAuthenticatedPrincipal(context.Background(), "sub:alice")))
	require.Equal(t, "embedded", searchContinuationOwner(context.Background()))
}

func TestCallDbRetrieveAppliesPropertyFilters(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)

	for _, node := range []*storage.Node{
		{ID: "active-source", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "policy document", "lifecycle": "active", "generation": int64(3), "artifact": "source"}},
		{ID: "active-summary-array", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "policy document", "lifecycle": "active", "generation": int64(4), "artifact": []string{"derived", "summary"}}},
		{ID: "archived", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "policy document", "lifecycle": "archived", "generation": int64(3), "artifact": "source"}},
		{ID: "wrong-generation", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "policy document", "lifecycle": "active", "generation": int64(5), "artifact": "source"}},
		{ID: "wrong-artifact", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "policy document", "lifecycle": "active", "generation": int64(3), "artifact": "derived"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}

	service := search.NewService(store)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)

	for _, filterKey := range []string{"filters", "propertyFilters", "property_filters"} {
		t.Run(filterKey, func(t *testing.T) {
			result, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{
				"request": map[string]interface{}{
					"query": "policy document",
					"limit": int64(10),
					filterKey: map[string]interface{}{
						"lifecycle":  "active",
						"generation": []interface{}{int64(3), int64(4)},
						"artifact":   []interface{}{"source", "summary"},
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, result.Rows, 2)
			ids := make([]string, 0, len(result.Rows))
			for _, row := range result.Rows {
				node, ok := row[0].(*storage.Node)
				require.True(t, ok)
				ids = append(ids, string(node.ID))
			}
			require.ElementsMatch(t, []string{"active-source", "active-summary-array"}, ids)
		})
	}
}

func TestApplyAdaptiveCandidateOptions(t *testing.T) {
	opts := search.DefaultSearchOptions()
	applyAdaptiveCandidateOptions(opts, map[string]interface{}{
		"adaptive_overfetch":      false,
		"candidateTarget":         int64(75),
		"initial_overfetch_ratio": 2.0,
		"maxOverfetchRatio":       8.0,
		"overfetch_growth_factor": 1.5,
		"maxCandidateLimit":       int64(10_000),
	})

	require.False(t, opts.AdaptiveOverfetch)
	require.Equal(t, 75, opts.CandidateTarget)
	require.Equal(t, 2.0, opts.InitialOverfetchRatio)
	require.Equal(t, 8.0, opts.MaxOverfetchRatio)
	require.Equal(t, 1.5, opts.OverfetchGrowthFactor)
	require.Equal(t, 10_000, opts.MaxCandidateLimit)
}

func TestFailClosedCandidatePolicyAllowsValuesAboveDefaultBudget(t *testing.T) {
	opts := search.DefaultSearchOptions()
	failClosed, err := applyRetrievalPolicyOptions(opts, map[string]interface{}{
		"failClosed":        true,
		"candidateTarget":   int64(8_000),
		"maxCandidateLimit": int64(10_000),
	})

	require.NoError(t, err)
	require.True(t, failClosed)
	require.Equal(t, 8_000, opts.CandidateTarget)
	require.Equal(t, 10_000, opts.MaxCandidateLimit)
}

func TestApplyRetrievalPolicyOptions(t *testing.T) {
	tests := []struct {
		name string
		req  map[string]interface{}
	}{
		{
			name: "camel case",
			req: map[string]interface{}{
				"rrfK": 42.0, "vectorWeight": 0.25, "bm25Weight": 1.75,
				"minRRFScore": 0.0, "fallbackEnabled": false,
				"candidateTarget": int64(50), "adaptiveOverfetch": false,
				"propertyFilters": map[string]interface{}{
					"generation": []interface{}{int64(3), int64(4)}, "lifecycle": "active",
				},
			},
		},
		{
			name: "snake case",
			req: map[string]interface{}{
				"rrf_k": 42.0, "vector_weight": 0.25, "bm25_weight": 1.75,
				"min_rrf_score": 0.0, "fallback_enabled": false,
				"candidate_target": int64(50), "adaptive_overfetch": false,
				"property_filters": map[string]interface{}{
					"generation": []interface{}{int64(3), int64(4)}, "lifecycle": "active",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := search.DefaultSearchOptions()
			_, err := applyRetrievalPolicyOptions(opts, test.req)
			require.NoError(t, err)
			require.Equal(t, 42.0, opts.RRFK)
			require.Equal(t, 0.25, opts.VectorWeight)
			require.Equal(t, 1.75, opts.BM25Weight)
			require.Equal(t, 0.0, opts.MinRRFScore)
			require.NotNil(t, opts.FallbackEnabled)
			require.False(t, *opts.FallbackEnabled)
			require.Equal(t, 50, opts.CandidateTarget)
			require.False(t, opts.AdaptiveOverfetch)
			require.Equal(t, map[string][]string{
				"generation": {"3", "4"}, "lifecycle": {"active"},
			}, opts.Filters)
		})
	}
}

func TestApplyRetrievalPolicyOptionsRejectsInvalidBoundaries(t *testing.T) {
	opts := search.DefaultSearchOptions()
	_, err := applyRetrievalPolicyOptions(opts, map[string]interface{}{
		"rrfK": -1, "vectorWeight": 0, "bm25Weight": -2,
		"minRRFScore": -0.1, "fallbackEnabled": "not-a-bool",
		"candidateTarget": 0, "adaptiveOverfetch": "not-a-bool",
		"filters": "not-a-map",
	})
	require.NoError(t, err)

	require.Equal(t, 60.0, opts.RRFK)
	require.Equal(t, 1.0, opts.VectorWeight)
	require.Equal(t, 1.0, opts.BM25Weight)
	require.Equal(t, 0.01, opts.MinRRFScore)
	require.Nil(t, opts.FallbackEnabled)
	require.Zero(t, opts.CandidateTarget)
	require.True(t, opts.AdaptiveOverfetch)
	require.Nil(t, opts.Filters)
}

func TestApplyRetrievalPolicyOptionsFailClosed(t *testing.T) {
	opts := search.DefaultSearchOptions()
	opts.Limit = 10
	failClosed, err := applyRetrievalPolicyOptions(opts, map[string]interface{}{
		"failClosed": true,
		"filters": map[string]interface{}{
			"lifecycle": "active",
		},
	})
	require.NoError(t, err)
	require.True(t, failClosed)
	require.True(t, opts.AdaptiveOverfetch)
	require.Equal(t, 1.5, opts.InitialOverfetchRatio)
	require.Zero(t, opts.CandidateTarget)
	require.Equal(t, 60.0, opts.RRFK)
	require.Equal(t, 0.01, opts.MinRRFScore)
	require.Nil(t, opts.MinSimilarity)
	require.NotNil(t, opts.FallbackEnabled)
	require.False(t, *opts.FallbackEnabled)
	require.Equal(t, map[string][]string{"lifecycle": {"active"}}, opts.Filters)
}

func TestApplyRetrievalPolicyOptionsFailClosedRejectsNonFiniteValues(t *testing.T) {
	opts := search.DefaultSearchOptions()
	_, err := applyRetrievalPolicyOptions(opts, map[string]interface{}{
		"fail_closed": true,
		"rrfK":        math.NaN(),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failClosed")
	require.Contains(t, err.Error(), "rrfK")
}

func TestCallDbRetrieveFailClosedRequiresEmbedding(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	_, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', failClosed: true})", nil)
	require.Error(t, err)
	var unavailable *localization.LocalizedError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, string(localization.MessageCypherSubqueriesRAGFailClosedEmbeddingUnavailable), unavailable.Code)
	var inner *localization.LocalizedError
	require.ErrorAs(t, unavailable.Cause, &inner)
	require.Equal(t, string(localization.MessageCypherCoreEmbedderNotConfigured), inner.Code)
}

func TestCallDbRetrieveFailClosedPreservesEmbedderCause(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&failingVectorEmbedder{err: context.DeadlineExceeded})
	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	_, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', failClosed: true})", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "embedding")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestCallDbRetrieveFailOpenReturnsEmbeddingFallbackReason(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "alpha fallback document"}})
	require.NoError(t, err)
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&failingVectorEmbedder{err: context.DeadlineExceeded})
	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	result, err := exec.Execute(ctx, `CALL db.retrieve({query: 'alpha'})
YIELD node, fallback_triggered, fallback_reason
RETURN node, fallback_triggered, fallback_reason`, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][1])
	require.Equal(t, "query_embedding_failed", result.Rows[0][2])
}

func TestCallDbRetrieveFailClosedRejectsStringEmbedding(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(ctx))
	exec.SetSearchService(svc)

	_, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{
		"request": map[string]interface{}{
			"query":      "alpha",
			"failClosed": true,
			"embedding":  "not-a-vector",
		},
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errRetrieveEmbeddingNotAVector)
}

func TestApplyRetrievalPolicyOptionsFailClosedRejectsRerankAndFractionalCounts(t *testing.T) {
	tests := []struct {
		name  string
		req   map[string]interface{}
		field string
	}{
		{
			name:  "rerank min score NaN",
			req:   map[string]interface{}{"failClosed": true, "rerankMinScore": math.NaN()},
			field: "rerankMinScore",
		},
		{
			name:  "rerank top k infinity",
			req:   map[string]interface{}{"failClosed": true, "rerankTopK": math.Inf(1)},
			field: "rerankTopK",
		},
		{
			name:  "fractional candidate target",
			req:   map[string]interface{}{"failClosed": true, "candidateTarget": 1.9},
			field: "candidateTarget",
		},
		{
			name:  "fractional limit",
			req:   map[string]interface{}{"failClosed": true, "limit": 10.5},
			field: "limit",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := search.DefaultSearchOptions()
			_, err := applyRetrievalPolicyOptions(opts, tt.req)
			require.Error(t, err)
			require.Contains(t, err.Error(), "failClosed")
			require.Contains(t, err.Error(), tt.field)
		})
	}
}

func TestResolveRetrieveEmbeddingFailClosedRejectsMalformedElements(t *testing.T) {
	_, _, err := resolveSuppliedRetrieveEmbedding(map[string]interface{}{
		"embedding": []interface{}{1, "bad", 0},
	}, true)
	require.Error(t, err)
	require.ErrorIs(t, err, errRetrieveEmbeddingNotAVector)

	_, _, err = resolveSuppliedRetrieveEmbedding(map[string]interface{}{
		"embedding": []interface{}{1, "2", 0},
	}, true)
	require.Error(t, err)
	require.ErrorIs(t, err, errRetrieveEmbeddingNotAVector)
}

func TestResolveRetrieveEmbeddingFailOpenDropsInvalidElements(t *testing.T) {
	embedding, supplied, err := resolveSuppliedRetrieveEmbedding(map[string]interface{}{
		"embedding": []interface{}{float64(1), "bad", float64(0)},
	}, false)
	require.NoError(t, err)
	require.True(t, supplied)
	require.Equal(t, []float32{1, 0}, embedding)
}

func TestParseRetrievalFiltersEdgeCases(t *testing.T) {
	require.Nil(t, parseRetrievalFilters(nil))
	require.Nil(t, parseRetrievalFilters("not-a-map"))
	require.Nil(t, parseRetrievalFilters(map[string]interface{}{}))
	require.Nil(t, parseRetrievalFilters(map[string]interface{}{
		"": nil, "empty": []interface{}{nil},
	}))

	require.Equal(t, map[string][]string{
		"bool": {"true"}, "float": {"1.5"}, "mixed": {"3", "active"},
	}, parseRetrievalFilters(map[string]interface{}{
		"bool": true, "float": 1.5, "mixed": []interface{}{int64(3), nil, "active"},
	}))
}

func TestCallDbInfer(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetInferenceManager(&stubInferenceManager{})

	genRes, err := exec.Execute(ctx, "CALL db.infer({prompt: 'hello world', max_tokens: 32, temperature: 0.2})", nil)
	require.NoError(t, err)
	require.Len(t, genRes.Rows, 1)
	assert.Equal(t, "generated: hello world", genRes.Rows[0][0])
	assert.Equal(t, "stop", genRes.Rows[0][5])

	chatRes, err := exec.Execute(ctx, "CALL db.infer({messages: [{role: 'user', content: 'hi'}], model: 'stub-model'})", nil)
	require.NoError(t, err)
	require.Len(t, chatRes.Rows, 1)
	assert.Equal(t, "chat-response", chatRes.Rows[0][0])
	assert.Equal(t, "stub-model", chatRes.Rows[0][2])
	usage, ok := chatRes.Rows[0][3].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 5, usage["total_tokens"])
}

func TestCallDbRerankCandidates(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	res, err := exec.Execute(ctx, "CALL db.rerank({query: 'alpha', candidates: [{id: 'a', content: 'alpha text', score: 0.9}, {id: 'b', content: 'beta text', score: 0.4}]})", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "content", "original_rank", "new_rank", "bi_score", "cross_score", "final_score"}, res.Columns)
	require.Len(t, res.Rows, 2)
	assert.Equal(t, "a", res.Rows[0][0])
}

func TestCallDbRerankCandidates_StrictValidationBranches(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	_, err := exec.Execute(ctx, "CALL db.rerank({query: 'alpha', candidates: []})", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires non-empty candidates")

	_, err = exec.Execute(ctx, "CALL db.rerank({query: 'alpha', candidates: [{content: 'missing-id'}]})", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "candidate id is required")
}

func TestCallDbIndexVectorEmbed(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetEmbedder(&stubVectorEmbedder{vec: []float32{0.1, 0.2, 0.3, 0.4}})

	res, err := exec.Execute(ctx, "CALL db.index.vector.embed('hello world') YIELD embedding", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"embedding"}, res.Columns)
	require.Len(t, res.Rows, 1)
	embedding, ok := res.Rows[0][0].([]float32)
	require.True(t, ok)
	assert.Equal(t, []float32{0.1, 0.2, 0.3, 0.4}, embedding)
}

func TestCallDbRetrieveWrappers_ParseErrors(t *testing.T) {
	ctx := context.Background()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))

	_, err := exec.callDbRetrieve(ctx, "CALL db.retrieve(")
	require.Error(t, err)

	_, err = exec.callDbRRetrieve(ctx, "CALL db.rretrieve(")
	require.Error(t, err)
}

func TestCallRagHelpers_MessageAndCandidateParsingBranches(t *testing.T) {
	// toChatMessages: non-list, mixed list, missing role/content filtering.
	assert.Nil(t, toChatMessages("bad"))
	msgs := toChatMessages([]interface{}{
		map[string]interface{}{"role": "user", "content": "hello"},
		map[string]interface{}{"role": "", "content": "skip-role"},
		map[string]interface{}{"role": "assistant", "content": ""},
		"bad",
	})
	require.Len(t, msgs, 1)
	assert.Equal(t, "user", msgs[0].Role)
	assert.Equal(t, "hello", msgs[0].Content)

	// parseRerankCandidates: non-list -> nil,nil
	cands, err := parseRerankCandidates("bad")
	require.NoError(t, err)
	assert.Nil(t, cands)

	// parseRerankCandidates: valid list with fallback keys.
	cands, err = parseRerankCandidates([]interface{}{
		map[string]interface{}{"node_id": "n1", "text": "alpha", "bi_score": 0.5},
		map[string]interface{}{"id": "n2", "content": "beta", "rrf_score": 0.2},
		"bad",
	})
	require.NoError(t, err)
	require.Len(t, cands, 2)
	assert.Equal(t, "n1", cands[0].ID)
	assert.Equal(t, "alpha", cands[0].Content)
	assert.Equal(t, "n2", cands[1].ID)
	assert.Equal(t, "beta", cands[1].Content)

	// parseRerankCandidates: missing id should error.
	_, err = parseRerankCandidates([]interface{}{
		map[string]interface{}{"content": "no id"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "candidate id is required")
}
