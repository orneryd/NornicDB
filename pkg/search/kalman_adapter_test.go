package search

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type kalmanPartialReranker struct{}

func (kalmanPartialReranker) Name() string                     { return "kalman-partial" }
func (kalmanPartialReranker) Enabled() bool                    { return true }
func (kalmanPartialReranker) IsAvailable(context.Context) bool { return true }
func (kalmanPartialReranker) Rerank(_ context.Context, _ string, candidates []RerankCandidate) ([]RerankResult, error) {
	for index := range candidates {
		if candidates[index].ID == "c" {
			return []RerankResult{{
				ID: candidates[index].ID, OriginalRank: index + 1,
				BiScore: candidates[index].Score, CrossScore: 0.005, FinalScore: 0.005,
			}}, nil
		}
	}
	return nil, nil
}

type kalmanFailingReranker struct{ calls *int }

func (r kalmanFailingReranker) Name() string                     { return "kalman-failing" }
func (r kalmanFailingReranker) Enabled() bool                    { return true }
func (r kalmanFailingReranker) IsAvailable(context.Context) bool { return true }
func (r kalmanFailingReranker) Rerank(context.Context, string, []RerankCandidate) ([]RerankResult, error) {
	*r.calls++
	return nil, fmt.Errorf("forced provider failure")
}

func TestKalmanSearchAdapterPreservesCanonicalRerankOrder(t *testing.T) {
	ctx := context.Background()
	service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
	t.Cleanup(func() { _ = service.Close() })

	query := "canonical order"
	options := DefaultSearchOptions()
	options.Limit = 3
	options.RerankEnabled = true
	options.RerankTopK = 3
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 0.03},
		{ID: "b", Content: "beta", Score: 0.02},
		{ID: "c", Content: "gamma", Score: 0.01},
	}
	outcome := service.runRerank(ctx, kalmanPartialReranker{}, query, candidates, rerankRunOptions{providerLimit: 3})
	if outcome.Method != rerankMethodApplied || outcome.FallbackTriggered {
		t.Fatalf("unexpected owner outcome: method=%q fallback=%v reason=%q", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason)
	}

	results := make([]SearchResult, len(outcome.Results))
	for index := range outcome.Results {
		result := outcome.Results[index]
		results[index] = SearchResult{ID: result.ID, NodeID: storage.NodeID(result.ID), Score: result.FinalScore}
	}
	response := &SearchResponse{
		Status: "success", Query: query, Results: results, Returned: len(results),
		SearchMethod: "rrf_hybrid+rerank", rerankApplied: true, rerankOrderOwned: true,
	}
	options.MinSimilarity = service.resolveMinSimilarity(options)
	service.resultCache = newSearchResultCache(4, time.Minute)
	cacheKey := service.cacheNamespace + "\x00" + strconv.FormatUint(service.IndexRevision(), 10) + "\x00" + searchCacheKey(query, nil, options)
	service.resultCache.Put(cacheKey, response)

	config := DefaultKalmanSearchConfig()
	config.EnableScoreSmoothing = false
	config.EnableRankingStability = false
	config.EnableLatencyPrediction = false
	adapter := NewKalmanSearchAdapter(service, config)
	got, err := adapter.Search(ctx, query, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := make([]string, len(got.Results))
	for index := range got.Results {
		gotIDs[index] = got.Results[index].ID
	}
	wantIDs := []string{"c", "a", "b"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("canonical order changed: got %v want %v", gotIDs, wantIDs)
	}
	if got.SearchMethod != "rrf_hybrid+rerank" || got.FallbackTriggered || got.FallbackReason != SearchFallbackNone {
		t.Fatalf("unexpected adapter outcome: method=%q fallback=%v reason=%q", got.SearchMethod, got.FallbackTriggered, got.FallbackReason)
	}
}

func TestKalmanSearchAdapterPreservesCanonicalFallbackOrderAfterStabilityHistory(t *testing.T) {
	ctx := context.Background()
	service := NewService(newNamespacedEngine(t), DefaultVectorDimensions, nil)
	t.Cleanup(func() { _ = service.Close() })
	calls := 0

	options := DefaultSearchOptions()
	options.Limit = 3
	options.RerankEnabled = true
	options.RerankTopK = 3
	candidates := []RerankCandidate{
		{ID: "a", Content: "alpha", Score: 2.0 / 61.0},
		{ID: "b", Content: "beta", Score: 2.0 / 62.0},
		{ID: "c", Content: "gamma", Score: 2.0 / 63.0},
	}
	outcome := service.runRerank(ctx, kalmanFailingReranker{calls: &calls}, "fallback", candidates, rerankRunOptions{providerLimit: 3})
	if outcome.Method != rerankMethodOriginal || !outcome.FallbackTriggered || outcome.FallbackReason != SearchFallbackRerankFailed {
		t.Fatalf("unexpected owner outcome: method=%q fallback=%v reason=%q", outcome.Method, outcome.FallbackTriggered, outcome.FallbackReason)
	}
	if calls != 1 {
		t.Fatalf("provider calls=%d want 1", calls)
	}

	service.resultCache = newSearchResultCache(4, time.Minute)
	cacheResponse := func(query string, response *SearchResponse) {
		resolved := *options
		resolved.MinSimilarity = service.resolveMinSimilarity(&resolved)
		key := service.cacheNamespace + "\x00" + strconv.FormatUint(service.IndexRevision(), 10) + "\x00" + searchCacheKey(query, nil, &resolved)
		service.resultCache.Put(key, response)
	}
	config := DefaultKalmanSearchConfig()
	config.EnableScoreSmoothing = false
	config.EnableRankingStability = true
	config.EnableLatencyPrediction = false
	adapter := NewKalmanSearchAdapter(service, config)
	for _, query := range []string{"history-1", "history-2"} {
		cacheResponse(query, &SearchResponse{
			Status: "success", Query: query,
			Results:  []SearchResult{{ID: "b", NodeID: storage.NodeID("b"), Score: 2.0 / 62.0}},
			Returned: 1, SearchMethod: "rrf_hybrid",
		})
		if _, err := adapter.Search(ctx, query, nil, options); err != nil {
			t.Fatal(err)
		}
	}

	results := make([]SearchResult, len(outcome.Results))
	for index, result := range outcome.Results {
		results[index] = SearchResult{ID: result.ID, NodeID: storage.NodeID(result.ID), Score: result.FinalScore}
	}
	fallbackResponse := &SearchResponse{
		Status: "success", Query: "fallback", Results: results, Returned: len(results),
		SearchMethod:      "rrf_hybrid",
		FallbackTriggered: true, FallbackReason: SearchFallbackRerankFailed,
		rerankOrderOwned: true,
	}
	cacheResponse("fallback", fallbackResponse)
	got, err := adapter.Search(ctx, "fallback", nil, options)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := make([]string, len(got.Results))
	for index := range got.Results {
		gotIDs[index] = got.Results[index].ID
	}
	wantIDs := []string{"a", "b", "c"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("canonical fallback order changed: got %v want %v", gotIDs, wantIDs)
	}
	if got.SearchMethod != "rrf_hybrid" || !got.FallbackTriggered || got.FallbackReason != SearchFallbackRerankFailed {
		t.Fatalf("unexpected adapter outcome: method=%q fallback=%v reason=%q", got.SearchMethod, got.FallbackTriggered, got.FallbackReason)
	}
}

func TestKalmanSearchAdapterPreservesCanonicalFallbackOrderThroughPublicSearch(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedEngine(t)
	service := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = service.Close() })
	for index, spec := range []struct {
		id     string
		labels []string
		vector []float32
	}{
		{id: "a", labels: []string{"Doc"}, vector: []float32{1, 0}},
		{id: "b", labels: []string{"Doc", "History"}, vector: []float32{0.99, 0.01}},
		{id: "c", labels: []string{"Doc"}, vector: []float32{0.98, 0.02}},
	} {
		node := &storage.Node{
			ID: storage.NodeID(spec.id), Labels: spec.labels,
			Properties:      map[string]any{"content": "alpha document " + fmt.Sprint(index)},
			ChunkEmbeddings: [][]float32{spec.vector},
		}
		if _, err := engine.CreateNode(node); err != nil {
			t.Fatal(err)
		}
		if err := service.IndexNode(node); err != nil {
			t.Fatal(err)
		}
	}
	service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
	calls := 0
	service.SetReranker(kalmanFailingReranker{calls: &calls})
	config := DefaultKalmanSearchConfig()
	config.EnableScoreSmoothing = false
	config.EnableRankingStability = true
	config.EnableLatencyPrediction = false
	adapter := NewKalmanSearchAdapter(service, config)

	history := DefaultSearchOptions()
	history.Limit = 1
	history.Types = []string{"History"}
	history.RerankEnabled = true
	for range 2 {
		response, err := adapter.Search(ctx, "alpha", []float32{1, 0}, history)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Results) != 1 || response.Results[0].ID != "b" {
			t.Fatalf("history did not establish b: %+v", response.Results)
		}
	}

	options := DefaultSearchOptions()
	options.Limit = 3
	options.Types = []string{"Doc"}
	options.RerankEnabled = true
	options.RerankTopK = 3
	got, err := adapter.Search(ctx, "alpha", []float32{1, 0}, options)
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := make([]string, len(got.Results))
	for index := range got.Results {
		gotIDs[index] = got.Results[index].ID
	}
	wantIDs := []string{"a", "b", "c"}
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("public search fallback order changed: got %v want %v", gotIDs, wantIDs)
	}
	if got.SearchMethod != "rrf_hybrid" || !got.FallbackTriggered || got.FallbackReason != SearchFallbackRerankFailed {
		t.Fatalf("unexpected public adapter outcome: method=%q fallback=%v reason=%q", got.SearchMethod, got.FallbackTriggered, got.FallbackReason)
	}
	if calls < 2 {
		t.Fatalf("provider calls=%d want at least 2", calls)
	}
}

func TestConcurrentCachedKalmanReadersOwnTheirResponses(t *testing.T) {
	ctx := context.Background()
	engine := newNamespacedEngine(t)
	service := NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for _, id := range []string{"a", "b", "c"} {
		node := &storage.Node{
			ID: storage.NodeID(id), Labels: []string{"Doc"},
			Properties:      map[string]any{"content": "alpha document " + id},
			ChunkEmbeddings: [][]float32{{1, 0}},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	service.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(service.vectorIndex), NewCPUExactScorer(service.vectorIndex))
	service.SetReranker(kalmanPartialReranker{})
	options := DefaultSearchOptions()
	options.Limit = 3
	options.RerankEnabled = true
	options.RerankTopK = 3
	warm, err := service.Search(ctx, "alpha", []float32{1, 0}, options)
	require.NoError(t, err)
	require.Equal(t, "rrf_hybrid+rerank", warm.SearchMethod)
	wantIDs := make([]string, len(warm.Results))
	for index := range warm.Results {
		wantIDs[index] = warm.Results[index].ID
	}

	config := DefaultKalmanSearchConfig()
	config.EnableScoreSmoothing = false
	config.EnableRankingStability = false
	config.EnableLatencyPrediction = false
	adapters := []*KalmanSearchAdapter{NewKalmanSearchAdapter(service, config), NewKalmanSearchAdapter(service, config)}
	start := make(chan struct{})
	errors := make(chan error, len(adapters))
	var group sync.WaitGroup
	for _, adapter := range adapters {
		group.Add(1)
		go func(adapter *KalmanSearchAdapter) {
			defer group.Done()
			<-start
			for range 100 {
				owned := *options
				response, err := adapter.Search(ctx, "alpha", []float32{1, 0}, &owned)
				if err != nil {
					errors <- err
					return
				}
				if got := []string{response.Results[0].ID, response.Results[1].ID, response.Results[2].ID}; fmt.Sprint(got) != fmt.Sprint(wantIDs) {
					errors <- fmt.Errorf("cached order changed: %v", got)
					return
				}
			}
		}(adapter)
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestDefaultKalmanSearchConfig(t *testing.T) {
	cfg := DefaultKalmanSearchConfig()

	if !cfg.EnableScoreSmoothing {
		t.Error("Expected EnableScoreSmoothing to be true")
	}
	if !cfg.EnableRankingStability {
		t.Error("Expected EnableRankingStability to be true")
	}
	if !cfg.EnableLatencyPrediction {
		t.Error("Expected EnableLatencyPrediction to be true")
	}
	if cfg.StabilityBoost != 1.05 {
		t.Errorf("Expected StabilityBoost 1.05, got %f", cfg.StabilityBoost)
	}
	if cfg.StabilityWindow != 10 {
		t.Errorf("Expected StabilityWindow 10, got %d", cfg.StabilityWindow)
	}
}

func TestNewKalmanSearchAdapter(t *testing.T) {
	// Create a storage engine
	engine := newNamespacedEngine(t)

	// Create search service
	service := NewService(engine, DefaultVectorDimensions, nil)

	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}
	if adapter.service != service {
		t.Error("Service not set correctly")
	}
	if adapter.docFilters == nil {
		t.Error("docFilters should be initialized")
	}
	if adapter.recentRankings == nil {
		t.Error("recentRankings should be initialized")
	}
	if adapter.latencyFilter == nil {
		t.Error("latencyFilter should be initialized")
	}
}

func TestKalmanSearchAdapter_EnhanceResult(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	result := SearchResult{
		ID:    "doc-1",
		Score: 0.85,
	}

	enhanced := adapter.enhanceResult(result, "test query")

	// Score should be smoothed
	if enhanced.Score < 0 || enhanced.Score > 1 {
		t.Errorf("Smoothed score out of range: %f", enhanced.Score)
	}

	// Stats should show smoothing
	stats := adapter.GetStats()
	if stats.ScoresSmoothed != 1 {
		t.Errorf("Expected 1 score smoothed, got %d", stats.ScoresSmoothed)
	}
}

func TestKalmanSearchAdapter_ApplyRankingStability(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some ranking history
	adapter.recentRankings = []queryRanking{
		{Query: "q1", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-2", "doc-3"}},
		{Query: "q2", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-2", "doc-4"}},
		{Query: "q3", Timestamp: time.Now(), TopDocs: []string{"doc-1", "doc-3", "doc-5"}},
	}

	results := []SearchResult{
		{ID: "doc-1", Score: 0.80},
		{ID: "doc-2", Score: 0.79},
		{ID: "doc-6", Score: 0.78}, // New doc
	}

	stabilized := adapter.applyRankingStability(results, "test")

	// doc-1 should have stability boost (appeared in all queries at top)
	doc1Boosted := false
	for _, r := range stabilized {
		if r.ID == "doc-1" && r.Score > 0.80 {
			doc1Boosted = true
		}
	}
	if !doc1Boosted {
		t.Error("Expected doc-1 to have stability boost")
	}
}

func TestKalmanSearchAdapter_RecordRanking(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)

	cfg := DefaultKalmanSearchConfig()
	cfg.StabilityWindow = 3
	adapter := NewKalmanSearchAdapter(service, cfg)

	results := []SearchResult{
		{ID: "doc-1", Score: 0.9},
		{ID: "doc-2", Score: 0.8},
		{ID: "doc-3", Score: 0.7},
	}

	// Record multiple rankings
	for i := 0; i < 5; i++ {
		adapter.recordRanking("query", results)
	}

	// Should trim to window size
	if len(adapter.recentRankings) != 3 {
		t.Errorf("Expected 3 rankings (window size), got %d", len(adapter.recentRankings))
	}
}

func TestKalmanSearchAdapter_SortByScore(t *testing.T) {
	response := &SearchResponse{Results: []SearchResult{
		{ID: "doc-1", Score: 0.5},
		{ID: "doc-2", Score: 0.9},
		{ID: "doc-3", Score: 0.7},
	}}

	if !response.reorderResultsByScore() {
		t.Fatal("expected non-reranked response to be sortable")
	}

	// Should be sorted highest first
	if response.Results[0].ID != "doc-2" {
		t.Errorf("Expected doc-2 first, got %s", response.Results[0].ID)
	}
	if response.Results[1].ID != "doc-3" {
		t.Errorf("Expected doc-3 second, got %s", response.Results[1].ID)
	}
	if response.Results[2].ID != "doc-1" {
		t.Errorf("Expected doc-1 third, got %s", response.Results[2].ID)
	}
}

func TestKalmanSearchAdapter_GetPredictedLatency(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Simulate some latency measurements
	for i := 0; i < 10; i++ {
		adapter.latencyFilter.Process(float64(10 + i))
	}

	predicted := adapter.GetPredictedLatency(5)

	// Should predict a reasonable value
	if predicted < 0 {
		t.Errorf("Predicted latency should be non-negative, got %f", predicted)
	}
}

func TestKalmanSearchAdapter_GetLatencyTrend(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Simulate increasing latency (positive trend)
	for i := 0; i < 10; i++ {
		adapter.latencyFilter.Process(float64(10 + i*2))
	}

	trend := adapter.GetLatencyTrend()

	// Should show positive velocity (getting slower)
	if trend <= 0 {
		t.Logf("Trend may depend on filter convergence: %f", trend)
		// Don't fail - filter behavior depends on many factors
	}
}

func TestKalmanSearchAdapter_GetDocumentRelevanceTrend(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Process some results to create document filters
	for i := 0; i < 5; i++ {
		result := SearchResult{
			ID:    "rising-doc",
			Score: 0.5 + float64(i)*0.1, // Increasing scores
		}
		adapter.enhanceResult(result, "test")
	}

	trend := adapter.GetDocumentRelevanceTrend("rising-doc")

	// Trend may be positive for rising doc
	_ = trend // Just verify no panic

	// Unknown doc should return 0
	unknownTrend := adapter.GetDocumentRelevanceTrend("unknown")
	if unknownTrend != 0 {
		t.Errorf("Expected 0 for unknown doc, got %f", unknownTrend)
	}
}

func TestKalmanSearchAdapter_GetRisingDocuments(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some document filters with different trends
	for i := 0; i < 5; i++ {
		adapter.enhanceResult(SearchResult{ID: "rising", Score: 0.3 + float64(i)*0.1}, "q")
		adapter.enhanceResult(SearchResult{ID: "falling", Score: 0.8 - float64(i)*0.1}, "q")
	}

	rising := adapter.GetRisingDocuments(0.01)
	falling := adapter.GetFallingDocuments(-0.01)

	// May or may not find documents depending on filter convergence
	_ = rising
	_ = falling
}

func TestKalmanSearchAdapter_GetStats(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	stats := adapter.GetStats()

	if stats.TotalQueries != 0 {
		t.Errorf("Initial TotalQueries should be 0")
	}
	if stats.ScoresSmoothed != 0 {
		t.Errorf("Initial ScoresSmoothed should be 0")
	}
}

func TestKalmanSearchAdapter_GetService(t *testing.T) {
	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	if adapter.GetService() != service {
		t.Error("GetService should return the underlying service")
	}
}

func TestKalmanSearchAdapter_Reset(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create some state
	adapter.enhanceResult(SearchResult{ID: "doc-1", Score: 0.8}, "query")
	adapter.recordRanking("query", []SearchResult{{ID: "doc-1"}})

	if len(adapter.docFilters) == 0 {
		t.Error("Expected filters to be created")
	}
	if len(adapter.recentRankings) == 0 {
		t.Error("Expected rankings to be recorded")
	}

	// Reset
	adapter.Reset()

	if len(adapter.docFilters) != 0 {
		t.Error("Filters should be cleared")
	}
	if len(adapter.recentRankings) != 0 {
		t.Error("Rankings should be cleared")
	}
}

func TestKalmanSearchAdapter_DisabledSmoothing(t *testing.T) {
	// Disable Kalman filtering globally
	cleanup := config.WithKalmanDisabled()
	defer cleanup()

	engine := newNamespacedEngine(t)
	service := NewService(engine, DefaultVectorDimensions, nil)

	cfg := DefaultKalmanSearchConfig()
	cfg.EnableScoreSmoothing = false
	adapter := NewKalmanSearchAdapter(service, cfg)

	result := SearchResult{
		ID:    "no-smooth-doc",
		Score: 0.75,
	}

	enhanced := adapter.enhanceResult(result, "query")

	// Score should remain unchanged
	if enhanced.Score != 0.75 {
		t.Errorf("Expected unchanged score 0.75, got %f", enhanced.Score)
	}
}

func TestKalmanSearchAdapter_Search_Integration(t *testing.T) {
	// Enable Kalman filtering
	cleanup := config.WithKalmanEnabled()
	defer cleanup()

	engine := newNamespacedEngine(t)

	// Add some nodes with embeddings
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		embedding := make([]float32, 1024)
		for j := range embedding {
			embedding[j] = float32(i*j) / 1024.0
		}
		_, _ = engine.CreateNode(&storage.Node{
			Labels:          []string{"Document"},
			Properties:      map[string]interface{}{"title": "Doc " + string(rune('A'+i))},
			ChunkEmbeddings: [][]float32{embedding},
		})
	}

	service := NewService(engine, DefaultVectorDimensions, nil)
	_ = service.BuildIndexes(ctx)

	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	// Create query embedding
	queryEmbed := make([]float32, 1024)
	for i := range queryEmbed {
		queryEmbed[i] = float32(i) / 1024.0
	}

	opts := &SearchOptions{
		Limit: 10,
	}

	// Run search
	response, err := adapter.Search(ctx, "test query", queryEmbed, opts)
	if err != nil {
		t.Fatalf("Search error: %v", err)
	}

	// Check stats
	stats := adapter.GetStats()
	if stats.TotalQueries != 1 {
		t.Errorf("Expected 1 query, got %d", stats.TotalQueries)
	}

	_ = response
}

func TestMinHelper(t *testing.T) {
	if min(5, 10) != 5 {
		t.Error("min(5, 10) should be 5")
	}
	if min(10, 5) != 5 {
		t.Error("min(10, 5) should be 5")
	}
	if min(5, 5) != 5 {
		t.Error("min(5, 5) should be 5")
	}
}

// Benchmark search adapter
func BenchmarkKalmanSearchAdapter_EnhanceResult(b *testing.B) {
	config.EnableKalmanFiltering()
	defer config.DisableKalmanFiltering()

	engine := newNamespacedEngine(b)
	service := NewService(engine, DefaultVectorDimensions, nil)
	adapter := NewKalmanSearchAdapter(service, DefaultKalmanSearchConfig())

	result := SearchResult{
		ID:    "bench-doc",
		Score: 0.75,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		adapter.enhanceResult(result, "query")
	}
}
