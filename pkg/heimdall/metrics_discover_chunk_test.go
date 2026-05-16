package heimdall

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
)

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func chunkTestText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, countTestTokens)
}

type stubQueryDB struct{}

func (s *stubQueryDB) Query(ctx context.Context, cypher string, params map[string]interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func (s *stubQueryDB) Stats() interface{} { return nil }
func (s *stubQueryDB) NodeCount() (int64, error) {
	return 0, nil
}
func (s *stubQueryDB) EdgeCount() (int64, error) {
	return 0, nil
}

type testEmbedder struct {
	mu sync.Mutex

	failIfTokensGreater int
	calls               int
	maxLen              int
	maxTokens           int
}

func (e *testEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.calls++
	if len(text) > e.maxLen {
		e.maxLen = len(text)
	}
	tokens, err := countTestTokens(text)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if tokens > e.maxTokens {
		e.maxTokens = tokens
	}
	fail := e.failIfTokensGreater > 0 && tokens > e.failIfTokensGreater
	e.mu.Unlock()

	if fail {
		return nil, fmt.Errorf("simulated tokenizer overflow for tokens=%d", tokens)
	}
	return []float32{0.1, 0.2}, nil
}

func (e *testEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

type testSearcher struct {
	mu sync.Mutex

	hybridCalls []string
	searchCalls []string
}

func (s *testSearcher) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SemanticSearchResult, error) {
	s.mu.Lock()
	s.hybridCalls = append(s.hybridCalls, query)
	s.mu.Unlock()

	return []*SemanticSearchResult{
		{
			ID:         "node-1",
			Labels:     []string{"Memory"},
			Properties: map[string]interface{}{"title": "hello"},
			Score:      0.03,
		},
	}, nil
}

func (s *testSearcher) Search(ctx context.Context, query string, labels []string, limit int) ([]*SemanticSearchResult, error) {
	s.mu.Lock()
	s.searchCalls = append(s.searchCalls, query)
	s.mu.Unlock()
	return nil, nil
}

func (s *testSearcher) Neighbors(ctx context.Context, nodeID string) ([]string, error) {
	return nil, nil
}
func (s *testSearcher) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	return nil, nil
}
func (s *testSearcher) GetNode(ctx context.Context, nodeID string) (*NodeData, error) {
	return nil, nil
}

func loadLargeDocQuery(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "features", "gpu-acceleration.md")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	query := string(data)
	tokens, err := countTestTokens(query)
	require.NoError(t, err)
	require.Greater(t, tokens, 512)
	return query
}

// TestQueryExecutor_Discover_SimilarityIsBestScore is a regression test for the
// scoring bug where the outer RRF rank-decay score (`1/(60+rank)`, capped at
// ~0.0164 for rank 1) leaked through as `Similarity` instead of the strongest
// underlying score reported by the searcher. The bug made `min_similarity`
// thresholds useless across the discover surface.
//
// The test stages three candidate hits at different scores per chunk and
// asserts the executor surfaces the strongest score per node (max across
// chunks), not the rank-derived RRF value.
func TestQueryExecutor_Discover_SimilarityIsBestScore(t *testing.T) {
	emb := &testEmbedder{}
	searcher := &scoringSearcher{
		// Two chunks each see the same node "match" at different scores.
		// Best score = 0.92; the outer-RRF top-rank score would be 1/(60+1) = ~0.0164.
		batches: [][]*SemanticSearchResult{
			{
				{ID: "match", Labels: []string{"Memory"}, Properties: map[string]interface{}{"title": "match"}, Score: 0.55},
				{ID: "weak", Labels: []string{"Memory"}, Properties: map[string]interface{}{"title": "weak"}, Score: 0.20},
			},
			{
				{ID: "match", Labels: []string{"Memory"}, Properties: map[string]interface{}{"title": "match"}, Score: 0.92},
			},
		},
	}
	exec := NewQueryExecutorWithSearch(&stubQueryDB{}, searcher, emb, 5*time.Second)

	// Long query forces the multi-chunk fusion path.
	longQuery := loadLargeDocQuery(t)
	res, err := exec.Discover(context.Background(), longQuery, nil, 5, 1)
	require.NoError(t, err)
	require.Equal(t, "vector", res.Method)
	require.NotEmpty(t, res.Results)

	var matchSim float64
	for _, r := range res.Results {
		if r.Title == "match" {
			matchSim = r.Similarity
			break
		}
	}
	require.InDelta(t, 0.92, matchSim, 1e-6,
		"expected match.Similarity = best raw score across chunks (0.92), got %.6f — looks like RRF rank score leaked through", matchSim)
}

// scoringSearcher rotates through a list of result batches on each
// HybridSearch call so tests can stage per-chunk scoring scenarios.
type scoringSearcher struct {
	mu      sync.Mutex
	batches [][]*SemanticSearchResult
	idx     int
}

func (s *scoringSearcher) HybridSearch(ctx context.Context, query string, queryEmbedding []float32, labels []string, limit int) ([]*SemanticSearchResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.batches) == 0 {
		return nil, nil
	}
	batch := s.batches[s.idx%len(s.batches)]
	s.idx++
	return batch, nil
}

func (s *scoringSearcher) Search(ctx context.Context, query string, labels []string, limit int) ([]*SemanticSearchResult, error) {
	return nil, nil
}
func (s *scoringSearcher) Neighbors(ctx context.Context, nodeID string) ([]string, error) {
	return nil, nil
}
func (s *scoringSearcher) GetEdgesForNode(ctx context.Context, nodeID string) ([]*GraphEdge, error) {
	return nil, nil
}
func (s *scoringSearcher) GetNode(ctx context.Context, nodeID string) (*NodeData, error) {
	return nil, nil
}

func TestQueryExecutor_Discover_ChunksLongQueriesForVectorSearch(t *testing.T) {
	emb := &testEmbedder{failIfTokensGreater: 512}
	searcher := &testSearcher{}
	exec := NewQueryExecutorWithSearch(&stubQueryDB{}, searcher, emb, 5*time.Second)

	longQuery := loadLargeDocQuery(t)
	res, err := exec.Discover(context.Background(), longQuery, nil, 10, 1)
	require.NoError(t, err)
	require.Equal(t, "vector", res.Method)
	require.NotEmpty(t, res.Results)

	emb.mu.Lock()
	calls := emb.calls
	maxTokens := emb.maxTokens
	emb.mu.Unlock()
	require.GreaterOrEqual(t, calls, 2, "expected multiple chunk embeddings")
	require.LessOrEqual(t, maxTokens, 512, "expected no embedding call on query chunks > 512 tokens")

	searcher.mu.Lock()
	hybridCalls := append([]string(nil), searcher.hybridCalls...)
	searchCalls := append([]string(nil), searcher.searchCalls...)
	searcher.mu.Unlock()
	require.GreaterOrEqual(t, len(hybridCalls), 2, "expected multiple per-chunk hybrid searches")
	require.Empty(t, searchCalls, "expected no text-only fallback when chunked vector search succeeds")

	maxQTokens := 0
	for _, q := range hybridCalls {
		tok, err := countTestTokens(q)
		require.NoError(t, err)
		if tok > maxQTokens {
			maxQTokens = tok
		}
	}
	require.LessOrEqual(t, maxQTokens, 512, "expected no hybrid search call on query chunks > 512 tokens")
}
