package main

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type benchmarkRerankScorer struct {
	scores map[string]float32
	err    error
}

func (s benchmarkRerankScorer) Score(_ context.Context, _, document string) (float32, error) {
	if s.err != nil {
		return 0, s.err
	}
	return s.scores[document], nil
}

func TestDatabaseRetrieverRRFOptions(t *testing.T) {
	t.Run("default preset uses equal weights", func(t *testing.T) {
		opts, err := (databaseRetriever{rrfPreset: "default", minRRFScore: -1}).rrfOptions("long scientific claim query")
		require.NoError(t, err)
		assert.Equal(t, 1.0, opts.VectorWeight)
		assert.Equal(t, 1.0, opts.BM25Weight)
		assert.Equal(t, 0.01, opts.MinRRFScore)
	})

	t.Run("overrides replace selected preset", func(t *testing.T) {
		opts, err := (databaseRetriever{rrfPreset: "adaptive", rrfK: 30, vectorWeight: 0.25, bm25Weight: 1.75, minRRFScore: 0}).rrfOptions("long scientific claim query")
		require.NoError(t, err)
		assert.Equal(t, 30.0, opts.RRFK)
		assert.Equal(t, 0.25, opts.VectorWeight)
		assert.Equal(t, 1.75, opts.BM25Weight)
		assert.Equal(t, 0.0, opts.MinRRFScore)
	})

	_, err := (databaseRetriever{rrfPreset: "unknown"}).rrfOptions("query")
	require.Error(t, err)
}

func TestBenchmarkConfigProjectsCanonicalContentFields(t *testing.T) {
	config := benchmarkConfig()

	require.Equal(t, []string{"title", "text"}, config.Memory.SearchBM25Properties)
}

func TestBenchmarkRunResultsCanonicalDocumentRerank(t *testing.T) {
	results := []*nornicdb.SearchResult{
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-1", "title": "First", "text": "lower"}}, Score: 0.9},
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-2", "title": "Second", "text": "higher"}}, Score: 0.8},
	}

	run, err := benchmarkRunResults(context.Background(), "query", results, &benchmarkRerankConfig{scorer: benchmarkRerankScorer{
		scores: map[string]float32{"First\n\nlower": 0.1, "Second\n\nhigher": 0.9},
	}, topK: 2, maxDocChars: 1000})
	require.NoError(t, err)
	require.Len(t, run, 2)
	assert.Equal(t, "doc-2", run[0].DocumentID)
	assert.Equal(t, "doc-1", run[1].DocumentID)
	assert.Greater(t, run[0].Score, run[1].Score)
}

func TestBenchmarkRunResultsRejectsNonCanonicalAndRerankFailures(t *testing.T) {
	duplicate := []*nornicdb.SearchResult{
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-1", "text": "one"}}},
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-1", "text": "two"}}},
	}
	_, err := benchmarkRunResults(context.Background(), "query", duplicate, nil)
	require.ErrorContains(t, err, "duplicate BEIR document")

	unique := duplicate[:1]
	_, err = benchmarkRunResults(context.Background(), "query", unique, &benchmarkRerankConfig{scorer: benchmarkRerankScorer{err: errors.New("inference failed")}, topK: 1, maxDocChars: 100})
	require.ErrorContains(t, err, "inference failed")
}

type benchmarkBatchRerankStub struct {
	scores   map[string]float64
	err      error
	received []search.RerankCandidate
}

func (s *benchmarkBatchRerankStub) Rerank(_ context.Context, _ string, candidates []search.RerankCandidate) ([]search.RerankResult, error) {
	s.received = candidates
	if s.err != nil {
		return nil, s.err
	}
	results := make([]search.RerankResult, 0, len(candidates))
	for _, candidate := range candidates {
		if score, ok := s.scores[candidate.ID]; ok {
			results = append(results, search.RerankResult{ID: candidate.ID, CrossScore: score})
		}
	}
	return results, nil
}

// Regression: ingest opened the database with embedding disabled, so storage
// skipped the pending-embed marker and the run stored no corpus vectors.
func TestIngestConfigEnablesEmbeddingForProvider(t *testing.T) {
	cfg := ingestConfig("openai", 512, 50, 10)
	assert.True(t, cfg.Memory.EmbeddingEnabled)
	assert.Equal(t, 10, cfg.EmbeddingWorker.NumWorkers)
	assert.Equal(t, 512, cfg.EmbeddingWorker.ChunkSize)
	assert.Equal(t, 50, cfg.EmbeddingWorker.ChunkOverlap)
	assert.Equal(t, []string{"title", "text"}, cfg.Memory.SearchBM25Properties)

	assert.False(t, ingestConfig("", 512, 50, 1).Memory.EmbeddingEnabled)
}

func TestConfigureEmbedderOpenAIRequiresAPIKey(t *testing.T) {
	t.Setenv("RECALL_BENCH_TEST_KEY", "")
	err := configureEmbedder(nil, "openai", "titan-embed-v2", "http://127.0.0.1:4000", "RECALL_BENCH_TEST_KEY", 1024)
	require.ErrorContains(t, err, "$RECALL_BENCH_TEST_KEY")

	err = configureEmbedder(nil, "voyage", "model", "", "RECALL_BENCH_TEST_KEY", 1024)
	require.ErrorContains(t, err, "unsupported embedding provider")
}

func TestBenchmarkRunResultsBatchRerank(t *testing.T) {
	results := []*nornicdb.SearchResult{
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-1", "title": "First", "text": "lower"}}, Score: 0.9},
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-2", "title": "Second", "text": "higher"}}, Score: 0.8},
		{Node: &nornicdb.Node{Properties: map[string]any{"beir_id": "doc-3", "title": "Third", "text": "unranked"}}, Score: 0.7},
	}

	stub := &benchmarkBatchRerankStub{scores: map[string]float64{"doc-1": 0.1, "doc-2": 0.9}}
	run, err := benchmarkRunResults(context.Background(), "query", results, &benchmarkRerankConfig{batch: stub, topK: 2, maxDocChars: 8})
	require.NoError(t, err)
	require.Len(t, stub.received, 2, "only the top-k candidates are sent")
	assert.Equal(t, "First\n\nl", stub.received[0].Content, "documents are truncated to maxDocChars")
	require.Len(t, run, 3)
	assert.Equal(t, []string{"doc-2", "doc-1", "doc-3"}, []string{run[0].DocumentID, run[1].DocumentID, run[2].DocumentID})

	missing := &benchmarkBatchRerankStub{scores: map[string]float64{"doc-1": 0.1}}
	_, err = benchmarkRunResults(context.Background(), "query", results, &benchmarkRerankConfig{batch: missing, topK: 2, maxDocChars: 100})
	require.ErrorContains(t, err, `no score for document "doc-2"`)

	failing := &benchmarkBatchRerankStub{err: errors.New("rerank status 429")}
	_, err = benchmarkRunResults(context.Background(), "query", results, &benchmarkRerankConfig{batch: failing, topK: 2, maxDocChars: 100})
	require.ErrorContains(t, err, "rerank status 429")
}
