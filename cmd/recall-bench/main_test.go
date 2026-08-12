package main

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
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
