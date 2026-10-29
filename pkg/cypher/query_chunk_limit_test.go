package cypher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type chunkCountingEmbedder struct {
	chunks []string
	calls  int
}

func (e *chunkCountingEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls++
	return []float32{1, 0}, nil
}

func (e *chunkCountingEmbedder) ChunkText(string, int, int) ([]string, error) { return e.chunks, nil }

// A vector procedure's text query over NORNICDB_SEARCH_QUERY_MAX_CHUNKS
// chunks fails, whatever NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW says: its rows
// (node, score) can't report dropped chunks (#907). Within the
// cap every chunk is embedded.
func TestEmbedQueryChunkedHonoursChunkLimit(t *testing.T) {
	ctx := context.Background()
	embedder := &chunkCountingEmbedder{chunks: []string{"a", "b", "c", "d"}}
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	for _, overflow := range []string{"", "truncate", "error"} {
		embedder.calls = 0
		t.Setenv(search.EnvSearchQueryChunkOverflow, overflow)
		_, err := embedQueryChunked(ctx, embedder, "long query")
		require.ErrorContains(t, err, "search query has 4 chunks; at most 2 are searched", overflow)
		require.Zero(t, embedder.calls, overflow)
	}

	embedder.calls = 0
	t.Setenv(search.EnvSearchQueryMaxChunks, "4")
	vector, err := embedQueryChunked(ctx, embedder, "long query")
	require.NoError(t, err)
	require.NotEmpty(t, vector)
	require.Equal(t, 4, embedder.calls)

	// Through the procedure.
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(embedder)
	_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Document"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	_, err = exec.Execute(ctx, "CALL db.index.vector.queryNodes('idx', 5, 'long query') YIELD node RETURN node", nil)
	require.ErrorContains(t, err, "at most 2 are searched")
}

// A continued db.retrieve answers with one page row even when nothing
// matched, and that row reports how many of a long query's chunks were
// searched: a caller can tell "nothing matched" from "the searched part
// matched nothing" (#907).
func TestRetrieveContinuationReportsQueryChunksWithoutHits(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&chunkCountingEmbedder{chunks: []string{"alpha", "beta", "gamma", "delta"}})
	service := search.NewService(store, 2, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	exec.SetSearchService(service)
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	t.Setenv(search.EnvSearchQueryChunkOverflow, "")

	plain, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha beta gamma delta', limit: 5})", nil)
	require.NoError(t, err)
	require.Empty(t, plain.Rows)

	continued, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha beta gamma delta', limit: 5, n: 5}) YIELD page RETURN page", nil)
	require.NoError(t, err)
	require.Len(t, continued.Rows, 1)
	page := continued.Rows[0][0].(map[string]interface{})
	require.Empty(t, page["results"])
	require.Equal(t, int64(2), page["query_chunks_searched"])
	require.Equal(t, int64(4), page["query_chunks_total"])
}

// A continued db.retrieve page reports a long query's chunk counts.
func TestSearchContinuationPageReportsQueryChunks(t *testing.T) {
	page := executeSearchContinuationPage(&search.SearchContinuationPage{ExpiresAt: time.Now(), QueryChunksSearched: 3, QueryChunksTotal: 5}, "")
	row := page.Rows[0][0].(map[string]interface{})
	require.Equal(t, int64(3), row["query_chunks_searched"])
	require.Equal(t, int64(5), row["query_chunks_total"])
	page = executeSearchContinuationPage(&search.SearchContinuationPage{ExpiresAt: time.Now()}, "")
	require.NotContains(t, page.Rows[0][0].(map[string]interface{}), "query_chunks_total")
}

type failingQueryChunkReranker struct{}

func (failingQueryChunkReranker) Name() string                     { return "failing" }
func (failingQueryChunkReranker) Enabled() bool                    { return true }
func (failingQueryChunkReranker) IsAvailable(context.Context) bool { return true }
func (failingQueryChunkReranker) Rerank(context.Context, string, []search.RerankCandidate) ([]search.RerankResult, error) {
	return nil, errors.New("rerank unavailable")
}

// db.retrieve and db.rretrieve rows report how many of a long query's chunks
// were searched, whatever the fallback reason: here a failed rerank holds
// fallback_reason, and the counts still show that chunks were dropped
// (#907).
func TestRetrieveRowsReportQueryChunks(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	exec := NewStorageExecutor(store)
	exec.SetEmbedder(&chunkCountingEmbedder{chunks: []string{"alpha", "beta", "gamma", "delta"}})
	_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	service.SetReranker(failingQueryChunkReranker{})
	exec.SetSearchService(service)
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	t.Setenv(search.EnvSearchQueryChunkOverflow, "")

	column := func(result *ExecuteResult, name string) interface{} {
		for index, candidate := range result.Columns {
			if candidate == name {
				return result.Rows[0][index]
			}
		}
		t.Fatalf("no column %s in %v", name, result.Columns)
		return nil
	}
	result, err := exec.Execute(ctx, "CALL db.rretrieve({query: 'alpha beta gamma delta', limit: 5})", nil)
	require.NoError(t, err)
	require.NotEmpty(t, result.Rows)
	require.Equal(t, int64(2), column(result, "query_chunks_searched"))
	require.Equal(t, int64(4), column(result, "query_chunks_total"))
	require.Equal(t, string(search.SearchFallbackRerankFailed), column(result, "fallback_reason"))

	yielded, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha beta gamma delta', limit: 5}) YIELD node, query_chunks_searched, query_chunks_total RETURN query_chunks_searched, query_chunks_total", nil)
	require.NoError(t, err)
	require.Equal(t, []interface{}{int64(2), int64(4)}, yielded.Rows[0])

	// A query searched whole reports 0 and 0.
	exec.SetEmbedder(&chunkCountingEmbedder{chunks: []string{"alpha"}})
	result, err = exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 5})", nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), column(result, "query_chunks_total"))
}

type sizedVectorEmbedder struct {
	vector []float32
	dims   int
}

func (e sizedVectorEmbedder) Embed(context.Context, string) ([]float32, error) { return e.vector, nil }
func (e sizedVectorEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (e sizedVectorEmbedder) Dimensions() int { return e.dims }

// db.retrieve takes an empty or wrong-dimension query vector as a failed
// embedding: the search falls back with query_embedding_failed, or the call
// fails under failClosed, instead of searching without a vector.
func TestRetrieveUnusableQueryEmbeddingFails(t *testing.T) {
	ctx := context.Background()
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	_, err := store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Document"}, Properties: map[string]interface{}{"content": "alpha"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	service := search.NewService(store, 2, nil)
	require.NoError(t, service.BuildIndexes(ctx))
	for name, embedder := range map[string]sizedVectorEmbedder{
		"empty":           {vector: []float32{}, dims: 2},
		"other dimension": {vector: []float32{1, 0, 0}, dims: 2},
	} {
		exec := NewStorageExecutor(store)
		exec.SetEmbedder(embedder)
		exec.SetSearchService(service)
		result, err := exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 5}) YIELD fallback_reason RETURN fallback_reason", nil)
		require.NoError(t, err, name)
		require.NotEmpty(t, result.Rows, name)
		require.Equal(t, string(search.SearchFallbackQueryEmbeddingFailed), result.Rows[0][0], name)
		_, err = exec.Execute(ctx, "CALL db.retrieve({query: 'alpha', limit: 5, failClosed: true}) YIELD node RETURN node", nil)
		require.Error(t, err, name)
	}
	require.Zero(t, queryEmbedderDimensions(&chunkCountingEmbedder{}))
}
