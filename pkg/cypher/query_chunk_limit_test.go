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

// A vector procedure's text query embeds at most
// NORNICDB_SEARCH_QUERY_MAX_CHUNKS chunks, or fails over them with
// NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW=error: its rows can't report dropped
// chunks (#907).
func TestEmbedQueryChunkedHonoursChunkLimit(t *testing.T) {
	ctx := context.Background()
	embedder := &chunkCountingEmbedder{chunks: []string{"a", "b", "c", "d"}}
	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	t.Setenv(search.EnvSearchQueryChunkOverflow, "")
	vector, err := embedQueryChunked(ctx, embedder, "long query")
	require.NoError(t, err)
	require.NotEmpty(t, vector)
	require.Equal(t, 2, embedder.calls)

	embedder.calls = 0
	t.Setenv(search.EnvSearchQueryChunkOverflow, "error")
	_, err = embedQueryChunked(ctx, embedder, "long query")
	require.ErrorContains(t, err, "at most 2 are searched")
	require.Zero(t, embedder.calls)
}

// A continued db.retrieve page reports a long query's chunk counts.
func TestSearchContinuationPageReportsQueryChunks(t *testing.T) {
	page := executeSearchContinuationPage(&search.SearchContinuationPage{ExpiresAt: time.Now(), QueryChunksSearched: 3, QueryChunksTotal: 5})
	row := page.Rows[0][0].(map[string]interface{})
	require.Equal(t, int64(3), row["query_chunks_searched"])
	require.Equal(t, int64(5), row["query_chunks_total"])
	page = executeSearchContinuationPage(&search.SearchContinuationPage{ExpiresAt: time.Now()})
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
	service := search.NewServiceWithDimensions(store, 2)
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
