package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
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
