package nornicdb

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/stretchr/testify/require"
)

// Query embedding embeds at most NORNICDB_SEARCH_QUERY_MAX_CHUNKS chunks of
// a long query, or fails over them with NORNICDB_SEARCH_QUERY_CHUNK_OVERFLOW=error;
// chunking itself returns every chunk (#907).
func TestEmbedQueryChunksHonoursChunkLimit(t *testing.T) {
	ctx := context.Background()
	db := &DB{}
	embedder := &chunkingTestEmbedder{dims: 4}
	query := strings.Repeat("evidence ", 3000)
	all, err := db.chunkQueryWithEmbedder(ctx, embedder, query)
	require.NoError(t, err)
	require.Greater(t, len(all), 2)

	t.Setenv(search.EnvSearchQueryMaxChunks, "2")
	t.Setenv(search.EnvSearchQueryChunkOverflow, "")
	chunks, embeddings, err := db.embedQueryChunksWithEmbedder(ctx, embedder, query)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Len(t, embeddings, 2)

	t.Setenv(search.EnvSearchQueryChunkOverflow, "error")
	_, _, err = db.embedQueryChunksWithEmbedder(ctx, embedder, query)
	require.ErrorContains(t, err, "at most 2 are searched")
}
