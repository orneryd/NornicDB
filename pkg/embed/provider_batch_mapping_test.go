package embed_test

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestProviderHTTPDocumentChunkMapping(t *testing.T) {
	mock := &batchHTTPMock{}
	e := embed.NewTracedEmbedder(newBatchProvider(t, "voyage", "text", mock))
	texts := []string{"0 1 2 3 4", "5 6"}
	results, err := e.EmbedDocumentBatchChunks(context.Background(), texts, 1, 0)
	require.NoError(t, err)
	require.Equal(t, []int{7}, mock.sizes)
	require.Equal(t, []string{"0", "1", "2", "3", "4"}, results[0].Chunks)
	require.Equal(t, [][]float32{{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 1}}, results[0].Embeddings)
	require.Equal(t, []string{"5", "6"}, results[1].Chunks)
	require.Equal(t, [][]float32{{5, 1}, {6, 1}}, results[1].Embeddings)
}
