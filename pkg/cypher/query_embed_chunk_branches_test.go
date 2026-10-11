package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmbedQueryChunked_Branches_MaxChunkCapAndDimMismatch(t *testing.T) {
	ctx := context.Background()

	// Over the chunk cap the query fails before any chunk is embedded.
	longText := strings.Repeat("alpha beta gamma delta epsilon ", 5000)
	capped := &sequenceEmbedder{}
	_, err := embedQueryChunked(ctx, capped, longText)
	require.ErrorContains(t, err, "are searched")
	assert.Zero(t, capped.calls, "no chunk is embedded for a query over the cap")

	// A chunk embedded with another dimension than the first fails the query.
	dimMismatch := &sequenceEmbedder{
		embs: [][]float32{
			{1, 0},
			{9, 9, 9},
			{0, 1},
		},
	}
	_, err = embedQueryChunked(ctx, dimMismatch, strings.Repeat("chunk ", 1400))
	require.ErrorContains(t, err, "the embedding of query chunk 2 has 3 dimensions; the first chunk's has 2")

	// So does an empty embedding of a later chunk.
	_, err = embedQueryChunked(ctx, &sequenceEmbedder{embs: [][]float32{{1, 0}, {}, {0, 1}}}, strings.Repeat("chunk ", 1400))
	require.ErrorContains(t, err, "no embeddings produced")
}
