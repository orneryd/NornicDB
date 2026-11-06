package testfixture

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFixedEmbedding2DReturnsDetachedVectors(t *testing.T) {
	embedder := FixedEmbedding2D{}

	first, err := embedder.Embed(context.Background(), "first")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0}, first)

	first[0], first[1] = 7, 8

	second, err := embedder.Embed(context.Background(), "second")
	require.NoError(t, err)
	require.Equal(t, []float32{1, 0}, second, "a caller's mutation leaked into a later embedding")
}

func TestFixedEmbedding2DRejectsCanceledContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	vector, err := FixedEmbedding2D{}.Embed(canceled, "text")
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, vector)

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	vector, err = FixedEmbedding2D{}.Embed(expired, "text")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, vector)
}

func TestFixedBatchEmbedding2DReturnsDetachedRows(t *testing.T) {
	embedder := FixedBatchEmbedding2D{}
	texts := []string{"first", "second", "third"}

	batch, err := embedder.EmbedBatch(context.Background(), texts)
	require.NoError(t, err)
	require.Len(t, batch, len(texts))
	for index := range batch {
		require.Equal(t, []float32{1, 0}, batch[index])
	}

	batch[0][0] = 9
	require.Equal(t, []float32{1, 0}, batch[1], "a row mutation leaked into a sibling row")
	require.Equal(t, []float32{1, 0}, batch[2], "a row mutation leaked into a sibling row")

	next, err := embedder.EmbedBatch(context.Background(), []string{"again"})
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 0}}, next, "a caller's mutation leaked into a later batch")
}

func TestFixedBatchEmbedding2DRejectsCanceledContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	batch, err := FixedBatchEmbedding2D{}.EmbedBatch(canceled, []string{"first", "second"})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, batch)

	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	batch, err = FixedBatchEmbedding2D{}.EmbedBatch(expired, []string{"first"})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, batch)
}
