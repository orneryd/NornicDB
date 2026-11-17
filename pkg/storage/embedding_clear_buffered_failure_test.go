package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingResetStopsBeforeWritesWhenAcknowledgedDrainFails(t *testing.T) {
	failure := errors.New("acknowledged generation could not be applied")
	b := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{
		manualDrain:  true,
		finalOutcome: func(owner testing.TB, err error) { require.ErrorIs(owner, err, failure) },
	})
	b.writeBehind.apply = func(*CommitBuffer) error { return failure }
	bufferedCreate(t, b, &Node{ID: "test:pending-vector", Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": "keep"}, ChunkEmbeddings: [][]float32{{1, 0}}, EmbedMeta: map[string]interface{}{"has_embedding": true}})
	count, err := b.ClearAllEmbeddingsForPrefix("test:")
	require.ErrorIs(t, err, failure)
	require.Zero(t, count)
	retained, err := b.GetNode(t.Context(), "test:pending-vector")
	require.NoError(t, err)
	require.Equal(t, [][]float32{{1, 0}}, retained.ChunkEmbeddings)
	require.Equal(t, "keep", retained.Properties["text"])
}
