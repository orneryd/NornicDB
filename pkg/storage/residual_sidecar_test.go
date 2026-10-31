package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResidualSidecarDurableCache(t *testing.T) {
	b := newSidecarTestBadger(t)
	payload := publicationProofPayload(t, b)
	require.NoError(t, b.UpdateNodeEmbeddingSidecar(payload))
	_, err := b.GetStoredNode(payload.ID)
	require.NoError(t, err)
	next := embeddingWriteback(t, b, payload.ID, [][]float32{{3, 4}}, map[string]any{"chunk_count": 1}, time.Time{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	t.Cleanup(setPublicationStageHook(func(uint64) { close(entered); <-release }))
	done := make(chan error, 1)
	go func() { done <- b.UpdateNodeEmbeddingSidecar(next) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sidecar did not reach its physical publication boundary")
	}
	stored, readErr := b.GetStoredNode(payload.ID)
	unblock()
	require.NoError(t, <-done)
	require.NoError(t, readErr)
	require.Equal(t, next.ChunkEmbeddings, stored.ChunkEmbeddings)
}
