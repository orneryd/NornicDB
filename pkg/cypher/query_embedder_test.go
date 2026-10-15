package cypher

import (
	"context"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestQueryEmbedderSharedBeforeReadiness(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "ready")
	base := NewStorageExecutor(store)
	scoped := NewStorageExecutor(store)
	scoped.ShareEmbedderFrom(base)
	clones := []*StorageExecutor{scoped, scoped.cloneForStorage(store), scoped.cloneWithStorage(store)}
	for _, exec := range clones {
		require.Nil(t, exec.GetEmbedder())
		_, err := exec.Execute(context.Background(), "RETURN 1", nil)
		require.NoError(t, err)
	}
	embedder := &stubVectorEmbedder{vec: []float32{1, 0}}
	base.SetEmbedder(embedder)
	for _, exec := range clones {
		require.Same(t, embedder, exec.GetEmbedder())
		result, err := exec.Execute(context.Background(), "CALL db.index.vector.embed('alpha')", nil)
		require.NoError(t, err)
		require.Equal(t, []float32{1, 0}, result.Rows[0][0])
	}
	base.SetEmbedder(nil)
	for _, exec := range clones {
		require.Nil(t, exec.GetEmbedder())
	}
}

func TestQueryEmbedderReplacementIsolatesInflightCache(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "replace")
	base := NewStorageExecutor(store)
	scoped := NewStorageExecutor(store)
	scoped.ShareEmbedderFrom(base)
	old := &blockingEmbedder{started: make(chan struct{}, 1), release: make(chan struct{}), result: []float32{1, 0}}
	base.SetEmbedder(old)
	oldResult := make(chan []float32, 1)
	oldError := make(chan error, 1)
	go func() {
		vec, err := scoped.embedVectorQueryText(context.Background(), "alpha")
		oldResult <- vec
		oldError <- err
	}()
	<-old.started
	replacement := &stubVectorEmbedder{vec: []float32{0, 1}}
	base.SetEmbedder(replacement)
	vec, err := scoped.embedVectorQueryText(context.Background(), "alpha")
	require.NoError(t, err)
	require.Equal(t, replacement.vec, vec)
	close(old.release)
	require.Equal(t, []float32{1, 0}, <-oldResult)
	require.NoError(t, <-oldError)
	vec, err = scoped.embedVectorQueryText(context.Background(), "alpha")
	require.NoError(t, err)
	require.Equal(t, replacement.vec, vec, "old request must not populate the replacement's cache")
}

func TestQueryEmbedderConcurrentPublication(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "concurrent")
	base := NewStorageExecutor(store)
	scoped := NewStorageExecutor(store)
	scoped.ShareEmbedderFrom(base)
	first := &stubVectorEmbedder{vec: []float32{1, 0}}
	second := &stubVectorEmbedder{vec: []float32{0, 1}}
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for range 100 {
				if emb := scoped.GetEmbedder(); emb != nil {
					_, _ = emb.Embed(context.Background(), "alpha")
				}
				_, _ = scoped.embedVectorQueryText(context.Background(), "alpha")
			}
		})
	}
	for range 50 {
		base.SetEmbedder(first)
		base.SetEmbedder(second)
	}
	workers.Wait()
	require.Same(t, second, scoped.GetEmbedder())
}
