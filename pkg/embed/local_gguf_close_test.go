//go:build localllm

package embed

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/localllm"
	"github.com/stretchr/testify/require"
)

// Close waits for an embedding already running on the model instead of
// freeing the model under it, and later embeddings are refused (F-EMBED).
func TestLocalGGUFCloseWaitsForInFlightEmbed(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	original := embedModel
	embedModel = func(context.Context, *localllm.Model, string) ([]float32, error) {
		close(entered)
		<-release
		return []float32{1}, nil
	}
	t.Cleanup(func() { embedModel = original })
	e := &LocalGGUFEmbedder{stopWarmup: make(chan struct{}), warmupStopped: make(chan struct{})}
	close(e.warmupStopped) // no warmup goroutine

	embedded := make(chan error, 1)
	go func() {
		_, err := e.Embed(context.Background(), "in flight")
		embedded <- err
	}()
	<-entered
	closed := make(chan struct{})
	go func() {
		_ = e.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while an embedding was still running on the model")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-embedded:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("the embedding never finished")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close never returned after the embedding finished")
	}
	_, err := e.Embed(context.Background(), "after close")
	require.Error(t, err, "a closed embedder refuses new embeddings")
}
