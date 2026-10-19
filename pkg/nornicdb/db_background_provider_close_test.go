package nornicdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A native provider can observe cancellation while still using its model.
// Worker cancellation must not release that model before the call returns.
type heldBatchProvider struct {
	mockEmbedder
	entered, canceled, release, closed chan struct{}
	active, unsafeCloses               atomic.Int32
	once                               sync.Once
}

func (p *heldBatchProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	p.active.Add(1)
	defer p.active.Add(-1)
	close(p.entered)
	<-ctx.Done()
	close(p.canceled)
	<-p.release
	return nil, ctx.Err()
}
func (p *heldBatchProvider) Close() error {
	if p.active.Load() != 0 {
		p.unsafeCloses.Add(1)
	}
	p.once.Do(func() { close(p.closed) })
	return nil
}

type heldTypedBatchProvider struct{ *heldBatchProvider }

func (p *heldTypedBatchProvider) EmbedBatchWithInputType(ctx context.Context, texts []string, _ string) ([][]float32, error) {
	return p.EmbedBatch(ctx, texts)
}
func (p *heldTypedBatchProvider) EmbedWithInputType(ctx context.Context, text string, _ string) ([]float32, error) {
	return p.Embed(ctx, text)
}

func TestDBCloseJoinsActualBackgroundProviderCall(t *testing.T) {
	for _, typed := range []bool{false, true} {
		t.Run(map[bool]string{false: "batch", true: "typed_batch"}[typed], func(t *testing.T) {
			config := DefaultConfig()
			config.Memory.EmbeddingEnabled = true
			config.Memory.EmbeddingDimensions = 4
			config.Memory.KmeansClusterInterval = 0
			config.EmbeddingWorker.NumWorkers = 1
			config.EmbeddingWorker.TriggerDebounceDelay = 0
			db, err := Open(t.TempDir(), config)
			require.NoError(t, err)
			p := &heldBatchProvider{mockEmbedder: mockEmbedder{dims: 4, model: "held-batch"}, entered: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(p.release) }) }
			t.Cleanup(func() { release(); require.NoError(t, db.Close()) })
			var provider embed.Embedder = p
			if typed {
				provider = &heldTypedBatchProvider{p}
			}
			require.True(t, db.SetEmbedder(provider))
			_, err = db.GetStorage().CreateNode(&storage.Node{ID: "held-document", Labels: []string{"File"}, Properties: map[string]any{"content": "a document that must remain owned during provider shutdown"}})
			require.NoError(t, err)
			db.GetEmbedQueue().Trigger()
			select {
			case <-p.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("background provider call did not start")
			}
			done := make(chan error, 1)
			go func() { done <- db.Close() }()
			select {
			case <-p.canceled:
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown did not cancel the actual provider")
			}
			select {
			case <-p.closed:
				t.Error("provider released while its background call still uses it")
			case <-time.After(100 * time.Millisecond):
			}
			release()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join the released provider call")
			}
			require.Zero(t, p.active.Load())
			require.Zero(t, p.unsafeCloses.Load(), "provider Close ran before EmbedBatch returned")
			select {
			case <-p.closed:
			default:
				t.Fatal("provider was not released after its call joined")
			}
		})
	}
}
