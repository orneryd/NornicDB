package nornicdb

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerListenerCanChangeProvider(t *testing.T) {
	t.Cleanup(config.WithWALEnabled())
	for _, buffered := range []bool{false, true} {
		for _, walWrapped := range []bool{false, true} {
			for _, setter := range []string{"single", "resolver"} {
				t.Run(fmt.Sprintf("buffered=%v/wal=%v/%s", buffered, walWrapped, setter), func(t *testing.T) {
					b, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					defer b.Close()
					b.SetEmbeddingsEnabled(true)
					var engine storage.Engine = b
					if walWrapped {
						walConfig := storage.DefaultWALConfig()
						log, err := storage.NewWAL(t.TempDir(), walConfig)
						require.NoError(t, err)
						defer log.Close()
						engine = storage.NewWALEngine(b, log)
					}
					store := storage.NewNamespacedEngine(engine, "lease")
					_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "alpha beta gamma"}})
					require.NoError(t, err)
					worker := NewEmbedWorker(newMockEmbedder(), store, &EmbedWorkerConfig{NumWorkers: 0, DeferWorkerStart: true, EmbedBatchSize: 1, ChunkSize: 512, MaxRetries: 1})
					defer worker.Close()
					entered, changed := make(chan struct{}), make(chan struct{})
					var timedOut atomic.Bool
					store.OnNodeUpdated(func(n *storage.Node) {
						if len(n.ChunkEmbeddings) == 0 {
							return
						}
						close(entered)
						// The setter is initiated by the real callback. A deadline frees
						// a broken baseline so it reports a failure without leaking a lab job.
						go func() {
							if setter == "single" {
								worker.SetEmbedder(newMockEmbedder())
							} else {
								worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return newMockEmbedder(), nil })
							}
							close(changed)
						}()
						select {
						case <-changed:
						case <-time.After(time.Second):
							timedOut.Store(true)
						}
					})
					done := make(chan bool, 1)
					go func() { done <- worker.processNextBatch() }()
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("real worker did not enter its outcome callback")
					}
					select {
					case ok := <-done:
						require.True(t, ok)
					case <-time.After(5 * time.Second):
						t.Fatal("worker did not finish")
					}
					<-changed
					b.WaitForListeners()
					require.False(t, timedOut.Load(), "provider lease retained while worker waited for callback completion")
					n, err := store.GetNode("doc")
					require.NoError(t, err)
					require.NotEmpty(t, n.ChunkEmbeddings)
				})
			}
		}
	}
}
