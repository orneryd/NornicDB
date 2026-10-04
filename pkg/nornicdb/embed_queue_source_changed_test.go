package nornicdb

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// changingEmbedder changes the node once, inside the worker's first
// embedding call, as a business write landing mid-embedding (#889).
type changingEmbedder struct {
	*mockEmbedder
	once   sync.Once
	change func()
	mu     sync.Mutex
	texts  []string
}

func (e *changingEmbedder) seen(texts ...string) {
	e.mu.Lock()
	e.texts = append(e.texts, texts...)
	e.mu.Unlock()
	e.once.Do(e.change)
}
func (e *changingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.seen(text)
	return e.mockEmbedder.Embed(ctx, text)
}
func (e *changingEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	e.seen(texts...)
	return e.mockEmbedder.EmbedBatch(ctx, texts)
}
func (e *changingEmbedder) embedded(fragment string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, text := range e.texts {
		if strings.Contains(text, fragment) {
			return true
		}
	}
	return false
}

// A node changed while the worker embeds it gets its new content embedded,
// and the old content's embedding is never stored, whichever way the change
// is made (#889).
func TestEmbedWorkerReembedsNodeChangedWhileEmbedding(t *testing.T) {
	for _, route := range []string{"UpdateNode keeping UpdatedAt", "UpdateNode with a new UpdatedAt", "Cypher SET"} {
		t.Run(route, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			base.SetEmbeddingsEnabled(true)
			engine := storage.NewNamespacedEngine(base, "test")
			_, err := engine.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Memory"}, Properties: map[string]any{"content": "first version alpha"}})
			require.NoError(t, err)

			embedder := &changingEmbedder{mockEmbedder: newMockEmbedder()}
			embedder.change = func() {
				if route == "Cypher SET" {
					_, err := cypher.NewStorageExecutor(engine).Execute(context.Background(), "MATCH (n:Memory) SET n.content = 'second version omega'", nil)
					require.NoError(t, err)
					return
				}
				node, err := engine.GetNode("doc")
				require.NoError(t, err)
				node.Properties = map[string]any{"content": "second version omega"}
				if route == "UpdateNode with a new UpdatedAt" {
					node.UpdatedAt = node.UpdatedAt.Add(time.Second)
				}
				require.NoError(t, engine.UpdateNode(node))
			}
			worker := NewEmbedWorker(embedder, engine, &EmbedWorkerConfig{NumWorkers: 1, ScanInterval: 50 * time.Millisecond, BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 512, ChunkOverlap: 50})
			defer worker.Close()
			worker.Trigger()

			// The stale writeback leaves the node pending behind the
			// recently-processed wait; age that wait instead of sleeping it.
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) && !embedder.embedded("omega") {
				node, err := engine.GetNode("doc")
				require.NoError(t, err)
				require.Empty(t, node.ChunkEmbeddings, "the first version's embedding must not be stored")
				worker.mu.Lock()
				for id := range worker.recentlyProcessed {
					worker.recentlyProcessed[id] = time.Now().Add(-time.Minute)
				}
				worker.mu.Unlock()
				worker.Trigger()
				time.Sleep(20 * time.Millisecond)
			}
			require.True(t, embedder.embedded("omega"), "the second version was never embedded")
			require.Eventually(t, func() bool {
				node, err := engine.GetNode("doc")
				return err == nil && len(node.ChunkEmbeddings) == 1 && node.EmbedMeta["has_embedding"] == true
			}, 10*time.Second, 20*time.Millisecond)
			require.Zero(t, base.PendingEmbeddingsCount())
		})
	}
}

// markRecentlyProcessed starts the wait on a worker whose map wasn't made yet.
func TestEmbedWorkerMarkRecentlyProcessedStartsTheWait(t *testing.T) {
	worker := &EmbedWorker{}
	worker.markRecentlyProcessed("node")
	require.True(t, worker.wasRecentlyProcessed("node"))
	require.False(t, worker.wasRecentlyProcessed("other"))
}
