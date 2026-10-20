package nornicdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Pause the public capability query after dispatch has copied the provider,
// before it claims any node. No worker or epoch state is changed by the seam.
type reviewDispatchSnapshotProvider struct {
	*retryDocumentEmbedder
	once             sync.Once
	selected, resume chan struct{}
}

func (p *reviewDispatchSnapshotProvider) UsesDocumentProperties() bool {
	p.once.Do(func() { close(p.selected); <-p.resume })
	return false
}

func (p *reviewDispatchSnapshotProvider) EmbedDocumentPropertyChunks(ctx context.Context, text string, _ map[string]any, size, overlap int) (*embed.DocumentChunkResult, error) {
	results, err := p.EmbedDocumentBatchChunks(ctx, []string{text}, size, overlap)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

func TestReviewProviderSnapshotPublicWorker(t *testing.T) {
	for _, setter := range []string{"single", "resolver"} {
		t.Run(setter, func(t *testing.T) {
			worker, queue, oldBase := nodeSaveRetryFixture(t, "document-batch", 2, 0)
			oldBase.model = "old-model"
			old := &reviewDispatchSnapshotProvider{retryDocumentEmbedder: &retryDocumentEmbedder{oldBase}, selected: make(chan struct{}), resume: make(chan struct{})}
			worker.SetEmbedder(old)
			published := make(chan string, 2)
			worker.SetOnEmbedded(func(node *storage.Node) { published <- node.EmbedMeta["embedding_model"].(string) })
			worker.config.NumWorkers = 1
			worker.StartWorkers()
			select {
			case <-old.selected:
			case <-time.After(3 * time.Second):
				t.Fatal("dispatch never read old provider")
			}
			replacement := newMockEmbedder()
			replacement.model, replacement.dims = "replacement-model", 3
			if setter == "single" {
				worker.SetEmbedder(replacement)
			} else {
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return replacement, nil })
			}
			// The setter has returned before any node or provider request is claimed.
			close(old.resume)
			for i := 0; i < 2; i++ {
				select {
				case <-published:
				case <-time.After(5 * time.Second):
					t.Fatal("worker did not publish")
				}
			}
			worker.Close()
			t.Logf("old inputs=%d replacement inputs=%d publications=%v processed=%d pending=%v epoch=%d", oldBase.GetEmbedCount(), replacement.GetEmbedCount(), published, worker.processed.Load(), queue.pending, worker.embeddingEpoch.Load())
			for _, id := range []storage.NodeID{"node-000", "node-001"} {
				node, err := queue.GetNode(id)
				require.NoError(t, err)
				if len(node.ChunkEmbeddings) > 0 {
					assertModel := node.EmbedMeta["embedding_model"]
					require.Equal(t, replacement.Model(), assertModel, "old dispatch snapshot must not acquire a new epoch and commit after completed setter")
				}
			}
		})
	}
}
