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

type reviewOldProviderFailure struct {
	*mockEmbedder
	started, resume chan struct{}
	once            sync.Once
}

func (p *reviewOldProviderFailure) EmbedBatch(ctx context.Context, _ []string) ([][]float32, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.resume:
		return nil, &embed.ProviderError{Provider: "old-model", StatusCode: 400, Body: "old provider rejects this input"}
	}
}

func TestReviewOldProviderFailureAfterSetter(t *testing.T) {
	for _, setter := range []string{"single", "resolver"} {
		t.Run(setter, func(t *testing.T) {
			disk, err := storage.NewBadgerEngine(t.TempDir())
			require.NoError(t, err)
			defer disk.Close()
			disk.SetEmbeddingsEnabled(true)
			_, err = disk.CreateNode(&storage.Node{ID: "review:doc", Properties: map[string]any{"text": "input accepted by the replacement provider"}})
			require.NoError(t, err)
			events := make(chan *storage.Node, 4)
			disk.OnNodeUpdated(func(node *storage.Node) { events <- storage.CopyNode(node) })
			old := &reviewOldProviderFailure{mockEmbedder: newMockEmbedder(), started: make(chan struct{}), resume: make(chan struct{})}
			worker := NewEmbedWorker(old, disk, &EmbedWorkerConfig{NumWorkers: 1, DeferWorkerStart: true, MaxRetries: 1, ChunkSize: 8192, EmbedBatchSize: 1, ScanInterval: time.Hour})
			defer worker.Close()
			worker.StartWorkers()
			select {
			case <-old.started:
			case <-time.After(3 * time.Second):
				t.Fatal("old provider request never started")
			}
			replacement := newMockEmbedder()
			replacement.model = "replacement-model"
			if setter == "single" {
				worker.SetEmbedder(replacement)
			} else {
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return replacement, nil })
			}
			close(old.resume)
			var event *storage.Node
			select {
			case event = <-events:
			case <-time.After(5 * time.Second):
				t.Fatal("worker never published a result or failure")
			}
			worker.Close()
			node, err := disk.GetNode("review:doc")
			require.NoError(t, err)
			t.Logf("after completed %s setter: replacement calls=%d pending=%d parked=%d stored vectors=%d event metadata=%v stored metadata=%v", setter, replacement.GetEmbedCount(), disk.PendingEmbeddingsCount(), worker.parked.Load(), len(node.ChunkEmbeddings), event.EmbedMeta, node.EmbedMeta)
			require.NotEqual(t, true, node.EmbedMeta["embedding_failed"], "the old generation cannot terminally park replacement-provider work")
			require.Equal(t, replacement.Model(), node.EmbedMeta["embedding_model"])
		})
	}
}
