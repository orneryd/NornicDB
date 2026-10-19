package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerRetryOwnerAllowsFreshWork(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 2, 1000)
	queue.attempts["node-001"] = 1000 // Only node-000 is permanently rejected.
	require.True(t, worker.processNextBatch())
	for i := 0; i < 4 && worker.processed.Load() == 0; i++ {
		worker.processNextBatch()
	}
	require.Equal(t, int64(1), worker.processed.Load(), "retained rejection cannot starve a fresh node")
	require.Equal(t, 2, provider.GetEmbedCount())
	saved, err := queue.GetNode("node-001")
	require.NoError(t, err)
	require.NotEmpty(t, saved.ChunkEmbeddings)
}

func TestEmbedWorkerRetryOwnerReportsIdleDuringBackoff(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 1000)
	require.True(t, worker.processNextBatch())
	for i := 0; i < 20; i++ {
		require.False(t, worker.processNextBatch(), "continuing rejection is not progress")
	}
	require.Equal(t, 1, queue.attempts["node-000"], "backoff must suppress repeated writes, not just provider requests")
	require.Equal(t, 1, provider.GetEmbedCount())
}

func TestEmbedWorkerRetryOwnerRejectsChangedProvider(t *testing.T) {
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		for _, change := range []string{"embedder", "resolver", "live-resolver"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				worker, queue, old := nodeSaveRetryFixture(t, mode, 1, 1)
				var selected embed.Embedder = worker.currentEmbedder()
				if change != "embedder" {
					worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return selected, nil })
				}
				require.True(t, worker.processNextBatch())
				oldCalls := old.GetEmbedCount()
				require.Positive(t, oldCalls)
				replacement := newMockEmbedder()
				replacement.model = "replacement-model"
				replacement.dims = 3
				switch change {
				case "embedder":
					worker.SetEmbedderResolver(nil)
					worker.SetEmbedder(replacement)
				case "resolver":
					worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return replacement, nil })
				case "live-resolver":
					selected = replacement
				}
				deadline := time.Now().Add(3 * time.Second)
				for worker.processed.Load() == 0 && time.Now().Before(deadline) {
					worker.processNextBatch()
					time.Sleep(time.Millisecond)
				}
				saved, err := queue.GetNode("node-000")
				require.NoError(t, err)
				require.Equal(t, replacement.Model(), saved.EmbedMeta["embedding_model"])
				require.Len(t, saved.ChunkEmbeddings[0], replacement.Dimensions())
				require.Positive(t, replacement.GetEmbedCount())
				require.Equal(t, oldCalls, old.GetEmbedCount())
			})
		}
	}
}
