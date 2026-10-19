package nornicdb

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func makeNodeSaveRetriesReady(worker *EmbedWorker) {
	worker.claimMu.Lock()
	defer worker.claimMu.Unlock()
	for _, result := range worker.embeddingRetries {
		result.retryAt = time.Time{}
	}
}

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
	makeNodeSaveRetriesReady(worker)
	require.False(t, worker.processNextBatch(), "a due retry rejected again must report no progress")
	require.Equal(t, 2, queue.attempts["node-000"])
	worker.claimMu.Lock()
	retry := worker.embeddingRetries["node-000"]
	require.Equal(t, 2, retry.failures)
	require.Greater(t, time.Until(retry.retryAt), time.Second)
	worker.claimMu.Unlock()
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
					makeNodeSaveRetriesReady(worker)
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

func TestEmbedWorkerRetryOwnerInterleavesDueRetries(t *testing.T) {
	worker, queue, _ := nodeSaveRetryFixture(t, "whole", 3, 1000)
	queue.attempts["node-001"] = 1000
	queue.attempts["node-002"] = 1000
	require.True(t, worker.processNextBatch()) // Retain node-000.
	makeNodeSaveRetriesReady(worker)
	require.False(t, worker.processNextBatch()) // One due retry, rejected.
	makeNodeSaveRetriesReady(worker)
	require.True(t, worker.processNextBatch()) // Fresh node-001 must win this turn.
	require.Equal(t, 2, queue.attempts["node-000"])
	require.Equal(t, int64(1), worker.processed.Load())
	makeNodeSaveRetriesReady(worker)
	require.False(t, worker.processNextBatch()) // A retry still gets a turn.
	makeNodeSaveRetriesReady(worker)
	require.True(t, worker.processNextBatch()) // Fresh node-002.
	require.Equal(t, int64(2), worker.processed.Load())
}

func TestEmbedWorkerRetryOwnerPublicWorkerBacksOffAndRecovers(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 1, 1)
	worker.config.ScanInterval = time.Hour
	worker.config.NumWorkers = 1
	var published atomic.Int64
	worker.SetOnEmbedded(func(*storage.Node) { published.Add(1) })
	worker.StartWorkers()
	require.Eventually(t, func() bool {
		queue.writeMu.Lock()
		defer queue.writeMu.Unlock()
		return queue.attempts["node-000"] == 1
	}, 3*time.Second, time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	queue.writeMu.Lock()
	attempts := queue.attempts["node-000"]
	queue.writeMu.Unlock()
	require.Equal(t, 1, attempts, "the active drain must stop instead of writing every 50ms")
	require.Eventually(t, func() bool { return published.Load() == 1 }, 3*time.Second, time.Millisecond)
	worker.Close()
	require.Equal(t, 1, provider.GetEmbedCount(), "deadline wakes must not wait for the hour-long recovery scan")
}
