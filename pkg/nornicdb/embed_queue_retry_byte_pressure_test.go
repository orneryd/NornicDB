package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Permanent version of the r4 review probe. The old cooldown is replaced by
// advancing admission's clock directly, so elapsed time cannot release pressure.
func TestReviewRetryBytePressurePersistsUntilDrain(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 4, 1000)
	require.True(t, worker.processNextBatch())
	size := worker.nodeEmbeddingRetryBytes()
	worker.retryMaxBytes = size + 1
	require.True(t, worker.processNextBatch())
	require.Len(t, worker.embeddingRetries, 1)
	require.Equal(t, 2, provider.GetEmbedCount())
	worker.claimMu.Lock()
	for _, retry := range worker.embeddingRetries {
		retry.retryAt = time.Now().Add(2 * time.Hour)
	}
	worker.retryTurn = false
	candidate := worker.nextEmbeddingNodeID(time.Now().Add(time.Hour))
	worker.claimMu.Unlock()
	require.Empty(t, candidate, "elapsed time cannot admit fresh purchases without retained capacity draining")
	makeNodeSaveRetriesReady(worker)
	worker.processNextBatch()
	require.Len(t, worker.embeddingRetries, 1)
	require.Equal(t, size, worker.nodeEmbeddingRetryBytes())
	require.Equal(t, 2, provider.GetEmbedCount(), "byte pressure must not buy another unretainable result before retention drains")
	require.Greater(t, time.Until(worker.embeddingRetries["node-000"].retryAt), time.Second, "retained writes still back off")
	queue.rejections = 0
	makeNodeSaveRetriesReady(worker)
	for i := 0; i < 8 && worker.processed.Load() < 4; i++ {
		worker.processNextBatch()
	}
	require.Equal(t, int64(4), worker.processed.Load(), "successful retained save releases pressure")
	require.Empty(t, worker.embeddingRetries)
}

func TestEmbedWorkerRetryBytePressureInvalidation(t *testing.T) {
	for _, setter := range []string{"embedder", "resolver"} {
		t.Run(setter, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, "whole", 2, 1000)
			require.True(t, worker.processNextBatch())
			worker.retryMaxBytes = worker.nodeEmbeddingRetryBytes() + 1
			require.True(t, worker.processNextBatch())
			require.True(t, worker.retryBudgetBlocked)
			if setter == "embedder" {
				worker.SetEmbedder(provider)
			} else {
				selected := worker.currentEmbedder()
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return selected, nil })
			}
			require.False(t, worker.retryBudgetBlocked)
			require.Empty(t, worker.embeddingRetries)
			queue.rejections = 0
			for i := 0; i < 4 && worker.processed.Load() < 2; i++ {
				worker.processNextBatch()
			}
			require.Equal(t, int64(2), worker.processed.Load(), "invalidation releases pressure for fresh configuration work")
		})
	}
}
