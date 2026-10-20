package nornicdb

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Permanent version of the r4 probe, adapted to the maintainer's new rule:
// elapsed time and a failed retry never hold MAIN.
func TestReviewDLQBackoffNeverBlocksMain(t *testing.T) {
	worker, queue, provider := nodeSaveRetryFixture(t, "whole", 4, 1000)
	require.True(t, worker.processNextBatch())
	require.True(t, worker.processNextBatch())
	require.Equal(t, 2, provider.GetEmbedCount())
	worker.claimMu.Lock()
	for _, retry := range worker.nodeRetries {
		retry.retryAt = time.Now().Add(2 * time.Hour)
	}
	candidate := worker.nextEmbeddingNodeID(time.Now().Add(time.Hour), false)
	worker.claimMu.Unlock()
	require.Equal(t, storage.NodeID("node-002"), candidate, "MAIN always precedes DLQ regardless of elapsed time")
	makeNodeSaveRetriesReady(worker)
	worker.processNextBatch()
	require.Equal(t, 3, provider.GetEmbedCount(), "one first attempt for each MAIN node")
	require.True(t, worker.processNextBatch()) // Last MAIN item.
	worker.processNextBatch()                  // Only now may DLQ run.
	require.Greater(t, time.Until(worker.nodeRetries["node-000"].retryAt), time.Second, "rejected retries still back off")
	queue.rejections = 0
	makeNodeSaveRetriesReady(worker)
	for i := 0; i < 8 && worker.processed.Load() < 4; i++ {
		worker.processNextBatch()
	}
	require.Equal(t, int64(4), worker.processed.Load(), "fresh retry results finish the DLQ")
}

func TestEmbedWorkerDLQProviderChangeRequeuesFailures(t *testing.T) {
	for _, setter := range []string{"embedder", "resolver"} {
		t.Run(setter, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, "whole", 2, 1000)
			require.True(t, worker.processNextBatch())
			require.True(t, worker.processNextBatch())
			require.Len(t, worker.nodeRetries, 2)
			if setter == "embedder" {
				worker.SetEmbedder(provider)
			} else {
				selected := worker.currentEmbedder()
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return selected, nil })
			}
			require.Empty(t, worker.nodeRetries)
			queue.rejections = 0
			for i := 0; i < 4 && worker.processed.Load() < 2; i++ {
				worker.processNextBatch()
			}
			require.Equal(t, int64(2), worker.processed.Load(), "invalidation releases pressure for fresh configuration work")
		})
	}
}
