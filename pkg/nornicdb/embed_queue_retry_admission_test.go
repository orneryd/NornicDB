package nornicdb

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerRetryAdmissionZonedTime(t *testing.T) {
	stamp := time.Date(2026, 10, 19, 12, 0, 0, 0, time.FixedZone("provider-zone", 3600))
	require.Less(t, nodeEmbeddingRetryValueBytes(reflect.ValueOf(stamp), 4096, 0), int64(4096), "a supported immutable time must not be classified as an unknown pointer graph")
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1)
			node, err := queue.GetNode("node-000")
			require.NoError(t, err)
			node.Properties["event_time"] = stamp
			require.NoError(t, queue.UpdateNode(node))
			require.True(t, worker.processNextBatch())
			require.Len(t, worker.embeddingRetries, 1)
			require.False(t, worker.retryBudgetBlocked)
			calls := provider.GetEmbedCount()
			makeNodeSaveRetriesReady(worker)
			require.True(t, worker.processNextBatch())
			require.Equal(t, calls, provider.GetEmbedCount())
			saved, err := queue.GetNode("node-000")
			require.NoError(t, err)
			require.NotEmpty(t, saved.ChunkEmbeddings)
			require.Empty(t, worker.embeddingRetries)
		})
	}
}
