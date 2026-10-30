package nornicdb

import (
	"testing"
	"time"

	storageapi "github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerRetryWithZonedTimeInputs(t *testing.T) {
	stamp := time.Date(2026, 10, 19, 12, 0, 0, 0, time.FixedZone("provider-zone", 3600))
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1)
			node, err := queue.GetNode("node-000")
			require.NoError(t, err)
			node.Properties["event_time"] = stamp
			require.NoError(t, queue.UpdateNode(storageapi.NodeUpdateRequest{After: node}))
			require.True(t, worker.processNextBatch())
			calls := provider.GetEmbedCount()
			makeNodeSaveRetriesReady(worker)
			require.True(t, worker.processNextBatch())
			require.Equal(t, 2*calls, provider.GetEmbedCount())
			saved, err := queue.GetNode("node-000")
			require.NoError(t, err)
			require.NotEmpty(t, saved.ChunkEmbeddings)
		})
	}
}
