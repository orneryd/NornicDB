package nornicdb

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestEmbeddingAttemptCanonicalPending(t *testing.T) {
	for _, cached := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncached", true: "cached"}[cached], func(t *testing.T) {
			worker, queue, _ := nodeSaveRetryFixture(t, "whole", 2, 1)
			if !cached {
				worker.retryMaxBytes = 1
			}
			require.Equal(t, 2, worker.PendingEmbeddingsCount())
			node, _, _ := worker.claimNextNodeSource()
			require.NotNil(t, node)
			// The same ID may also be restored to MAIN before its claim ends.
			queue.AddToPendingEmbeddings(node.ID)
			require.Equal(t, 2, worker.PendingEmbeddingsCount())
			provider, err := worker.resolveAttemptEmbedder(node)
			require.NoError(t, err)
			worker.processClaimedNode(node, provider)
			worker.claimMu.Lock()
			worker.nodeRetries[node.ID].retryAt = time.Now().Add(time.Hour)
			worker.claimMu.Unlock()
			require.Equal(t, cached, len(worker.embeddingRetries) != 0)
			require.Equal(t, 2, worker.PendingEmbeddingsCount(), "idle DLQ must remain pending")
			require.Equal(t, 2, worker.QueueLen())
			require.Equal(t, 2, worker.Stats().Pending)
		})
	}
}
