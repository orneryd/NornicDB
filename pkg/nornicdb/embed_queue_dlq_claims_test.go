package nornicdb

import (
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

// The maintainer's MAIN-first rule supersedes global admission backpressure.
// Failed IDs never block MAIN or acquire another active claim.
func TestEmbedWorkerDLQMainBeforeBackoff(t *testing.T) {
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, mode, 8, 1000)
			for i := 0; i < 12; i++ {
				worker.processNextBatch()
			}
			require.Len(t, worker.nodeRetries, 8, "MAIN completes all first attempts despite earlier failed nodes")
			calls := provider.GetEmbedCount()
			for i := 0; i < 12; i++ {
				worker.processNextBatch()
			}
			require.Equal(t, calls, provider.GetEmbedCount(), "DLQ backoff prevents immediate repurchases")
			queue.rejections = 0
			for i := 0; i < 20 && worker.processed.Load() < 8; i++ {
				makeNodeSaveRetriesReady(worker)
				worker.processNextBatch()
			}
			require.Equal(t, int64(8), worker.processed.Load())
			require.Empty(t, worker.nodeRetries)
		})
	}
}
func TestEmbedWorkerDLQConcurrentClaims(t *testing.T) {
	worker, _, _ := nodeSaveRetryFixture(t, "whole", 32, 1000)
	var wg sync.WaitGroup
	claims := make(chan *storage.Node, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, _ := worker.claimNextNodeResult()
			if node != nil {
				claims <- node
			}
		}()
	}
	wg.Wait()
	close(claims)
	require.Len(t, worker.claimedNodes, 32, "failed-node bookkeeping never blocks MAIN claims")
	for node := range claims {
		worker.releaseNodeClaim(node.ID)
	}
}

func TestEmbedWorkerRejectedRetryContinuesSamePass(t *testing.T) {
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		t.Run(mode, func(t *testing.T) {
			worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1000)
			require.True(t, worker.processNextBatch())
			calls := provider.GetEmbedCount()
			_, err := queue.CreateNode(&storage.Node{ID: "healthy", Labels: []string{"Doc"}, Properties: map[string]any{"text": "healthy pending source"}})
			require.NoError(t, err)
			queue.AddToPendingEmbeddings("healthy")
			queue.attempts["healthy"] = 1000
			makeNodeSaveRetriesReady(worker)
			// MAIN must win even when the failed save is ready.
			require.True(t, worker.processNextBatch(), "a rejected retry must continue to fresh work in this pass")
			require.Equal(t, int64(1), worker.processed.Load())
			require.Equal(t, 1, queue.attempts["node-000"], "DLQ is ignored while MAIN is nonempty")
			stored, err := queue.GetNode("healthy")
			require.NoError(t, err)
			require.NotEmpty(t, stored.ChunkEmbeddings)
			require.Equal(t, calls+1, provider.GetEmbedCount())
			require.False(t, worker.processNextBatch(), "a failed retry alone is still no progress")
			require.Greater(t, provider.GetEmbedCount(), calls+1, "the failed retry re-requested its provider input")
		})
	}
}

func TestEmbedWorkerDLQClaimExclusive(t *testing.T) {
	worker, _, _ := nodeSaveRetryFixture(t, "whole", 2, 1000)
	require.True(t, worker.processNextBatch())
	require.True(t, worker.processNextBatch())
	makeNodeSaveRetriesReady(worker)
	first, _ := worker.claimNextNodeResult()
	require.NotNil(t, first)
	defer worker.releaseNodeClaim(first.ID)
	second, _ := worker.claimNextNodeResult()
	require.NotNil(t, second)
	defer worker.releaseNodeClaim(second.ID)
	require.NotEqual(t, first.ID, second.ID, "an in-flight DLQ claim cannot be selected again")
	third, _ := worker.claimNextNodeResult()
	require.Nil(t, third)
	require.Equal(t, 2, worker.PendingEmbeddingsCount(), "the two active DLQ claims count once each")
}
