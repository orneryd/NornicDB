package nornicdb

import (
 "reflect"
 "strings"
 "sync"
 "testing"
 "time"

 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

func TestEmbedWorkerRetryBudgetCount(t *testing.T) {
 for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
  t.Run(mode, func(t *testing.T) {
   worker, queue, provider := nodeSaveRetryFixture(t, mode, 8, 1000)
   worker.retryMaxCount = 2
   for i := 0; i < 12; i++ { worker.processNextBatch() }
   require.Len(t, worker.embeddingRetries, 2)
   calls := provider.GetEmbedCount()
   for i := 0; i < 12; i++ { worker.processNextBatch() }
   require.Equal(t, calls, provider.GetEmbedCount(), "full retention must stop fresh provider calls")
   require.LessOrEqual(t, worker.nodeEmbeddingRetryBytes(), nodeEmbeddingRetryMaxBytes)
   queue.rejections = 0
   for i := 0; i < 20 && worker.processed.Load() < 8; i++ {
    makeNodeSaveRetriesReady(worker)
    worker.processNextBatch()
   }
   require.Equal(t, int64(8), worker.processed.Load(), "successful retries release admission for the pending backlog")
   require.Empty(t, worker.embeddingRetries)
  })
 }
}

func TestEmbedWorkerRetryBudgetBytes(t *testing.T) {
 worker, queue, provider := nodeSaveRetryFixture(t, "whole", 4, 1000)
 require.True(t, worker.processNextBatch())
 size := worker.nodeEmbeddingRetryBytes()
 require.Positive(t, size)
 worker.retryMaxBytes = size + 1 // First payload fits; the next must not.
 require.True(t, worker.processNextBatch())
 require.Len(t, worker.embeddingRetries, 1)
 require.LessOrEqual(t, worker.nodeEmbeddingRetryBytes(), worker.retryMaxBytes)
 calls := provider.GetEmbedCount()
 for i := 0; i < 20; i++ { worker.processNextBatch() }
 require.Equal(t, calls, provider.GetEmbedCount())
 require.Equal(t, 2, calls)
 queue.rejections = 0
 makeNodeSaveRetriesReady(worker)
 for i := 0; i < 8 && worker.processed.Load() < 4; i++ { worker.processNextBatch() }
 require.Equal(t, int64(4), worker.processed.Load())
}

func TestEmbedWorkerRetryBudgetOversized(t *testing.T) {
 worker, _, provider := nodeSaveRetryFixture(t, "whole", 3, 1000)
 worker.retryMaxBytes = 64
 require.True(t, worker.processNextBatch())
 require.Empty(t, worker.embeddingRetries, "an oversized payload cannot bypass the byte cap")
 for i := 0; i < 20; i++ { require.False(t, worker.processNextBatch()) }
 require.Equal(t, 1, provider.GetEmbedCount())
 require.LessOrEqual(t, worker.nodeEmbeddingRetryDelay(time.Hour), 30*time.Second)
 worker.claimMu.Lock()
 worker.retryBudgetBlockedUntil = time.Time{} // Deterministic expiry, no sleeps.
 worker.claimMu.Unlock()
 require.True(t, worker.processNextBatch(), "oversized rejection must not permanently stop admission")
 require.Equal(t, 2, provider.GetEmbedCount())
}

func TestEmbedWorkerRetryBudgetChargesPayload(t *testing.T) {
 const budget = int64(4096)
 for _, value := range []any{
  map[string]any{"property": strings.Repeat("p", 5000)},
  map[string]any{"metadata": []map[string]any{{"spans": make([]int64, 1000)}}},
  [][]float32{make([]float32, 1, 2000)},
 } {
  require.Greater(t, nodeEmbeddingRetryValueBytes(reflect.ValueOf(value), budget, 0), budget)
 }
 cyclic := map[string]any{}
 cyclic["self"] = cyclic
 require.Greater(t, nodeEmbeddingRetryValueBytes(reflect.ValueOf(cyclic), budget, 0), budget)
}

func TestEmbedWorkerRetryBudgetReservesConcurrentClaims(t *testing.T) {
 worker, _, _ := nodeSaveRetryFixture(t, "whole", 32, 1000)
 worker.retryMaxCount = 2
 var wg sync.WaitGroup
 claims := make(chan *storage.Node, 32)
 for i := 0; i < 32; i++ {
  wg.Add(1)
  go func() { defer wg.Done(); node, _, _ := worker.claimNextNodeSource(); if node != nil { claims <- node } }()
 }
 wg.Wait()
 close(claims)
 require.Len(t, worker.claimedNodes, 2, "in-flight fresh requests reserve count slots")
 for node := range claims { worker.releaseNodeClaim(node.ID) }
}

func TestEmbedWorkerRejectedRetryContinuesSamePass(t *testing.T) {
 for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
  t.Run(mode, func(t *testing.T) {
   worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1000)
   require.True(t, worker.processNextBatch())
   _, err := queue.CreateNode(&storage.Node{ID:"healthy", Labels:[]string{"Doc"}, Properties:map[string]any{"text":"healthy pending source"}})
   require.NoError(t, err)
   queue.AddToPendingEmbeddings("healthy")
   queue.attempts["healthy"] = 1000
   makeNodeSaveRetriesReady(worker)
   worker.retryTurn = true // Start this pass with the permanently rejected retry.
   require.True(t, worker.processNextBatch(), "a rejected retained write must continue to fresh work in this pass")
   require.Equal(t, int64(1), worker.processed.Load())
   require.Equal(t, 2, queue.attempts["node-000"])
   stored, err := queue.GetNode("healthy")
   require.NoError(t, err)
   require.NotEmpty(t, stored.ChunkEmbeddings)
   require.Equal(t, 2, provider.GetEmbedCount())
   require.False(t, worker.processNextBatch(), "retained failure alone is still no progress")
  })
 }
}
