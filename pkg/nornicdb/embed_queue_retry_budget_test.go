package nornicdb

import (
 "reflect"
 "strings"
 "sync"
 "testing"
 "github.com/orneryd/nornicdb/pkg/storage"
 "github.com/stretchr/testify/require"
)

// The maintainer's MAIN-first rule supersedes global admission backpressure.
// Cache count/bytes stay bounded, including payloads currently being saved.
func TestEmbedWorkerRetryBudgetCount(t *testing.T) {
 for _,mode:=range []string{"whole","chunked","document-batch","resolved-document-batch","property","property-batch"} {
  t.Run(mode,func(t *testing.T){
   worker,queue,provider:=nodeSaveRetryFixture(t,mode,8,1000)
   worker.retryMaxCount=2
   for i:=0;i<12;i++ {worker.processNextBatch()}
   require.Len(t,worker.embeddingRetries,2)
   require.Len(t,worker.nodeRetries,8,"MAIN completes all first attempts despite a full cache")
   calls:=provider.GetEmbedCount()
   for i:=0;i<12;i++ {worker.processNextBatch()}
   require.Equal(t,calls,provider.GetEmbedCount(),"DLQ backoff prevents immediate repurchases")
   require.LessOrEqual(t,worker.nodeEmbeddingRetryBytes(),nodeEmbeddingRetryMaxBytes)
   queue.rejections=0
   for i:=0;i<20 && worker.processed.Load()<8;i++ {makeNodeSaveRetriesReady(worker);worker.processNextBatch()}
   require.Equal(t,int64(8),worker.processed.Load())
   require.Empty(t,worker.embeddingRetries);require.Empty(t,worker.nodeRetries)
  })
 }
}
func TestEmbedWorkerRetryBudgetBytes(t *testing.T) {
 worker,queue,provider:=nodeSaveRetryFixture(t,"whole",4,1000)
 require.True(t,worker.processNextBatch()); size:=worker.nodeEmbeddingRetryBytes();require.Positive(t,size)
 worker.retryMaxBytes=size+1
 for i:=0;i<12;i++ {worker.processNextBatch()}
 require.Len(t,worker.embeddingRetries,1);require.Len(t,worker.nodeRetries,4)
 require.Equal(t,4,provider.GetEmbedCount());require.LessOrEqual(t,worker.nodeEmbeddingRetryBytes(),worker.retryMaxBytes)
 queue.rejections=0;makeNodeSaveRetriesReady(worker)
 for i:=0;i<8 && worker.processed.Load()<4;i++ {worker.processNextBatch()}
 require.Equal(t,int64(4),worker.processed.Load())
}
func TestEmbedWorkerRetryBudgetOversized(t *testing.T) {
 worker,queue,provider:=nodeSaveRetryFixture(t,"whole",3,1000);worker.retryMaxBytes=64
 for i:=0;i<3;i++ {require.True(t,worker.processNextBatch())}
 require.Empty(t,worker.embeddingRetries,"oversized payloads never bypass the cap")
 require.Len(t,worker.nodeRetries,3,"only small DLQ records remain")
 for i:=0;i<20;i++ {require.False(t,worker.processNextBatch())}
 require.Equal(t,3,provider.GetEmbedCount(),"each MAIN item runs exactly once before DLQ backoff")
 queue.rejections=0;makeNodeSaveRetriesReady(worker)
 for i:=0;i<3;i++ {require.True(t,worker.processNextBatch())}
 require.Equal(t,int64(3),worker.processed.Load())
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
		go func() {
			defer wg.Done()
			node, _, _ := worker.claimNextNodeSource()
			if node != nil {
				claims <- node
			}
		}()
	}
	wg.Wait()
	close(claims)
	require.Len(t, worker.claimedNodes, 32, "payload cache capacity never blocks MAIN claims")
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
			require.True(t, worker.processNextBatch(), "a rejected retained write must continue to fresh work in this pass")
			require.Equal(t, int64(1), worker.processed.Load())
			require.Equal(t, 1, queue.attempts["node-000"], "DLQ is ignored while MAIN is nonempty")
			stored, err := queue.GetNode("healthy")
			require.NoError(t, err)
			require.NotEmpty(t, stored.ChunkEmbeddings)
			require.Equal(t, calls+1, provider.GetEmbedCount())
			require.False(t, worker.processNextBatch(), "retained failure alone is still no progress")
		})
	}
}

func TestEmbedWorkerRetryBudgetChargesClaimedRetry(t *testing.T) {
	worker, _, _ := nodeSaveRetryFixture(t, "whole", 2, 1000)
	worker.retryMaxCount = 1
	require.True(t, worker.processNextBatch())
	bytes := worker.nodeEmbeddingRetryBytes()
	makeNodeSaveRetriesReady(worker)
	// Drain the other MAIN node before choosing a DLQ entry.
	worker.processNextBatch()
	node, retained, _ := worker.claimNextNodeSource()
	require.NotNil(t, node)
	require.NotNil(t, retained)
	require.Len(t, worker.embeddingRetries, 1)
	require.Equal(t, bytes, worker.nodeEmbeddingRetryBytes(), "claimed payload stays charged until the write finishes")
	fresh, _, _ := worker.claimNextNodeSource()
	require.Nil(t, fresh, "a retained save in flight must not free an admission slot")
	worker.releaseNodeClaim(node.ID)
}
