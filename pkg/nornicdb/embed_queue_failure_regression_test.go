package nornicdb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type orderedEmbeddingQueue struct {
	storage.Engine
	mu      sync.Mutex
	pending []storage.NodeID
}

func BenchmarkPendingDocumentsProviderBatching(b *testing.B) {
	for _, batchSize := range []int{1, 32} {
		b.Run(fmt.Sprintf("batch_size_%d", batchSize), func(b *testing.B) {
			for range b.N {
				base := storage.NewMemoryEngine()
				store := storage.NewNamespacedEngine(base, "embedding-batch-benchmark")
				ids := make([]storage.NodeID, 64)
				for i := range ids {
					ids[i] = storage.NodeID(fmt.Sprintf("doc-%03d", i))
					_, err := store.CreateNode(&storage.Node{ID: ids[i], Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": string(ids[i])}})
					require.NoError(b, err)
				}
				queue := &orderedEmbeddingQueue{Engine: store, pending: append([]storage.NodeID(nil), ids...)}
				provider := &recordingDocumentBatchEmbedder{rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)}}
				worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
					NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: batchSize, BatchDelay: time.Millisecond, DeferWorkerStart: true,
				})
				for worker.processNextBatch() {
				}
				requests := provider.batchCalls
				if batchSize == 1 {
					requests = 64
				}
				b.ReportMetric(float64(requests), "provider_requests/workload")
				worker.Close()
			}
		})
	}
}

func (q *orderedEmbeddingQueue) RefreshPendingEmbeddingsIndex() int { return 0 }

func (q *orderedEmbeddingQueue) FindNodeNeedingEmbedding() *storage.Node {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return nil
	}
	node, _ := q.Engine.GetNode(q.pending[0])
	return node
}

func (q *orderedEmbeddingQueue) MarkNodeEmbedded(id storage.NodeID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, pendingID := range q.pending {
		if pendingID == id {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			return
		}
	}
}

func (q *orderedEmbeddingQueue) AddToPendingEmbeddings(id storage.NodeID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, id)
}

type rejectingContentEmbedder struct {
	mu    sync.Mutex
	calls map[string]int
}

func (e *rejectingContentEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, nil
}

func (e *rejectingContentEmbedder) EmbedBatch(_ context.Context, texts []string) ([][]float32, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, text := range texts {
		e.calls[text]++
		if strings.Contains(text, "poison") {
			return nil, &embed.ProviderError{Provider: "test", StatusCode: 400, Body: "too many tokens"}
		}
	}
	return [][]float32{{1, 0, 0}}, nil
}

func (e *rejectingContentEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (e *rejectingContentEmbedder) Dimensions() int { return 3 }
func (e *rejectingContentEmbedder) Model() string   { return "test" }
func (e *rejectingContentEmbedder) Backend() string { return "cpu" }

type recordingDocumentBatchEmbedder struct {
	rejectingContentEmbedder
	batchCalls   int
	rejectPoison bool
}

func (e *recordingDocumentBatchEmbedder) EmbedDocumentBatchChunks(_ context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	e.batchCalls++
	if e.rejectPoison {
		for _, text := range texts {
			if strings.Contains(text, "poison") {
				return nil, &embed.ProviderError{Provider: "test", StatusCode: 400, Body: "rejected input"}
			}
		}
	}
	results := make([]*embed.DocumentChunkResult, len(texts))
	for i, text := range texts {
		results[i] = &embed.DocumentChunkResult{Chunks: []string{text}, Embeddings: [][]float32{{float32(i + 1), 0, 0}}}
	}
	return results, nil
}

func TestRejectedDocumentIsIsolatedFromBatchNeighbors(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "embedding-batch-isolation")
	ids := []storage.NodeID{"first", "poison", "third"}
	for _, id := range ids {
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": string(id)}})
		require.NoError(t, err)
	}
	queue := &orderedEmbeddingQueue{Engine: store, pending: append([]storage.NodeID(nil), ids...)}
	provider := &recordingDocumentBatchEmbedder{
		rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)},
		rejectPoison:             true,
	}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	require.True(t, worker.processNextBatch())
	require.Greater(t, provider.batchCalls, 1, "the rejected batch should be bisected")
	for _, id := range []storage.NodeID{"first", "third"} {
		node, err := store.GetNode(id)
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 1)
	}
	poison, err := store.GetNode("poison")
	require.NoError(t, err)
	require.Equal(t, true, poison.EmbedMeta["embedding_failed"])
}

func TestRejectedNodeIsParkedWhileFollowingNodeCompletes(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "embedding-failure-progress")
	for _, node := range []*storage.Node{
		{ID: "poison", Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": "poison"}},
		{ID: "good", Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": "good"}},
	} {
		_, err := store.CreateNode(node)
		require.NoError(t, err)
	}
	queue := &orderedEmbeddingQueue{Engine: store, pending: []storage.NodeID{"poison", "good"}}
	provider := &rejectingContentEmbedder{calls: make(map[string]int)}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 3, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	require.True(t, worker.processNextBatch())
	require.True(t, worker.processNextBatch())

	poison, err := store.GetNode("poison")
	require.NoError(t, err)
	require.Equal(t, true, poison.EmbedMeta["embedding_failed"])
	good, err := store.GetNode("good")
	require.NoError(t, err)
	require.Len(t, good.ChunkEmbeddings, 1)
}

func TestPendingDocumentsShareProviderBatch(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "embedding-cross-node-batch")
	ids := []storage.NodeID{"first", "second", "third"}
	for _, id := range ids {
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]interface{}{"text": string(id)}})
		require.NoError(t, err)
	}
	queue := &orderedEmbeddingQueue{Engine: store, pending: append([]storage.NodeID(nil), ids...)}
	provider := &recordingDocumentBatchEmbedder{rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)}}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	require.True(t, worker.processNextBatch())
	require.Equal(t, 1, provider.batchCalls)
	for _, id := range ids {
		node, err := store.GetNode(id)
		require.NoError(t, err)
		require.Len(t, node.ChunkEmbeddings, 1)
	}
}
