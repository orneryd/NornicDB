package nornicdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type rejectingSidecarQueue struct {
	*orderedEmbeddingQueue
	writeMu    sync.Mutex
	attempts   map[storage.NodeID]int
	rejections int
}

func (q *rejectingSidecarQueue) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	q.writeMu.Lock()
	q.attempts[node.ID]++
	reject := q.attempts[node.ID] <= q.rejections
	q.writeMu.Unlock()
	if reject {
		return errors.New("test: sidecar write rejected")
	}
	return q.orderedEmbeddingQueue.UpdateNodeEmbeddingSidecar(node)
}

type retryDocumentEmbedder struct{ *mockEmbedder }

func (e *retryDocumentEmbedder) EmbedDocumentBatchChunks(ctx context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	vectors, err := e.EmbedBatch(ctx, texts)
	if err != nil {
		return nil, err
	}
	results := make([]*embed.DocumentChunkResult, len(texts))
	for i, text := range texts {
		results[i] = &embed.DocumentChunkResult{Chunks: []string{text}, Embeddings: [][]float32{vectors[i]}, Model: e.Model(), ChunkerVersion: "retry-test"}
	}
	return results, nil
}

type retryPropertyEmbedder struct{ *retryDocumentEmbedder }

func (*retryPropertyEmbedder) UsesDocumentProperties() bool { return true }
func (e *retryPropertyEmbedder) EmbedDocumentPropertyChunks(ctx context.Context, text string, properties map[string]any, size, overlap int) (*embed.DocumentChunkResult, error) {
	results, err := e.EmbedDocumentBatchChunks(ctx, []string{fmt.Sprint(properties["_embedding_content"]) + text}, size, overlap)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

type retryPropertyBatchEmbedder struct{ *retryPropertyEmbedder }

func (e *retryPropertyBatchEmbedder) EmbedDocumentPropertyBatchChunks(ctx context.Context, texts []string, properties []map[string]any, size, overlap int) ([]*embed.DocumentChunkResult, error) {
	inputs := make([]string, len(texts))
	for i, text := range texts {
		inputs[i] = fmt.Sprint(properties[i]["_embedding_content"]) + text
	}
	return e.EmbedDocumentBatchChunks(ctx, inputs, size, overlap)
}

func nodeSaveRetryFixture(tb testing.TB, mode string, nodes, rejections int) (*EmbedWorker, *rejectingSidecarQueue, *mockEmbedder) {
	tb.Helper()
	base := storage.NewMemoryEngine()
	tb.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "node-save-retry")
	ids := make([]storage.NodeID, nodes)
	for i := range ids {
		ids[i] = storage.NodeID(fmt.Sprintf("node-%03d", i))
		_, err := store.CreateNode(&storage.Node{ID: ids[i], Labels: []string{"Doc"}, Properties: map[string]any{"text": strings.Repeat("alpha beta ", 20), "_embedding_content": "original"}})
		require.NoError(tb, err)
	}
	queue := &rejectingSidecarQueue{orderedEmbeddingQueue: &orderedEmbeddingQueue{Engine: store, pending: ids}, attempts: make(map[storage.NodeID]int), rejections: rejections}
	plain := newMockEmbedder()
	plain.dims = 2
	var provider embed.Embedder = plain
	cfg := &EmbedWorkerConfig{NumWorkers: 4, DeferWorkerStart: true, MaxRetries: 1, ChunkSize: 8192, EmbedBatchSize: 16, ScanInterval: time.Second}
	if mode == "chunked" {
		cfg.ChunkSize = 32
		cfg.ChunkOverlap = 1
	}
	document := &retryDocumentEmbedder{plain}
	property := &retryPropertyEmbedder{document}
	switch mode {
	case "document-batch", "resolved-document-batch":
		provider = document
	case "property":
		provider = property
	case "property-batch":
		provider = &retryPropertyBatchEmbedder{property}
	}
	worker := NewEmbedWorker(provider, queue, cfg)
	tb.Cleanup(worker.Close)
	if mode == "resolved-document-batch" || mode == "property-batch" {
		worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
	}
	return worker, queue, plain
}

func TestEmbedWorkerSaveRetryRetainsProviderResult(t *testing.T) {
	for _, mode := range []string{"whole", "chunked", "document-batch", "resolved-document-batch", "property", "property-batch"} {
		for _, change := range []string{"unchanged", "text", "labels", "properties"} {
			t.Run(mode+"/"+change, func(t *testing.T) {
				worker, queue, provider := nodeSaveRetryFixture(t, mode, 1, 1)
				// Exclude the structured input from ordinary text: property providers
				// still consume it, so comparing built text alone is insufficient.
				worker.config.PropertiesInclude = []string{"text"}
				worker.config.IncludeLabels = true
				var published int
				worker.SetOnEmbedded(func(*storage.Node) { published++ })
				require.True(t, worker.processNextBatch())
				calls := provider.GetEmbedCount()
				require.Positive(t, calls)
				require.Zero(t, published)
				node, err := queue.GetNode("node-000")
				require.NoError(t, err)
				require.Empty(t, node.ChunkEmbeddings, "rejected results must not publish")
				switch change {
				case "text":
					node.Properties["text"] = strings.Repeat("gamma zeta ", 20)
				case "labels":
					node.Labels = []string{"Changed"}
				case "properties":
					node.Properties["_embedding_content"] = "changed"
				}
				if change != "unchanged" {
					require.NoError(t, queue.UpdateNode(node))
				}
				for i := 0; i < 4 && published == 0; i++ {
					worker.processNextBatch()
				}
				require.Equal(t, 1, published)
				wantCalls := calls
				if change != "unchanged" {
					wantCalls *= 2
				}
				require.Equal(t, wantCalls, provider.GetEmbedCount(), "retry must retain unchanged results and discard changed sources")
				saved, err := queue.GetNode(node.ID)
				require.NoError(t, err)
				require.NotEmpty(t, saved.ChunkEmbeddings)
				require.Equal(t, provider.Model(), saved.EmbedMeta["embedding_model"])
				require.Equal(t, node.Properties, saved.Properties)
			})
		}
	}
}

func TestEmbedWorkerSaveRetryAcrossWorkers(t *testing.T) {
	worker, _, provider := nodeSaveRetryFixture(t, "whole", 16, 2)
	var published atomic.Int64
	worker.SetOnEmbedded(func(*storage.Node) { published.Add(1) })
	worker.StartWorkers()
	require.Eventually(t, func() bool { return published.Load() == 16 }, 5*time.Second, time.Millisecond)
	worker.Close()
	require.Equal(t, 16, provider.GetEmbedCount(), "two rejected writes per node must not buy more vectors")
	require.Equal(t, int64(16), worker.processed.Load())
	require.Zero(t, worker.inFlight.Load())
}

func BenchmarkNodeEmbeddingSaveRetry(b *testing.B) {
	// Worker progress logging would split Go's benchmark metric lines.
	sink, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(b, err)
	stdout := os.Stdout
	os.Stdout = sink
	b.Cleanup(func() { os.Stdout = stdout; _ = sink.Close() })
	for _, mode := range []string{"whole", "chunked", "document-batch"} {
		for _, rejected := range []int{0, 1} {
			b.Run(fmt.Sprintf("%s/rejected_%d", mode, rejected), func(b *testing.B) {
				worker, queue, provider := nodeSaveRetryFixture(b, mode, 1, rejected)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					queue.attempts = make(map[storage.NodeID]int)
					worker.recentlyProcessed = make(map[string]time.Time)
					queue.AddToPendingEmbeddings("node-000")
					before := worker.processed.Load()
					for worker.processed.Load() == before {
						worker.processNextBatch()
					}
				}
				b.ReportMetric(float64(provider.GetEmbedCount())/float64(b.N), "provider_inputs/op")
			})
		}
	}
}
