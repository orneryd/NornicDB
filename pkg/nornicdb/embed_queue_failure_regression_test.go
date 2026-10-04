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

func (e *orderedEmbeddingQueue) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	return e.Engine.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(node)
}

type delayedTransientEmbeddingError struct{ delay time.Duration }

type blockingBisectEmbedder struct {
	recordingDocumentBatchEmbedder
	started chan struct{}
	release chan struct{}
}

func (e *blockingBisectEmbedder) EmbedDocumentBatchChunks(ctx context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	if len(texts) > 1 {
		return nil, &embed.ProviderError{Provider: "test", StatusCode: 400, Body: "split batch"}
	}
	select {
	case e.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.release:
		return []*embed.DocumentChunkResult{{Embeddings: [][]float32{{1, 0, 0}}, Model: e.Model()}}, nil
	}
}

func (e delayedTransientEmbeddingError) Error() string             { return "provider asked for a cooldown" }
func (e delayedTransientEmbeddingError) Retryable() bool           { return true }
func (e delayedTransientEmbeddingError) RetryDelay() time.Duration { return e.delay }

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
	batchCalls    int
	rejectPoison  bool
	transient     bool
	maxBatchBytes int
}

func (e *recordingDocumentBatchEmbedder) EmbedDocumentBatchChunks(_ context.Context, texts []string, _, _ int) ([]*embed.DocumentChunkResult, error) {
	e.batchCalls++
	if e.maxBatchBytes > 0 {
		batchBytes := 0
		for _, text := range texts {
			batchBytes += len(text)
		}
		if batchBytes > e.maxBatchBytes {
			return nil, fmt.Errorf("document batch exceeds local request budget")
		}
	}
	if e.transient {
		return nil, &embed.ProviderError{Provider: "test", StatusCode: 503, Body: "temporarily unavailable"}
	}
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

func TestUnclassifiedDocumentBatchLimitIsBisected(t *testing.T) {
	provider := &recordingDocumentBatchEmbedder{
		rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)},
		maxBatchBytes:            8,
	}
	worker := NewEmbedWorker(provider, storage.NewMemoryEngine(), &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	results, errs := worker.embedDocumentBatchIsolated(provider, []string{"aaaa", "bbbb", "cccc", "dddd"})
	require.Equal(t, 3, provider.batchCalls)
	require.Len(t, results, 4)
	for i := range results {
		require.NotNil(t, results[i])
		require.NoError(t, errs[i])
	}
}

func TestTransientDocumentBatchFailureDoesNotBisectProviderOutage(t *testing.T) {
	provider := &recordingDocumentBatchEmbedder{
		rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)},
		transient:                true,
	}
	worker := NewEmbedWorker(provider, storage.NewMemoryEngine(), &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	results, errs := worker.embedDocumentBatchIsolated(provider, []string{"one", "two", "three", "four"})
	require.Equal(t, 1, provider.batchCalls, "a provider-wide transient failure must not be retried once per document")
	require.Len(t, results, 4)
	require.Len(t, errs, 4)
	for _, err := range errs {
		require.Error(t, err)
		require.True(t, isRetryableEmbeddingError(err))
	}
}

func TestTransientProviderCooldownIsSharedAndHonorsRequestedDelay(t *testing.T) {
	provider := &recordingDocumentBatchEmbedder{rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)}}
	worker := NewEmbedWorker(provider, storage.NewMemoryEngine(), &EmbedWorkerConfig{
		NumWorkers:              0,
		DeferWorkerStart:        true,
		ProviderRetryBackoff:    5 * time.Millisecond,
		ProviderRetryBackoffMax: 20 * time.Millisecond,
	})
	t.Cleanup(worker.Close)

	worker.recordProviderFailure(provider, delayedTransientEmbeddingError{delay: 50 * time.Millisecond})
	state := worker.providerRetries[providerRetryKey(provider)]
	require.Equal(t, 1, state.failures)
	require.GreaterOrEqual(t, time.Until(state.retryAt), 40*time.Millisecond)

	started := time.Now()
	require.True(t, worker.waitForProviderRetry(provider))
	require.GreaterOrEqual(t, time.Since(started), 35*time.Millisecond)
	worker.clearProviderFailure(provider)
	require.Empty(t, worker.providerRetries)
}

func TestTerminalEmbeddingFailuresRemainDiscoverableAndRetryableAfterWorkerRestart(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "embedding-failure-recovery")
	_, err := store.CreateNode(&storage.Node{
		ID:         "picture",
		Labels:     []string{"Image"},
		Properties: map[string]any{"caption": "recover me"},
	})
	require.NoError(t, err)

	first := NewEmbedWorker(nil, store, &EmbedWorkerConfig{NumWorkers: 0, DeferWorkerStart: true})
	first.markNodeEmbeddingFailed("picture", &embed.ProviderError{Provider: "test", StatusCode: 400, Body: "invalid image"})
	first.Close()

	restarted := NewEmbedWorker(nil, store, &EmbedWorkerConfig{NumWorkers: 0, DeferWorkerStart: true})
	t.Cleanup(restarted.Close)
	failures, err := restarted.ParkedEmbeddingFailures(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, failures, 1)
	require.Equal(t, []EmbeddingFailure{{
		NodeID:   "picture",
		Error:    "test returned 400: invalid image",
		FailedAt: failures[0].FailedAt,
	}}, failures)
	require.NotEmpty(t, failures[0].FailedAt)
	require.Equal(t, 1, restarted.Stats().Parked)

	retried, err := restarted.RetryParkedEmbeddingFailures(context.Background(), []storage.NodeID{"picture"})
	require.NoError(t, err)
	require.Equal(t, 1, retried)
	require.Zero(t, restarted.Stats().Parked)
	node, err := store.GetNode("picture")
	require.NoError(t, err)
	require.True(t, storage.NodeNeedsEmbedding(node))
	require.Nil(t, node.EmbedMeta)
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

func TestEmbedWorkerReportsClaimedNodesDuringBatchBisection(t *testing.T) {
	base := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(base, "embedding-bisect-stats")
	ids := []storage.NodeID{"first", "second"}
	for _, id := range ids {
		_, err := store.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": string(id)}})
		require.NoError(t, err)
	}
	queue := &orderedEmbeddingQueue{Engine: store, pending: append([]storage.NodeID(nil), ids...)}
	provider := &blockingBisectEmbedder{
		recordingDocumentBatchEmbedder: recordingDocumentBatchEmbedder{rejectingContentEmbedder: rejectingContentEmbedder{calls: make(map[string]int)}},
		started:                        make(chan struct{}, 1),
		release:                        make(chan struct{}),
	}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
		NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true,
	})
	t.Cleanup(worker.Close)

	done := make(chan struct{})
	go func() {
		worker.processNextBatch()
		close(done)
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("bisected provider request did not start")
	}
	stats := worker.Stats()
	require.True(t, stats.Running)
	require.Equal(t, 2, stats.InFlight)

	close(provider.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("bisected batch did not finish")
	}
	stats = worker.Stats()
	require.False(t, stats.Running)
	require.Zero(t, stats.InFlight)
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
