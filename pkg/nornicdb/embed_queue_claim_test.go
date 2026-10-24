package nornicdb

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type delayedPendingVisibilityEngine struct {
	storage.Engine
	nodeID storage.NodeID
}

func (e *delayedPendingVisibilityEngine) FindNodeNeedingEmbedding() *storage.Node {
	node, err := e.Engine.GetNode(e.nodeID)
	if err != nil || len(node.ChunkEmbeddings) > 0 {
		return nil
	}
	return node
}

func (e *delayedPendingVisibilityEngine) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	return e.Engine.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(node)
}
func (*delayedPendingVisibilityEngine) MarkNodeEmbedded(storage.NodeID) {}
func (*delayedPendingVisibilityEngine) PendingEmbeddingsCount() int     { return 0 }

type blockingStructuredEmbedder struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

type structuredBatchCountingEmbedder struct {
	singleCalls int
	batchCalls  int
}

func (*structuredBatchCountingEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}
func (*structuredBatchCountingEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}
func (*structuredBatchCountingEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (*structuredBatchCountingEmbedder) Dimensions() int              { return 2 }
func (*structuredBatchCountingEmbedder) Model() string                { return "structured-batch-model" }
func (*structuredBatchCountingEmbedder) Backend() string              { return "test" }
func (*structuredBatchCountingEmbedder) UsesDocumentProperties() bool { return true }
func (e *structuredBatchCountingEmbedder) EmbedDocumentPropertyChunks(context.Context, string, map[string]any, int, int) (*embed.DocumentChunkResult, error) {
	e.singleCalls++
	return &embed.DocumentChunkResult{Embeddings: [][]float32{{1, 0}}, Model: e.Model()}, nil
}
func (e *structuredBatchCountingEmbedder) EmbedDocumentPropertyBatchChunks(_ context.Context, texts []string, properties []map[string]any, _, _ int) ([]*embed.DocumentChunkResult, error) {
	e.batchCalls++
	results := make([]*embed.DocumentChunkResult, len(texts))
	for index := range results {
		results[index] = &embed.DocumentChunkResult{Embeddings: [][]float32{{1, 0}}, Model: e.Model()}
	}
	return results, nil
}

func (e *blockingStructuredEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func (e *blockingStructuredEmbedder) EmbedBatch(context.Context, []string) ([][]float32, error) {
	return nil, nil
}

func (e *blockingStructuredEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

func (*blockingStructuredEmbedder) Dimensions() int              { return 2 }
func (*blockingStructuredEmbedder) Model() string                { return "structured-model" }
func (*blockingStructuredEmbedder) Backend() string              { return "test" }
func (*blockingStructuredEmbedder) UsesDocumentProperties() bool { return true }
func (e *blockingStructuredEmbedder) EmbedDocumentPropertyChunks(context.Context, string, map[string]any, int, int) (*embed.DocumentChunkResult, error) {
	e.calls.Add(1)
	e.started <- struct{}{}
	<-e.release
	return &embed.DocumentChunkResult{Embeddings: [][]float32{{1, 0}}, Model: e.Model()}, nil
}

func TestEmbedWorkerClaimsStructuredNodeOnceAcrossConcurrentWorkers(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	node := &storage.Node{ID: "test:visual", Properties: map[string]any{"title": "diagram"}}
	_, err := base.CreateNode(node)
	require.NoError(t, err)

	engine := &delayedPendingVisibilityEngine{Engine: base, nodeID: node.ID}
	provider := &blockingStructuredEmbedder{
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
	worker := NewEmbedWorker(provider, engine, &EmbedWorkerConfig{
		NumWorkers:       0,
		EmbedBatchSize:   1,
		ChunkSize:        512,
		MaxRetries:       1,
		DeferWorkerStart: true,
	})
	defer worker.Close()
	worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		worker.processNextBatch()
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("first provider call did not start")
	}
	db := &DB{baseStorage: engine, embedQueue: worker}
	stats := worker.Stats()
	require.True(t, stats.Running)
	require.Equal(t, 1, stats.InFlight)
	require.Equal(t, 1, db.PendingEmbeddingsCount(),
		"claimed work must remain visible after its durable pending marker is removed")
	waitDone := make(chan error, 1)
	go func() { waitDone <- db.WaitForEmbeddings(context.Background()) }()
	select {
	case err := <-waitDone:
		t.Fatalf("wait returned while provider work was still in flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	go func() {
		defer waitGroup.Done()
		worker.processNextBatch()
	}()

	time.Sleep(50 * time.Millisecond)
	close(provider.release)
	waitGroup.Wait()
	require.Equal(t, int32(1), provider.calls.Load())
	stats = worker.Stats()
	require.False(t, stats.Running)
	require.Zero(t, stats.InFlight)
	select {
	case err := <-waitDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("wait did not return after claimed work completed")
	}
}

func TestEmbedWorkerBatchesStructuredNodesForTheSameProvider(t *testing.T) {
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	nodes := []*storage.Node{
		{ID: "test:first", Properties: map[string]any{"title": "first"}},
		{ID: "test:second", Properties: map[string]any{"title": "second"}},
	}
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	queue := &sequenceEmbeddingEngine{Engine: engine, nodes: nodes}
	provider := &structuredBatchCountingEmbedder{}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{
		NumWorkers: 0, EmbedBatchSize: 16, ChunkSize: 512, MaxRetries: 1, DeferWorkerStart: true,
	})
	defer worker.Close()
	worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })

	require.True(t, worker.processNextBatch())
	require.Equal(t, 1, provider.batchCalls)
	require.Zero(t, provider.singleCalls)
}
