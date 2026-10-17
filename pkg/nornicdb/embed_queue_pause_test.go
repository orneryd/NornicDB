package nornicdb

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// gatedEmbedder holds each provider request until released, so a test can
// pause while a node is in flight.
type gatedEmbedder struct {
	*mockEmbedder
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newGatedEmbedder() *gatedEmbedder {
	return &gatedEmbedder{mockEmbedder: newMockEmbedder(), entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (g *gatedEmbedder) wait(ctx context.Context) {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func (g *gatedEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	g.wait(ctx)
	return g.mockEmbedder.Embed(ctx, text)
}

func (g *gatedEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	g.wait(ctx)
	return g.mockEmbedder.EmbedBatch(ctx, texts)
}

func (g *gatedEmbedder) open() { g.once.Do(func() { close(g.release) }) }

// pausedQuiet asserts that over the observation the paused workers made no
// loop pass and read no queue entry, whatever is enqueued meanwhile.
func pausedQuiet(t *testing.T, worker *EmbedWorker, store *idleCountingEngine, during func()) {
	t.Helper()
	// Wait until every worker has finished its pass and sleeps in the pause.
	require.Eventually(t, func() bool {
		return worker.pausedWaiting.Load() == int32(worker.config.NumWorkers)
	}, 3*time.Second, time.Millisecond, "every worker settles in the paused wait")
	passes, claims, refreshes := worker.passes.Load(), store.claims.Load(), store.refreshes.Load()
	if during != nil {
		during()
	}
	time.Sleep(1500 * time.Millisecond) // longer than the old 1s pause retry
	require.Equal(t, passes, worker.passes.Load(), "a paused worker must sleep, not loop")
	require.Equal(t, claims, store.claims.Load(), "a paused worker must not read the queue")
	require.Equal(t, refreshes, store.refreshes.Load(), "a paused worker must not scan the store")
}

// Paused, workers take no node and don't wake for writes; the nodes queued
// meanwhile stay pending and are all embedded once resumed.
func TestEmbedWorkerPauseHoldsBacklogUntilResume(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			worker, store := idleTestWorker(t, workers)
			waitForIdleStartup(t, worker, store)
			idleTestCreate(t, store, "before")
			worker.Enqueue("before")
			require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 3*time.Second, time.Millisecond)

			worker.SetPaused(true)
			worker.SetPaused(true) // idempotent
			require.True(t, worker.Paused())
			require.True(t, worker.Stats().Paused)
			pausedQuiet(t, worker, store, func() {
				for i := range 5 {
					id := fmt.Sprintf("backlog-%d", i)
					idleTestCreate(t, store, id)
					worker.Enqueue(id)
				}
			})
			stats := worker.Stats()
			require.Equal(t, 1, stats.Processed, "no node is embedded while paused")
			require.Zero(t, stats.Failed)
			require.Zero(t, stats.Parked)

			worker.SetPaused(false)
			require.False(t, worker.Stats().Paused)
			require.Eventually(t, func() bool { return worker.Stats().Processed == 6 }, 5*time.Second, time.Millisecond,
				"the backlog queued during the pause is embedded after the resume")
		})
	}
}

// A node already claimed when the pause comes is embedded and written; the
// next pending node waits for the resume, neither failed nor parked.
func TestEmbedWorkerPauseLetsInFlightNodeFinish(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	gated := newGatedEmbedder()
	t.Cleanup(gated.open)
	worker.SetEmbedder(gated)
	waitForIdleStartup(t, worker, store)

	idleTestCreate(t, store, "inflight")
	worker.Enqueue("inflight")
	select {
	case <-gated.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the worker never sent the provider request")
	}
	require.Equal(t, 1, worker.Stats().InFlight)
	idleTestCreate(t, store, "waiting")
	worker.Enqueue("waiting")

	worker.SetPaused(true)
	gated.open()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 && worker.Stats().InFlight == 0 }, 3*time.Second, time.Millisecond,
		"the in-flight node is finished, not abandoned")
	pausedQuiet(t, worker, store, nil)
	stats := worker.Stats()
	require.Equal(t, 1, stats.Processed)
	require.Zero(t, stats.Failed)
	require.Zero(t, stats.Parked)

	worker.SetPaused(false)
	require.Eventually(t, func() bool { return worker.Stats().Processed == 2 }, 5*time.Second, time.Millisecond)
}

// A queue created paused (StartPaused: a restart or provider reload during
// a pause) never claims a node until resumed.
func TestEmbedWorkerStartPaused(t *testing.T) {
	worker, store := idleTestWorker(t, 2)
	worker.config.StartPaused = true // read only by NewEmbedWorker; set it there
	paused := NewEmbedWorker(newMockEmbedder(), store, worker.config)
	t.Cleanup(paused.Close)
	require.True(t, paused.Paused())
	idleTestCreate(t, store, "queued")
	paused.Enqueue("queued")
	paused.StartWorkers()
	time.Sleep(1500 * time.Millisecond)
	require.Zero(t, store.claims.Load(), "a queue created paused claims nothing")
	require.Zero(t, paused.Stats().Processed)

	paused.SetPaused(false)
	require.Eventually(t, func() bool { return paused.Stats().Processed == 1 }, 5*time.Second, time.Millisecond)
}

// pausingQueue pauses the worker as the first node is handed out, standing
// in for an administrator who pauses while a batch is being filled.
type pausingQueue struct {
	*sequenceEmbeddingEngine
	worker *EmbedWorker
	paused chan struct{}
}

func (q *pausingQueue) FindNodeNeedingEmbedding() *storage.Node {
	node := q.sequenceEmbeddingEngine.FindNodeNeedingEmbedding()
	if q.next == 1 {
		// The claim holds claimMu, which SetPaused waits on, so pause from
		// another goroutine and continue once the flag is set.
		go func() {
			q.worker.SetPaused(true)
			close(q.paused)
		}()
		for !q.worker.Paused() {
			runtime.Gosched()
		}
	}
	return node
}

func (q *pausingQueue) RefreshPendingEmbeddingsIndex() int { return 0 }

// A pause issued while a batch is being filled stops the claims: only the
// node claimed before it is embedded, the rest stay pending, and the resume
// embeds them. Covers both batching paths.
func TestEmbedWorkerPauseStopsBatchFill(t *testing.T) {
	for _, path := range []string{"resolved", "document"} {
		t.Run(path, func(t *testing.T) {
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			nodes := []*storage.Node{
				{ID: "docs:first", Properties: map[string]any{"content": "first"}},
				{ID: "docs:second", Properties: map[string]any{"content": "second"}},
				{ID: "docs:third", Properties: map[string]any{"content": "third"}},
			}
			for _, node := range nodes {
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
			}
			queue := &pausingQueue{sequenceEmbeddingEngine: &sequenceEmbeddingEngine{Engine: engine, nodes: nodes}, paused: make(chan struct{})}
			provider := &resolvedDocumentBatchEmbedder{}
			config := &EmbedWorkerConfig{NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true}
			var worker *EmbedWorker
			if path == "resolved" {
				worker = NewEmbedWorker(newMockEmbedder(), queue, config)
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return provider, nil })
			} else {
				worker = NewEmbedWorker(provider, queue, config)
			}
			t.Cleanup(worker.Close)
			queue.worker = worker

			require.True(t, worker.processNextBatch())
			<-queue.paused
			require.Equal(t, []int{1}, provider.batchSizes, "no node is claimed after the pause")
			require.Equal(t, 1, queue.next, "the queue is not read after the pause")
			require.Zero(t, worker.Stats().InFlight)
			for _, node := range nodes[1:] {
				stored, err := engine.GetNode(node.ID)
				require.NoError(t, err)
				require.Empty(t, stored.ChunkEmbeddings, "%s stays pending", node.ID)
			}
			require.False(t, worker.processNextBatch(), "a paused worker takes nothing")

			worker.SetPaused(false)
			require.True(t, worker.processNextBatch())
			require.Equal(t, []int{1, 2}, provider.batchSizes, "the resume embeds the pending nodes")
			for _, node := range nodes {
				stored, err := engine.GetNode(node.ID)
				require.NoError(t, err)
				require.NotEmpty(t, stored.ChunkEmbeddings, "%s is embedded", node.ID)
			}
		})
	}
}

// blockingQueue holds the first queue read, inside a claim (claimMu held,
// past its pause check), until released.
type blockingQueue struct {
	*sequenceEmbeddingEngine
	entered chan struct{}
	release chan struct{}
}

func (q *blockingQueue) FindNodeNeedingEmbedding() *storage.Node {
	if q.next == 0 {
		close(q.entered)
		<-q.release
	}
	return q.sequenceEmbeddingEngine.FindNodeNeedingEmbedding()
}

func (q *blockingQueue) RefreshPendingEmbeddingsIndex() int { return 0 }

// A pause that finds the flag already set still waits out a claim in
// progress: no SetPaused(true) returns while a node can still be claimed.
func TestEmbedWorkerRepeatedPauseWaitsForClaim(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	nodes := []*storage.Node{{ID: "docs:first", Properties: map[string]any{"content": "first"}}}
	_, err := engine.CreateNode(nodes[0])
	require.NoError(t, err)
	queue := &blockingQueue{sequenceEmbeddingEngine: &sequenceEmbeddingEngine{Engine: engine, nodes: nodes}, entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(queue.release) }) }
	t.Cleanup(release)
	provider := &resolvedDocumentBatchEmbedder{}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true})
	t.Cleanup(worker.Close)

	worked := make(chan bool, 1)
	go func() { worked <- worker.processNextBatch() }()
	<-queue.entered
	first, second := make(chan struct{}), make(chan struct{})
	go func() {
		worker.SetPaused(true)
		close(first)
	}()
	require.Eventually(t, worker.Paused, 3*time.Second, time.Millisecond)
	go func() {
		worker.SetPaused(true) // the flag is already set
		close(second)
	}()
	select {
	case <-second:
		t.Fatal("a repeated pause returned while a claim was still in progress")
	case <-first:
		t.Fatal("the pause returned while a claim was still in progress")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	for _, done := range []chan struct{}{first, second} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("a pause never returned after the claim finished")
		}
	}
	require.True(t, <-worked)
	require.Equal(t, []int{1}, provider.batchSizes, "the claim already under way is finished, and nothing after it")
}

// A restore hold waits out a claim in progress, as a pause does: once
// holdForRestore returns, no node is claimed until the hold is released.
func TestEmbedWorkerRestoreHoldWaitsForClaim(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	nodes := []*storage.Node{
		{ID: "docs:first", Properties: map[string]any{"content": "first"}},
		{ID: "docs:second", Properties: map[string]any{"content": "second"}},
	}
	for _, node := range nodes {
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
	}
	queue := &blockingQueue{sequenceEmbeddingEngine: &sequenceEmbeddingEngine{Engine: engine, nodes: nodes}, entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(queue.release) })
	t.Cleanup(release)
	provider := &resolvedDocumentBatchEmbedder{}
	worker := NewEmbedWorker(provider, queue, &EmbedWorkerConfig{NumWorkers: 0, MaxRetries: 1, ChunkSize: 512, EmbedBatchSize: 8, DeferWorkerStart: true})
	t.Cleanup(worker.Close)

	worked := make(chan bool, 1)
	go func() { worked <- worker.processNextBatch() }()
	<-queue.entered
	held := make(chan func(), 1)
	go func() { held <- worker.holdForRestore(0) }()
	select {
	case <-held:
		t.Fatal("the restore hold returned while a claim was still in progress")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	var releaseHold func()
	select {
	case releaseHold = <-held:
	case <-time.After(3 * time.Second):
		t.Fatal("the restore hold never returned after the claim finished")
	}
	require.True(t, <-worked)
	require.Equal(t, []int{1}, provider.batchSizes, "the claim under way is finished, and nothing is claimed after the hold")
	releaseHold()
}
