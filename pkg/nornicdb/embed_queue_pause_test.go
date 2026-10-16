package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
