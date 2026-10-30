package nornicdb

import (
	"context"
	"errors"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Count queue reads rather than sampling CPU: even a cheap empty lookup must
// not run repeatedly while the worker has no work.
type idleCountingEngine struct {
	storage.Engine
	claims    atomic.Int64
	refreshes atomic.Int64
	onEmpty   func()
}

func (e *idleCountingEngine) FindNodeNeedingEmbedding() *storage.Node {
	e.claims.Add(1)
	node := e.Engine.(EmbeddingFinder).FindNodeNeedingEmbedding()
	if node == nil && e.onEmpty != nil {
		e.onEmpty()
	}
	return node
}

func (e *idleCountingEngine) RefreshPendingEmbeddingsIndex() int {
	e.refreshes.Add(1)
	return e.Engine.(EmbeddingIndexManager).RefreshPendingEmbeddingsIndex()
}

func (e *idleCountingEngine) MarkNodeEmbedded(id storage.NodeID) {
	e.Engine.(EmbeddingIndexManager).MarkNodeEmbedded(id)
}

func (e *idleCountingEngine) UpdateNodeEmbeddingSidecar(node *storage.Node) error {
	return e.Engine.(storage.EmbeddingSidecarUpdater).UpdateNodeEmbeddingSidecar(node)
}

func idleTestWorker(t *testing.T, workers int) (*EmbedWorker, *idleCountingEngine) {
	t.Helper()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	base.SetEmbeddingsEnabled(true)
	store := &idleCountingEngine{Engine: storage.NewNamespacedEngine(base, "idle")}
	config := DefaultEmbedWorkerConfig()
	config.NumWorkers = workers
	config.DeferWorkerStart = true
	config.ScanInterval = 5 * time.Millisecond // Old loop scans on every tick.
	config.TriggerDebounceDelay = 20 * time.Millisecond
	config.BatchDelay = 0
	worker := NewEmbedWorker(newMockEmbedder(), store, config)
	t.Cleanup(worker.Close)
	return worker, store
}

func waitForIdleStartup(t *testing.T, worker *EmbedWorker, store *idleCountingEngine) {
	t.Helper()
	worker.StartWorkers()
	require.Eventually(t, func() bool { return store.claims.Load() >= int64(worker.config.NumWorkers) }, 3*time.Second, time.Millisecond)
}

func idleTestCreate(t *testing.T, store storage.Engine, id string) {
	t.Helper()
	_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"content": id}})
	require.NoError(t, err)
}

func TestEmbedWorkerIdleClaimAttempts(t *testing.T) {
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			worker, store := idleTestWorker(t, workers)
			waitForIdleStartup(t, worker, store)
			claims, refreshes := store.claims.Load(), store.refreshes.Load()
			const observation = 2 * time.Second
			time.Sleep(observation)
			delta := store.claims.Load() - claims
			t.Logf("empty queue: %d claims in %s (%.2f claims/s); refresh delta %d", delta, observation, float64(delta)/observation.Seconds(), store.refreshes.Load()-refreshes)
			require.Zero(t, delta, "idle workers must block instead of scanning on short ticks")
			require.Equal(t, refreshes, store.refreshes.Load())
		})
	}
}

func TestEmbedWorkerIdleEnqueueWake(t *testing.T) {
	worker, store := idleTestWorker(t, 4)
	waitForIdleStartup(t, worker, store)
	for i := range 3 {
		id := fmt.Sprintf("wake-%d", i)
		idleTestCreate(t, store, id)
		worker.Enqueue(id)
		require.Eventually(t, func() bool { return worker.Stats().Processed >= i+1 }, 400*time.Millisecond, time.Millisecond, "enqueue must wake a sleeping worker")
	}
}

func TestEmbedWorkerIdleEnqueueRacesEmptyClaim(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	claiming, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var once sync.Once
	store.onEmpty = func() { once.Do(func() { close(claiming); <-release }) }
	worker.StartWorkers()
	select {
	case <-claiming:
	case <-time.After(3 * time.Second):
		t.Fatal("worker never checked the queue")
	}
	idleTestCreate(t, store, "racing")
	worker.Enqueue("racing")
	close(release)
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 400*time.Millisecond, time.Millisecond, "the empty-to-wait transition must not lose enqueue signals")
}

func TestEmbedWorkerIdleStartupPendingAfterRestart(t *testing.T) {
	dir := t.TempDir()
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	if base != nil {
		fixtureowner.Own(t, base)
	}
	require.NoError(t, err)
	base.SetEmbeddingsEnabled(true)
	store := storage.NewNamespacedEngine(base, "idle")
	idleTestCreate(t, store, "persisted")
	require.NoError(t, base.Close())
	base, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	base.SetEmbeddingsEnabled(true)
	worker := NewEmbedWorker(newMockEmbedder(), storage.NewNamespacedEngine(base, "idle"), DefaultEmbedWorkerConfig())
	t.Cleanup(worker.Close)
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 3*time.Second, time.Millisecond)
}

func TestEmbedWorkerIdleShutdown(t *testing.T) {
	for _, startup := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_startup_%t", startup), func(t *testing.T) {
			worker, store := idleTestWorker(t, 4)
			if startup {
				waitForIdleStartup(t, worker, store)
			} else {
				worker.StartWorkers()
			}
			done := make(chan struct{})
			go func() { worker.Close(); close(done) }()
			select {
			case <-done:
			case <-time.After(200 * time.Millisecond):
				t.Fatal("idle shutdown did not return promptly")
			}
		})
	}
}

func TestEmbedWorkerIdlePauseResume(t *testing.T) {
	for _, resolved := range []bool{false, true} {
		t.Run(fmt.Sprintf("resolved_%t", resolved), func(t *testing.T) {
			worker, store := idleTestWorker(t, 1)
			if resolved {
				worker.SetEmbedderResolver(func(storage.NodeID) (embed.Embedder, error) { return worker.currentEmbedder(), nil })
			}
			waitForIdleStartup(t, worker, store)
			var paused atomic.Bool
			paused.Store(true)
			worker.SetShouldYield(paused.Load)
			idleTestCreate(t, store, "paused")
			worker.Enqueue("paused")
			claims := store.claims.Load()
			time.Sleep(300 * time.Millisecond)
			require.Zero(t, worker.Stats().Processed)
			require.Equal(t, claims, store.claims.Load(), "paused workers must not claim nodes")
			paused.Store(false) // Existing pressure callback has no notification API.
			require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 2*time.Second, time.Millisecond)
			worker.SetEmbedder(nil)
			idleTestCreate(t, store, "no-provider")
			worker.Enqueue("no-provider")
			time.Sleep(100 * time.Millisecond)
			require.Equal(t, 1, worker.Stats().Processed)
			require.Zero(t, worker.Stats().Failed)
			worker.SetEmbedder(newMockEmbedder())
			require.Eventually(t, func() bool { return worker.Stats().Processed == 2 }, 400*time.Millisecond, time.Millisecond)
		})
	}
}

func TestEmbedWorkerIdleRecoveryWithoutNotification(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	waitForIdleStartup(t, worker, store)
	// A storage/import path that doesn't call Enqueue is still discovered by
	// the long recovery scan, independently of normal write notifications.
	idleTestCreate(t, store, "external")
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 35*time.Second, 20*time.Millisecond)
	require.LessOrEqual(t, store.claims.Load(), int64(5))
}

func TestEmbedWorkerIdleRestoreReleaseRefreshesPending(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	waitForIdleStartup(t, worker, store)
	release := worker.holdForRestore(time.Second)
	idleTestCreate(t, store, "restored")
	store.MarkNodeEmbedded("restored") // Simulate a restored node missing its pending marker.
	claims := store.claims.Load()
	worker.TriggerImmediate()
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, claims, store.claims.Load())
	release()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 400*time.Millisecond, time.Millisecond)
}

func TestEmbedWorkerIdleModelArrival(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	worker.SetEmbedder(nil)
	idleTestCreate(t, store, "before-model")
	worker.StartWorkers()
	time.Sleep(600 * time.Millisecond) // Past the startup delay, still waiting for the model.
	require.Zero(t, store.claims.Load())
	require.Zero(t, store.refreshes.Load())
	worker.SetEmbedder(newMockEmbedder())
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 400*time.Millisecond, time.Millisecond)
}

type idleFailureScanEngine struct {
	*idleCountingEngine
}

func (*idleFailureScanEngine) StreamParkedEmbeddingFailures(context.Context, func(storage.NodeID, map[string]any) error) (int, error) {
	return 0, errors.New("failure metadata temporarily unavailable")
}

func TestEmbedWorkerIdleStartupContinuesAfterFailureScanError(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	worker.storage = &idleFailureScanEngine{store}
	idleTestCreate(t, store, "pending-after-scan-error")
	worker.StartWorkers()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 3*time.Second, time.Millisecond)
}

func TestEmbedWorkerIdleQueueEmptyCallback(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	worker.config.ClusterDebounceDelay = time.Millisecond
	worker.config.ClusterMinBatchSize = 1
	empty := make(chan int, 1)
	worker.SetOnQueueEmpty(func(processed int) { empty <- processed })
	idleTestCreate(t, store, "clustered")
	worker.StartWorkers()
	select {
	case processed := <-empty:
		require.Equal(t, 1, processed)
	case <-time.After(3 * time.Second):
		t.Fatal("draining the queue did not schedule clustering")
	}
}
