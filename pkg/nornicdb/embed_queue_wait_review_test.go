package nornicdb

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorkerResumeRacesEnqueue(t *testing.T) {
	worker, store := idleTestWorker(t, 1)
	worker.config.TriggerDebounceDelay = 0
	empty := make(chan struct{})
	var emptyOnce sync.Once
	store.onEmpty = func() { emptyOnce.Do(func() { close(empty) }) }
	worker.StartWorkers()
	select {
	case <-empty:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish its initial empty lookup")
	}
	claims := store.claims.Load()

	var stateMu sync.Mutex
	paused, armed, captured := true, false, false
	observed, resume := make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	worker.SetShouldYield(func() bool {
		stateMu.Lock()
		// Only stop the worker after it consumed the enqueue signal. Holding
		// stateMu across Enqueue below excludes the pre-send interleaving.
		capture := armed && !captured && len(worker.trigger) == 0
		if capture {
			captured = true
			close(observed)
		}
		answer := paused
		stateMu.Unlock()
		if capture {
			<-resume
		}
		return answer
	})
	idleTestCreate(t, store, "resume-race")
	stateMu.Lock()
	armed = true
	worker.Enqueue("resume-race")
	stateMu.Unlock()
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not consume the enqueue under pressure")
	}
	require.Zero(t, worker.Stats().Processed)
	require.Equal(t, claims, store.claims.Load(), "the paused pass must not read the pending queue")
	// The pressure callback returns its captured true after resume. The next
	// sample is false, reproducing the lost readiness retry deterministically.
	stateMu.Lock()
	paused = false
	stateMu.Unlock()
	unblock()
	require.Eventually(t, func() bool { return worker.Stats().Processed == 1 }, 2*time.Second, time.Millisecond,
		"resume must recheck pending work without another enqueue")
}

type deleteDiscoveredEmbeddingEngine struct {
	*idleCountingEngine
	deleted atomic.Bool
}

func (e *deleteDiscoveredEmbeddingEngine) FindNodeNeedingEmbedding() *storage.Node {
	node := e.idleCountingEngine.FindNodeNeedingEmbedding()
	if node != nil && node.ID == "a" && e.deleted.CompareAndSwap(false, true) {
		_ = e.Engine.DeleteNode(node.ID)
	}
	return node
}

type deleteDuringEmbeddingProvider struct {
	*mockEmbedder
	store storage.Engine
	once  sync.Once
}

func (p *deleteDuringEmbeddingProvider) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	p.once.Do(func() { _ = p.store.DeleteNode("a") })
	return p.mockEmbedder.EmbedBatch(ctx, texts)
}

func TestEmbedWorkerDrainContinuesAfterDeletedCandidate(t *testing.T) {
	for _, when := range []string{"discovery", "writeback"} {
		t.Run(when, func(t *testing.T) {
			worker, store := idleTestWorker(t, 0)
			worker.config.TriggerDebounceDelay = 0
			// Match an already-started pool: an opportunistic refresh must not
			// accidentally rescue a drainer that stopped on the deleted item.
			worker.lastRefreshAt = time.Now()
			idleTestCreate(t, store, "a")
			idleTestCreate(t, store, "b")
			if when == "discovery" {
				worker.storage = &deleteDiscoveredEmbeddingEngine{idleCountingEngine: store}
			} else {
				worker.SetEmbedder(&deleteDuringEmbeddingProvider{mockEmbedder: newMockEmbedder(), store: store})
			}
			worker.Enqueue("a")
			worker.Enqueue("b")
			<-worker.trigger // One coalesced notification has woken the drainer.
			worker.processUntilEmpty()
			require.Equal(t, 1, worker.Stats().Processed, "skipping a deleted candidate must continue with b")
			node, err := store.GetNode("b")
			require.NoError(t, err)
			require.NotEmpty(t, node.ChunkEmbeddings)
			require.LessOrEqual(t, store.claims.Load(), int64(4), "a truly empty queue still ends the drain")
		})
	}
}

func TestEmbedWorkerModelArrivalWakesEveryWorker(t *testing.T) {
	const poolSize = 4
	worker, store := idleTestWorker(t, poolSize)
	worker.SetEmbedder(nil)
	worker.config.EmbedBatchSize = 1
	first, waiting := make(chan struct{}, poolSize), make(chan struct{}, poolSize)
	initialChecks := make(chan struct{})
	var probes atomic.Int32
	worker.SetShouldYield(func() bool {
		call := probes.Add(1)
		if call <= poolSize {
			first <- struct{}{}
			<-initialChecks
		} else if call <= 2*poolSize {
			waiting <- struct{}{}
		}
		return false
	})
	// Discard setup-only notifications before starting; no worker is waiting yet.
	select {
	case <-worker.trigger:
	default:
	}
	for i := range poolSize {
		idleTestCreate(t, store, fmt.Sprintf("model-%d", i))
	}
	provider := &blockingStructuredEmbedder{started: make(chan struct{}, poolSize), release: make(chan struct{})}
	t.Cleanup(func() { close(provider.release) })
	var initialOnce sync.Once
	unblockInitial := func() { initialOnce.Do(func() { close(initialChecks) }) }
	t.Cleanup(unblockInitial)
	worker.StartWorkers()
	for range poolSize {
		select {
		case <-first:
		case <-time.After(3 * time.Second):
			t.Fatal("not all workers reached their first readiness check")
		}
	}
	unblockInitial()
	for range poolSize {
		select {
		case <-waiting:
		case <-time.After(3 * time.Second):
			t.Fatal("not all workers reached the nil-model wait")
		}
	}
	worker.SetEmbedder(provider)
	for range poolSize {
		select {
		case <-provider.started:
		case <-time.After(2 * time.Second):
			t.Fatal("model arrival must start every worker before any provider request completes")
		}
	}
	require.Equal(t, int32(poolSize), provider.calls.Load())
}
