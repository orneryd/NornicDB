package nornicdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelOnFindEngine struct {
	*queueBranchEngine
	cancel context.CancelFunc
}

func (e *cancelOnFindEngine) FindNodeNeedingEmbedding() *storage.Node {
	n := e.queueBranchEngine.FindNodeNeedingEmbedding()
	if n != nil && e.cancel != nil {
		e.cancel()
	}
	return n
}

func TestEmbedWorker_StartWorkers_NumWorkersBranches(t *testing.T) {
	newWorker := func(numWorkers int) *EmbedWorker {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "start-workers")
		return NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:       numWorkers,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
	}

	t.Run("zero workers disables worker pool", func(t *testing.T) {
		w := newWorker(0)
		t.Cleanup(func() { w.Close() })

		w.StartWorkers()
		require.True(t, w.workersStarted)
		require.Zero(t, w.config.NumWorkers)
	})

	t.Run("negative worker count defaults to one", func(t *testing.T) {
		w := newWorker(-1)
		t.Cleanup(func() { w.Close() })

		w.StartWorkers()
		require.True(t, w.workersStarted)
		require.Equal(t, 1, w.config.NumWorkers)
	})

	t.Run("num workers greater than one starts worker pool", func(t *testing.T) {
		w := newWorker(2)
		t.Cleanup(func() { w.Close() })

		w.StartWorkers()
		require.True(t, w.workersStarted)
	})
}

func TestEmbedWorker_TriggerDebounce_ClosedAndStaleSequence(t *testing.T) {
	newWorker := func() *EmbedWorker {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "trigger-debounce")
		return NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:           1,
			ScanInterval:         time.Hour,
			BatchDelay:           time.Millisecond,
			MaxRetries:           1,
			ChunkSize:            128,
			ChunkOverlap:         16,
			DeferWorkerStart:     true,
			TriggerDebounceDelay: 20 * time.Millisecond,
		})
	}

	t.Run("closed worker suppresses debounce callback", func(t *testing.T) {
		w := newWorker()
		t.Cleanup(func() { w.Close() })

		w.Trigger()
		w.closed.Store(true)
		time.Sleep(35 * time.Millisecond)
		require.Zero(t, w.QueueLen())
	})

	t.Run("stale debounce sequence does not signal trigger", func(t *testing.T) {
		w := newWorker()
		t.Cleanup(func() { w.Close() })

		w.Trigger()
		w.triggerDebounceSeq.Add(1) // invalidate the callback sequence
		time.Sleep(35 * time.Millisecond)
		require.Zero(t, w.QueueLen())
	})
}

func TestEmbedWorker_ProcessNextBatch_ShouldYieldBranch(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "yield")
	w := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
		NumWorkers:       1,
		ScanInterval:     time.Hour,
		BatchDelay:       time.Millisecond,
		MaxRetries:       1,
		ChunkSize:        128,
		ChunkOverlap:     16,
		DeferWorkerStart: true,
	})
	t.Cleanup(func() { w.Close() })

	w.SetShouldYield(func() bool { return true })
	didWork := w.processNextBatch()
	require.False(t, didWork)
	require.Equal(t, 1, w.QueueLen())
}

func TestEmbedWorker_EmbedBatchHelpers_EdgeBranches(t *testing.T) {
	t.Run("embedChunksInBatches empty input returns nil", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "batch-empty")
		w := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:       1,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
		t.Cleanup(func() { w.Close() })

		embs, err := w.embedChunksInBatches(nil, "node-1")
		require.NoError(t, err)
		require.Nil(t, embs)
	})

	t.Run("embedBatchWithRetry exits on context cancellation during backoff", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "batch-cancel")
		w := NewEmbedWorker(&flakyBatchEmbedder{dims: 3, failUntil: 10}, engine, &EmbedWorkerConfig{
			NumWorkers:       1,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       2,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
		t.Cleanup(func() { w.Close() })

		go func() {
			time.Sleep(20 * time.Millisecond)
			w.cancel()
		}()

		embs, err := w.embedBatchWithRetry([]string{"a", "b"})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		require.Nil(t, embs)
	})
}

func TestEmbedWorker_ProcessNextBatch_AdditionalBranches(t *testing.T) {
	t.Run("canceled context at start returns without work", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ew := &EmbedWorker{
			ctx:     ctx,
			trigger: make(chan struct{}, 1),
		}

		require.False(t, ew.processNextBatch())
	})

	t.Run("canceled context after claim releases and exits", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "cancel-after-claim")
		node := &storage.Node{ID: storage.NodeID("n-cancel"), Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		qe := &queueBranchEngine{Engine: engine, findNode: &storage.Node{ID: node.ID}}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  &cancelOnFindEngine{queueBranchEngine: qe, cancel: cancel},
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      ctx,
			trigger:  make(chan struct{}, 1),
		}

		require.False(t, ew.processNextBatch())
	})

	t.Run("recently processed node is temporarily skipped", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "recently-processed")
		node := &storage.Node{ID: storage.NodeID("n-recent"), Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{Engine: engine, findNode: &storage.Node{ID: node.ID}}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
			recentlyProcessed: map[string]time.Time{
				string(node.ID): time.Now(),
			},
		}

		require.False(t, ew.processNextBatch())
		require.True(t, ew.loggedSkip[string(node.ID)])
	})

	t.Run("update embedding error requeues node and records failure", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "update-error")
		node := &storage.Node{ID: storage.NodeID("n-update-err"), Labels: []string{"Doc"}, Properties: map[string]any{"content": "x"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)

		qe := &queueBranchEngine{
			Engine:             engine,
			findNode:           &storage.Node{ID: node.ID},
			updateEmbeddingErr: errors.New("update failed"),
		}
		ew := &EmbedWorker{
			embedder: newMockEmbedder(),
			storage:  qe,
			config:   &EmbedWorkerConfig{BatchDelay: time.Millisecond, MaxRetries: 1, ChunkSize: 64, ChunkOverlap: 8},
			ctx:      context.Background(),
			trigger:  make(chan struct{}, 1),
		}

		require.True(t, ew.processNextBatch())
		require.Equal(t, int64(1), ew.failed.Load())
		require.Equal(t, []storage.NodeID{node.ID}, qe.added)
	})
}

func TestEmbedWorker_ProcessUntilEmpty_RefreshCleanupBranch(t *testing.T) {
	qe := &queueBranchEngine{Engine: storage.NewNamespacedEngine(storage.NewMemoryEngine(), "refresh-cleanup"), refreshCount: 1}
	ew := &EmbedWorker{
		storage: qe,
		ctx:     context.Background(),
		trigger: make(chan struct{}, 1),
	}

	start := time.Now()
	ew.processUntilEmpty()
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestEmbedWorker_NewEmbedWorker_MultiWorkerStartupBranch(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "startup-multi")
	w := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
		NumWorkers:       2,
		ScanInterval:     time.Hour,
		BatchDelay:       time.Millisecond,
		MaxRetries:       1,
		ChunkSize:        64,
		ChunkOverlap:     8,
		DeferWorkerStart: false,
	})
	t.Cleanup(func() { w.Close() })
}
