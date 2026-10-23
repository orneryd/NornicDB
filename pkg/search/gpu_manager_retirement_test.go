package search

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This checks source-token publication through the real CPU factory. A disabled
// manager cannot select GPU brute force; this is not a GPU ranking original.
func TestGPUManagerChangeRetiresConcurrentFactory(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx))
	node := &storage.Node{ID: "nornic:item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	manager, err := gpu.NewManager(&gpu.Config{Enabled: false})
	require.NoError(t, err)
	require.False(t, manager.IsEnabled())
	svc.SetGPUManager(manager)
	generation := svc.hnswGeneration.Load()

	h := &changingIntervalLog{}
	var releaseOnce [3]sync.Once
	for i := range h.entered {
		h.entered[i], h.release[i] = make(chan struct{}), make(chan struct{})
	}
	release := func(i int) { releaseOnce[i].Do(func() { close(h.release[i]) }) }
	svc.SetLogger(slog.New(h))
	release(0)
	release(2)
	svc.mu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(svc.mu.Unlock) }
	setterDone, factoryDone := make(chan struct{}), make(chan struct{})
	factoryStarted := false
	var factoryErr error
	defer func() {
		cancel()
		unlock()
		for i := range h.release {
			release(i)
		}
		for _, done := range []<-chan struct{}{setterDone, factoryDone} {
			if done == factoryDone && !factoryStarted {
				continue
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("manager/factory worker did not join before service cleanup")
			}
		}
	}()
	setterID := make(chan string, 1)
	go func() {
		setterID <- serviceTestGoroutineID()
		svc.SetGPUManager(nil)
		close(setterDone)
	}()
	setter := <-setterID
	require.Eventually(t, func() bool {
		return serviceStackContains(setter, "(*Service).SetGPUManager", "sync.(*RWMutex).Lock")
	}, 5*time.Second, time.Millisecond, "setter did not reach its source writer")
	factoryID := make(chan string, 1)
	factoryStarted = true
	go func() {
		factoryID <- serviceTestGoroutineID()
		_, factoryErr = svc.getOrCreateVectorPipeline(ctx)
		close(factoryDone)
	}()
	factory := <-factoryID
	require.Eventually(t, func() bool {
		return serviceStackContains(factory, "(*Service).getOrCreateVectorPipeline", "sync.(*RWMutex).RLock")
	}, 5*time.Second, time.Millisecond, "factory did not capture its token before the source read")
	unlock()
	select {
	case <-h.entered[1]:
	case <-time.After(5 * time.Second):
		t.Fatal("factory did not reach its final publication log")
	}
	// A correct setter can be waiting for this factory's pipelineMu. Observe
	// retirement without requiring the setter to finish across the held gate.
	require.Eventually(t, func() bool {
		if svc.hnswGeneration.Load() > generation {
			return true
		}
		select {
		case <-setterDone:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
	assert.Greater(t, svc.hnswGeneration.Load(), generation, "configuration replacement must retire the factory's token")
	release(1)
	for _, done := range []<-chan struct{}{factoryDone, setterDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("manager/factory worker did not finish after gate release")
		}
	}
	require.NoError(t, factoryErr)
	svc.pipelineMu.RLock()
	cached := svc.vectorPipeline
	svc.pipelineMu.RUnlock()
	assert.Nil(t, cached, "an old-token factory must not survive the setter's final cache barrier")
}

// The completed native graph is real, but no enabled GPU manager or hardware
// selection is involved. This isolates the setter's mutation/publication owner.
func TestGPUManagerChangeSerializesCompletedTransition(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	ctx := context.Background()
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	require.NoError(t, svc.BuildIndexes(ctx))
	node := &storage.Node{ID: "nornic:item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}
	_, err := engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
	manager, err := gpu.NewManager(&gpu.Config{Enabled: false})
	require.NoError(t, err)
	require.False(t, manager.IsEnabled())
	svc.SetGPUManager(manager)
	_, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	graph, err := svc.getOrCreateHNSWIndex(ctx, 3)
	require.NoError(t, err)
	require.NotNil(t, graph)
	// Construction retires the cached descriptor; reacquire the real CPU pipeline.
	_, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	generation := svc.hnswGeneration.Load()

	svc.strategyTransitionMu.Lock()
	var unlockOnce sync.Once
	unlock := func() { unlockOnce.Do(svc.strategyTransitionMu.Unlock) }
	transitionDone, setterDone := make(chan struct{}), make(chan struct{})
	setterStarted := false
	defer func() {
		unlock()
		for _, done := range []<-chan struct{}{transitionDone, setterDone} {
			if done == setterDone && !setterStarted {
				continue
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("manager/transition worker did not join before service cleanup")
			}
		}
	}()
	transitionID := make(chan string, 1)
	go func() {
		transitionID <- serviceTestGoroutineID()
		svc.runStrategyTransition(strategyModeHNSW)
		close(transitionDone)
	}()
	transition := <-transitionID
	require.Eventually(t, func() bool {
		return serviceStackContains(transition, "(*Service).runStrategyTransition", "(*Service).replayTransitionDeltasLocked", "sync.(*Mutex).Lock")
	}, 5*time.Second, time.Millisecond, "wrapper did not reach replay while owning indexMu")
	setterID := make(chan string, 1)
	setterStarted = true
	go func() {
		setterID <- serviceTestGoroutineID()
		svc.SetGPUManager(nil)
		close(setterDone)
	}()
	setter := <-setterID
	require.Eventually(t, func() bool {
		select {
		case <-setterDone:
			return true
		default:
			return serviceStackContains(setter, "(*Service).SetGPUManager", "sync.(*RWMutex).Lock")
		}
	}, 5*time.Second, time.Millisecond, "setter neither finished nor waited for mutation ownership")
	select {
	case <-setterDone:
		assert.Fail(t, "setter returned while a completed transition still owned indexMu")
	default:
	}
	unlock()
	for _, done := range []<-chan struct{}{transitionDone, setterDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("manager/transition worker did not finish after replay release")
		}
	}
	assert.Greater(t, svc.hnswGeneration.Load(), generation)
	svc.pipelineMu.RLock()
	cached := svc.vectorPipeline
	svc.pipelineMu.RUnlock()
	assert.Nil(t, cached, "the setter must retire a wrapper published before its field replacement")
	svc.hnswMu.RLock()
	currentGraph := svc.hnswIndex
	svc.hnswMu.RUnlock()
	assert.Same(t, graph, currentGraph, "backend retirement preserves the equivalent native graph")
}

// These sequential adjacent controls use an actual CPU-backed GPU object and
// an injected descriptor. They do not exercise real enabled-GPU publication.
func TestGPUManagerDisablePreservesCappedCurrentResultsWithoutHardware(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	for _, mode := range []string{"nil", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { require.NoError(t, engine.Close()) })
			svc := NewServiceWithDimensions(engine, 3)
			t.Cleanup(func() { require.NoError(t, svc.Close()) })
			require.NoError(t, svc.BuildIndexes(ctx))
			node := &storage.Node{ID: "nornic:item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
			cfg := gpu.DefaultEmbeddingIndexConfig(3)
			cfg.GPUEnabled, cfg.AutoSync, cfg.InitialCap = false, false, 2
			captured := gpu.NewEmbeddingIndex(nil, cfg)
			require.NoError(t, captured.Add("nornic:item", []float32{1, 0, 0}))
			t.Cleanup(captured.Release)
			svc.indexMu.Lock()
			svc.mu.Lock()
			svc.gpuEmbeddingIndex = captured
			svc.mu.Unlock()
			svc.pipelineMu.Lock()
			svc.vectorPipeline = NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(captured), &IdentityExactScorer{})
			svc.pipelineMu.Unlock()
			svc.indexMu.Unlock()
			var manager *gpu.Manager
			if mode == "disabled" {
				manager, err = gpu.NewManager(&gpu.Config{Enabled: false})
				require.NoError(t, err)
				require.False(t, manager.IsEnabled())
			}
			svc.SetGPUManager(manager)
			svc.mu.RLock()
			currentManager, currentIndex := svc.gpuManager, svc.gpuEmbeddingIndex
			svc.mu.RUnlock()
			assert.Same(t, manager, currentManager)
			assert.Nil(t, currentIndex)
			svc.pipelineMu.RLock()
			cached := svc.vectorPipeline
			svc.pipelineMu.RUnlock()
			assert.Nil(t, cached)
			replacement := &storage.Node{ID: "nornic:item", ChunkEmbeddings: [][]float32{{0, 1, 0}}}
			require.NoError(t, engine.UpdateNode(replacement))
			require.NoError(t, svc.IndexNode(replacement))
			hits, err := svc.VectorSearchCandidates(ctx, []float32{0, 1, 0}, &SearchOptions{Limit: 1, MaxCandidateLimit: 1})
			require.NoError(t, err)
			if assert.Len(t, hits, 1) {
				assert.Equal(t, "nornic:item", hits[0].ID)
				assert.InDelta(t, 1, hits[0].Score, 1e-6)
			}
			assert.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
			oldHits, err := captured.Search([]float32{0, 1, 0}, 1)
			require.NoError(t, err)
			if assert.Len(t, oldHits, 1) {
				assert.InDelta(t, 0, oldHits[0].Score, 1e-6, "captured CPU-backed object was not retargeted")
			}
		})
	}
}
