package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func testVec4(i int) []float32 {
	return []float32{
		float32((i%7)+1) / 7.0,
		float32((i%11)+1) / 11.0,
		float32((i%13)+1) / 13.0,
		float32((i%17)+1) / 17.0,
	}
}

func indexTestNode(t *testing.T, svc *Service, id string, vec []float32) {
	t.Helper()
	node := &storage.Node{
		ID:              storage.NodeID(id),
		Labels:          []string{"Doc"},
		ChunkEmbeddings: [][]float32{vec},
		Properties: map[string]any{
			"content": id,
		},
	}
	_, err := svc.engine.CreateNode(node)
	require.NoError(t, err)
	require.NoError(t, svc.IndexNode(node))
}

func waitForStrategy(t *testing.T, svc *Service, want strategyMode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if svc.currentPipelineStrategy() == want {
			svc.strategyTransitionMu.Lock()
			inProgress := svc.strategyTransitionInProgress
			svc.strategyTransitionMu.Unlock()
			if !inProgress {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, want, svc.currentPipelineStrategy())
}

func TestRuntimeStrategy_DefaultsToHNSW(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	require.NotNil(t, svc)

	_, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())
}

func TestRuntimeStrategy_CPUBruteForceThresholdOptIn(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "10")
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	require.NotNil(t, svc)

	_, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
}

func TestSwitchBruteStrategyFromGPUToCPUWithoutHardware(t *testing.T) {
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 4)
	svc.vectorPipeline = NewVectorSearchPipeline(NewGPUBruteForceCandidateGen(nil), &IdentityExactScorer{})
	svc.hnswIndex = NewHNSWIndex(4, DefaultHNSWConfig())

	require.False(t, svc.switchBruteStrategy(strategyModeBruteGPU), "switching to the active strategy must be a no-op")
	require.True(t, svc.switchBruteStrategy(strategyModeBruteCPU))
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	require.Nil(t, svc.hnswIndex)

	svc.pipelineMu.RLock()
	pipeline := svc.vectorPipeline
	svc.pipelineMu.RUnlock()
	require.IsType(t, &BruteForceCandidateGen{}, pipeline.candidateGen)
	require.IsType(t, &CPUExactScorer{}, pipeline.exactScorer)
	require.False(t, svc.switchBruteStrategy(strategyModeBruteGPU), "CPU-to-GPU must fail cleanly without a synchronized GPU index")
}

func TestRuntimeStrategyTransition_DisabledByDefaultDoesNotScheduleBuild(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	require.False(t, svc.RuntimeStrategyTransitionsEnabled())

	_, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())

	for i := 0; i < 8; i++ {
		indexTestNode(t, svc, fmt.Sprintf("node-%d", i), testVec4(i))
	}

	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())
	svc.strategyTransitionMu.Lock()
	defer svc.strategyTransitionMu.Unlock()
	require.Equal(t, uint64(0), svc.strategyTransitionStarts)
	require.Nil(t, svc.strategyTransitionTimer)
	require.False(t, svc.strategyTransitionInProgress)
}

func TestRuntimeStrategy_HNSWDefaultIndexesLiveWrites(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	require.NotNil(t, svc)

	_, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())

	for i := 0; i < 8; i++ {
		indexTestNode(t, svc, fmt.Sprintf("seed-%d", i), testVec4(i))
	}
	indexTestNode(t, svc, "late-node", []float32{0, 1, 0, 0})

	opts := DefaultSearchOptions()
	opts.Limit = 20
	require.Eventually(t, func() bool {
		resp, err := svc.vectorSearchOnly(context.Background(), []float32{0, 1, 0, 0}, opts)
		if err != nil {
			return false
		}
		for _, r := range resp.Results {
			if string(r.NodeID) == "late-node" {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "late-node must be searchable after transition replay")
}

func TestRuntimeStrategyTransition_CPUToGPUBruteNoRebuild(t *testing.T) {
	cfg := gpu.DefaultConfig()
	cfg.Enabled = true
	cfg.FallbackOnError = true
	manager, err := gpu.NewManager(cfg)
	require.NoError(t, err)
	if !manager.IsEnabled() {
		t.Skip("GPU not available in this environment")
	}

	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	svc.SetRuntimeStrategyTransitionsEnabled(true)
	svc.SetGPUManager(manager)

	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)

	for i := 0; i < 6000; i++ {
		indexTestNode(t, svc, fmt.Sprintf("gpu-node-%d", i), testVec4(i))
	}

	waitForStrategy(t, svc, strategyModeBruteGPU, 45*time.Second)
	svc.hnswMu.RLock()
	defer svc.hnswMu.RUnlock()
	require.Nil(t, svc.hnswIndex)
}

func TestRuntimeStrategyTransition_ReplayAndClearDeltaLog(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)

	vi := svc.vectorIndex
	require.NotNil(t, vi)
	require.NoError(t, vi.Add("delta-a", []float32{1, 0, 0, 0}))
	require.NoError(t, vi.Add("delta-b", []float32{0, 1, 0, 0}))

	target := NewHNSWIndex(4, HNSWConfigFromEnv())
	require.NotNil(t, target)

	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()

	svc.appendStrategyDelta("delta-a", true)
	svc.appendStrategyDelta("delta-b", true)

	last := svc.replayTransitionDeltas(target, strategyModeHNSW, vi, nil, 0)
	require.Greater(t, last, uint64(0))

	results, err := target.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 0.0)
	require.NoError(t, err)
	foundA := false
	for _, r := range results {
		if r.ID == "delta-a" {
			foundA = true
			break
		}
	}
	require.True(t, foundA, "added delta should be replayed into HNSW target")

	// Replay only deltas after the prior sequence and validate remove path.
	svc.appendStrategyDelta("delta-a", false)
	next := svc.replayTransitionDeltas(target, strategyModeHNSW, vi, nil, last)
	require.Greater(t, next, last)

	svc.clearTransitionDeltaLogLocked(next)
	results, err = target.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 0.0)
	require.NoError(t, err)
	foundA = false
	for _, r := range results {
		if r.ID == "delta-a" {
			foundA = true
			break
		}
	}
	require.False(t, foundA, "remove delta should be replayed into HNSW target")

	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = false
	svc.strategyTransitionMu.Unlock()
}

func TestRuntimeStrategyTransition_ReplayHelpers_BruteGPUAndClear(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewServiceWithDimensions(engine, 4)
	vi := svc.vectorIndex
	require.NotNil(t, vi)
	require.NoError(t, vi.Add("gpu-delta", []float32{0, 1, 0, 0}))

	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()

	svc.appendStrategyDelta("gpu-delta", true)
	svc.appendStrategyDelta("gpu-delta", false)

	// BruteGPU branch with nil gpuEmbeddingIndex should still be a no-op without panic.
	last := svc.replayTransitionDeltas(nil, strategyModeBruteGPU, vi, nil, 0)
	require.Greater(t, last, uint64(0))

	// applied == 0 clears full log branch.
	svc.clearTransitionDeltaLogLocked(0)
	svc.strategyTransitionMu.Lock()
	require.Len(t, svc.strategyTransitionDeltas, 0)
	svc.strategyTransitionMu.Unlock()

	// selective clear keeps only seq > applied branch.
	svc.appendStrategyDelta("gpu-delta", true)
	svc.appendStrategyDelta("gpu-delta", false)
	svc.strategyTransitionMu.Lock()
	require.Len(t, svc.strategyTransitionDeltas, 2)
	firstSeq := svc.strategyTransitionDeltas[0].seq
	secondSeq := svc.strategyTransitionDeltas[1].seq
	svc.strategyTransitionMu.Unlock()

	svc.clearTransitionDeltaLogLocked(firstSeq)
	svc.strategyTransitionMu.Lock()
	require.Len(t, svc.strategyTransitionDeltas, 1)
	require.Equal(t, secondSeq, svc.strategyTransitionDeltas[0].seq)
	svc.strategyTransitionInProgress = false
	svc.strategyTransitionMu.Unlock()
}

func TestHNSWSearch_ExcludesDeletedEntryPointFromResults(t *testing.T) {
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	require.NotNil(t, idx)
	require.NoError(t, idx.Add("a", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0, 0}))

	idx.mu.Lock()
	deletedEntryID := idx.entryPoint
	deletedID := idx.internalToID[deletedEntryID]
	idx.deleted[deletedEntryID] = true
	idx.liveCount--
	idx.mu.Unlock()

	results, err := idx.Search(context.Background(), []float32{1, 0, 0, 0}, 10, 0.0)
	require.NoError(t, err)
	for _, result := range results {
		require.NotEqual(t, deletedID, result.ID)
	}
}
