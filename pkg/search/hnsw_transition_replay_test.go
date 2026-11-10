package search

// Author: hnsw-stall implementation lane (Claude Opus 5.5), 9 Nov 2026.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// With runtime strategy transitions on, a transition to a graph that already
// serves does not replay its change log into it: every logged change has
// reached that graph already, and the replay re-inserted each one (a new slot
// and a tombstone per change) while every writer waited.
func TestRuntimeTransitionDoesNotReplayIntoServingGraph(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	engine := newNamespacedEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	vectors := map[string][]float32{}
	put := func(id string, vec []float32) {
		t.Helper()
		node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{vec}}
		if _, err := engine.GetNode(t.Context(), node.ID); err == nil {
			require.NoError(t, engine.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
		} else {
			_, err := engine.CreateNode(t.Context(), node)
			require.NoError(t, err)
		}
		require.NoError(t, svc.IndexNode(node))
		vectors[id] = vec
	}
	for i := 0; i < 30; i++ {
		put(fmt.Sprintf("v-%02d", i), []float32{float32(i%4) + 1, float32(i%3) - 1, float32(i)})
	}
	graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	svc.runtimeStrategyTransitions.Store(true)
	// The launcher reserves this before the transition starts; changes from
	// here on are logged for it and reach the serving graph live.
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()
	for i := 0; i < 10; i++ {
		put(fmt.Sprintf("v-%02d", i), []float32{-float32(i) - 1, 2, float32(i % 2)})
	}
	graph.mu.RLock()
	slots := len(graph.nodeLevel)
	graph.mu.RUnlock()
	svc.runStrategyTransition(strategyModeHNSW)
	require.Equal(t, strategyModeHNSW, svc.currentPipelineStrategy())
	svc.hnswMu.RLock()
	serving := svc.servingHNSW()
	svc.hnswMu.RUnlock()
	require.Same(t, graph, serving)
	graph.mu.RLock()
	after := len(graph.nodeLevel)
	graph.mu.RUnlock()
	require.Equal(t, slots, after, "the transition re-inserted changes the serving graph already held")
	svc.strategyTransitionMu.Lock()
	logged := len(svc.strategyTransitionDeltas)
	svc.strategyTransitionMu.Unlock()
	require.Zero(t, logged, "the transition left changes in its log")
	// The serving graph holds exactly the keys written, each with its own
	// current vector.
	require.NoError(t, hnswGraphMismatch(graph, vectors))
}
