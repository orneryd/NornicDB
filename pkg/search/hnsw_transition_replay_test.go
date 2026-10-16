package search

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRuntimeStrategyTransitionRejectsRetiredReplayBeforeMutatingLiveGraph(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100")
	svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
	t.Cleanup(func() { _ = svc.Close() })
	svc.SetVectorIndexPath(filepath.Join(t.TempDir(), "vectors"))
	svc.SetPersistenceEnabled(true)
	sourceRetirementNode(t, svc, "anchor", []float32{1, 0, 0})
	sourceRetirementNode(t, svc, "item", []float32{1, 0, 0})
	graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	_, err = svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.Equal(t, strategyModeBruteCPU, svc.currentPipelineStrategy())
	// Reserve the wrapper as the real launcher does, before running it. Record
	// a delete/reinsert through public mutation so the shared graph is current.
	svc.strategyTransitionMu.Lock()
	svc.strategyTransitionInProgress = true
	svc.strategyTransitionMu.Unlock()
	require.NoError(t, svc.RemoveNode("item"))
	require.NoError(t, svc.IndexNode(&storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{0, 1, 0}}}))
	require.Equal(t, 2, graph.Size())
	svc.strategyTransitionMu.Lock()
	var strategyOnce, mutationOnce sync.Once
	releaseStrategy := func() { strategyOnce.Do(svc.strategyTransitionMu.Unlock) }
	defer releaseStrategy()
	svc.indexMu.Lock()
	releaseMutation := func() { mutationOnce.Do(svc.indexMu.Unlock) }
	defer releaseMutation()
	done := make(chan struct{})
	id := make(chan string, 1)
	go func() {
		id <- serviceTestGoroutineID()
		svc.runStrategyTransition(strategyModeHNSW)
		close(done)
	}()
	wrapperID := <-id
	// Before the repair the wrapper waits at the early replay's log lock;
	// afterward it waits at mutation admission. Both have captured old sources.
	require.Eventually(t, func() bool {
		return serviceStackContains(wrapperID, "(*Service).runStrategyTransition", "sync.(*Mutex).Lock") ||
			serviceStackContains(wrapperID, "(*Service).runStrategyTransition", "sync.(*RWMutex).Lock")
	}, 5*time.Second, time.Millisecond)
	generation := svc.hnswGeneration.Load()
	moved := make(chan struct{})
	go func() { svc.SetPersistenceEnabled(false); close(moved) }()
	// The live move retires admission before waiting on indexMu. Thus even if
	// the wrapper wins that mutex next, its token already rejects the replay.
	require.Eventually(t, func() bool { return svc.hnswGeneration.Load() != generation }, 5*time.Second, time.Millisecond)
	releaseMutation()
	select {
	case <-moved:
	case <-time.After(5 * time.Second):
		t.Fatal("live storage move did not finish")
	}
	require.Nil(t, svc.vectorFileStore)
	svc.hnswMu.RLock()
	installed := svc.hnswIndex
	svc.hnswMu.RUnlock()
	require.Same(t, graph, installed, "equivalent storage move retains the ready graph")
	require.Equal(t, 2, graph.Size())
	releaseStrategy()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retired wrapper did not finish")
	}
	// A retired file lookup cannot replay the add after the delete. The token
	// check must precede BOTH, preserving the canonical graph's live membership.
	assert.Equal(t, 2, graph.Size(), "retired wrapper mutated the retained live graph")
	results, err := graph.Search(context.Background(), []float32{0, 1, 0}, 2, 0)
	require.NoError(t, err)
	assert.Contains(t, results, ANNResult{ID: "item", Score: 1})
	// The public CPU-exact pipeline remains a correctness control in this
	// fixture, not evidence of a public-query counterexample.
	public, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 1, 0}, &SearchOptions{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, public)
	require.Equal(t, "item", public[0].ID)
}
