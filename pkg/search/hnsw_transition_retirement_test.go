package search

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRuntimeStrategyTransitionDoesNotRepublishRetiredGraph(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1")
	for _, operation := range []string{"clear", "reset-for-build"} {
		for _, fileBacked := range []bool{false, true} {
			mode := "memory"
			if fileBacked {
				mode = "file"
			}
			t.Run(operation+"/"+mode, func(t *testing.T) {
				svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
				t.Cleanup(func() { _ = svc.Close() })
				if fileBacked {
					vfs, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), 3)
					require.NoError(t, err)
					svc.vectorFileStore = vfs
					require.NoError(t, vfs.Add("item", []float32{1, 0, 0}))
				} else {
					require.NoError(t, svc.vectorIndex.Add("item", []float32{1, 0, 0}))
				}
				graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
				require.NoError(t, err)
				require.NotNil(t, graph)
				svc.pipelineMu.Lock()
				if fileBacked {
					svc.vectorPipeline = NewVectorSearchPipeline(NewFileStoreBruteForceCandidateGen(svc.vectorFileStore), NewCPUExactScorer(svc.vectorFileStore))
				} else {
					svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))
				}
				svc.pipelineMu.Unlock()
				svc.indexMu.Lock()
				var once sync.Once
				release := func() { once.Do(svc.indexMu.Unlock) }
				defer release()
				done := make(chan struct{})
				id := make(chan string, 1)
				go func() {
					id <- serviceTestGoroutineID()
					svc.runStrategyTransition(strategyModeHNSW)
					close(done)
				}()
				transitionID := <-id
				// Gate the completed wrapper at mutation admission. Retirement
				// cancels its generation before waiting for this same mutex.
				require.Eventually(t, func() bool {
					return serviceStackContains(transitionID, "(*Service).runStrategyTransition", "sync.(*RWMutex).Lock")
				}, 5*time.Second, time.Millisecond, "transition did not reach post-build mutation gate")
				generation := svc.hnswGeneration.Load()
				retired := make(chan struct{})
				go func() {
					if operation == "clear" {
						svc.ClearVectorIndex()
					} else {
						svc.resetANNForBuild()
					}
					close(retired)
				}()
				require.Eventually(t, func() bool { return svc.hnswGeneration.Load() != generation }, 5*time.Second, time.Millisecond)
				release()
				select {
				case <-retired:
				case <-time.After(5 * time.Second):
					t.Fatal("retirement did not finish")
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("retired transition did not stop")
				}
				svc.hnswMu.RLock()
				installed := svc.hnswIndex
				svc.hnswMu.RUnlock()
				svc.pipelineMu.RLock()
				pipeline := cachedVectorPipeline(svc)
				svc.pipelineMu.RUnlock()
				if installed != nil {
					t.Error("transition republished a graph captured before retirement")
				}
				if pipeline != nil {
					t.Error("transition cached a pipeline captured before retirement")
				}
				if operation == "clear" {
					node := &storage.Node{ID: "replacement", ChunkEmbeddings: [][]float32{{0, 1, 0}}}
					_, err = svc.engine.CreateNode(node)
					require.NoError(t, err)
					require.NoError(t, svc.IndexNode(node))
					results, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 1, 0}, nil)
					require.NoError(t, err)
					require.Len(t, results, 1, "replacement vector was hidden by a retired scorer")
					require.Equal(t, "replacement", results[0].ID)
				}
			})
		}
	}
}
