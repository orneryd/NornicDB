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

func factoryRetirementService(t *testing.T, fileBacked bool) *Service {
	t.Helper()
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
	_, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
	require.NoError(t, err)
	return svc
}

func TestVectorFactorySnapshotRetiredByReset(t *testing.T) {
	for _, brute := range []bool{false, true} {
		for _, fileBacked := range []bool{false, true} {
			mode := "hnsw/memory"
			if brute {
				mode = "brute/memory"
			}
			if fileBacked {
				mode += "/file"
			}
			t.Run(mode, func(t *testing.T) {
				maximum := "1"
				if brute {
					maximum = "10"
				}
				t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", maximum)
				svc := factoryRetirementService(t, fileBacked)
				// Release prepares an optional GPU source before taking pipelineMu.
				// Hold a read lock so its initial cache read/preparation can finish;
				// park only the factory's writer section before gating its source snapshot.
				svc.pipelineMu.RLock()
				var factoryOnce sync.Once
				openFactory := func() { factoryOnce.Do(svc.pipelineMu.RUnlock) }
				defer openFactory()
				type result struct {
					pipeline *VectorSearchPipeline
					err      error
				}
				factoryDone := make(chan result, 1)
				factoryID := make(chan string, 1)
				go func() {
					factoryID <- serviceTestGoroutineID()
					p, err := svc.getOrCreateVectorPipeline(context.Background())
					factoryDone <- result{p, err}
				}()
				id := <-factoryID
				require.Eventually(t, func() bool {
					return serviceStackContains(id, "(*Service).getOrCreateVectorPipeline", "sync.(*RWMutex).Lock")
				}, 5*time.Second, time.Millisecond, "factory did not reach its publication lock")
				// The real factory holds pipelineMu and has captured its generation
				// before blocking in the shared source Count snapshot.
				var unlockCount func()
				if fileBacked {
					svc.vectorFileStore.mu.Lock()
					unlockCount = svc.vectorFileStore.mu.Unlock
				} else {
					svc.vectorIndex.mu.Lock()
					unlockCount = svc.vectorIndex.mu.Unlock
				}
				var countOnce, indexOnce sync.Once
				releaseCount := func() { countOnce.Do(unlockCount) }
				defer releaseCount()
				svc.indexMu.Lock()
				releaseIndex := func() { indexOnce.Do(svc.indexMu.Unlock) }
				defer releaseIndex()
				openFactory()

				require.Eventually(t, func() bool {
					return serviceStackContains(id, "(*Service).getOrCreateVectorPipeline", "(*Service).embeddingCountLocked", "sync.(*RWMutex).RLock")
				}, 5*time.Second, time.Millisecond, "factory did not enter source snapshot")
				resetDone := make(chan struct{})
				resetID := make(chan string, 1)
				go func() {
					resetID <- serviceTestGoroutineID()
					svc.resetANNForBuild()
					close(resetDone)
				}()
				id = <-resetID
				require.Eventually(t, func() bool {
					return serviceStackContains(id, "(*Service).resetANNForBuild", "sync.(*RWMutex).Lock")
				}, 5*time.Second, time.Millisecond, "reset did not retire generation before mutation boundary")
				releaseCount()
				var answer result
				select {
				case answer = <-factoryDone:
				case <-time.After(5 * time.Second):
					t.Fatal("factory waited for reset's mutation boundary")
				}
				svc.pipelineMu.RLock()
				cached := svc.vectorPipeline
				svc.pipelineMu.RUnlock()
				releaseIndex()
				select {
				case <-resetDone:
				case <-time.After(5 * time.Second):
					t.Fatal("reset did not finish after source snapshot")
				}
				require.NoError(t, answer.err)
				require.NotNil(t, answer.pipeline)
				require.Nil(t, cached, "factory cached a snapshot whose generation reset already retired")
				_, readyHNSW := answer.pipeline.candidateGen.(*HNSWCandidateGen)
				require.False(t, readyHNSW, "paused construction returned a retired ready graph")
				svc.pipelineMu.RLock()
				cached = svc.vectorPipeline
				svc.pipelineMu.RUnlock()
				require.Nil(t, cached)
				svc.ClearVectorIndex()
				node := &storage.Node{ID: "replacement", ChunkEmbeddings: [][]float32{{0, 1, 0}}}
				_, err := svc.engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(node))
				candidates, err := svc.VectorSearchCandidates(context.Background(), []float32{0, 1, 0}, &SearchOptions{Limit: 1, MaxCandidateLimit: 1})
				require.NoError(t, err)
				require.Len(t, candidates, 1)
				require.Equal(t, "replacement", candidates[0].ID)
			})
		}
	}
}

func TestVectorFactoryDoesNotCacheChangingSources(t *testing.T) {
	for _, maximum := range []string{"1", "10"} {
		for _, fileBacked := range []bool{false, true} {
			mode := maximum + "/memory"
			if fileBacked {
				mode += "/file"
			}
			t.Run(mode, func(t *testing.T) {
				t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", maximum)
				svc := factoryRetirementService(t, fileBacked)
				svc.hnswVectorsChanging.Store(true)
				defer svc.hnswVectorsChanging.Store(false)
				p, err := svc.getOrCreateVectorPipeline(context.Background())
				require.NoError(t, err)
				require.NotNil(t, p)
				svc.pipelineMu.RLock()
				cached := svc.vectorPipeline
				svc.pipelineMu.RUnlock()
				require.Nil(t, cached, "factory cached a source while base vectors were changing")
				_, readyHNSW := p.candidateGen.(*HNSWCandidateGen)
				require.False(t, readyHNSW)
			})
		}
	}
}
