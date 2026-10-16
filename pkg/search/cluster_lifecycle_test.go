package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// clusteredService returns a memory service with two separated vector groups,
// clustered on CPU, so the standard resolver selects cluster routing.
func clusteredService(t *testing.T) *Service {
	t.Helper()
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	t.Setenv("NORNICDB_KMEANS_NUM_CLUSTERS", "")
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	svc.SetMinEmbeddingsForClustering(4)
	for i := 0; i < 8; i++ {
		require.NoError(t, svc.vectorIndex.Add(fmt.Sprintf("a%d", i), []float32{1, float32(i) * 0.01, 0}))
		require.NoError(t, svc.vectorIndex.Add(fmt.Sprintf("b%d", i), []float32{0, 1, float32(i) * 0.01}))
	}
	svc.EnableClustering(nil, 2)
	require.NoError(t, svc.TriggerClustering(context.Background()))
	clusterIndex, _ := svc.clusterState()
	require.True(t, clusterIndex.IsClustered())
	return svc
}

func TestClusterReplacementRetiresClusteredDescriptors(t *testing.T) {
	svc := clusteredService(t)
	ctx := context.Background()
	old, oldGeneration := svc.clusterState()

	pipeline, err := svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	gen, ok := pipeline.candidateGen.(*KMeansCandidateGen)
	require.True(t, ok, "cluster routing selected: %T", pipeline.candidateGen)
	require.Same(t, old, gen.clusterIndex)
	svc.pipelineMu.RLock()
	require.Same(t, pipeline, cachedVectorPipeline(svc), "the descriptor is cached")
	svc.pipelineMu.RUnlock()
	svc.clusterHNSWMu.Lock()
	svc.clusterHNSW = map[int]*HNSWIndex{0: NewHNSWIndex(3, DefaultHNSWConfig())}
	svc.clusterHNSWMu.Unlock()

	// The DB replaces the cluster object when the GPU manager changes after
	// SetGPUManager has already retired its own descriptors.
	svc.mu.Lock()
	svc.clusterUsesGPU = true
	svc.mu.Unlock()
	svc.EnableClustering(nil, 2)

	replacement, generation := svc.clusterState()
	require.NotSame(t, old, replacement)
	require.NotEqual(t, oldGeneration, generation)
	svc.pipelineMu.RLock()
	cached := cachedVectorPipeline(svc)
	svc.pipelineMu.RUnlock()
	require.Nil(t, cached, "a descriptor routing through the retired cluster object stayed cached")
	svc.clusterHNSWMu.RLock()
	require.Nil(t, svc.clusterHNSW, "graphs built from the retired cluster object stayed published")
	svc.clusterHNSWMu.RUnlock()

	// Re-clustering the replacement routes through it, selector included.
	require.NoError(t, svc.TriggerClustering(ctx))
	pipeline, err = svc.getOrCreateVectorPipeline(ctx)
	require.NoError(t, err)
	gen, ok = pipeline.candidateGen.(*KMeansCandidateGen)
	require.True(t, ok)
	require.Same(t, replacement, gen.clusterIndex)
	candidates, err := svc.VectorSearchCandidates(ctx, []float32{1, 0, 0}, &SearchOptions{Limit: 3})
	require.NoError(t, err)
	require.NotEmpty(t, candidates)
	require.Equal(t, "a0", candidates[0].ID)
}

func TestClusterGraphsFromRetiredGenerationAreNotPublished(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_IVF_HNSW_MIN_CLUSTER_SIZE", "1")
	svc := clusteredService(t)
	ctx := context.Background()
	clusterIndex, generation := svc.clusterState()

	// Retirement during construction: the build must not publish afterwards.
	svc.resetANNForBuild()
	require.NoError(t, svc.rebuildClusterHNSWIndexes(ctx, clusterIndex, generation))
	svc.clusterHNSWMu.RLock()
	require.Nil(t, svc.clusterHNSW, "graphs from a retired generation were published")
	svc.clusterHNSWMu.RUnlock()

	_, current := svc.clusterState()
	require.NoError(t, svc.rebuildClusterHNSWIndexes(ctx, clusterIndex, current))
	svc.clusterHNSWMu.RLock()
	require.NotEmpty(t, svc.clusterHNSW, "the current generation publishes")
	svc.clusterHNSWMu.RUnlock()
}

func TestVectorTeardownJoinsClusteringFromAnyContext(t *testing.T) {
	for _, operation := range []string{"clear", "close"} {
		t.Run(operation, func(t *testing.T) {
			svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
			if operation == "clear" {
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
			}
			// A DB timer's context outlives a per-database reset or drop.
			runCtx, finish, ok := svc.runWorker(context.Background(), workerClustering)
			require.True(t, ok)
			var finished bool
			defer func() {
				if !finished {
					finish()
				}
			}()

			done := make(chan struct{})
			go func() {
				if operation == "clear" {
					svc.ClearVectorIndex()
				} else {
					_ = svc.Close()
				}
				close(done)
			}()
			select {
			case <-runCtx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("teardown did not cancel clustering started with a background context")
			}
			select {
			case <-done:
				t.Fatal("teardown returned while clustering still owned the population")
			case <-time.After(50 * time.Millisecond):
			}
			finish()
			finished = true
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("teardown did not finish after clustering returned")
			}

			_, again, ok := svc.runWorker(context.Background(), workerClustering)
			if operation == "close" {
				require.False(t, ok, "a closed service admitted clustering")
				require.ErrorIs(t, svc.TriggerClustering(context.Background()), context.Canceled)
				return
			}
			require.True(t, ok, "clustering is admitted again after Clear")
			again()
		})
	}
}
