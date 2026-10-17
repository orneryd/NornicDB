package search

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// A query serves from the cluster generation it took at its start: a
// retirement or a newer publication while it runs changes neither its
// routing, its graphs nor its population, and the next query sees the new
// state. Pipelines hold no generation.

func cgHits(t *testing.T, svc *Service, vec []float32, id string) bool {
	t.Helper()
	hits, err := svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 100}, nil)
	require.NoError(t, err)
	for _, h := range hits {
		if h.ID == id {
			return true
		}
	}
	return false
}

// A CPU/GPU mode change retires the generation and replaces the index
// (release review: EnableClustering kept a pipeline routing on the replaced
// index). A query in flight completes on its generation; the cached
// clustered pipeline is dropped before the revision advances; the next
// search and a following live insert answer from the new state, with no
// TriggerClustering in between.
func TestClusterGenerationModeChangeDuringQuery(t *testing.T) {
	f := newCGFixture(t, 20, false)
	vec, ok := f.svc.getVectorLookup()("n-0005")
	require.True(t, ok)
	ctx, pipeline, release, err := f.svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	require.IsType(t, &IVFHNSWCandidateGen{}, pipeline.candidateGen)
	revision := f.svc.indexRevision.Load()

	cgSwitchClusteringMode(f.svc)

	candidates, err := pipeline.candidateGen.SearchCandidates(ctx, vec, 5, -1)
	require.NoError(t, err, "the in-flight query finishes on the generation it took")
	require.NotEmpty(t, candidates)
	require.Equal(t, "n-0005", candidates[0].ID)
	release()

	f.svc.pipelineMu.RLock()
	cached := f.svc.vectorPipeline
	f.svc.pipelineMu.RUnlock()
	require.Nil(t, cached, "the mode change drops the cached clustered pipeline")
	require.Greater(t, f.svc.indexRevision.Load(), revision)
	require.True(t, cgHits(t, f.svc, vec, "n-0005"), "the next search answers from the new state")
	_, next, releaseNext, err := f.svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	releaseNext()
	require.Nil(t, cgGeneratorServing(next), "nothing is clustered after the mode change until clustering runs")

	id := f.add(2, "gamma live")
	live, ok := f.svc.getVectorLookup()(id)
	require.True(t, ok)
	require.True(t, cgHits(t, f.svc, live, id), "a live insert after the mode change is found")
}

// A clustered pipeline cached after its generation was retired is not used:
// the query resolves the current pipeline instead of erroring.
func TestClusterGenerationStaleClusteredPipelineIsReplaced(t *testing.T) {
	f := newCGFixture(t, 20, false)
	vec, _ := f.svc.getVectorLookup()("n-0007")
	stale, err := f.svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	require.NoError(t, f.svc.ClearVectorIndex())
	f.add(1, "beta after clear")
	f.svc.pipelineMu.Lock()
	f.svc.vectorPipeline = stale // as if built just before the retirement
	f.svc.pipelineMu.Unlock()

	_, pipeline, release, err := f.svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	release()
	require.NotSame(t, stale, pipeline)
	_, err = f.svc.VectorQueryNodesWithOptions(context.Background(), vec, VectorQuerySpec{Limit: 10}, nil)
	require.NoError(t, err)
}

// A retirement during a query: the query completes with the correct result
// on the retired generation, and the next query sees the retirement.
func TestClusterGenerationRetireDuringQuery(t *testing.T) {
	f := newCGFixture(t, 20, false)
	vec, _ := f.svc.getVectorLookup()("n-0011")
	ctx, pipeline, release, err := f.svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	f.svc.resetANNForBuild() // retires and replaces the index with an empty one
	candidates, err := pipeline.candidateGen.SearchCandidates(ctx, vec, 5, -1)
	require.NoError(t, err)
	require.Equal(t, "n-0011", candidates[0].ID)
	release()
	_, gen, done := f.svc.acquireClusterSnapshot(context.Background())
	done()
	require.Nil(t, gen, "the next query holds no retired generation")
}

// Adaptive overfetch runs the generator several times per query; a newer
// generation published in between (here K grows from 2 to 4) does not reach
// the query's later rounds: every round routes on the generation it started
// with, with that generation's graphs.
func TestClusterGenerationQueryRoundsSpanRepublication(t *testing.T) {
	f := newCGFixture(t, 20, false)
	// Publish a 2-cluster generation (restored from two of the graphs).
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, SaveIVFHNSW(path, map[int]*HNSWIndex{0: f.svc.clusterGen.graphs[0], 1: f.svc.clusterGen.graphs[1]}))
	f.svc.SetHNSWIndexPath(path)
	f.svc.SetPersistenceEnabled(true)
	require.True(t, f.svc.tryRestoreClusteredWarmupFromDisk(context.Background(), f.svc.clusterIndex, path, nil))
	require.Equal(t, 2, f.svc.clusterIndex.NumClusters())

	vec, _ := f.svc.getVectorLookup()("n-0061")
	ctx, pipeline, release, err := f.svc.vectorPipelineForQuery(context.Background())
	require.NoError(t, err)
	defer release()
	_, started, done := f.svc.clusterGenerationFor(ctx)
	done()
	require.Equal(t, 2, started.routing.NumClusters())

	for round, limit := range []int{5, 20, 80} {
		if round == 1 {
			f.recluster() // publishes a 4-cluster generation on the same index
			require.Equal(t, 4, f.svc.clusterIndex.NumClusters())
		}
		_, view, viewDone := f.svc.clusterQueryView(ctx)
		viewDone()
		require.Same(t, started.routing, view.routing, "round %d routes on the generation the query started with", round)
		candidates, err := pipeline.candidateGen.SearchCandidates(ctx, vec, limit, -1)
		require.NoError(t, err, "round %d", round)
		require.Equal(t, "n-0061", candidates[0].ID, "round %d", round)
	}
	require.Equal(t, 2, started.routing.NumClusters(), "a superseded generation's routing is left as it was")
	_, latest, latestDone := f.svc.acquireClusterSnapshot(context.Background())
	latestDone()
	require.Equal(t, 4, latest.routing.NumClusters(), "the next query takes the newer generation")
}

// A retired index's GPU buffers are released only after the last query
// holding one of its generations finishes: GPU subset scoring reads the
// device without the index's lock, so an earlier release would free memory
// in use.
func TestClusterGenerationRetiredIndexReleasedAfterHolders(t *testing.T) {
	f := newCGFixture(t, 20, false)
	var released atomic.Pointer[gpu.ClusterIndex]
	clusterIndexReleaseHook = func(ci *gpu.ClusterIndex) { released.Store(ci) }
	t.Cleanup(func() { clusterIndexReleaseHook = nil })

	old := f.svc.clusterIndex
	_, gen, release := f.svc.acquireClusterSnapshot(context.Background())
	require.NotNil(t, gen)
	cgSwitchClusteringMode(f.svc)
	require.NotSame(t, old, f.svc.clusterIndex)
	require.Never(t, func() bool { return released.Load() == old }, 200*time.Millisecond, 10*time.Millisecond,
		"a held generation's index is not released")
	release()
	require.Eventually(t, func() bool { return released.Load() == old }, 5*time.Second, 10*time.Millisecond,
		"the index is released once its last holder finishes")

	// Without holders the retired index is released at once.
	replaced := f.svc.clusterIndex
	cgSwitchClusteringMode(f.svc)
	require.Eventually(t, func() bool { return released.Load() == replaced }, 5*time.Second, 10*time.Millisecond)
}
