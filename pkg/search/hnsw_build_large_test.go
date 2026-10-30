package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// The resident build corpus holds every inserted vector. Above 2^30
// float32 elements (4 GiB; 1,048,576 vectors at 1,024 dimensions) buffer
// writes panicked with "slice bounds out of range" (pkg/gpu/metal). These
// tests allocate several GiB and require explicit backend qualification.
type largeGPUAccelerator interface {
	HNSWBuildAccelerator
	HNSWResidentBuildAccelerator
}

const (
	largeGPUTestDim       = 1024
	largeGPUBoundaryNodes = (1 << 30) / largeGPUTestDim
)

// largeGPUTestVector returns vector i of a clustered corpus: one of 4,096
// unit centroids plus Gaussian noise of norm ~0.35, normalized. It is a pure
// function of i so tests can regenerate any vector without holding the corpus.
// Per-dimension noise is scaled by 1/sqrt(dim): unscaled noise would swamp the
// centroids and make the corpus effectively random, which no HNSW build
// searches well.
func largeGPUTestVector(i int, centroids [][]float32) []float32 {
	rng := rand.New(rand.NewSource(int64(i) + 1))
	c := centroids[i%len(centroids)]
	noise := float32(0.35 / math.Sqrt(largeGPUTestDim))
	v := make([]float32, largeGPUTestDim)
	for d := range v {
		v[d] = c[d] + noise*float32(rng.NormFloat64())
	}
	return vector.Normalize(v)
}

func largeGPUTestCentroids() [][]float32 {
	rng := rand.New(rand.NewSource(20261019))
	centroids := make([][]float32, 4096)
	for i := range centroids {
		c := make([]float32, largeGPUTestDim)
		for d := range c {
			c[d] = float32(rng.NormFloat64())
		}
		centroids[i] = vector.Normalize(c)
	}
	return centroids
}

// largeGPUProbeIndexes are vectors on both sides of the 4 GiB boundary.
func largeGPUProbeIndexes(total int) []int {
	return []int{0, 1, largeGPUBoundaryNodes - 1, largeGPUBoundaryNodes, largeGPUBoundaryNodes + 1, largeGPUBoundaryNodes + 2047, total - 1}
}

// checkLargeGPUResidentBuildBeyondFourGiB grows resident storage past 4 GiB
// through AppendInserted and checks that GPU scoring finds vectors stored on
// both sides of the boundary.
func checkLargeGPUResidentBuildBeyondFourGiB(t *testing.T, accel largeGPUAccelerator) {

	total := largeGPUBoundaryNodes + 8192
	// Metal reserves one growing buffer; CUDA/Vulkan retain insertion shards.
	require.NoError(t, accel.Prepare(largeGPUTestDim, largeGPUBoundaryNodes))
	require.True(t, accel.ResidentReady(), "GPU resident storage is not ready")

	centroids := largeGPUTestCentroids()
	const batch = 2048
	vecs := make([][]float32, 0, batch)
	for i := 0; i < total; i++ {
		vecs = append(vecs, largeGPUTestVector(i, centroids))
		if len(vecs) == batch || i == total-1 {
			require.NoError(t, accel.AppendInserted(vecs), "append ending at vector %d", i)
			vecs = vecs[:0]
		}
	}
	t.Logf("uploaded %d vectors: %d bytes across resident storage", total, uint64(total)*largeGPUTestDim*4)

	probes := largeGPUProbeIndexes(total)
	queries := make([][]float32, len(probes))
	for i, p := range probes {
		queries[i] = largeGPUTestVector(p, centroids)
	}
	ids, dists, err := accel.CandidateSearchInserted(context.Background(), queries, 10)
	require.NoError(t, err)
	for i, p := range probes {
		require.NotEmpty(t, ids[i], "probe %d returned no candidates", p)
		require.Equal(t, uint32(p), ids[i][0], "probe %d: top candidate", p)
		require.InDelta(t, 0, dists[i][0], 1e-3, "probe %d: self distance", p)
	}
}

// checkLargeGPUHNSWBuildBeyondFourGiB runs the startup build path on a corpus
// the size of the reporting database, and checks the requested GPU stayed active.
// The
// index finds vectors stored past the 4 GiB boundary.
func checkLargeGPUHNSWBuildBeyondFourGiB(t *testing.T, accel largeGPUAccelerator, backend string) {
	checkGPUHNSWBuild(t, accel, backend, 1_101_077)
}

func checkGPUHNSWBuild(t *testing.T, accel largeGPUAccelerator, backend string, total int) {
	centroids := largeGPUTestCentroids()
	if total < largeGPUBoundaryNodes {
		centroids = centroids[:64]
	}
	pairs := make([]hnswBuildPair, total)
	for i := range pairs {
		pairs[i] = hnswBuildPair{id: fmt.Sprintf("v%07d", i), vec: largeGPUTestVector(i, centroids)}
	}

	config := DefaultHNSWConfig()
	config.EfConstruction = 100 // the "fast" profile the reporting server used
	config.EfSearch = 50
	started := time.Now()
	built, stats, err := buildHNSWWithOptionalGPU(context.Background(), largeGPUTestDim, config, nil, total, hnswPairSliceIterator(pairs), accel)
	require.NoError(t, err)
	t.Logf("built %d vectors in %s: strategy=%s backend=%s batches=%d kernel_errors=%d fallback=%v",
		built.Size(), time.Since(started).Round(time.Second), stats.Strategy, stats.Backend, stats.Batches, stats.KernelErrors, stats.Fallback)
	require.False(t, stats.Fallback, "build fell back to the CPU")
	require.Zero(t, stats.KernelErrors)
	require.True(t, strings.Contains(strings.ToLower(stats.Backend), strings.ToLower(backend)), "backend %q", stats.Backend)
	require.Equal(t, total, built.Size())

	probes := []int{0, total - 1}
	if total > largeGPUBoundaryNodes+2047 {
		probes = largeGPUProbeIndexes(total)
	}
	rng := rand.New(rand.NewSource(7))
	for len(probes) < 200 {
		probes = append(probes, rng.Intn(total))
	}
	found, foundPastBoundary, pastBoundary := 0, 0, 0
	for _, p := range probes {
		results, err := built.Search(context.Background(), pairs[p].vec, 10, -1)
		require.NoError(t, err)
		hit := false
		for _, r := range results {
			if r.ID == pairs[p].id {
				hit = true
				break
			}
		}
		if p >= largeGPUBoundaryNodes {
			pastBoundary++
		}
		if hit {
			found++
			if p >= largeGPUBoundaryNodes {
				foundPastBoundary++
			}
		}
	}
	t.Logf("self-recall@10: %d/%d overall, %d/%d past the 4 GiB boundary", found, len(probes), foundPastBoundary, pastBoundary)
	require.GreaterOrEqual(t, float64(found)/float64(len(probes)), 0.95)
	if total > largeGPUBoundaryNodes {
		require.Positive(t, pastBoundary)
		require.GreaterOrEqual(t, float64(foundPastBoundary)/float64(pastBoundary), 0.95, "vectors stored past 4 GiB")
	}
}
