//go:build darwin && cgo && !nometal

package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/stretchr/testify/require"
)

// The resident Metal build buffer holds every inserted vector. Above 2^30
// float32 elements (4 GiB; 1,048,576 vectors at 1,024 dimensions) buffer
// writes panicked with "slice bounds out of range" (pkg/gpu/metal). These
// tests allocate several GiB and only run with NORNICDB_LARGE_METAL_TEST=1.
const (
	largeMetalTestDim       = 1024
	largeMetalBoundaryNodes = (1 << 30) / largeMetalTestDim
)

func requireLargeMetalTest(t *testing.T) *MetalHNSWBuildAccelerator {
	t.Helper()
	if os.Getenv("NORNICDB_LARGE_METAL_TEST") != "1" {
		t.Skip("set NORNICDB_LARGE_METAL_TEST=1 to run (allocates several GiB)")
	}
	accel, err := NewMetalHNSWBuildAccelerator()
	if err != nil {
		t.Skipf("Metal not available: %v", err)
	}
	return accel
}

// largeMetalTestVector returns vector i of a clustered corpus: one of 4,096
// unit centroids plus Gaussian noise of norm ~0.35, normalized. It is a pure
// function of i so tests can regenerate any vector without holding the corpus.
// Per-dimension noise is scaled by 1/sqrt(dim): unscaled noise would swamp the
// centroids and make the corpus effectively random, which no HNSW build
// searches well.
func largeMetalTestVector(i int, centroids [][]float32) []float32 {
	rng := rand.New(rand.NewSource(int64(i) + 1))
	c := centroids[i%len(centroids)]
	noise := float32(0.35 / math.Sqrt(largeMetalTestDim))
	v := make([]float32, largeMetalTestDim)
	for d := range v {
		v[d] = c[d] + noise*float32(rng.NormFloat64())
	}
	return vector.Normalize(v)
}

func largeMetalTestCentroids() [][]float32 {
	rng := rand.New(rand.NewSource(20261019))
	centroids := make([][]float32, 4096)
	for i := range centroids {
		c := make([]float32, largeMetalTestDim)
		for d := range c {
			c[d] = float32(rng.NormFloat64())
		}
		centroids[i] = vector.Normalize(c)
	}
	return centroids
}

// largeMetalProbeIndexes are vectors on both sides of the 4 GiB boundary.
func largeMetalProbeIndexes(total int) []int {
	return []int{0, 1, largeMetalBoundaryNodes - 1, largeMetalBoundaryNodes, largeMetalBoundaryNodes + 1, largeMetalBoundaryNodes + 2047, total - 1}
}

// TestMetalHNSWResidentBuildBeyondFourGiB grows the resident buffer past 4 GiB
// through AppendInserted and checks that GPU scoring finds vectors stored on
// both sides of the boundary.
func TestMetalHNSWResidentBuildBeyondFourGiB(t *testing.T) {
	accel := requireLargeMetalTest(t)
	defer accel.Close()

	total := largeMetalBoundaryNodes + 8192
	// Reserve exactly 4 GiB so the append that crosses it doubles the buffer.
	require.NoError(t, accel.Prepare(largeMetalTestDim, largeMetalBoundaryNodes))
	require.True(t, accel.ResidentReady(), "Metal could not reserve the resident buffer")

	centroids := largeMetalTestCentroids()
	const batch = 2048
	vecs := make([][]float32, 0, batch)
	for i := 0; i < total; i++ {
		vecs = append(vecs, largeMetalTestVector(i, centroids))
		if len(vecs) == batch || i == total-1 {
			require.NoError(t, accel.AppendInserted(vecs), "append ending at vector %d", i)
			vecs = vecs[:0]
		}
	}
	require.Equal(t, total, accel.residentN)
	require.Greater(t, accel.residentCap*largeMetalTestDim, 1<<30, "resident buffer did not grow past 4 GiB")

	probes := largeMetalProbeIndexes(total)
	queries := make([][]float32, len(probes))
	for i, p := range probes {
		queries[i] = largeMetalTestVector(p, centroids)
	}
	ids, dists, err := accel.CandidateSearchInserted(context.Background(), queries, 10)
	require.NoError(t, err)
	for i, p := range probes {
		require.NotEmpty(t, ids[i], "probe %d returned no candidates", p)
		require.Equal(t, uint32(p), ids[i][0], "probe %d: top candidate", p)
		require.InDelta(t, 0, dists[i][0], 1e-3, "probe %d: self distance", p)
	}
}

// TestMetalHNSWBuildBeyondFourGiB runs the startup build path that panicked
// (buildHNSWWithOptionalGPU with the Metal accelerator) on a corpus the size
// of the reporting database, and checks the build stayed on Metal and the
// index finds vectors stored past the 4 GiB boundary.
func TestMetalHNSWBuildBeyondFourGiB(t *testing.T) {
	accel := requireLargeMetalTest(t)

	total := 1_101_077 // vectors in the database that reported the panic
	centroids := largeMetalTestCentroids()
	pairs := make([]hnswBuildPair, total)
	for i := range pairs {
		pairs[i] = hnswBuildPair{id: fmt.Sprintf("v%07d", i), vec: largeMetalTestVector(i, centroids)}
	}

	config := DefaultHNSWConfig()
	config.EfConstruction = 100 // the "fast" profile the reporting server used
	config.EfSearch = 50
	started := time.Now()
	built, stats, err := buildHNSWWithOptionalGPU(context.Background(), largeMetalTestDim, config, nil, total, hnswPairSliceIterator(pairs), accel)
	require.NoError(t, err)
	t.Logf("built %d vectors in %s: strategy=%s backend=%s batches=%d kernel_errors=%d fallback=%v",
		built.Size(), time.Since(started).Round(time.Second), stats.Strategy, stats.Backend, stats.Batches, stats.KernelErrors, stats.Fallback)
	require.False(t, stats.Fallback, "build fell back to the CPU")
	require.Zero(t, stats.KernelErrors)
	require.True(t, strings.Contains(stats.Backend, "Metal"), "backend %q", stats.Backend)
	require.Equal(t, total, built.Size())

	probes := largeMetalProbeIndexes(total)
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
		if p >= largeMetalBoundaryNodes {
			pastBoundary++
		}
		if hit {
			found++
			if p >= largeMetalBoundaryNodes {
				foundPastBoundary++
			}
		}
	}
	t.Logf("self-recall@10: %d/%d overall, %d/%d past the 4 GiB boundary", found, len(probes), foundPastBoundary, pastBoundary)
	require.GreaterOrEqual(t, float64(found)/float64(len(probes)), 0.95)
	require.Positive(t, pastBoundary)
	require.GreaterOrEqual(t, float64(foundPastBoundary)/float64(pastBoundary), 0.95, "vectors stored past 4 GiB")
}
