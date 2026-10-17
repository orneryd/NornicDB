package search

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

func ivfParallelFixture(tb testing.TB) (*IVFHNSWCandidateGen, []float32) {
	tb.Helper()
	const dim, clusters, points = 64, 8, 512
	cfg := gpu.DefaultEmbeddingIndexConfig(dim)
	cfg.GPUEnabled = false
	ci := gpu.NewClusterIndex(nil, cfg, &gpu.KMeansConfig{NumClusters: 1, MaxIterations: 2, Tolerance: .001, InitMethod: "kmeans++"})
	require.NoError(tb, ci.Add("route", makeIVFVector(dim, 1)))
	require.NoError(tb, ci.Cluster())
	graphs := make([]*HNSWIndex, clusters)
	rng := rand.New(rand.NewSource(73))
	for c := range graphs {
		config := DefaultHNSWConfig()
		config.UseGPUBuild = false
		graphs[c] = NewHNSWIndex(dim, config)
		for i := 0; i < points; i++ {
			v := make([]float32, dim)
			for j := range v {
				v[j] = rng.Float32()*2 - 1
			}
			require.NoError(tb, graphs[c].Add(fmt.Sprintf("%d/%d", c, i), v))
		}
	}
	gen := NewIVFHNSWCandidateGen(ci, func(c int) *HNSWIndex { return graphs[c] }, clusters)
	gen.SetClusterSelector(func(context.Context, []float32, int) []int { return []int{0, 1, 2, 3, 4, 5, 6, 7} })
	return gen, makeIVFVector(dim, 1)
}

func makeIVFVector(dim int, value float32) []float32 {
	v := make([]float32, dim)
	v[0] = value
	return v
}

func TestIVFHNSWMultiClusterMatchesIndependentMerge(t *testing.T) {
	gen, q := ivfParallelFixture(t)
	const k = 20
	expected := map[string]float64{}
	for c := 0; c < 8; c++ {
		got, err := gen.getClusterHNSW(c).SearchWithEf(context.Background(), q, k, -1, 0)
		require.NoError(t, err)
		for _, hit := range got {
			expected[hit.ID] = float64(hit.Score)
		}
	}
	ranked := make([]Candidate, 0, len(expected))
	for id, score := range expected {
		ranked = append(ranked, Candidate{ID: id, Score: score})
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].Score > ranked[j].Score })
	got, err := gen.SearchCandidates(context.Background(), q, k, -1)
	require.NoError(t, err)
	require.Len(t, got, k)
	require.Equal(t, ranked[:k], got)
	for i, hit := range got {
		require.Equal(t, expected[hit.ID], hit.Score)
		if i > 0 {
			require.GreaterOrEqual(t, got[i-1].Score, hit.Score)
		}
		for _, score := range expected {
			require.LessOrEqual(t, score, got[0].Score)
		}
	}
	// A failure in any selected graph must fail the whole query.
	_, err = gen.SearchCandidates(context.Background(), q[:3], k, -1)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gen.SearchCandidates(ctx, q, k, -1)
	require.ErrorIs(t, err, context.Canceled)
}

func BenchmarkIVFHNSWMultiCluster(b *testing.B) {
	gen, q := ivfParallelFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := gen.SearchCandidates(context.Background(), q, 20, -1)
		if err != nil {
			b.Fatal(err)
		}
	}
}
