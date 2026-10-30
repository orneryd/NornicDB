package search

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// clusteredRouteFixture returns a clustered index for IVF-HNSW routing over
// 16-D queries; routes are chosen by each test's cluster selector.
func clusteredRouteFixture(t *testing.T, clusters int) *Service {
	t.Helper()
	svc := NewService(nil, 16, nil)
	svc.EnableClustering(nil, clusters)
	query := make([]float32, 16)
	query[0] = 1
	require.NoError(t, svc.clusterIndex.Add("a", query))
	// Members along one axis per cluster, so every cluster the tests route to
	// is populated, as in a real clustered index.
	for c := 0; c < clusters; c++ {
		for i := 0; i < 20; i++ {
			member := make([]float32, 16)
			member[c%16] = 1
			member[15] = float32(i) / 100
			require.NoError(t, svc.clusterIndex.Add(fmt.Sprintf("member-%d-%d", c, i), member))
		}
	}
	require.NoError(t, svc.clusterIndex.Cluster())
	return svc
}

// Pending PQ searches original coordinates until its graph has 256 vectors,
// so graphs with one compression policy can differ in activation. Any active
// graph widens the rescoring pool, whichever route comes first (sol r9:
// TestSolR9MixedPQOversampling).
func TestIVFHNSWOversamplingFollowsAnyActiveGraph(t *testing.T) {
	cfg := DefaultHNSWConfig()
	cfg.Quantization = HNSWQuantizationConfig{Mode: HNSWQuantizationProduct}
	small, large := NewHNSWIndex(16, cfg), NewHNSWIndex(16, cfg)
	for i := 0; i < 301; i++ {
		vec := make([]float32, 16)
		vec[0] = 1
		vec[1+i%15] = float32(i+1) / 1000
		if i == 0 {
			require.NoError(t, small.Add("small", vec))
		} else {
			require.NoError(t, large.Add(fmt.Sprint(i), vec))
		}
	}
	ctx, err := withQuantizationOptions(context.Background(), &SearchOptions{QuantizationOversampling: 2})
	require.NoError(t, err)
	require.False(t, small.quantizationActive(ctx), "the fixture straddles the PQ threshold")
	require.True(t, large.quantizationActive(ctx))

	svc := clusteredRouteFixture(t, 2)
	gen := NewIVFHNSWCandidateGen(svc.clusterIndex, func(cid int) *HNSWIndex {
		if cid == 0 {
			return small
		}
		return large
	}, 2)
	query := make([]float32, 16)
	query[0] = 1
	for _, order := range [][]int{{1, 0}, {0, 1}} {
		gen.SetClusterSelector(func(context.Context, []float32, int) []int { return order })
		got, err := gen.SearchCandidates(ctx, query, 10, 0)
		require.NoError(t, err)
		require.Len(t, got, 20, "both routes include the PQ-active graph; order=%v", order)
	}
}

// Oversampling beyond the graph's EfSearch needs a wider beam: IVF-HNSW
// widens each cluster graph's beam to the pool as single-graph search does
// (sol r9: TestSolR9OversamplingBeam).
func TestIVFHNSWBeamHoldsOversampledPool(t *testing.T) {
	cfg := DefaultHNSWConfig()
	cfg.EfSearch = 10
	cfg.Quantization = HNSWQuantizationConfig{Mode: HNSWQuantizationBinary}
	graph := NewHNSWIndex(16, cfg)
	for i := 0; i < 100; i++ {
		vec := make([]float32, 16)
		vec[0] = 1
		vec[1+i%15] = float32(i+1) / 1000
		require.NoError(t, graph.Add(fmt.Sprint(i), vec))
	}
	ctx, err := withQuantizationOptions(context.Background(), &SearchOptions{QuantizationOversampling: 2})
	require.NoError(t, err)
	query := make([]float32, 16)
	query[0] = 1

	single, err := NewHNSWCandidateGen(graph).SearchCandidates(ctx, query, 10, 0)
	require.NoError(t, err)
	require.Len(t, single, 20, "single graph widens its beam for rescoring")
	svc := clusteredRouteFixture(t, 1)
	gen := NewIVFHNSWCandidateGen(svc.clusterIndex, func(int) *HNSWIndex { return graph }, 1)
	ivf, err := gen.SearchCandidates(ctx, query, 10, 0)
	require.NoError(t, err)
	require.Len(t, ivf, 20, "IVF widens the beam past EfSearch=10 for the same request")

	require.Equal(t, 20, ivfSearchBeam(graph, 0, 20), "configured EfSearch widened to the pool")
	require.Equal(t, 64, ivfSearchBeam(graph, 64, 20), "an explicitly larger ef is kept")
	require.Equal(t, 20, ivfSearchBeam(graph, 5, 20), "a smaller explicit ef is widened to the pool")
}
