package search

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeGpuCandidateSearcher is a deterministic gpuBuildCandidateSearcher: it
// returns the first topK frontier entries (optionally reversed) and records
// the frontier sizes it was called with.
type fakeGpuCandidateSearcher struct {
	callFrontierLens []int
	reversed         bool
}

func (f *fakeGpuCandidateSearcher) candidateSearch(ctx context.Context, queries [][]float32, frontier [][]float32, topK int, wantDistances bool) ([][]int, [][]float32, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	f.callFrontierLens = append(f.callFrontierLens, len(frontier))
	outIdx := make([][]int, len(queries))
	var outDist [][]float32
	if wantDistances {
		outDist = make([][]float32, len(queries))
	}
	for qi := range queries {
		n := min(topK, len(frontier))
		row := make([]int, 0, n)
		for i := 0; i < n; i++ {
			idx := i
			if f.reversed {
				idx = n - 1 - i
			}
			row = append(row, idx)
		}
		outIdx[qi] = row
		if wantDistances {
			outDist[qi] = make([]float32, len(row))
		}
	}
	return outIdx, outDist, nil
}

// TestCandidateSearchGraphKernel_Orchestration pins the shared beam
// orchestration behind the CUDA/Metal/Vulkan CandidateSearchGraph wrappers:
// seeding from the graph entry point, per-iteration expand→local-search→remap,
// topK truncation and env-sized query grouping.
func TestCandidateSearchGraphKernel_Orchestration(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BEAM_WIDTH", "4")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BEAM_ITERS", "2")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BEAM_UNION_MAX", "64")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BEAM_QUERY_GROUP", "2")

	graph := &hnswBuildGraphSnapshot{
		dim:           2,
		entryPoint:    0,
		hasEntryPoint: true,
		vectors: [][]float32{
			{1, 0},
			{0, 1},
			{-1, 0},
			{0, -1},
			{0.7, 0.7},
		},
		neighbors: [][]uint32{
			{1, 2},
			{0, 3},
			{0, 3},
			{1, 2},
			{0},
		},
		seen: make([]uint32, 5),
	}
	queries := [][]float32{{0.5, 0.5}, {0.3, -0.2}, {0.9, 0.1}}
	fake := &fakeGpuCandidateSearcher{reversed: true}

	idx, dist, err := candidateSearchGraphBatched(context.Background(), fake, queries, graph, 3)
	require.NoError(t, err)
	require.Equal(t, len(queries), len(idx))
	require.Equal(t, len(queries), len(dist))
	for qi := range idx {
		require.NotEmpty(t, idx[qi])
		require.LessOrEqual(t, len(idx[qi]), 3, "beams must be truncated to topK")
		for _, id := range idx[qi] {
			require.Less(t, int(id), len(graph.vectors), "remapped IDs must be valid union IDs")
		}
	}

	// Queries without their own seeds all start from the global entry point's
	// neighbourhood, so with the same deterministic kernel they get the same
	// beam, in both groups.
	require.Equal(t, idx[0], idx[1])
	require.Equal(t, idx[0], idx[2])

	// Three queries with group size 2 -> two groups; at most
	// NORNICDB_HNSW_BUILD_GPU_BEAM_ITERS (2) local searches per group.
	require.NotEmpty(t, fake.callFrontierLens)
	require.LessOrEqual(t, len(fake.callFrontierLens), 4)
}

// A query's own seeds (graph.querySeeds: its level-0 entry point, lexical
// seeds) start its beam: the first local search scores them.
func TestCandidateSearchGraphKernel_QuerySeedsStartTheBeam(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_BEAM_ITERS", "1")
	graph := &hnswBuildGraphSnapshot{
		dim: 2, entryPoint: 0, hasEntryPoint: true,
		vectors:    [][]float32{{1, 0}, {0, 1}, {-1, 0}, {0, -1}},
		neighbors:  [][]uint32{{1}, {0}, {3}, {2}},
		seen:       make([]uint32, 4),
		querySeeds: [][]uint32{{2}},
	}
	idx, _, err := candidateSearchGraphBatched(context.Background(), &fakeGpuCandidateSearcher{}, [][]float32{{-1, 0}}, graph, 4)
	require.NoError(t, err)
	require.ElementsMatch(t, []uint32{2, 3}, idx[0], "node 2 and its neighbour, not the global entry point's region")
}

// TestCandidateSearchGraphKernel_Cancellation pins that cancellation aborts
// the shared orchestration and propagates to the caller.
func TestCandidateSearchGraphKernel_Cancellation(t *testing.T) {
	graph := &hnswBuildGraphSnapshot{
		dim:           2,
		entryPoint:    0,
		hasEntryPoint: true,
		vectors:       [][]float32{{1, 0}, {0, 1}},
		neighbors:     [][]uint32{{1}, {0}},
		seen:          make([]uint32, 2),
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := candidateSearchGraphBatched(cancelled, &fakeGpuCandidateSearcher{}, [][]float32{{0.5, 0.5}}, graph, 2)
	require.Error(t, err)
}
