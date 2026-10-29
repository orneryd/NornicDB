package search

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// memberBoundGenerator controls which members a restricted ANN pass surfaces.
// Its score error is zero; it makes no completeness claim about other members.
type memberBoundGenerator struct{ candidates []Candidate }

func (g memberBoundGenerator) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	return g.candidates, nil
}

func (memberBoundGenerator) candidateScoreBound() float64 { return 0 }

func TestCandidateNodeBoundIncludesUnsurfacedMembers(t *testing.T) {
	index := NewVectorIndex(2)
	var candidates []Candidate
	members := map[string][]string{}
	for n := 0; n < 2*stageBatch; n++ {
		id := fmt.Sprintf("rival-%03d", n)
		require.NoError(t, index.Add(id, []float32{0.8, 0.6}))
		candidates = append(candidates, Candidate{ID: id, Score: 0.8})
		members[id] = []string{id}
	}
	require.NoError(t, index.Add(nodeChunkVectorKey("winner", 0), []float32{0.1, float32(math.Sqrt(0.99))}))
	require.NoError(t, index.Add(nodeChunkVectorKey("winner", 1), []float32{1, 0}))
	candidates = append(candidates, Candidate{ID: nodeChunkVectorKey("winner", 0), Score: 0.1})
	members["winner"] = []string{nodeChunkVectorKey("winner", 0), nodeChunkVectorKey("winner", 1)}
	ctx := withVectorMembers(context.Background(), func([]string) map[string][]string { return members })
	for _, duplicate := range []bool{false, true} {
		pool := candidates
		if duplicate {
			// Two copies of the weak member must not stand in for the two
			// distinct indexed members when proving the node's bound.
			pool = append(append([]Candidate(nil), candidates...), candidates[len(candidates)-1])
		}
		pipeline := NewVectorSearchPipeline(memberBoundGenerator{pool}, NewCPUExactScorer(index))
		for _, k := range []int{1, 10, 0} {
			for _, threshold := range []float64{-1, 0.95} {
				t.Run(fmt.Sprintf("duplicate=%t/k=%d/threshold=%g", duplicate, k, threshold), func(t *testing.T) {
					got, err := pipeline.Search(ctx, []float32{1, 0}, k, threshold)
					require.NoError(t, err)
					require.NotEmpty(t, got)
					require.Equal(t, nodeChunkVectorKey("winner", 1), got[0].ID)
					require.InDelta(t, 1, got[0].Score, 1e-6)
					full, _, err := pipeline.scoreNodes(ctx, []float32{1, 0}, pool, k, threshold, math.Inf(1))
					require.NoError(t, err)
					require.Equal(t, full, got, "the bound must preserve the all-member ranking")
				})
			}
		}
	}
}

func TestServiceSearchDoesNotPruneUnsurfacedBestMember(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	var candidates []Candidate
	for n := 0; n <= 2*stageBatch; n++ {
		id := fmt.Sprintf("rival-%03d", n)
		vectors := [][]float32{{0.8, 0.6}}
		if n == 2*stageBatch {
			id = "winner"
			vectors = [][]float32{{0.1, float32(math.Sqrt(0.99))}, {1, 0}}
		}
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, ChunkEmbeddings: vectors}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
		candidates = append(candidates, Candidate{ID: svc.nodeChunkVectors[id][0], Score: float64(vectors[0][0])})
	}
	svc.vectorPipeline = NewVectorSearchPipeline(memberBoundGenerator{candidates}, NewCPUExactScorer(svc.vectorIndex))
	for _, threshold := range []float64{-1, 0.95} {
		t.Run(fmt.Sprintf("threshold=%g", threshold), func(t *testing.T) {
			opts := DefaultSearchOptions()
			opts.Limit, opts.MaxCandidateLimit = 10, len(candidates)
			opts.MinSimilarity = &threshold
			fallback := false
			opts.FallbackEnabled = &fallback
			response, err := svc.Search(context.Background(), "", []float32{1, 0}, opts)
			require.NoError(t, err)
			require.NotEmpty(t, response.Results)
			require.Equal(t, "winner", response.Results[0].ID)
			require.InDelta(t, 1, response.Results[0].Similarity, 1e-6)
		})
	}
}

func TestIVFPQOverflowCopiesDoNotInflateCappedPopulation(t *testing.T) {
	t.Setenv(EnvSearchResultCacheEnabled, "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100000")
	const count, dims = 64, 4
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic")
	svc := NewService(engine, dims, nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	store, err := NewVectorFileStore(filepath.Join(t.TempDir(), "vectors"), dims)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	want := make([]string, count)
	for n := 0; n < count; n++ {
		id := fmt.Sprintf("doc-%02d", n)
		v := make([]float32, dims)
		v[n%dims] = 1
		v[(n+1)%dims] = float32(n) / 1000
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{v}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
		require.NoError(t, store.Add(svc.nodeChunkVectors[id][0], v))
		want[n] = id
	}
	idx, _, err := BuildIVFPQFromVectorStore(context.Background(), store, IVFPQProfile{
		Dimensions: dims, IVFLists: 4, PQSegments: 2, PQBits: 2, NProbe: 1,
		RerankTopK: 8, TrainingSampleMax: count, KMeansMaxIterations: 6, OverflowMax: 8,
	}, nil)
	require.NoError(t, err)
	require.Len(t, idx.overflow, 8)
	require.Equal(t, count, idx.populationCount, "union count is ready before publication")
	gen := NewIVFPQCandidateGen(idx, 1)
	query := []float32{1, 0, 0, 0}
	t.Run("unique population", func(t *testing.T) {
		require.Equal(t, count, gen.candidatePopulation(context.Background()))
	})
	t.Run("public candidates at unique cap", func(t *testing.T) {
		candidates, err := gen.SearchCandidates(context.Background(), query, count, -1)
		require.NoError(t, err)
		require.Len(t, candidates, count)
		_, evidence, err := gen.searchCandidatesWithEvidence(context.Background(), query, count, -1)
		require.NoError(t, err)
		require.True(t, evidence.exhausted)
		require.True(t, evidence.routedComplete)
	})
	t.Run("service cap covers every live vector", func(t *testing.T) {
		svc.vectorPipeline = NewVectorSearchPipeline(gen, NewCPUExactScorer(svc.vectorIndex))
		opts := DefaultSearchOptions()
		opts.Limit, opts.CandidateTarget, opts.MaxCandidateLimit = count, count, count
		threshold, fallback := -1.0, false
		opts.MinSimilarity, opts.FallbackEnabled = &threshold, &fallback
		response, err := svc.Search(context.Background(), "", query, opts)
		require.NoError(t, err)
		require.ElementsMatch(t, want, exactStageIDs(response.Results))
		require.True(t, response.RetrievalExhausted)
	})
}

func TestIVFPQOverflowPopulationSurvivesReload(t *testing.T) {
	idx := &IVFPQIndex{
		profile: IVFPQProfile{Dimensions: 2},
		lists:   []ivfpqList{{IDs: []string{"base", "second"}}},
		overflow: []ivfpqOverflowVector{
			{ID: "base", Vector: []float32{1, 0}},
			{ID: "base", Vector: []float32{1, 0}},
			{ID: "overflow-only", Vector: []float32{0, 1}},
			{ID: "overflow-only", Vector: []float32{0, 1}},
		},
	}
	path := filepath.Join(t.TempDir(), "vectors")
	require.NoError(t, SaveIVFPQBundle(path, idx))
	loaded, err := LoadIVFPQBundle(path)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, 3, loaded.populationCount, "reload prepares the unique population before publication")
	for _, index := range []*IVFPQIndex{idx, loaded} {
		counts := make(chan int, 8)
		var workers sync.WaitGroup
		for n := 0; n < cap(counts); n++ {
			workers.Add(1)
			go func() { defer workers.Done(); counts <- index.population() }()
		}
		workers.Wait()
		close(counts)
		for count := range counts {
			require.Equal(t, 3, count)
		}
	}
}

func TestIVFPQPopulationCountsLiveUniqueOverlayIDs(t *testing.T) {
	idx := &IVFPQIndex{
		profile:      IVFPQProfile{Dimensions: 2, NProbe: 1},
		centroids:    [][]float32{{1, 0}, {0.5, 0.8660254}},
		centroidNorm: [][]float32{{1, 0}, {0.5, 0.8660254}},
		codebooks:    []ivfpqCodebook{{SubDim: 2, Codeword: [][]float32{{0, 0}}}},
		lists: []ivfpqList{
			{IDs: []string{"a", "b"}, CodeSize: 1, Codes: []byte{0, 0}},
			{IDs: []string{"c", "d"}, CodeSize: 1, Codes: []byte{0, 0}},
		},
		overflow: []ivfpqOverflowVector{{ID: "c", Vector: []float32{0.5, 0.8660254}}},
	}
	for _, tc := range []struct {
		name    string
		updated []string
		removed []string
		want    []string
	}{
		{name: "updates are not new IDs", updated: []string{"a", "b", "c", "d"}, want: []string{"a", "b", "c", "d"}},
		{name: "new IDs count once", updated: []string{"a", "fresh-1", "fresh-2"}, want: []string{"a", "b", "c", "d", "fresh-1", "fresh-2"}},
		{name: "removals reduce the full-probe cap", updated: []string{"fresh"}, removed: []string{"a", "b"}, want: []string{"c", "d", "fresh"}},
		{name: "empty live population", removed: []string{"a", "b", "c", "d"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			overlay := newANNMutationOverlay()
			for _, id := range tc.updated {
				overlay.Add(id, []float32{0, 1})
			}
			for _, id := range tc.removed {
				overlay.Remove(id)
			}
			overlay.Remove("never-in-base")
			gen := NewIVFPQCandidateGenWithOverlay(idx, 1, overlay)
			counts := make(chan int, 8)
			var readers sync.WaitGroup
			for n := 0; n < cap(counts); n++ {
				readers.Add(1)
				go func() { defer readers.Done(); counts <- gen.candidatePopulation(context.Background()) }()
			}
			readers.Wait()
			close(counts)
			for count := range counts {
				require.Equal(t, len(tc.want), count)
			}
			for repetition := 0; repetition < 2; repetition++ {
				require.Equal(t, len(tc.want), gen.candidatePopulation(context.Background()))
				candidates, err := gen.SearchCandidates(context.Background(), []float32{1, 0}, max(1, len(tc.want)), -1)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.want, candidateIDs(candidates))
				_, evidence, err := gen.searchCandidatesWithEvidence(context.Background(), []float32{1, 0}, max(1, len(tc.want)), -1)
				require.NoError(t, err)
				require.True(t, evidence.exhausted)
				require.True(t, evidence.routedComplete)
			}
			overlay.Reset()
			require.Equal(t, 4, gen.candidatePopulation(context.Background()))
			overlay.Add("fresh-1", []float32{1, 0})
			require.Equal(t, 5, gen.candidatePopulation(context.Background()))
			overlay.Remove("fresh-1")
			require.Equal(t, 4, gen.candidatePopulation(context.Background()))
		})
	}
}
