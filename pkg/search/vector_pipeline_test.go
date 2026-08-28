package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBruteForceCandidateGen(t *testing.T) {
	idx := NewVectorIndex(4)
	gen := NewBruteForceCandidateGen(idx)

	// Add test vectors
	require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("vec3", []float32{0, 0, 1, 0}))

	query := []float32{1, 0, 0, 0}
	candidates, err := gen.SearchCandidates(context.Background(), query, 2, 0.0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(candidates), 1)
	assert.Equal(t, "vec1", candidates[0].ID)
	assert.Greater(t, candidates[0].Score, 0.9) // Should be very similar
}

func TestHNSWCandidateGen(t *testing.T) {
	idx := NewHNSWIndex(4, DefaultHNSWConfig())
	gen := NewHNSWCandidateGen(idx)

	// Add test vectors
	require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("vec3", []float32{0, 0, 1, 0}))

	query := []float32{1, 0, 0, 0}
	candidates, err := gen.SearchCandidates(context.Background(), query, 2, 0.0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(candidates), 1)
	assert.Equal(t, "vec1", candidates[0].ID)
	assert.Greater(t, candidates[0].Score, 0.9) // Should be very similar
}

func TestCPUExactScorer(t *testing.T) {
	idx := NewVectorIndex(4)
	scorer := NewCPUExactScorer(idx)

	// Add test vectors
	require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
	require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0}))
	require.NoError(t, idx.Add("vec3", []float32{0, 0, 1, 0}))

	query := []float32{1, 0, 0, 0}
	candidates := []Candidate{
		{ID: "vec1", Score: 0.9}, // Approximate score
		{ID: "vec2", Score: 0.1},
		{ID: "vec3", Score: 0.0},
	}

	scored, err := scorer.ScoreCandidates(context.Background(), query, candidates)
	require.NoError(t, err)
	require.Equal(t, 3, len(scored))

	// Should be sorted by exact score descending
	assert.Equal(t, "vec1", scored[0].ID)
	assert.Greater(t, scored[0].Score, 0.9) // Exact score should be high
	// vec2 and vec3 are orthogonal to query, so both should have ~0.0 score
	// Order may vary for equal scores, so just check they're both low
	assert.Less(t, scored[1].Score, 0.1)
	assert.Less(t, scored[2].Score, 0.1)
}

func TestVectorSearchPipeline(t *testing.T) {
	t.Run("brute force pipeline", func(t *testing.T) {
		idx := NewVectorIndex(4)
		candidateGen := NewBruteForceCandidateGen(idx)
		exactScorer := NewCPUExactScorer(idx)
		pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

		// Add test vectors
		require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
		require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0}))
		require.NoError(t, idx.Add("vec3", []float32{0, 0, 1, 0}))

		query := []float32{1, 0, 0, 0}
		results, err := pipeline.Search(context.Background(), query, 2, 0.0)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1)
		assert.Equal(t, "vec1", results[0].ID)
		assert.Greater(t, results[0].Score, 0.9)
	})

	t.Run("HNSW pipeline", func(t *testing.T) {
		idx := NewVectorIndex(4)
		hnswIdx := NewHNSWIndex(4, DefaultHNSWConfig())

		// Populate both indexes
		require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
		require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0}))
		require.NoError(t, hnswIdx.Add("vec1", []float32{1, 0, 0, 0}))
		require.NoError(t, hnswIdx.Add("vec2", []float32{0, 1, 0, 0}))

		candidateGen := NewHNSWCandidateGen(hnswIdx)
		exactScorer := NewCPUExactScorer(idx)
		pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

		query := []float32{1, 0, 0, 0}
		results, err := pipeline.Search(context.Background(), query, 2, 0.0)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(results), 1)
		assert.Equal(t, "vec1", results[0].ID)
		assert.Greater(t, results[0].Score, 0.9)
	})

	t.Run("minSimilarity filtering", func(t *testing.T) {
		idx := NewVectorIndex(4)
		candidateGen := NewBruteForceCandidateGen(idx)
		exactScorer := NewCPUExactScorer(idx)
		pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

		// Add test vectors
		require.NoError(t, idx.Add("vec1", []float32{1, 0, 0, 0}))
		require.NoError(t, idx.Add("vec2", []float32{0, 1, 0, 0})) // Orthogonal, similarity = 0

		query := []float32{1, 0, 0, 0}
		results, err := pipeline.Search(context.Background(), query, 10, 0.5) // High threshold
		require.NoError(t, err)
		// Should only return vec1 (similarity ~1.0), not vec2 (similarity ~0.0)
		require.Equal(t, 1, len(results))
		assert.Equal(t, "vec1", results[0].ID)
	})
}

func TestVectorSearchPipeline_Cancellation(t *testing.T) {
	idx := NewVectorIndex(4)
	candidateGen := NewBruteForceCandidateGen(idx)
	exactScorer := NewCPUExactScorer(idx)
	pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

	// Add many vectors to make search take time
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 4)
		vec[i%4] = 1.0
		idx.Add(fmt.Sprintf("vec%d", i), vec)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	query := []float32{1, 0, 0, 0}
	_, err := pipeline.Search(ctx, query, 10, 0.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestVectorSearchPipeline_Timeout(t *testing.T) {
	idx := NewVectorIndex(4)
	candidateGen := NewBruteForceCandidateGen(idx)
	exactScorer := NewCPUExactScorer(idx)
	pipeline := NewVectorSearchPipeline(candidateGen, exactScorer)

	// Add many vectors
	for i := 0; i < 1000; i++ {
		vec := make([]float32, 4)
		vec[i%4] = 1.0
		idx.Add(fmt.Sprintf("vec%d", i), vec)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	query := []float32{1, 0, 0, 0}
	_, err := pipeline.Search(ctx, query, 10, 0.0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestBoundCandidateLimit(t *testing.T) {
	tests := []struct {
		name     string
		k        int
		expected int
	}{
		{"small k", 5, 5},
		{"medium k", 20, 20},
		{"large k", 100, 100},
		{"very large k", 5001, MaxCandidates},
		{"zero k", 0, 0},
		{"negative k", -1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := boundCandidateLimit(tt.k)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVectorSearchCandidates_UsesPipeline(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	// Add test nodes
	node1 := &storage.Node{
		ID:              storage.NodeID("node1"),
		Labels:          []string{"Test"},
		ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
	}
	node2 := &storage.Node{
		ID:              storage.NodeID("node2"),
		Labels:          []string{"Test"},
		ChunkEmbeddings: [][]float32{{0, 1, 0, 0}},
	}

	require.NoError(t, svc.IndexNode(node1))
	require.NoError(t, svc.IndexNode(node2))

	query := []float32{1, 0, 0, 0}
	opts := DefaultSearchOptions()
	opts.Limit = 10

	candidates, err := svc.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(candidates), 1)
	assert.Equal(t, "node1", candidates[0].ID)
	assert.Greater(t, candidates[0].Score, 0.9)
}

func TestVectorSearchCandidates_AutoStrategyDefaultsToHNSW(t *testing.T) {
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	for i := 0; i < 12; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("node%d", i)),
			ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
		}
		require.NoError(t, svc.IndexNode(node))
	}

	query := []float32{1, 0, 0, 0}
	opts := DefaultSearchOptions()
	opts.Limit = 10

	candidates, err := svc.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(candidates), 1)
	_, ok := svc.vectorPipeline.candidateGen.(*HNSWCandidateGen)
	require.True(t, ok)
}

func TestVectorSearchCandidates_CPUBruteForceThresholdOptIn(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "100")
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	for i := 0; i < 12; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("node%d", i)),
			ChunkEmbeddings: [][]float32{{float32(i % 4), float32((i + 1) % 4), 0, 0}},
		}
		require.NoError(t, svc.IndexNode(node))
	}

	query := []float32{1, 0, 0, 0}
	opts := DefaultSearchOptions()
	opts.Limit = 10

	candidates, err := svc.VectorSearchCandidates(context.Background(), query, opts)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(candidates), 1)
	_, ok := svc.vectorPipeline.candidateGen.(*BruteForceCandidateGen)
	require.True(t, ok)
}

func TestVectorSearchPipeline_UsesBruteForceWhenClusteredAndThresholdOptIn(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "50")
	engine := storage.NewMemoryEngine()
	svc := NewServiceWithDimensions(engine, 4)

	// Enable clustering and force it to cluster with a dataset below the CPU brute-force threshold.
	svc.EnableClustering(nil, 2)
	svc.SetMinEmbeddingsForClustering(1)

	for i := 0; i < 20; i++ {
		node := &storage.Node{
			ID:              storage.NodeID(fmt.Sprintf("node%d", i)),
			ChunkEmbeddings: [][]float32{{1, 0, 0, 0}},
		}
		require.NoError(t, svc.IndexNode(node))
	}

	require.NoError(t, svc.TriggerClustering(context.Background()))

	pipeline, err := svc.getOrCreateVectorPipeline(context.Background())
	require.NoError(t, err)
	_, ok := pipeline.candidateGen.(*BruteForceCandidateGen)
	require.True(t, ok)
}

func TestFileStoreBruteForceCandidateGen_SearchAndCancel(t *testing.T) {
	path := fmt.Sprintf("%s/vectors", t.TempDir())
	vfs, err := NewVectorFileStore(path, 3)
	require.NoError(t, err)
	defer vfs.Close()

	require.NoError(t, vfs.Add("a", []float32{1, 0, 0}))
	require.NoError(t, vfs.Add("b", []float32{0.9, 0.1, 0}))
	require.NoError(t, vfs.Add("c", []float32{0, 1, 0}))

	gen := NewFileStoreBruteForceCandidateGen(vfs)
	cands, err := gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 2, 0.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gen.SearchCandidates(cancelled, []float32{1, 0, 0}, 2, 0.0)
	require.ErrorIs(t, err, context.Canceled)

	nilGen := &FileStoreBruteForceCandidateGen{}
	cands, err = nilGen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 2, 0.0)
	require.NoError(t, err)
	require.Nil(t, cands)
}

func TestGPUExactAndBruteForceGenerators_Fallbacks(t *testing.T) {
	idx := NewVectorIndex(3)
	require.NoError(t, idx.Add("a", []float32{1, 0, 0}))
	require.NoError(t, idx.Add("b", []float32{0, 1, 0}))
	cpu := NewCPUExactScorer(idx)

	gpuScorer := NewGPUExactScorer(nil, cpu)
	scored, err := gpuScorer.ScoreCandidates(context.Background(), []float32{1, 0, 0}, []Candidate{
		{ID: "a", Score: 0.5},
		{ID: "b", Score: 0.4},
	})
	require.NoError(t, err)
	require.NotEmpty(t, scored)
	assert.Equal(t, "a", scored[0].ID)

	gen := NewGPUBruteForceCandidateGen(nil)
	_, err = gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 3, 0.0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unavailable")

	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = true
	embCfg.AutoSync = true
	emb := gpu.NewEmbeddingIndex(nil, embCfg)
	require.NoError(t, emb.Add("a", []float32{1, 0, 0}))
	require.NoError(t, emb.Add("b", []float32{0, 1, 0}))
	gen = NewGPUBruteForceCandidateGen(emb)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gen.SearchCandidates(cancelled, []float32{1, 0, 0}, 3, 0.0)
	require.ErrorIs(t, err, context.Canceled)

	cands, err := gen.SearchCandidates(context.Background(), []float32{1, 0, 0}, 3, -1.0)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
}

func TestIdentityExactScorer_ContextAndSort(t *testing.T) {
	scorer := &IdentityExactScorer{}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := scorer.ScoreCandidates(cancelled, []float32{1, 0, 0}, []Candidate{{ID: "x", Score: 0.1}})
	require.ErrorIs(t, err, context.Canceled)

	out, err := scorer.ScoreCandidates(context.Background(), nil, []Candidate{
		{ID: "x", Score: 0.1},
		{ID: "y", Score: 0.9},
		{ID: "z", Score: 0.5},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"y", "z", "x"}, []string{out[0].ID, out[1].ID, out[2].ID})
}
