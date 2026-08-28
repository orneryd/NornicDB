package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type coverageVectorGetter struct {
	vectors map[string][]float32
}

func (g coverageVectorGetter) GetVector(id string) ([]float32, bool) {
	vec, ok := g.vectors[id]
	return vec, ok
}

type coverageCandidateGen struct {
	candidates []Candidate
	err        error
}

func (g coverageCandidateGen) SearchCandidates(ctx context.Context, query []float32, k int, minSimilarity float64) ([]Candidate, error) {
	return g.candidates, g.err
}

type coverageExactScorer struct {
	scored []ScoredCandidate
	err    error
}

func (s coverageExactScorer) ScoreCandidates(ctx context.Context, query []float32, candidates []Candidate) ([]ScoredCandidate, error) {
	return s.scored, s.err
}

func TestVectorPipelineExtraCandidateLimitAndScorers(t *testing.T) {
	require.Equal(t, 0, boundCandidateLimit(0))
	require.Equal(t, 5, boundCandidateLimit(5))
	require.Equal(t, MaxCandidates, boundCandidateLimit(MaxCandidates+1))

	identity := &IdentityExactScorer{}
	scored, err := identity.ScoreCandidates(context.Background(), nil, []Candidate{{ID: "low", Score: 0.1}, {ID: "high", Score: 0.9}})
	require.NoError(t, err)
	require.Equal(t, []ScoredCandidate{{ID: "high", Score: 0.9}, {ID: "low", Score: 0.1}}, scored)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = identity.ScoreCandidates(cancelled, nil, []Candidate{{ID: "x", Score: 1}})
	require.ErrorIs(t, err, context.Canceled)

	cpu := NewCPUExactScorer(coverageVectorGetter{vectors: map[string][]float32{
		"a": {1, 0},
		"b": {0, 1},
	}})
	scored, err = cpu.ScoreCandidates(context.Background(), []float32{1, 0}, []Candidate{{ID: "missing"}, {ID: "b"}, {ID: "a"}})
	require.NoError(t, err)
	require.Len(t, scored, 2)
	assert.Equal(t, "a", scored[0].ID)
	assert.InDelta(t, 1.0, scored[0].Score, 1e-6)
	assert.Equal(t, "b", scored[1].ID)
	assert.InDelta(t, 0.0, scored[1].Score, 1e-6)

	cpu = NewCPUExactScorer(nil)
	scored, err = cpu.ScoreCandidates(context.Background(), []float32{1, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Nil(t, scored)

	cpu = NewCPUExactScorer(coverageVectorGetter{vectors: map[string][]float32{"a": {1, 0}}})
	_, err = cpu.ScoreCandidates(cancelled, []float32{1, 0}, []Candidate{{ID: "a"}})
	require.ErrorIs(t, err, context.Canceled)

	gpuScorer := NewGPUExactScorer(nil, NewCPUExactScorer(coverageVectorGetter{vectors: map[string][]float32{"a": {1, 0}}}))
	scored, err = gpuScorer.ScoreCandidates(context.Background(), []float32{1, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Len(t, scored, 1)
	assert.Equal(t, "a", scored[0].ID)
	assert.InDelta(t, 1.0, scored[0].Score, 1e-6)

	gpuIndex := gpu.NewEmbeddingIndex(nil, &gpu.EmbeddingIndexConfig{Dimensions: 2, InitialCap: 1, GPUEnabled: false, AutoSync: false})
	gpuScorer = NewGPUExactScorer(gpuIndex, NewCPUExactScorer(coverageVectorGetter{vectors: map[string][]float32{"a": {1, 0}}}))
	scored, err = gpuScorer.ScoreCandidates(context.Background(), []float32{1, 0}, []Candidate{{ID: "a"}})
	require.NoError(t, err)
	require.Len(t, scored, 1)
	assert.Equal(t, "a", scored[0].ID)
	assert.InDelta(t, 1.0, scored[0].Score, 1e-6)
}

func TestVectorPipelineExtraSearchBranches(t *testing.T) {
	pipeline := NewVectorSearchPipeline(
		coverageCandidateGen{err: errors.New("candidate failed")},
		coverageExactScorer{},
	)
	_, err := pipeline.Search(context.Background(), []float32{1, 0}, 3, 0)
	require.ErrorContains(t, err, "candidate generation failed")

	pipeline = NewVectorSearchPipeline(
		coverageCandidateGen{candidates: []Candidate{}},
		coverageExactScorer{},
	)
	got, err := pipeline.Search(context.Background(), []float32{1, 0}, 3, 0)
	require.NoError(t, err)
	require.Empty(t, got)

	pipeline = NewVectorSearchPipeline(
		coverageCandidateGen{candidates: []Candidate{{ID: "a"}}},
		coverageExactScorer{err: errors.New("score failed")},
	)
	_, err = pipeline.Search(context.Background(), []float32{1, 0}, 3, 0)
	require.ErrorContains(t, err, "exact scoring failed")

	pipeline = NewVectorSearchPipeline(
		coverageCandidateGen{candidates: []Candidate{{ID: "a"}, {ID: "b"}, {ID: "c"}}},
		coverageExactScorer{scored: []ScoredCandidate{{ID: "a", Score: 0.9}, {ID: "b", Score: 0.7}, {ID: "c", Score: 0.4}}},
	)
	got, err = pipeline.Search(context.Background(), []float32{1, 0}, 0, 0.5)
	require.NoError(t, err)
	require.Equal(t, []ScoredCandidate{{ID: "a", Score: 0.9}, {ID: "b", Score: 0.7}}, got)

	gpuGen := NewGPUBruteForceCandidateGen(nil)
	_, err = gpuGen.SearchCandidates(context.Background(), []float32{1, 0}, 3, 0)
	require.ErrorContains(t, err, "gpu embedding index unavailable")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gpuGen.SearchCandidates(cancelled, []float32{1, 0}, 3, 0)
	require.ErrorContains(t, err, "gpu embedding index unavailable")
}
