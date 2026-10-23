package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type testCandidateGen struct {
	candidates []Candidate
	err        error
}

func (g testCandidateGen) SearchCandidates(context.Context, []float32, int, float64) ([]Candidate, error) {
	if g.err != nil {
		return nil, g.err
	}
	return g.candidates, nil
}

type testExactScorer struct {
	scored []ScoredCandidate
	err    error
}

func (s testExactScorer) ScoreCandidates(context.Context, []float32, []Candidate) ([]ScoredCandidate, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.scored, nil
}

func TestService_VectorSearchCandidates_MoreBranches(t *testing.T) {
	t.Run("empty embedding returns error", func(t *testing.T) {
		svc := NewServiceWithDimensions(newTestMemoryEngine(t), 3)
		_, err := svc.VectorSearchCandidates(context.Background(), nil, &SearchOptions{Limit: 2})
		require.Error(t, err)
		require.ErrorContains(t, err, "requires embedding")
	})

	t.Run("pipeline error is surfaced", func(t *testing.T) {
		svc := NewServiceWithDimensions(newTestMemoryEngine(t), 3)
		svc.vectorPipeline = NewVectorSearchPipeline(
			testCandidateGen{err: errors.New("candidate failure")},
			testExactScorer{},
		)
		_, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, &SearchOptions{Limit: 2})
		require.Error(t, err)
		require.ErrorContains(t, err, "candidate failure")
	})

	t.Run("nil opts uses defaults and returns collapsed candidates", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		svc := NewServiceWithDimensions(engine, 3)
		node := &storage.Node{
			ID:     "nornic:doc-1",
			Labels: []string{"Doc"},
			ChunkEmbeddings: [][]float32{
				{1, 0, 0},
				{0, 1, 0},
			},
		}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))

		out, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, nil)
		require.NoError(t, err)
		require.NotEmpty(t, out)
	})

	t.Run("type filtering and limit cap", func(t *testing.T) {
		engine := newTestMemoryEngine(t)
		svc := NewServiceWithDimensions(engine, 3)

		_, err := engine.CreateNode(&storage.Node{ID: "nornic:doc-1", Labels: []string{"Doc"}})
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: "nornic:other-1", Labels: []string{"Other"}})
		require.NoError(t, err)

		svc.vectorPipeline = NewVectorSearchPipeline(
			testCandidateGen{candidates: []Candidate{
				{ID: "nornic:doc-1", Score: 0.95},
				{ID: "nornic:other-1", Score: 0.90},
				{ID: "nornic:doc-2", Score: 0.85},
			}},
			testExactScorer{scored: []ScoredCandidate{
				{ID: "nornic:doc-1", Score: 0.95},
				{ID: "nornic:other-1", Score: 0.90},
				{ID: "nornic:doc-2", Score: 0.85},
			}},
		)

		out, err := svc.VectorSearchCandidates(context.Background(), []float32{1, 0, 0}, &SearchOptions{
			Limit: 1,
			Types: []string{"Doc"},
		})
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.Equal(t, "nornic:doc-1", out[0].ID)
	})
}
