package search

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Reuse the existing transient-vector-failure fixture, with a lexical-only
// source whose filter cannot decide it. The fallback vector population cannot
// replace the filter's error merely because the vector backend recovers.
func TestReviewR2LexicalFilterFailureDuringVectorRecovery(t *testing.T) {
	for _, failing := range []bool{false, true} {
		name := "ordinary backend failure remains recoverable"
		if failing {
			name = "lexical filter failure must survive backend recovery"
		}
		t.Run(name, func(t *testing.T) {
			engine := newNamespacedEngine(t)
			svc := NewService(engine, 2, nil)
			t.Cleanup(func() { _ = svc.Close() })
			for _, node := range []*storage.Node{
				{ID: "lexical-only", Labels: []string{"Src"}, Properties: map[string]any{"content": "walrus notes"}},
				{ID: "vector-only", Labels: []string{"Src"}, Properties: map[string]any{"content": "unrelated topic"}, ChunkEmbeddings: [][]float32{{1, 0}}},
			} {
				_, err := engine.CreateNode(t.Context(), node)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(node))
			}
			svc.vectorPipeline = NewVectorSearchPipeline(&errorThenCandidateGenerator{
				firstErr: errors.New("vector backend temporarily unavailable"),
				candidate: Candidate{ID: "vector-only", Score: 1},
			}, &IdentityExactScorer{})
			filterErr := errors.New("the lexical source's where failed")
			lexicalCalls := 0
			opts := DefaultSearchOptions()
			opts.Limit = 10
			opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
				if node.ID == "lexical-only" {
					lexicalCalls++
					if failing {
						return false, filterErr
					}
				}
				return true, nil
			}
			response, err := svc.Search(t.Context(), "walrus", []float32{1, 0}, opts)
			require.Greater(t, lexicalCalls, 0, "hybrid search must evaluate its lexical source")
			if failing {
				t.Logf("response=%+v, error=%v", response, err)
				require.ErrorIs(t, err, filterErr, "filter error must take precedence over a recoverable backend failure")
				require.ErrorIs(t, err, ErrFilterEvaluation)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				require.Len(t, response.Results, 1)
				require.Equal(t, "vector-only", response.Results[0].ID)
				require.Equal(t, SearchFallbackHybridSearchFailed, response.FallbackReason)
			}
		})
	}
}

type reviewR2RecoveringGenerator struct {
	failNext bool
	candidates []Candidate
}

func (g *reviewR2RecoveringGenerator) SearchCandidates(ctx context.Context, q []float32, k int, min float64) ([]Candidate, error) {
	candidates, _, err := g.searchCandidatesWithExhaustion(ctx, q, k, min)
	return candidates, err
}

func (g *reviewR2RecoveringGenerator) searchCandidatesWithExhaustion(_ context.Context, _ []float32, intLimit int, _ float64) ([]Candidate, bool, error) {
	if g.failNext {
		g.failNext = false
		return nil, false, errors.New("vector backend temporarily unavailable")
	}
	return g.candidates[:min(intLimit, len(g.candidates))], intLimit >= len(g.candidates), nil
}

func TestReviewR2LaterPageLexicalFilterFailureDuringVectorRecovery(t *testing.T) {
	engine := newNamespacedEngine(t)
	svc := NewService(engine, 2, nil)
	t.Cleanup(func() { _ = svc.Close() })
	generator := &reviewR2RecoveringGenerator{}
	for i := 0; i < 61; i++ {
		id := fmt.Sprintf("src%02d", i)
		node := &storage.Node{ID: storage.NodeID(id), Labels: []string{"Src"}, Properties: map[string]any{"content": "walrus notes"}}
		if i > 0 {
			node.ChunkEmbeddings = [][]float32{{1, 0}}
			generator.candidates = append(generator.candidates, Candidate{ID: id, Score: 1})
		}
		_, err := engine.CreateNode(t.Context(), node)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(node))
	}
	svc.vectorPipeline = NewVectorSearchPipeline(generator, &IdentityExactScorer{})
	filterErr := errors.New("division by zero in the lexical-only source's where")
	failFilter := false
	failedEvaluations := 0
	opts := DefaultSearchOptions()
	opts.Limit = 20
	opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
		if failFilter && node.ID == "src00" {
			failedEvaluations++
			return false, filterErr
		}
		return true, nil
	}
	embed := func(context.Context, string) ([]float32, error) { return []float32{1,0}, nil }
	page, err := svc.SearchTextContinuation(t.Context(), "walrus", opts, SearchContinuationRequest{Owner: "reader", N: 10}, nil, embed, svc.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.Len(t, page.Results, 10)
	page, err = svc.SearchTextContinuation(t.Context(), "", nil, SearchContinuationRequest{Owner: "reader", QID: page.QID, N: 10}, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, page.HasMore)
	// Model a later graph/property change causing a valid filter to error,
	// and an ordinary transient vector failure on the expansion that sees it.
	failFilter = true
	generator.failNext = true
	page, err = svc.SearchTextContinuation(t.Context(), "", nil, SearchContinuationRequest{Owner: "reader", QID: page.QID, N: 10}, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.Greater(t, failedEvaluations, 0)
	t.Logf("page=%+v, error=%v, failed evaluations=%d", page, err, failedEvaluations)
	require.ErrorIs(t, err, filterErr)
	require.Nil(t, page)
}
