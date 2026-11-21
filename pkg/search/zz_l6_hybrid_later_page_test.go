package search

// Lane L6 (closeout): G-hybrid-rrf, the later-page path of E53.21 that the
// earlier diagnostic did not log separately. Same fixture as
// TestReviewR2LaterPageLexicalFilterFailureDuringVectorRecovery; the search
// function handed to the continuation is wrapped so every Search call the
// later pages make is logged with its error, whether that error is the
// filter's, finalSearchError, and the response's fallback reason.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL6HybridLaterPageErrorPrecedence(t *testing.T) {
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
	opts := DefaultSearchOptions()
	opts.Limit = 20
	opts.Where = func(_ context.Context, node *storage.Node) (bool, error) {
		if failFilter && node.ID == "src00" {
			return false, filterErr
		}
		return true, nil
	}
	pageNo := 0
	wrapped := func(ctx context.Context, query string, embedding []float32, o *SearchOptions) (*SearchResponse, error) {
		resp, err := svc.Search(ctx, query, embedding, o)
		fallback := ""
		if resp != nil {
			fallback = fmt.Sprint(resp.FallbackReason)
		}
		t.Logf("L6_RRF_LATER page=%d Search err=%v isFilterErr=%v finalSearchError=%v fallback=%q vectorFailNextBefore=%v", pageNo, err, errors.Is(err, filterErr), finalSearchError(err), fallback, failFilter)
		return resp, err
	}
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	page, err := svc.SearchTextContinuation(t.Context(), "walrus", opts, SearchContinuationRequest{Owner: "reader", N: 10}, nil, embed, wrapped, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	pageNo = 1
	page, err = svc.SearchTextContinuation(t.Context(), "", nil, SearchContinuationRequest{Owner: "reader", QID: page.QID, N: 10}, nil, nil, nil, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	pageNo = 2
	failFilter = true
	generator.failNext = true
	page, err = svc.SearchTextContinuation(t.Context(), "", nil, SearchContinuationRequest{Owner: "reader", QID: page.QID, N: 10}, nil, nil, nil, ChunkedSearchErrorPolicy{})
	results := -1
	if page != nil {
		results = len(page.Results)
	}
	t.Logf("L6_RRF_LATER page=2 continuation err=%v isFilterErr=%v results=%d", err, errors.Is(err, filterErr), results)
	if !errors.Is(err, filterErr) {
		t.Errorf("CAUSE G-hybrid-rrf later page: the later page's Search takes the same vector-error-first return (search.go:5389-5393) and the filter error is lost")
	}
}
