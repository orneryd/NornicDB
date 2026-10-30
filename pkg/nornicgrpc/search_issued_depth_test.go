package nornicgrpc

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	gen "github.com/orneryd/nornicdb/pkg/nornicgrpc/gen"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSearchTextContinuationIssuedDepthCompletion(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "nornic")
	service := search.NewService(engine, search.DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 1000; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%04d", i)), Properties: map[string]any{"content": "library transcript"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	var depths []int
	producer := func(_ context.Context, _ string, _ []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
		depths = append(depths, opts.Limit)
		results := make([]search.SearchResult, 5)
		for i := range results {
			id := fmt.Sprintf("doc-%04d", i)
			results[i] = search.SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(10 - i)}
		}
		return &search.SearchResponse{Results: results, SearchMethod: "rrf_hybrid"}, nil
	}
	first, err := service.SearchTextContinuation(ctx, "transcript", &search.SearchOptions{Limit: 10, MaxCandidateLimit: 1000}, search.SearchContinuationRequest{Owner: "alice", Database: "nornic", N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	rpc, err := NewService(Config{DefaultDatabase: "nornic", OwnerFromContext: func(context.Context) string { return "alice" }}, nil, nil, service)
	require.NoError(t, err)
	last, err := rpc.SearchText(ctx, &gen.SearchTextRequest{Qid: first.QID, N: 20})
	require.NoError(t, err)
	require.False(t, last.HasMore)
	require.Equal(t, search.SearchContinuationCandidateComplete, last.Completion)
	require.Len(t, last.Hits, 4)
	require.Equal(t, []int{10, 20, 1000}, depths)
}
