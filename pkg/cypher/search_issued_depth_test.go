package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDbRetrieveContinuationIssuedDepthCompletion(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
	service := search.NewService(engine)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 1000; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%04d", i)), Properties: map[string]any{"content": "library transcript"}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	var depths []int
	producer := func(readerCtx context.Context, _ string, _ []float32, opts *search.SearchOptions) (*search.SearchResponse, error) {
		depths = append(depths, opts.Limit)
		results := make([]search.SearchResult, 5)
		for i := range results {
			id := fmt.Sprintf("doc-%04d", i)
			results[i] = search.SearchResult{ID: id, NodeID: storage.NodeID(id), Score: float64(10 - i)}
		}
		return &search.SearchResponse{Results: results, SearchMethod: "rrf_hybrid"}, nil
	}
	first, err := service.SearchTextContinuation(ctx, "transcript", &search.SearchOptions{Limit: 10, MaxCandidateLimit: 1000}, search.SearchContinuationRequest{Owner: searchContinuationOwner(ctx), Database: GetUseDatabaseFromContext(ctx), N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	exec := NewStorageExecutor(engine)
	exec.SetSearchService(service)
	result, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]any{"request": map[string]any{"qid": first.QID, "n": int64(20)}})
	require.NoError(t, err)
	page := result.Rows[0][0].(map[string]interface{})
	require.False(t, page["has_more"].(bool))
	require.Equal(t, search.SearchContinuationCandidateComplete, page["completion"])
	require.Len(t, page["results"], 4)
	require.Equal(t, []int{10, 20, 1000}, depths)
}
