package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestHTTPContinuationIssuedDepthCompletion(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	claims, err := authenticator.ValidateToken(token)
	require.NoError(t, err)
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	service, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
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
	first, err := service.SearchTextContinuation(context.Background(), "transcript", &search.SearchOptions{Limit: 10, MaxCandidateLimit: 1000}, search.SearchContinuationRequest{Owner: auth.PrincipalID(claims), Database: database, N: 1}, nil, nil, producer, search.ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	require.True(t, first.HasMore)
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"database": database, "qid": first.QID, "n": 20}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var page struct {
		HasMore    bool   `json:"has_more"`
		Completion string `json:"completion"`
		Results    []any  `json:"results"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&page))
	require.False(t, page.HasMore)
	require.Equal(t, search.SearchContinuationCandidateComplete, page.Completion)
	require.Len(t, page.Results, 4)
	require.Equal(t, []int{10, 20, 1000}, depths)
}
