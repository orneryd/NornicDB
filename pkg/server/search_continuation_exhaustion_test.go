package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestHTTPContinuationDoesNotLoseFilteredMatchesAfterShortBatch(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	nodes := make([]*storage.Node, 600)
	for i := range nodes {
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("exhaustion-%03d", i)), Properties: map[string]any{"content": "searchable library transcript"}}
	}
	require.NoError(t, engine.BulkCreateNodes(nodes))
	rebuild := makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{"database": database}, token)
	require.Equal(t, http.StatusOK, rebuild.Code, rebuild.Body.String())
	require.Eventually(t, func() bool {
		status := server.db.GetDatabaseSearchStatus(database)
		return status.Ready && !status.Building
	}, 5*time.Second, 10*time.Millisecond)

	type hit struct {
		Node struct {
			ID string `json:"id"`
		} `json:"node"`
	}
	allResponse := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"database": database, "query": "transcript", "limit": 600}, token)
	require.Equal(t, http.StatusOK, allResponse.Code, allResponse.Body.String())
	var all []hit
	require.NoError(t, json.NewDecoder(allResponse.Body).Decode(&all))
	require.Len(t, all, 600)
	want := []string{all[1].Node.ID, all[500].Node.ID}
	for _, id := range want {
		node, err := engine.GetNode(storage.NodeID(id))
		require.NoError(t, err)
		node.Properties["eligible"] = "yes"
		require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: node}))
	}
	type page struct {
		Results []hit  `json:"results"`
		QID     string `json:"qid"`
		HasMore bool   `json:"has_more"`
	}
	start := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": database, "query": "transcript", "limit": 2, "n": 1,
		"filters": map[string][]string{"eligible": {"yes"}},
	}, token)
	require.Equal(t, http.StatusOK, start.Code, start.Body.String())
	var first page
	require.NoError(t, json.NewDecoder(start.Body).Decode(&first))
	require.Len(t, first.Results, 1)
	require.True(t, first.HasMore, "the second eligible match is deeper in the BM25 population")
	require.NotEmpty(t, first.QID)
	pull := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"database": database, "qid": first.QID, "n": 1}, token)
	require.Equal(t, http.StatusOK, pull.Code, pull.Body.String())
	var last page
	require.NoError(t, json.NewDecoder(pull.Body).Decode(&last))
	require.Len(t, last.Results, 1)
	require.False(t, last.HasMore)
	require.Empty(t, last.QID)
	require.ElementsMatch(t, want, []string{first.Results[0].Node.ID, last.Results[0].Node.ID})
	replay := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{"database": database, "qid": first.QID, "n": 1}, token)
	require.Equal(t, http.StatusGone, replay.Code, replay.Body.String())
}
