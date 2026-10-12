package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// POST /nornicdb/search with relationships: true returns matching
// relationships, each with its type and endpoints; it can't be continued, and
// is refused on a database without relationship search (#1065).
func TestHandleSearchRelationships(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	database := server.dbManager.DefaultDatabaseName()
	engine, err := server.dbManager.GetStorage(database)
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"rel-a", "rel-b"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}})
		require.NoError(t, err)
	}
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "rel-r", StartNode: "rel-a", EndNode: "rel-b", Type: "CITES", Properties: map[string]any{"reason": "zephyrine bridge"}}))
	request := map[string]any{"database": database, "query": "zephyrine", "relationships": true}

	disabled := makeRequest(t, server, http.MethodPost, "/nornicdb/search", request, token)
	require.Equal(t, http.StatusBadRequest, disabled.Code, disabled.Body.String())
	require.Contains(t, disabled.Body.String(), "NORNICDB_SEARCH_RELATIONSHIPS_ENABLED")

	svc, err := server.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()

	type hit struct {
		Node         *struct{} `json:"node"`
		Relationship struct {
			ID         string         `json:"id"`
			Type       string         `json:"type"`
			StartNode  string         `json:"startNode"`
			EndNode    string         `json:"endNode"`
			Properties map[string]any `json:"properties"`
		} `json:"relationship"`
		BM25Rank int `json:"bm25_rank"`
	}
	var hits []hit
	require.Eventually(t, func() bool {
		response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", request, token)
		hits = nil
		return response.Code == http.StatusOK && json.NewDecoder(response.Body).Decode(&hits) == nil && len(hits) == 1
	}, 5*time.Second, 20*time.Millisecond)
	require.Nil(t, hits[0].Node)
	require.Equal(t, "rel-r", hits[0].Relationship.ID)
	require.Equal(t, "CITES", hits[0].Relationship.Type)
	require.Equal(t, "rel-a", hits[0].Relationship.StartNode)
	require.Equal(t, "rel-b", hits[0].Relationship.EndNode)
	require.Equal(t, "zephyrine bridge", hits[0].Relationship.Properties["reason"])
	require.Equal(t, 1, hits[0].BM25Rank)

	request["n"] = 5
	continued := makeRequest(t, server, http.MethodPost, "/nornicdb/search", request, token)
	require.Equal(t, http.StatusBadRequest, continued.Code)
	require.True(t, strings.Contains(continued.Body.String(), "can't be continued") || strings.Contains(continued.Body.String(), "can\u0027t be continued"), continued.Body.String())
}
