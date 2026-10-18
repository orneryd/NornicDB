package server

import (
	"encoding/json"
	"net/http"
	"testing"

	
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchStreamLifecycleHTTP(t *testing.T) {
	s, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	database := s.dbManager.DefaultDatabaseName()
	engine, err := s.dbManager.GetStorage(database)
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "library transcript"}})
		require.NoError(t, err)
	}
	svc, err := s.db.GetOrCreateSearchService(database, engine)
	require.NoError(t, err)
	require.NoError(t, svc.BuildIndexes(t.Context()))
	for _, mode := range []string{"id", "ranked", "ranked_then_id"} {
		t.Run(mode, func(t *testing.T) {
			for i := 0; i < 65; i++ {
				request := map[string]any{"database": database, "mode": mode, "query": "transcript", "limit": 3, "n": 1}
				count := 0
				for {
					response := makeRequest(t, s, http.MethodPost, "/nornicdb/search", request, token)
					require.Equal(t, http.StatusOK, response.Code, response.Body.String())
					var page struct { Results []any `json:"results"`; QID string `json:"qid"`; HasMore bool `json:"has_more"` }
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
					count += len(page.Results)
					if !page.HasMore { break }
					request = map[string]any{"qid": page.QID, "n": 1}
				}
				require.Equal(t, 3, count)
			}
		})
	}
}
