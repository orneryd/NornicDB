package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func BenchmarkSearchStreamHTTP(b *testing.B) {
	s, authenticator := setupTestServer(b)
	token := "Bearer " + getAuthToken(b, authenticator, "admin")
	database := s.dbManager.DefaultDatabaseName()
	store, err := s.dbManager.GetStorage(database)
	require.NoError(b, err)
	for _, id := range []storage.NodeID{"a", "b"} {
		_, err := store.CreateNode(&storage.Node{ID: id, Properties: map[string]any{"content": "library transcript"}})
		require.NoError(b, err)
	}
	svc, err := s.db.GetOrCreateSearchService(database, store)
	require.NoError(b, err)
	require.NoError(b, svc.BuildIndexes(b.Context()))
	b.Run("ordinary_search", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			response := makeRequest(b, s, http.MethodPost, "/nornicdb/search", map[string]any{"query": "transcript", "limit": 2}, token)
			require.Equal(b, http.StatusOK, response.Code)
		}
	})
	b.Run("read_to_end_and_discard", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			response := makeRequest(b, s, http.MethodPost, "/nornicdb/search", map[string]any{"mode": "id", "n": 1}, token)
			require.Equal(b, http.StatusOK, response.Code)
			var page struct {
				QID string `json:"qid"`
			}
			require.NoError(b, json.Unmarshal(response.Body.Bytes(), &page))
			require.NotEmpty(b, page.QID)
			for _, request := range []map[string]any{{"qid": page.QID, "n": 1}, {"qid": page.QID, "discard": true}} {
				response = makeRequest(b, s, http.MethodPost, "/nornicdb/search", request, token)
				require.Equal(b, http.StatusOK, response.Code, response.Body.String())
			}
		}
	})
}
