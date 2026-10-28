package server

import (
	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/multidb"

	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
					var page struct {
						Results []any  `json:"results"`
						QID     string `json:"qid"`
						HasMore bool   `json:"has_more"`
					}
					require.NoError(t, json.Unmarshal(response.Body.Bytes(), &page))
					count += len(page.Results)
					if !page.HasMore {
						break
					}
					request = map[string]any{"qid": page.QID, "n": 1}
				}
				require.Equal(t, 3, count)
			}
		})
	}
}

type disconnectedSearchWriter struct{ *httptest.ResponseRecorder }

func (w disconnectedSearchWriter) Write(data []byte) (int, error) {
	_, _ = w.ResponseRecorder.Write(data)
	return 0, io.ErrClosedPipe
}

func TestSearchStreamLifecycleHTTPEnds(t *testing.T) {
	for _, end := range []string{"last_page", "discard", "write", "backfill", "disconnect", "drop"} {
		t.Run(end, func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, authenticator, "admin")
			const database = "stream_lifecycle"
			require.NoError(t, s.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: database}))
			engine, err := s.dbManager.GetStorage(database)
			require.NoError(t, err)
			for _, id := range []storage.NodeID{"a", "b"} {
				_, err := engine.CreateNode(&storage.Node{ID: id})
				require.NoError(t, err)
			}
			var first struct {
				QID string `json:"qid"`
			}
			if end == "disconnect" {
				req := httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader(`{"database":"stream_lifecycle","mode":"id","n":1}`))
				req.Header.Set("Authorization", token)
				req.Header.Set("Content-Type", "application/json")
				writer := disconnectedSearchWriter{httptest.NewRecorder()}
				s.buildRouter().ServeHTTP(writer, req)
				require.NoError(t, json.Unmarshal(writer.Body.Bytes(), &first))
			} else {
				response := makeRequest(t, s, http.MethodPost, "/nornicdb/search", map[string]any{"database": database, "mode": "id", "n": 1}, token)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &first))
			}
			require.NotEmpty(t, first.QID)
			switch end {
			case "last_page":
				response := makeRequest(t, s, http.MethodPost, "/nornicdb/search", map[string]any{"qid": first.QID, "n": 1}, token)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			case "write":
				_, err := engine.CreateNode(&storage.Node{ID: "c"})
				require.NoError(t, err)
			case "backfill":
				node, err := engine.GetNode("b")
				require.NoError(t, err)
				node.ChunkEmbeddings = [][]float32{{1, 0}}
				require.NoError(t, engine.UpdateNode(node))
			case "drop":
				require.NoError(t, (&databaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(s.dbManager), db: s.db, server: s}).DropDatabase(t.Context(), database))
			}
			if end == "write" || end == "backfill" || end == "disconnect" {
				response := makeRequest(t, s, http.MethodPost, "/nornicdb/search", map[string]any{"qid": first.QID, "n": 1}, token)
				require.Equal(t, http.StatusGone, response.Code, response.Body.String())
			}
			for i := 0; i < 2; i++ {
				response := makeRequest(t, s, http.MethodPost, "/nornicdb/search", map[string]any{"qid": first.QID, "discard": true}, token)
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				var released struct {
					Released bool `json:"released"`
				}
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &released))
				require.True(t, released.Released)
			}
		})
	}
}
