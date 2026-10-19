package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestSearchContinuationPageSizeHTTP(t *testing.T) {
	server, auth := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, auth, "admin")
	response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"mode": "id", "n": 501,
	}, token)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "page_size_exceeded", body["request_status"])
	require.EqualValues(t, 500, body["max_page_size"])
}

func TestSearchContinuationPageSizeHTTPConfigured(t *testing.T) {
	for _, limit := range []int{500, 2, 600} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			t.Setenv("NORNICDB_SEARCH_CURSOR_MAX_PAGE_SIZE", fmt.Sprint(limit))
			cfg := nornicConfig.LoadFromEnv()
			cfg.Memory.SearchCursorMax = 8
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Database.AsyncWritesEnabled = false
			db, err := nornicdb.Open(t.TempDir(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			serverConfig := DefaultConfig()
			serverConfig.EmbeddingEnabled = false
			serverConfig.ProcessConfig = cfg
			server, err := New(db, nil, serverConfig)
			require.NoError(t, err)
			t.Cleanup(func() { stopTestServer(t, server) })
			store, err := server.dbManager.GetStorage(server.dbManager.DefaultDatabaseName())
			require.NoError(t, err)
			nodes := make([]*storage.Node, limit + 2)
			for i := range nodes {
				nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("page-%04d", i))}
			}
			require.NoError(t, store.BulkCreateNodes(nodes))
			request := func(input map[string]any, status int) map[string]any {
				response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", input, "")
				require.Equal(t, status, response.Code, response.Body.String())
				var body map[string]any
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
				return body
			}
			assertOversized := func(input map[string]any) {
				body := request(input, http.StatusBadRequest)
				require.Equal(t, "page_size_exceeded", body["request_status"])
				require.EqualValues(t, limit, body["max_page_size"])
			}
			assertOversized(map[string]any{"mode": "id", "n": limit + 1})
			first := request(map[string]any{"mode": "id", "n": 1}, http.StatusOK)
			require.EqualValues(t, limit, first["max_page_size"])
			assertOversized(map[string]any{"qid": first["qid"], "n": limit + 1})
			exact := request(map[string]any{"qid": first["qid"], "n": limit}, http.StatusOK)
			require.Len(t, exact["results"], limit)
			exact = request(map[string]any{"mode": "id", "n": limit}, http.StatusOK)
			require.Len(t, exact["results"], limit)
			bad := request(map[string]any{"qid": "bad-qid", "n": 1}, http.StatusBadRequest)
			require.Equal(t, "continuation_invalid", bad["request_status"])
			require.NotContains(t, bad, "max_page_size")
		})
	}
}
