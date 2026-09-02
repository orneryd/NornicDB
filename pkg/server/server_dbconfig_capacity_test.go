package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdminPutStaticCapacityReportsPendingRestart(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.memory.index.vector.max": "2m",
		}}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, true, body["pendingRestart"])
	require.Equal(t, false, body["rebuildTriggered"])
	require.Equal(t, "2097152", body["configured"].(map[string]any)["db.nornic.memory.index.vector.max"])
	require.Equal(t, "0", body["effective"].(map[string]any)["db.nornic.memory.index.vector.max"])

	response = makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.memory.index.vector.max": "-1",
		}}, "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, response.Code)
}

func TestAdminPutDynamicSearchCacheDoesNotRebuild(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.search_result_cache.max_entries": "12",
			"db.nornic.query_cache.ttl":                 "2m",
		}}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, false, body["pendingRestart"])
	require.Equal(t, false, body["rebuildTriggered"])
}
