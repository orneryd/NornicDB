package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func openPersistentDBConfigTestServer(t *testing.T, dataDir string) (*Server, *auth.Authenticator, *nornicdb.DB) {
	t.Helper()
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	dbConfig.Database.AsyncWritesEnabled = false
	database, err := nornicdb.Open(dataDir, dbConfig)
	require.NoError(t, err)

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewMemoryEngine())
	require.NoError(t, err)
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	server, err := New(database, authenticator, serverConfig)
	require.NoError(t, err)
	return server, authenticator, database
}

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
	service, err := server.db.GetOrCreateSearchService("nornic", nil)
	require.NoError(t, err)
	require.NotNil(t, service)

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

	maxEntries, ttl := service.SearchResultCachePolicy()
	require.Equal(t, 12, maxEntries)
	require.Equal(t, 2*time.Minute, ttl)
	reloadedService, err := server.db.GetOrCreateSearchService("nornic", nil)
	require.NoError(t, err)
	require.Same(t, service, reloadedService, "cache policy update must not rebuild the search service")
}

func TestAdminPutRestartSettingPersistsAcrossDatabaseReopen(t *testing.T) {
	dataDir := t.TempDir()
	serverBefore, authenticatorBefore, databaseBefore := openPersistentDBConfigTestServer(t, dataDir)
	tokenBefore := getAuthToken(t, authenticatorBefore, "admin")

	response := makeRequest(t, serverBefore, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.memory.index.vector.max": "2m",
		}}, "Bearer "+tokenBefore)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var putBody map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&putBody))
	require.Equal(t, true, putBody["pendingRestart"])
	require.Equal(t, "2097152", putBody["configured"].(map[string]any)["db.nornic.memory.index.vector.max"])
	require.Equal(t, "0", putBody["effective"].(map[string]any)["db.nornic.memory.index.vector.max"])

	require.NoError(t, serverBefore.Stop(context.Background()))
	require.NoError(t, databaseBefore.Close())

	serverAfter, authenticatorAfter, databaseAfter := openPersistentDBConfigTestServer(t, dataDir)
	t.Cleanup(func() {
		_ = serverAfter.Stop(context.Background())
		_ = databaseAfter.Close()
	})
	tokenAfter := getAuthToken(t, authenticatorAfter, "admin")
	response = makeRequest(t, serverAfter, http.MethodGet, "/admin/databases/nornic/config", nil, "Bearer "+tokenAfter)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var getBody map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&getBody))
	require.Equal(t, "2097152", getBody["overrides"].(map[string]any)["db.nornic.memory.index.vector.max"])
	require.Equal(t, "2097152", getBody["effective"].(map[string]any)["db.nornic.memory.index.vector.max"])
}
