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
	serverConfig.ProcessConfig = dbConfig
	server, err := New(database, authenticator, serverConfig)
	require.NoError(t, err)
	return server, authenticator, database
}

func TestAdminPutRestartSettingReportsPendingRestart(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.query_plan_cache.max_entries": "222",
		}}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, true, body["pendingRestart"])
	require.Equal(t, false, body["rebuildTriggered"])
	require.Equal(t, "222", body["configured"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])
	require.Equal(t, "500", body["effective"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])

	response = makeRequest(t, server, http.MethodGet, "/admin/databases/nornic/config", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, true, body["pendingRestart"])
	require.Equal(t, "222", body["configured"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])
	require.Equal(t, "500", body["effective"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])

	response = makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.query_plan_cache.max_entries": "-1",
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
			"db.nornic.search_result_cache.ttl":         "2m",
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

	response = makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{}}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	maxEntries, ttl = service.SearchResultCachePolicy()
	require.Equal(t, 1000, maxEntries)
	require.Equal(t, 5*time.Minute, ttl)
}

func TestAdminDatabaseConfigRedactsSecrets(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	const secret = "secret-api-key"

	response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.embedding.api.key": secret,
		}}, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), secret)
	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, "<REDACTED>", body["configured"].(map[string]any)["db.nornic.embedding.api.key"])

	response = makeRequest(t, server, http.MethodGet, "/admin/databases/nornic/config", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), secret)
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, "<REDACTED>", body["configured"].(map[string]any)["db.nornic.embedding.api.key"])
}

func TestAdminDatabaseConfigUsesInjectedProcessConfig(t *testing.T) {
	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	dbConfig.Memory.QueryCacheTTL = 17 * time.Minute
	database, err := nornicdb.Open("", dbConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{SecurityEnabled: false}, storage.NewMemoryEngine())
	require.NoError(t, err)
	serverConfig := DefaultConfig()
	serverConfig.ProcessConfig = dbConfig
	server, err := New(database, authenticator, serverConfig)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Stop(context.Background()) })

	response := makeRequest(t, server, http.MethodGet, "/admin/databases/nornic/config", nil, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, "1020000", body["effective"].(map[string]any)["db.nornic.query_cache.ttl"])
}

func TestNewExecutorForDatabaseUsesDatabaseQueryCacheTTL(t *testing.T) {
	server, _ := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, server.dbManager.CreateDatabase("fast-cache"))
	require.NoError(t, server.dbManager.CreateDatabase("slow-cache"))
	require.NoError(t, server.dbConfigStore.SetOverrides(ctx, "fast-cache", map[string]string{
		"db.nornic.query_cache.max_entries": "25",
		"db.nornic.query_cache.ttl":         "2000",
	}))
	require.NoError(t, server.dbConfigStore.SetOverrides(ctx, "slow-cache", map[string]string{
		"db.nornic.query_cache.max_entries": "2500",
		"db.nornic.query_cache.ttl":         "1800000",
	}))

	fast, err := server.newExecutorForDatabase("fast-cache")
	require.NoError(t, err)
	slow, err := server.newExecutorForDatabase("slow-cache")
	require.NoError(t, err)

	fastMaxEntries, fastTTL := fast.QueryCachePolicy()
	slowMaxEntries, slowTTL := slow.QueryCachePolicy()
	require.Equal(t, 25, fastMaxEntries)
	require.Equal(t, 2*time.Second, fastTTL)
	require.Equal(t, 2500, slowMaxEntries)
	require.Equal(t, 30*time.Minute, slowTTL)
}

func TestAdminPutRestartSettingPersistsAcrossDatabaseReopen(t *testing.T) {
	dataDir := t.TempDir()
	serverBefore, authenticatorBefore, databaseBefore := openPersistentDBConfigTestServer(t, dataDir)
	tokenBefore := getAuthToken(t, authenticatorBefore, "admin")

	response := makeRequest(t, serverBefore, http.MethodPut, "/admin/databases/nornic/config",
		map[string]any{"overrides": map[string]string{
			"db.nornic.query_plan_cache.max_entries": "222",
		}}, "Bearer "+tokenBefore)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var putBody map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&putBody))
	require.Equal(t, true, putBody["pendingRestart"])
	require.Equal(t, "222", putBody["configured"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])
	require.Equal(t, "500", putBody["effective"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])

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
	require.Equal(t, "222", getBody["overrides"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])
	require.Equal(t, "222", getBody["effective"].(map[string]any)["db.nornic.query_plan_cache.max_entries"])
}
