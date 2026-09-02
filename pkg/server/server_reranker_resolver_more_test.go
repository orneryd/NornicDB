package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestNew_PerDatabaseRerankerResolverBranches(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cfg := DefaultConfig()
	cfg.MCPEnabled = false
	cfg.EmbeddingEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		HeimdallEnabled:      false,
		SearchRerankEnabled:  true,
		SearchRerankProvider: "ollama",
		SearchRerankAPIURL:   "",
		SearchRerankModel:    "",
	}

	server, err := New(db, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, server.dbConfigStore)
	t.Cleanup(func() { _ = server.Stop(context.Background()) })

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)

	// enabled=false hot reload should rebuild with no reranker.
	response := makeRequest(t, server, http.MethodPut, "/admin/databases/"+dbName+"/config", map[string]any{
		"overrides": map[string]string{"NORNICDB_SEARCH_RERANK_ENABLED": "false"},
	}, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var body map[string]any
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.Equal(t, true, body["rebuildTriggered"])
	service, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	require.Empty(t, service.RerankerName())

	// local provider path with nil global resolver.
	require.NoError(t, server.dbConfigStore.SetOverrides(context.Background(), dbName, map[string]string{
		"NORNICDB_SEARCH_RERANK_ENABLED":  "true",
		"NORNICDB_SEARCH_RERANK_PROVIDER": "local",
	}))
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	// External ollama hot reload uses canonical effective keys and installs a cross encoder.
	response = makeRequest(t, server, http.MethodPut, "/admin/databases/"+dbName+"/config", map[string]any{
		"overrides": map[string]string{
			"NORNICDB_SEARCH_RERANK_ENABLED":  "true",
			"NORNICDB_SEARCH_RERANK_PROVIDER": "ollama",
			"NORNICDB_SEARCH_RERANK_API_URL":  "http://localhost:11434/rerank",
			"NORNICDB_SEARCH_RERANK_MODEL":    "reranker",
			"NORNICDB_SEARCH_RERANK_API_KEY":  "test-key",
		},
	}, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	service, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	require.Equal(t, "cross_encoder", service.RerankerName())

	// Second call exercises cached external reranker path.
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
}
