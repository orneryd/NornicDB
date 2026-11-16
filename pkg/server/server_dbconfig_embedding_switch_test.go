package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestDatabaseManagedEmbeddingSwitchAppliesWithoutRestart(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	for _, enabled := range []string{"false", "true"} {
		response := makeRequest(t, server, http.MethodPut, "/admin/databases/nornic/config", map[string]any{"overrides": map[string]string{"db.nornic.embedding.enabled": enabled}}, "Bearer "+token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var body map[string]any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
		require.Equal(t, enabled, body["effective"].(map[string]any)["db.nornic.embedding.enabled"])
		require.Equal(t, false, body["pendingRestart"])
	}
	reader := getAuthToken(t, authenticator, "reader")
	forbidden := makeRequest(t, server, http.MethodPatch, "/admin/databases/nornic/config", map[string]any{"overrides": map[string]string{"db.nornic.embedding.enabled": "false"}}, "Bearer "+reader)
	require.Equal(t, http.StatusForbidden, forbidden.Code)
	require.Equal(t, "true", server.dbConfigStore.GetOverrides("nornic")["db.nornic.embedding.enabled"])

}

func TestDatabaseManagedEmbeddingPatchPreservesOverridesAndRestart(t *testing.T) {
	directory := t.TempDir()
	before, authenticator, database := openPersistentDBConfigTestServer(t, directory)
	token := getAuthToken(t, authenticator, "admin")
	require.NoError(t, before.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "images"}))
	require.NoError(t, before.dbConfigStore.SetOverrides(t.Context(), "nornic", map[string]string{"db.nornic.embedding.api.key": "synthetic-preserved-key", "db.nornic.embedding.enabled": "true"}))
	require.NoError(t, before.dbConfigStore.Load(t.Context()))
	for _, testCase := range []struct{ name, value string }{{"nornic", "false"}, {"images", "true"}} {
		response := makeRequest(t, before, http.MethodPatch, "/admin/databases/"+testCase.name+"/config", map[string]any{"overrides": map[string]string{"db.nornic.embedding.enabled": testCase.value}}, "Bearer "+token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	}
	require.Equal(t, "synthetic-preserved-key", before.dbConfigStore.GetOverrides("nornic")["db.nornic.embedding.api.key"])
	require.Equal(t, "false", before.dbConfigStore.GetOverrides("nornic")["db.nornic.embedding.enabled"])
	require.Equal(t, "true", before.dbConfigStore.GetOverrides("images")["db.nornic.embedding.enabled"])
	denied := makeRequest(t, before, http.MethodPatch, "/admin/databases/nornic/config", map[string]any{"overrides": map[string]string{"db.nornic.embedding.enabled": "true"}}, "")
	require.Equal(t, http.StatusUnauthorized, denied.Code)
	require.NoError(t, before.Stop(context.Background()))
	require.NoError(t, database.Close())
	after, _, reopened := openPersistentDBConfigTestServer(t, directory)
	t.Cleanup(func() { _ = after.Stop(context.Background()); _ = reopened.Close() })
	var engine *storage.BadgerEngine
	for current := reopened.GetBaseStorageForManager(); current != nil; {
		if badger, ok := current.(*storage.BadgerEngine); ok {
			engine = badger
			break
		}
		wrapper, ok := current.(interface{ GetInnerEngine() storage.Engine })
		if !ok {
			break
		}
		current = wrapper.GetInnerEngine()
	}
	require.NotNil(t, engine)
	require.False(t, engine.IsDatabaseEmbeddingsEnabled("nornic"))
	require.True(t, engine.IsDatabaseEmbeddingsEnabled("images"))
}
