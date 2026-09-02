package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMCPHTTPAuthorizationResolvesScopeBeforeDatabaseAccess(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("tenant_a"))
	require.NoError(t, server.dbManager.CreateDatabase("private"))
	require.NoError(t, server.dbManager.CreateAlias("primary", "tenant_a"))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{"tenant_a"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", "tenant_a", true, false))
	token := getAuthToken(t, authenticator, "reader")

	var executorCalls int
	var storageCalls int
	var resolvedDatabase string
	server.mcpServer.SetDatabaseScopedExecutor(func(database string) (*cypher.StorageExecutor, func(context.Context, string) (*nornicdb.Node, error), error) {
		executorCalls++
		resolvedDatabase = database
		return nil, nil, nil
	})
	server.mcpServer.SetDatabaseScopedStorage(func(database string) (storage.Engine, error) {
		storageCalls++
		resolvedDatabase = database
		return nil, nil
	})
	router := server.buildRouter()

	callTool := func(body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/mcp/tools/call", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		return recorder
	}
	resetSpies := func() {
		executorCalls = 0
		storageCalls = 0
		resolvedDatabase = ""
	}

	for _, database := range []string{"private", "system", "unknown"} {
		t.Run("denies "+database+" before resolver", func(t *testing.T) {
			resetSpies()
			response := callTool(`{"name":"recall","arguments":{"database":"` + database + `","id":"node-1"}}`)

			require.Equal(t, http.StatusOK, response.Code)
			require.Contains(t, response.Body.String(), `"isError":true`)
			require.Zero(t, executorCalls)
			require.Zero(t, storageCalls)
		})
	}

	t.Run("read-only principal cannot dispatch write", func(t *testing.T) {
		resetSpies()
		response := callTool(`{"name":"store","arguments":{"database":"primary","content":"blocked"}}`)

		require.Equal(t, http.StatusOK, response.Code)
		require.Contains(t, response.Body.String(), `"isError":true`)
		require.Zero(t, executorCalls)
		require.Zero(t, storageCalls)
	})

	t.Run("authorized alias reaches canonical database", func(t *testing.T) {
		resetSpies()
		response := callTool(`{"name":"recall","arguments":{"database":" PRIMARY ","id":"node-1"}}`)

		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, 1, executorCalls)
		require.Zero(t, storageCalls)
		require.Equal(t, "tenant_a", resolvedDatabase)
	})

	t.Run("omission selects sole authorized standard database", func(t *testing.T) {
		resetSpies()
		response := callTool(`{"name":"recall","arguments":{"id":"node-1"}}`)

		require.Equal(t, http.StatusOK, response.Code)
		require.Equal(t, 1, executorCalls)
		require.Zero(t, storageCalls)
		require.Equal(t, "tenant_a", resolvedDatabase)
	})
}
