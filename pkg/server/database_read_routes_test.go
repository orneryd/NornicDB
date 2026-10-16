package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// The search and similarity routes read a database directly, so they need
// the principal's read privilege on that database, as the graph routes and
// Cypher do: a role that may use the database and holds the global read
// entitlement, but whose access to the database is write without read, is
// refused with Forbidden; a role with read on it is not.
func TestDirectReadRoutesRequireDatabaseReadPrivilege(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	server.allowlistStore = auth.NewAllowlistStore(system)
	server.privilegesStore = auth.NewPrivilegesStore(system)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	tokens := map[string]string{}
	for _, access := range []struct {
		role string
		read bool
	}{
		{"searcher", true},
		{"loader", false},
	} {
		require.NoError(t, server.roleStore.CreateRole(ctx, access.role))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, access.role, []string{"nornic"}))
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, access.role, "nornic", access.read, true))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, access.role, []string{"read", "write"}))
		_, err := users.CreateUser(access.role+"1", "password123", []auth.Role{auth.Role(access.role)})
		require.NoError(t, err)
		tokens[access.role] = "Bearer " + getAuthToken(t, users, access.role+"1")
	}

	for _, route := range []struct {
		path string
		body map[string]any
	}{
		{"/nornicdb/search", map[string]any{"database": "nornic", "query": "anything"}},
		{"/nornicdb/search", map[string]any{"query": "anything"}},
		{"/nornicdb/similar", map[string]any{"database": "nornic", "node_id": "missing"}},
		{"/nornicdb/similar", map[string]any{"node_id": "missing"}},
	} {
		denied := makeRequest(t, server, http.MethodPost, route.path, route.body, tokens["loader"])
		require.Equal(t, http.StatusForbidden, denied.Code, "%s %v: %s", route.path, route.body, denied.Body.String())
		require.Contains(t, denied.Body.String(), "Neo.ClientError.Security.Forbidden", route.path)

		allowed := makeRequest(t, server, http.MethodPost, route.path, route.body, tokens["searcher"])
		require.NotEqual(t, http.StatusForbidden, allowed.Code, "%s %v: %s", route.path, route.body, allowed.Body.String())
	}
}
