package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
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

func TestDatabaseInfoRequiresDatabaseAndConstituentRead(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	for _, database := range []string{"named", "open_part", "closed_part"} {
		require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: database}))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "portfolio", []multidb.ConstituentRef{
		{Alias: "open", DatabaseName: "open_part", Type: "local", AccessMode: "read_write"},
		{Alias: "closed", DatabaseName: "closed_part", Type: "local", AccessMode: "read_write"},
	}))
	for _, database := range []string{"nornic", "named", "open_part", "closed_part"} {
		engine, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		_, err = engine.CreateNode(&storage.Node{ID: storage.NodeID(database + "-node"), Labels: []string{"Doc"}})
		require.NoError(t, err)
	}

	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	server.allowlistStore = auth.NewAllowlistStore(system)
	server.privilegesStore = auth.NewPrivilegesStore(system)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	databases := []string{"nornic", "named", "portfolio", "open_part", "closed_part"}
	tokens := map[string]string{}
	for _, role := range []string{"info_reader", "info_loader", "info_partial"} {
		require.NoError(t, server.roleStore.CreateRole(ctx, role))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, role, databases))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, role, []string{"read", "write"}))
		_, err = users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
		require.NoError(t, err)
		tokens[role] = "Bearer " + getAuthToken(t, users, role)
	}
	for _, database := range databases {
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "info_reader", database, true, true))
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "info_loader", database, false, true))
		read := database != "closed_part"
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "info_partial", database, read, true))
	}

	for _, database := range []string{"nornic", "named"} {
		denied := makeRequest(t, server, http.MethodGet, "/db/"+database, nil, tokens["info_loader"])
		require.Equal(t, http.StatusForbidden, denied.Code, "%s: %s", database, denied.Body.String())
		allowed := makeRequest(t, server, http.MethodGet, "/db/"+database, nil, tokens["info_reader"])
		require.Equal(t, http.StatusOK, allowed.Code, "%s: %s", database, allowed.Body.String())
		require.Contains(t, allowed.Body.String(), `"nodeCount":1`)
	}

	partial := makeRequest(t, server, http.MethodGet, "/db/portfolio", nil, tokens["info_partial"])
	require.Equal(t, http.StatusForbidden, partial.Code, partial.Body.String())
	allowedComposite := makeRequest(t, server, http.MethodGet, "/db/portfolio", nil, tokens["info_reader"])
	require.Equal(t, http.StatusOK, allowedComposite.Code, allowedComposite.Body.String())
	require.Contains(t, allowedComposite.Body.String(), `"nodeCount":2`)

	server.auth = nil
	for _, database := range []string{"nornic", "named", "portfolio"} {
		open := makeRequest(t, server, http.MethodGet, "/db/"+database, nil, "")
		require.Equal(t, http.StatusOK, open.Code, "%s: %s", database, open.Body.String())
	}
}
