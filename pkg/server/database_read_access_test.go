package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A role whose access to a database is write without read (its global
// entitlements hold read and write) runs a statement that only writes, as
// Neo4j runs CREATE for a principal with WRITE but no READ privilege; a
// statement that reads the graph (MATCH, MERGE, a COUNT { } subquery) is
// refused with the read message, since NornicDB can't hide the records a
// read would see.
func TestWriteOnlyDatabaseAccessRunsOnlyWrites(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleStore = auth.NewRoleStore(system)
	require.NoError(t, server.roleStore.CreateRole(ctx, "loader"))
	server.allowlistStore = auth.NewAllowlistStore(system)
	server.privilegesStore = auth.NewPrivilegesStore(system)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "loader", []string{"nornic"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "loader", "nornic", false, true))
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "loader", []string{"read", "write"}))
	_, err = users.CreateUser("loader1", "password123", []auth.Role{"loader"})
	require.NoError(t, err)
	token := getAuthToken(t, users, "loader1")

	for _, prefix := range []string{"", "USE nornic ", ":USE nornic\n"} {
		for _, statement := range []string{
			"CREATE (:Loaded {n: 1})",
			"CREATE (a:Loaded {n: 2})-[:NEXT]->(b:Loaded {n: 3}) SET a.done = true",
			"UNWIND [4, 5] AS n CREATE (:Loaded {n: n})",
		} {
			require.Empty(t, txErrorCode(t, server, "nornic", token, prefix+statement), prefix+statement)
		}
		for _, statement := range []string{
			"MATCH (n:Loaded) RETURN n.n AS n",
			"MATCH (n:Loaded) SET n.seen = true",
			"MERGE (:Loaded {n: 1})",
			"CREATE (:Loaded {count: COUNT { (n:Loaded) }})",
		} {
			response := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": prefix + statement}}}, "Bearer "+token)
			require.Contains(t, response.Body.String(), "Neo.ClientError.Security.Forbidden", statement)
			require.Contains(t, response.Body.String(), "Read operations require read permission", statement)
		}
	}
	nornic, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	loaded, err := nornic.GetNodesByLabel("Loaded")
	require.NoError(t, err)
	require.Len(t, loaded, 15)
}

func TestHTTPDatabaseSelectionRefusesWritesWithoutWriteAccess(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	server.roleEntitlementsStore = auth.NewRoleEntitlementsStore(system)
	server.roleStore = auth.NewRoleStore(system)
	for _, access := range []struct {
		role string
		read bool
	}{
		{"observer", true},
		{"blocked", false},
	} {
		require.NoError(t, server.roleStore.CreateRole(ctx, access.role))
		require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, access.role, []string{"nornic"}))
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, access.role, "nornic", access.read, false))
		require.NoError(t, server.roleEntitlementsStore.Set(ctx, access.role, []string{"read", "write"}))
		_, err := users.CreateUser(access.role, "password123", []auth.Role{auth.Role(access.role)})
		require.NoError(t, err)
		token := getAuthToken(t, users, access.role)
		for _, prefix := range []string{"", "USE nornic ", ":USE nornic\n"} {
			body, _ := systemProcedureResponse(t, server, token, "nornic", prefix+"CREATE (:DeniedSelection)", false)
			require.Len(t, body.Errors, 1, access.role+": "+prefix)
			require.Equal(t, "Neo.ClientError.Security.Forbidden", body.Errors[0].Code)
			require.Contains(t, body.Errors[0].Message, "Write operations require write permission")
			require.Empty(t, body.Results)
		}
	}
	nornic, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	nodes, err := nornic.GetNodesByLabel("DeniedSelection")
	require.NoError(t, err)
	require.Empty(t, nodes)
}
