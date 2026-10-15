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

	for _, statement := range []string{
		"CREATE (:Loaded {n: 1})",
		"CREATE (a:Loaded {n: 2})-[:NEXT]->(b:Loaded {n: 3}) SET a.done = true",
		"UNWIND [4, 5] AS n CREATE (:Loaded {n: n})",
	} {
		require.Empty(t, txErrorCode(t, server, "nornic", token, statement), statement)
	}
	for _, statement := range []string{
		"MATCH (n:Loaded) RETURN n.n AS n",
		"MATCH (n:Loaded) SET n.seen = true",
		"MERGE (:Loaded {n: 1})",
		"CREATE (:Loaded {count: COUNT { (n:Loaded) }})",
	} {
		response := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": statement}}}, "Bearer "+token)
		require.Contains(t, response.Body.String(), "Neo.ClientError.Security.Forbidden", statement)
		require.Contains(t, response.Body.String(), "Read operations require read permission", statement)
	}
	nornic, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	loaded, err := nornic.GetNodesByLabel("Loaded")
	require.NoError(t, err)
	require.Len(t, loaded, 5)
}
