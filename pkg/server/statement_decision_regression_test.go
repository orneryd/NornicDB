package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestStatementHTTPUseTarget(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("other"))
	require.NoError(t, server.roleStore.CreateRole(ctx, "r2loader"))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "r2loader", []string{"read", "write"}))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "r2loader", []string{"nornic", "other"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "r2loader", "nornic", true, false))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "r2loader", "other", false, true))
	_, err := users.CreateUser("r2loader", "password123", []auth.Role{"r2loader"})
	require.NoError(t, err)
	token := getAuthToken(t, users, "r2loader")
	for _, prefix := range []string{"USE other ", ":USE other\n", "/* comment */ USE other ", "EXPLAIN USE other ", "PROFILE USE other ", "\xef\xbb\xbf USE other "} {
		t.Run(prefix, func(t *testing.T) {
			body, raw := systemProcedureResponse(t, server, token, "nornic", prefix+"CREATE (:R2)", false)
			t.Logf("response=%s", raw)
			require.Empty(t, body.Errors)
		})
	}
	body, _ := systemProcedureResponse(t, server, token, "other", ":USE other\nCREATE (:R2)", true)
	require.Empty(t, body.Errors)
	body, _ = systemProcedureResponse(t, server, token, "nornic", ":USE other\nCREATE (:RejectedSelection)", true)
	require.NotEmpty(t, body.Errors)
	require.Equal(t, "Neo.ClientError.Statement.AccessMode", body.Errors[0].Code)
	for database, want := range map[string]int{"nornic": 0, "other": 6} {
		store, err := server.dbManager.GetStorage(database)
		require.NoError(t, err)
		nodes, err := store.GetNodesByLabel("R2")
		require.NoError(t, err)
		require.Len(t, nodes, want)
		nodes, err = store.GetNodesByLabel("RejectedSelection")
		require.NoError(t, err)
		require.Empty(t, nodes)
	}
	for _, prefix := range []string{"USE other ", ":USE other\n"} {
		body, _ := systemProcedureResponse(t, server, token, "nornic", prefix+"CREATE (n:SelectedEntity) RETURN n, elementId(n) AS eid", false)
		require.Empty(t, body.Errors)
		require.Len(t, body.Results, 1)
		require.Len(t, body.Results[0].Data, 1)
		row := body.Results[0].Data[0]
		require.Contains(t, row.Row[1], "4:other:")
		require.Equal(t, row.Row[1], row.Meta[0].(map[string]interface{})["elementId"])
	}
}

func TestStatementHTTPFabricFragmentRequirements(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	for _, database := range []string{"r2source", "r2target"} {
		require.NoError(t, server.dbManager.CreateDatabase(database))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase("r2cmp", []multidb.ConstituentRef{
		{Alias: "source", DatabaseName: "r2source", Type: "local", AccessMode: "read_write"},
		{Alias: "target", DatabaseName: "r2target", Type: "local", AccessMode: "read_write"},
	}))
	source, err := server.dbManager.GetStorage("r2source")
	require.NoError(t, err)
	_, err = cypher.NewStorageExecutor(source).Execute(ctx, "CREATE (:R2Source {v: 1})", nil)
	require.NoError(t, err)
	require.NoError(t, server.roleStore.CreateRole(ctx, "r2fabric"))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "r2fabric", []string{"read", "write"}))
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "r2fabric", []string{"r2cmp", "r2source", "r2target"}))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "r2fabric", "r2cmp", true, false))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "r2fabric", "r2source", true, false))
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "r2fabric", "r2target", false, true))
	_, err = users.CreateUser("r2fabric", "password123", []auth.Role{"r2fabric"})
	require.NoError(t, err)
	token := getAuthToken(t, users, "r2fabric")
	query := "CALL { USE r2cmp.source MATCH (n:R2Source) RETURN n.v AS v } CALL { USE r2cmp.target WITH v CREATE (n:R2Loaded {v: v}) RETURN n.v AS loaded } RETURN loaded"
	exec, err := server.executorCache.Get("r2cmp")
	require.NoError(t, err)
	claims := &auth.JWTClaims{Roles: []string{"r2fabric"}}
	embedded, embeddedErr := exec.Execute(cypher.WithDatabasePermissionResolver(ctx, "r2cmp", server.principalAccess(claims).Allows), query, nil)
	t.Logf("embedded=%v err=%v", embedded, embeddedErr)
	require.NoError(t, embeddedErr)
	require.Equal(t, [][]interface{}{{int64(1)}}, embedded.Rows)
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "explicit"}[explicit], func(t *testing.T) {
			for _, prefix := range []string{"", "USE r2cmp ", ":USE r2cmp\n"} {
				body, raw := systemProcedureResponse(t, server, token, "r2cmp", prefix+query, explicit)
				t.Logf("response=%s", raw)
				require.Empty(t, body.Errors)
				require.Len(t, body.Results, 1)
				require.Len(t, body.Results[0].Data, 1)
				require.EqualValues(t, 1, body.Results[0].Data[0].Row[0])
			}
		})
	}
}
