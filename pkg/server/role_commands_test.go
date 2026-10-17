package server

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// userHoldsRole reports whether the user store gives user the role.
func userHoldsRole(t *testing.T, users *auth.Authenticator, user, role string) bool {
	t.Helper()
	stored, err := users.GetUser(user)
	require.NoError(t, err)
	for _, held := range stored.Roles {
		if auth.NormalizeRoleName(string(held)) == role {
			return true
		}
	}
	return false
}

// The Cypher role commands run through the server's role owner, the one the
// HTTP role API uses: CREATE ROLE adds to the role store, GRANT ROLE gives a
// user the role, SHOW ROLES WITH USERS lists it, RENAME and DROP move or
// remove the user's assignment with the role; an administrator alone may
// run them, on any database.
func TestCypherRoleCommandsUseTheServerRoleOwner(t *testing.T) {
	server, users := setupTestServer(t)
	require.NotNil(t, server.roleStore)
	_, err := users.CreateUser("roleadmin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	_, err = users.CreateUser("member", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	admin := getAuthToken(t, users, "roleadmin")

	require.Empty(t, txErrorCode(t, server, "system", admin, "CREATE ROLE agent"))
	require.True(t, server.roleStore.Exists("agent"))
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", txErrorCode(t, server, "system", admin, "CREATE ROLE agent"))
	require.Empty(t, txErrorCode(t, server, "nornic", admin, "GRANT ROLE agent TO member"))
	require.True(t, userHoldsRole(t, users, "member", "agent"))

	body, _ := systemProcedureResponse(t, server, admin, "system", "SHOW ROLES WITH USERS YIELD role, member WHERE role = 'agent'", false)
	require.Empty(t, body.Errors)
	require.Len(t, body.Results, 1)
	require.Len(t, body.Results[0].Data, 1)
	require.Equal(t, []interface{}{"agent", "member"}, body.Results[0].Data[0].Row)

	// A non-administrator may run none of them.
	member := getAuthToken(t, users, "member")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "system", member, "CREATE ROLE other"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "system", member, "SHOW ROLES"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "system", member, "REVOKE ROLE agent FROM member"))
	require.False(t, server.roleStore.Exists("other"))
	require.True(t, userHoldsRole(t, users, "member", "agent"))

	require.Empty(t, txErrorCode(t, server, "system", admin, "RENAME ROLE agent TO agents"))
	require.False(t, server.roleStore.Exists("agent"))
	require.True(t, userHoldsRole(t, users, "member", "agents"))
	require.Empty(t, txErrorCode(t, server, "system", admin, "DROP ROLE agents"))
	require.False(t, server.roleStore.Exists("agents"))
	require.False(t, userHoldsRole(t, users, "member", "agents"))
	require.Equal(t, "Neo.ClientError.Statement.ArgumentError", txErrorCode(t, server, "system", admin, "DROP ROLE admin"))
	require.True(t, userHoldsRole(t, users, "member", string(auth.RoleViewer)))
}
