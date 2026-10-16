package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A principal's read and write on a database come from one resolver
// (auth.ResolveDatabaseAccess): the global entitlements an administrator
// stored for its roles gate them, and the privileges matrix decides within
// them. HTTP, GraphQL and Bolt (TestBoltReadAndWriteFromTheProductionResolver)
// give the same answers.
func TestDatabaseAccessFollowsStoredEntitlements(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := "Bearer " + getAuthToken(t, users, "admin")
	for user, role := range map[string]auth.Role{"ed": auth.RoleEditor, "pat": "plain", "ann": "analyst"} {
		if role != auth.RoleEditor {
			require.NoError(t, server.roleStore.CreateRole(ctx, string(role)))
		}
		_, err := users.CreateUser(user, "password123", []auth.Role{role})
		require.NoError(t, err)
	}
	ed := getAuthToken(t, users, "ed")
	require.Empty(t, txErrorCode(t, server, "nornic", ed, "CREATE (:W {n: 1})"))

	// An editor demoted to read writes nothing, on HTTP and GraphQL, and
	// still reads.
	response := makeRequest(t, server, http.MethodPut, "/auth/role-entitlements", map[string]any{"role": "editor", "entitlements": []string{"read"}}, admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, auth.ResolvedAccess{Read: true}, server.GetResolvedAccessForRoles([]string{"editor"}, "nornic"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", ed, "CREATE (:W {n: 2})"))
	mutation := `mutation Execute($statement: String!) {
		executeCypher(input: {statement: $statement}) { rowCount }
	}`
	graphQL := executeGraphQLAuthorizationRequest(t, server, ed, mutation, "CREATE (:W {n: 3})")
	require.NotEmpty(t, graphQL.Errors)
	require.Contains(t, graphQL.Errors[0].Message, "write permission")
	rows, code := statementRows(t, server, "nornic", ed, "MATCH (n:W) RETURN count(n) AS c")
	require.Empty(t, code)
	require.Equal(t, [][]any{{float64(1)}}, rows)

	// A read privilege without the read entitlement reads nothing.
	require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "plain", "nornic", true, false))
	response = makeRequest(t, server, http.MethodPost, "/db/nornic/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "MATCH (n) RETURN count(n) AS c"}}}, "Bearer "+getAuthToken(t, users, "pat"))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.Equal(t, auth.ResolvedAccess{}, server.GetResolvedAccessForRoles([]string{"plain"}, "nornic"))

	// A custom role with the read entitlement and no privilege entry reads,
	// and doesn't write.
	response = makeRequest(t, server, http.MethodPut, "/auth/role-entitlements", map[string]any{"role": "analyst", "entitlements": []string{"read"}}, admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	ann := getAuthToken(t, users, "ann")
	require.Empty(t, txErrorCode(t, server, "nornic", ann, "MATCH (n) RETURN count(n) AS c"))
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", ann, "CREATE (:W)"))
}

// grantedAccess is the access the privileges matrix records for roles on
// database (auth.PrivilegesStore.Granted), zero when it records none.
func grantedAccess(store *auth.PrivilegesStore, roles []string, database string) auth.ResolvedAccess {
	access, _ := store.Granted(roles, database)
	return access
}
