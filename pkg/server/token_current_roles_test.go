package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A token issued while its user held a role stops carrying the role when
// the role is deleted (which detaches it from the user): a role created
// again under the same name and given the admin entitlement doesn't reach
// the old token, which Neo4j's immediate privilege changes also guarantee.
func TestTokenLosesDeletedRoleOverHTTP(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
	_, err := users.CreateUser("bob", "password123", []auth.Role{"ops"})
	require.NoError(t, err)
	bob := getAuthToken(t, users, "bob")

	response := makeRequest(t, server, "DELETE", "/auth/roles/ops", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	response = makeRequest(t, server, "POST", "/auth/roles", map[string]any{"name": "ops"}, "Bearer "+admin)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	response = makeRequest(t, server, "PUT", "/auth/role-entitlements", map[string]any{"role": "ops", "entitlements": []string{"admin"}}, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	response = makeRequest(t, server, "GET", "/auth/users", nil, "Bearer "+bob)
	require.Equal(t, http.StatusForbidden, response.Code, "the old token carries no role: %s", response.Body.String())
}

// Role-record writes over HTTP require the role: entitlements, a database
// allowlist and the privilege matrix for a role that doesn't exist (here,
// deleted) are refused with SchemaViolation, and a batch applies all of
// its entries or none.
func TestRoleRecordWritesOverHTTP(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
	response := makeRequest(t, server, "DELETE", "/auth/roles/ops", nil, "Bearer "+admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())

	for _, request := range []struct {
		path string
		body any
	}{
		{"/auth/role-entitlements", map[string]any{"role": "ops", "entitlements": []string{"admin"}}},
		{"/auth/access/databases", map[string]any{"role": "ops", "databases": []string{"system"}}},
		{"/auth/access/databases", map[string]any{"mappings": []map[string]any{{"role": "viewer", "databases": []string{"nornic"}}, {"role": "ops", "databases": []string{"system"}}}}},
		{"/auth/access/privileges", []map[string]any{{"role": "ops", "database": "nornic", "read": true, "write": true}}},
	} {
		response := makeRequest(t, server, "PUT", request.path, request.body, "Bearer "+admin)
		require.Equal(t, http.StatusBadRequest, response.Code, "%s: %s", request.path, response.Body.String())
		require.Contains(t, response.Body.String(), "Neo.ClientError.General.SchemaViolation", request.path)
	}
	response = makeRequest(t, server, "POST", "/auth/roles", map[string]any{"name": "ops"}, "Bearer "+admin)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.Empty(t, server.roleEntitlementsStore.Get("ops"))
	_, listed := server.allowlistStore.Allowlist()["ops"]
	require.False(t, listed)

	// A batch with an invalid entry, or an entry for a missing role,
	// changes nothing.
	require.NoError(t, server.roleStore.CreateRole(ctx, "ops3"))
	for _, second := range []map[string]any{
		{"role": "viewer", "entitlements": []string{"invalid-entitlement"}},
		{"role": "missing", "entitlements": []string{"read"}},
	} {
		response = makeRequest(t, server, "PUT", "/auth/role-entitlements", map[string]any{"mappings": []map[string]any{
			{"role": "ops3", "entitlements": []string{"admin"}}, second,
		}}, "Bearer "+admin)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Empty(t, server.roleEntitlementsStore.Get("ops3"), "the batch changed nothing")
		reloaded := auth.NewRoleEntitlementsStore(mustSystemStorage(t, server))
		require.NoError(t, reloaded.Load(ctx))
		require.Empty(t, reloaded.Get("ops3"), "nothing was stored")
	}
}

// A role-record write that fails for another reason than the role is the
// server's error.
func TestRoleRecordWriteStorageFailureIsServerError(t *testing.T) {
	server, _ := setupTestServer(t)
	recorder := httptest.NewRecorder()
	server.writeRoleRecordError(recorder, httptest.NewRequest(http.MethodPut, "/auth/access/databases", nil), errors.New("storage unavailable"))
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.Contains(t, recorder.Body.String(), "Neo.ClientError.General.UnknownError")
}

func mustSystemStorage(t *testing.T, server *Server) storage.Engine {
	t.Helper()
	system, err := server.dbManager.GetStorage("system")
	require.NoError(t, err)
	return system
}
