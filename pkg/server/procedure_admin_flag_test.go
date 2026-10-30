package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A procedure needs an administrator exactly when SHOW PROCEDURES lists it
// as admin (its Admin flag), as in Neo4j: db.stats.* and the other admin
// procedures are refused to a reader with Neo.ClientError.Security.Forbidden
// and run for an administrator, while the public DBMS procedures run for
// anyone.
func TestAdminProceduresFollowTheirAdminFlagOverHTTP(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	adminOnly := []string{
		"CALL db.stats.collect('QUERIES')",
		"CALL db.stats.retrieve('QUERIES')",
		"CALL db.stats.status()",
		"CALL db.stats.stop('QUERIES')",
		"CALL db.stats.clear('QUERIES')",
		"CALL db.stats.retrieveAllAnTheStats()",
		"CALL db.clearQueryCaches()",
		"CALL dbms.listConfig()",
		"CALL db.txlog.entries()",
	}
	for _, statement := range adminOnly {
		require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", reader, statement), statement)
		require.Empty(t, txErrorCode(t, server, "nornic", admin, statement), statement)
	}
	for _, statement := range []string{
		"CALL dbms.components()",
		"CALL dbms.info()",
		"CALL dbms.listConnections()",
		"CALL dbms.procedures()",
	} {
		require.Empty(t, txErrorCode(t, server, "nornic", reader, statement), statement)
	}
}

// An API token needs the admin entitlement, whatever the role holding it is
// called: a custom role with the admin entitlement may create one, a reader
// may not. The route's guard is the one decision (the handler makes none),
// so the reader is refused with the guard's answer and gets no token.
func TestAPITokenNeedsTheAdminEntitlement(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	require.NoError(t, server.roleStore.CreateRole(ctx, "ops"))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "ops", []string{"read", "admin"}))
	_, err := users.CreateUser("opsuser", "password123", []auth.Role{"ops"})
	require.NoError(t, err)
	response := makeRequest(t, server, "POST", "/auth/api-token", map[string]any{"subject": "svc"}, "Bearer "+getAuthToken(t, users, "opsuser"))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "token\"")
	response = makeRequest(t, server, "POST", "/auth/api-token", map[string]any{"subject": "svc"}, "Bearer "+getAuthToken(t, users, "reader"))
	require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "Neo.ClientError.Security.Forbidden")
	require.NotContains(t, response.Body.String(), "token\"")
}
