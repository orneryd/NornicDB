package server

import (
	"context"
	"encoding/json"
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

// Personal tokens preserve the authenticated identity and never confer admin.
func TestAPITokensPreserveTheAuthenticatedUsersPermissions(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	require.NoError(t, server.roleStore.CreateRole(ctx, "token-only"))
	require.NoError(t, server.roleEntitlementsStore.Set(ctx, "token-only", []string{}))
	_, err := users.CreateUser(ctx, "tokenuser", "password123", []auth.Role{"token-only"})
	require.NoError(t, err)
	for _, username := range []string{"reader", "tokenuser", "admin"} {
		t.Run(username, func(t *testing.T) {
			originalToken := getAuthToken(t, users, username)
			originalClaims, err := users.ValidateToken(originalToken)
			require.NoError(t, err)
			response := makeRequest(t, server, http.MethodPost, "/auth/api-token", map[string]any{"subject": "admin", "expires_in": "1d", "username": "admin", "roles": []string{"admin"}}, "Bearer "+originalToken)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			var body struct {
				Token string `json:"token"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
			claims, err := users.ValidateToken(body.Token)
			require.NoError(t, err)
			require.Equal(t, username, claims.Username)
			require.Equal(t, originalClaims.Sub, claims.Sub)
			require.ElementsMatch(t, originalClaims.Roles, claims.Roles)
			require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodGet, "/auth/me", nil, "Bearer "+body.Token).Code)
			if username != "admin" {
				require.Equal(t, http.StatusForbidden, makeRequest(t, server, http.MethodPost, "/admin/backup", map[string]any{"path": "must-not-be-created"}, "Bearer "+body.Token).Code)
			}
			if username == "tokenuser" {
				require.Equal(t, http.StatusForbidden, makeRequest(t, server, http.MethodGet, "/status", nil, "Bearer "+body.Token).Code)
			}
		})
	}
	require.Equal(t, http.StatusUnauthorized, makeRequest(t, server, http.MethodPost, "/auth/api-token", map[string]any{"subject": "test"}, "").Code)
}
