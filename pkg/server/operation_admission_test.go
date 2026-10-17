package server

import (
	"context"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

func TestHTTPDirectOperationContractsAndScope(t *testing.T) {
	expected := map[string]httpOperation{
		"decay.info":     {cypher.PrivilegeRead, false},
		"embed.trigger":  {cypher.PrivilegeRead | cypher.PrivilegeWrite, true},
		"embed.stats":    {cypher.PrivilegeRead, true},
		"embed.failures": {cypher.PrivilegeRead, true},
		"embed.retry":    {cypher.PrivilegeRead | cypher.PrivilegeWrite, true},
		"embed.target":   {cypher.PrivilegeWrite, false},
		"search.rebuild": {cypher.PrivilegeRead | cypher.PrivilegeWrite, false},
		"gdpr.export":    {cypher.PrivilegeRead, false},
		"gdpr.delete":    {cypher.PrivilegeRead | cypher.PrivilegeWrite, false},
		"status":         {cypher.PrivilegeRead, true},
	}
	require.Equal(t, expected, httpOperationClasses)
	s, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, s.dbManager.CreateDatabase("direct_other"))
	for _, role := range []string{"direct_blocked", "direct_partial"} {
		require.NoError(t, s.roleStore.CreateRole(ctx, role))
		require.NoError(t, s.roleEntitlementsStore.Set(ctx, role, []string{"read", "write", "admin", "delete"}))
		require.NoError(t, s.allowlistStore.SaveRoleDatabases(ctx, role, []string{"nornic", "direct_other"}))
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "nornic", role == "direct_partial", role == "direct_partial"))
		require.NoError(t, s.privilegesStore.SavePrivilege(ctx, role, "direct_other", false, false))
		_, err := users.CreateUser(role, "password123", []auth.Role{auth.Role(role)})
		require.NoError(t, err)
	}
	for _, tc := range []struct {
		path, method string
		body         any
		global       bool
	}{
		{"/nornicdb/decay", http.MethodGet, nil, false},
		{"/nornicdb/embed/trigger", http.MethodPost, nil, true},
		{"/nornicdb/embed/stats", http.MethodGet, nil, true},
		{"/nornicdb/embed/failures", http.MethodGet, nil, true},
		{"/nornicdb/embed/retry-failures", http.MethodPost, map[string]any{}, true},
		{"/nornicdb/search/rebuild", http.MethodPost, map[string]any{"database": "nornic"}, false},
		{"/nornicdb/embed/clear", http.MethodPost, map[string]any{"databases": []string{"nornic"}}, false},
		{"/gdpr/export", http.MethodPost, map[string]any{"user_id": "unimportant"}, false},
		{"/gdpr/delete", http.MethodPost, map[string]any{"user_id": "unimportant", "confirm": true}, false},
		{"/status", http.MethodGet, nil, true},
	} {
		t.Run(tc.path, func(t *testing.T) {
			roles := []string{"direct_blocked"}
			if tc.global {
				roles = append(roles, "direct_partial")
			}
			for _, role := range roles {
				before := s.db.GetDatabaseSearchStatus("nornic")
				response := makeRequest(t, s, tc.method, tc.path, tc.body, "Bearer "+getAuthToken(t, users, role))
				require.Equal(t, http.StatusForbidden, response.Code, response.Body.String())
				require.Contains(t, response.Body.String(), "Neo.ClientError.Security.Forbidden")
				require.Equal(t, before, s.db.GetDatabaseSearchStatus("nornic"))
			}
		})
	}
}

func TestHTTPAccountOperationsNeedAuthenticationWithoutGraphRead(t *testing.T) {
	s, users := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, s.roleStore.CreateRole(ctx, "account_only"))
	require.NoError(t, s.roleEntitlementsStore.Set(ctx, "account_only", []string{}))
	_, err := users.CreateUser("account_only", "password123", []auth.Role{"account_only"})
	require.NoError(t, err)
	token := "Bearer " + getAuthToken(t, users, "account_only")
	for _, tc := range []struct {
		method, path string
		body         any
	}{
		{http.MethodGet, "/auth/me", nil},
		{http.MethodGet, "/auth/entitlements", nil},
		{http.MethodPut, "/auth/profile", map[string]any{"email": "account@example.invalid"}},
		{http.MethodPost, "/auth/password", map[string]any{"old_password": "password123", "new_password": "password456"}},
	} {
		require.Equal(t, http.StatusUnauthorized, makeRequest(t, s, tc.method, tc.path, tc.body, "").Code, tc.path)
		response := makeRequest(t, s, tc.method, tc.path, tc.body, token)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	}
	require.Equal(t, http.StatusForbidden, makeRequest(t, s, http.MethodGet, "/auth/users", nil, token).Code)
	user, err := users.GetUser("account_only")
	require.NoError(t, err)
	require.Equal(t, "account@example.invalid", user.Email)
}
