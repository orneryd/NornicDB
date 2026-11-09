package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Pin the complete public collection/item contract while their handling moves
// into one resource owner. Existing role atomicity tests retain cascade and
// storage-failure coverage for these same routed operations.
func TestRoleHTTPResourceContract(t *testing.T) {
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	router := server.buildRouter()
	request := func(method, path, body, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Language", "en")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		return response
	}
	for _, test := range []struct {
		method, path, token string
		status              int
	}{
		{http.MethodGet, "/auth/roles", "", http.StatusUnauthorized},
		{http.MethodPost, "/auth/roles", reader, http.StatusForbidden},
		{http.MethodPatch, "/auth/roles/denied", reader, http.StatusForbidden},
		{http.MethodDelete, "/auth/roles/denied", reader, http.StatusForbidden},
		{http.MethodGet, "/auth/roles/", reader, http.StatusForbidden},
	} {
		response := request(test.method, test.path, `{"name":"denied"}`, test.token)
		require.Equal(t, test.status, response.Code, response.Body.String())
	}
	require.False(t, server.roleStore.Exists("denied"), "rejected requests cannot mutate role state")
	for _, path := range []string{"/auth/roles", "/auth/roles/", "/auth/roles/%20%20"} {
		response := request(http.MethodGet, path, "", admin)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		require.JSONEq(t, `["admin","editor","viewer"]`, response.Body.String())
	}
	for _, test := range []struct{ method, path, message string }{
		{http.MethodPatch, "/auth/roles", "GET or POST required"},
		{http.MethodDelete, "/auth/roles/", "GET or POST required"},
		{http.MethodGet, "/auth/roles/admin", "PATCH or DELETE required"},
		{http.MethodPost, "/auth/roles/admin", "PATCH or DELETE required"},
	} {
		response := request(test.method, test.path, `{"name":"unused"}`, admin)
		require.Equal(t, http.StatusMethodNotAllowed, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), test.message)
	}
	for _, test := range []struct{ method, path, body string }{
		{http.MethodPost, "/auth/roles", "{"},
		{http.MethodPatch, "/auth/roles/missing", "{"},
		{http.MethodPost, "/auth/roles", `{"name":" "}`},
		{http.MethodPatch, "/auth/roles/missing", `{"name":" "}`},
	} {
		response := request(test.method, test.path, test.body, admin)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "Neo.ClientError.Request.InvalidFormat")
	}
	response := request(http.MethodPost, "/auth/roles", `{"name":" Role_MiXeD "}`, admin)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	require.JSONEq(t, `{"name":"role_mixed"}`, response.Body.String(), "POST echoes lowercased input while storage normalizes its role_ prefix")
	require.True(t, server.roleStore.Exists("mixed"))
	response = request(http.MethodPost, "/auth/roles", `{"name":"mixed"}`, admin)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "role already exists")
	response = request(http.MethodPost, "/auth/roles", `{"name":"role_"}`, admin)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "Neo.ClientError.Request.InvalidFormat")
	response = request(http.MethodPatch, "/auth/roles/mixed", `{"name":"role_"}`, admin)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "new role name already exists")
	response = request(http.MethodPatch, "/auth/roles/missing", `{"name":"new"}`, admin)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "Neo.ClientError.General.SchemaViolation")
	response = request(http.MethodPatch, "/auth/roles/mixed", `{"name":" Role_ReNamed "}`, admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, `{"name":"Role_ReNamed"}`, response.Body.String(), "PATCH preserves trimmed input casing in its response")
	require.False(t, server.roleStore.Exists("mixed"))
	require.True(t, server.roleStore.Exists("renamed"))
	response = request(http.MethodDelete, "/auth/roles/renamed", "", admin)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.JSONEq(t, `{"status":"deleted"}`, response.Body.String())
	require.False(t, server.roleStore.Exists("renamed"))
	for _, method := range []string{http.MethodPatch, http.MethodDelete} {
		response = request(method, "/auth/roles/admin", `{"name":"new"}`, admin)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "Neo.ClientError.General.SchemaViolation")
	}
	require.True(t, server.roleStore.Exists("admin"))
	server.roleStore = nil
	for _, test := range []struct{ path, message string }{
		{"/auth/roles", "Roles are not configured (auth disabled or system DB unavailable)."},
		{"/auth/roles/", "Roles are not configured (auth disabled or system DB unavailable)."},
		{"/auth/roles/admin", "Roles are not configured."},
	} {
		response = request(http.MethodGet, test.path, "", admin)
		require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), test.message)
	}
}
