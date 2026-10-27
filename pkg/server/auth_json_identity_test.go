package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestHTTPLoginPreservesAdmittedJSONIdentity(t *testing.T) {
	server, users := setupTestServer(t)
	name := "http-user-" + string([]byte{0xed, 0xa0, 0x80})
	_, err := users.CreateUser(name, "synthetic-password", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/auth/token", strings.NewReader(`{"username":"http-user-\ud800","password":"synthetic-password","grant_type":"password"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code)
	var token auth.TokenResponse
	require.NoError(t, jsontext.Unmarshal(response.Body.Bytes(), &token))
	require.NotEmpty(t, token.AccessToken)
	claims, err := users.ValidateToken(token.AccessToken)
	require.NoError(t, err)
	require.Equal(t, name, claims.Username)
	require.Equal(t, []string{"viewer"}, claims.Roles)
}
