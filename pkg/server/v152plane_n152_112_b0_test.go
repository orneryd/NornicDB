package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// This is a reproduction of docs/compliance/rbac.md's Session Management
// promise that logout blacklists the token and later requests reject it. It
// deliberately distinguishes an expired browser cookie from bearer revocation.
// Tokens are synthetic, held only in this fixture, and never logged.
func TestNOR115DocumentedLogoutRevokesBearerToken(t *testing.T) {
	server, _ := setupTestServer(t)
	login := func(username string) string {
		t.Helper()
		response := makeRequest(t, server, http.MethodPost, "/auth/token", map[string]string{
			"username": username, "password": "password123",
		}, "")
		require.Equal(t, http.StatusOK, response.Code)
		var payload struct {
			AccessToken string `json:"access_token"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
		if payload.AccessToken == "" {
			t.Fatal("fixture login returned no access token")
		}
		return payload.AccessToken
	}
	loggedOut := login("admin")
	unrelated := login("reader")
	for _, token := range []string{loggedOut, unrelated} {
		before := makeRequest(t, server, http.MethodGet, "/auth/me", nil, "Bearer "+token)
		require.Equal(t, http.StatusOK, before.Code, "both fixture principals authenticate before logout")
	}
	noAuth := makeRequest(t, server, http.MethodGet, "/auth/me", nil, "")
	require.Equal(t, http.StatusUnauthorized, noAuth.Code)

	logout := makeRequest(t, server, http.MethodPost, "/auth/logout", nil, "Bearer "+loggedOut)
	require.Equal(t, http.StatusOK, logout.Code)
	expiredCookie := false
	for _, cookie := range logout.Result().Cookies() {
		if cookie.Name == "nornicdb_token" && cookie.Value == "" && cookie.MaxAge < 0 {
			expiredCookie = true
		}
	}
	require.True(t, expiredCookie, "logout must expire its browser session cookie")
	other := makeRequest(t, server, http.MethodGet, "/auth/me", nil, "Bearer "+unrelated)
	require.Equal(t, http.StatusOK, other.Code, "one principal's logout must preserve another principal's authentication")

	// A fresh request carries only the same bearer, not a remembered cookie.
	after := makeRequest(t, server, http.MethodGet, "/auth/me", nil, "Bearer "+loggedOut)
	if after.Code != http.StatusUnauthorized {
		t.Errorf("documented logout revocation: reused bearer returns HTTP %d, want HTTP %d; cookie expiration control passed", after.Code, http.StatusUnauthorized)
	}
}
