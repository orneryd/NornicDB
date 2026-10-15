package server

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// Over HTTP Basic, the old password stops working as soon as it is
// changed, though the server caches the password check.
func TestChangedPasswordStopsWorkingOverBasic(t *testing.T) {
	server, users := setupTestServer(t)
	basic := func(password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte("reader:"+password))
	}
	require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodGet, "/auth/me", nil, basic("password123")).Code)
	require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodGet, "/auth/me", nil, basic("password123")).Code, "cached")
	require.NoError(t, users.ChangePassword("reader", "password123", "newPassword456"))
	require.Equal(t, http.StatusUnauthorized, makeRequest(t, server, http.MethodGet, "/auth/me", nil, basic("password123")).Code)
	require.Equal(t, http.StatusOK, makeRequest(t, server, http.MethodGet, "/auth/me", nil, basic("newPassword456")).Code)
}
