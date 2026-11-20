package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestV152RCCLogoutValidationCause ends at the two named owners: logout runs,
// then the same bearer is passed directly to Authenticator.ValidateToken.
// The unrelated bearer control proves the authenticator remains operational.
func TestV152RCCLogoutValidationCause(t *testing.T) {
	server, _ := setupTestServer(t)
	login := func(username string) string {
		t.Helper()
		response := makeRequest(t, server, http.MethodPost, "/auth/token", map[string]string{"username": username, "password": "password123"}, "")
		require.Equal(t, http.StatusOK, response.Code)
		var payload struct {
			AccessToken string `json:"access_token"`
		}
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
		require.NotEmpty(t, payload.AccessToken)
		return payload.AccessToken
	}
	loggedOut, unrelated := login("admin"), login("reader")
	_, err := server.auth.ValidateToken(loggedOut)
	require.NoError(t, err, "pre-logout control")
	_, err = server.auth.ValidateToken(unrelated)
	require.NoError(t, err, "unrelated control before logout")

	logout := makeRequest(t, server, http.MethodPost, "/auth/logout", nil, "Bearer "+loggedOut)
	require.Equal(t, http.StatusOK, logout.Code)
	claims, validationErr := server.auth.ValidateToken(loggedOut)
	t.Logf("RCC_LOGOUT_TRIGGER logout=pkg/server/server_auth.go:75-85 validation=pkg/auth/auth.go:1193-1223 claims_nil=%v validation_error=%v", claims == nil, validationErr)
	assert.Error(t, validationErr, "the bearer used for logout must be rejected at the validation owner")
	assert.Nil(t, claims)

	otherClaims, otherErr := server.auth.ValidateToken(unrelated)
	t.Logf("RCC_LOGOUT_CONTROL unrelated_claims_nil=%v validation_error=%v", otherClaims == nil, otherErr)
	require.NoError(t, otherErr, "one logout must not revoke another bearer")
	require.NotNil(t, otherClaims)
}
