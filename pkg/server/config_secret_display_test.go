package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuthConfigSecretDisplay(t *testing.T) {
	const credential = "synthetic-auth-config-credential-314159"
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", credential)
	t.Setenv("NORNICDB_OAUTH_ISSUER", "http://user:"+credential+"@example.test")
	server := &Server{config: &Config{}}
	response := httptest.NewRecorder()
	server.handleAuthConfig(response, httptest.NewRequest(http.MethodGet, "http://nornic.test/auth/config", nil))
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"name":"oauth"`) {
		t.Fatal("configured auth handler was not exercised")
	}
	if strings.Contains(response.Body.String(), credential) {
		t.Error("auth config disclosed credential (output withheld)")
	}
}
