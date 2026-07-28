package server

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWithAuth_PrefersTokenOverBasic(t *testing.T) {
	server, authenticator := setupTestServer(t)
	handler := server.buildRouter()

	token := getAuthToken(t, authenticator, "admin")

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.AddCookie(&http.Cookie{Name: "nornicdb_token", Value: token})

	// Invalid Basic auth would fail if Basic is incorrectly prioritized.
	invalidBasic := base64.StdEncoding.EncodeToString([]byte("admin:wrongpassword"))
	req.Header.Set("Authorization", "Basic "+invalidBasic)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}
}

func TestWithAuth_BasicAuthSetsJWTTokenCookie(t *testing.T) {
	server, _ := setupTestServer(t)
	handler := server.buildRouter()

	validBasic := base64.StdEncoding.EncodeToString([]byte("admin:password123"))
	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	req.Header.Set("Authorization", "Basic "+validBasic)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want %d: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var tokenCookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == "nornicdb_token" && c.Value != "" {
			tokenCookie = c
			break
		}
	}
	if tokenCookie == nil {
		t.Fatalf("expected nornicdb_token cookie to be set")
	}
	if !tokenCookie.Secure {
		t.Fatal("expected nornicdb_token cookie to require HTTPS")
	}
}
