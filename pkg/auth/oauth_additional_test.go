package auth

import (
	"encoding/json"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func newOAuthTestManager(t *testing.T, issuer string) *OAuthManager {
	t.Helper()
	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", issuer)
	t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "client")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "secret")
	t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost/callback")

	cfg := DefaultAuthConfig()
	cfg.JWTSecret = []byte("oauth-additional-test-secret-32bytes")
	auth, err := NewAuthenticator(cfg, fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)))
	if err != nil {
		t.Fatalf("new authenticator: %v", err)
	}
	return NewOAuthManager(auth)
}

func TestOAuthManager_ValidateStateExpiredAndCleanup(t *testing.T) {
	m := NewOAuthManager(nil)
	m.states["expired"] = time.Now().Add(-time.Second)
	m.cleanupExpiredStates()
	if _, ok := m.states["expired"]; ok {
		t.Fatalf("expected cleanup to remove expired state")
	}

	m.states["old"] = time.Now().Add(-time.Second)
	if err := m.ValidateState("old"); err == nil {
		t.Fatalf("expected expired state validation error")
	}
	if _, ok := m.states["old"]; ok {
		t.Fatalf("expired state should be deleted after validation")
	}
}

func TestOAuthManager_ExchangeCodeAndGetUserInfoErrors(t *testing.T) {
	mgr := newOAuthTestManager(t, "http://127.0.0.1:1")
	if _, err := mgr.ExchangeCode("code"); err == nil {
		t.Fatalf("expected exchange code network error")
	}
	if _, err := mgr.GetUserInfo("token"); err == nil {
		t.Fatalf("expected get user info network error")
	}
}

func TestOAuthManager_HandleCallbackAndRefreshFlow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/v1/token":
			_ = r.ParseForm()
			grant := r.Form.Get("grant_type")
			w.Header().Set("Content-Type", "application/json")
			if grant == "authorization_code" {
				_ = json.NewEncoder(w).Encode(OAuthTokenData{AccessToken: "at-1", TokenType: "Bearer", ExpiresIn: 3600, RefreshToken: "rt-1"})
				return
			}
			if grant == "refresh_token" {
				_ = json.NewEncoder(w).Encode(OAuthTokenData{AccessToken: "at-2", TokenType: "Bearer", ExpiresIn: 1200, RefreshToken: "rt-2"})
				return
			}
			w.WriteHeader(http.StatusBadRequest)
		case "/oauth2/v1/userinfo":
			authz := r.Header.Get("Authorization")
			if authz == "Bearer at-1" || authz == "Bearer at-2" || authz == "Bearer valid" {
				_ = json.NewEncoder(w).Encode(OAuthUserInfo{Sub: "sub-1", Email: "u@example.com", PreferredUsername: "oauthuser", Roles: []string{"developer"}})
				return
			}
			w.WriteHeader(http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	mgr := newOAuthTestManager(t, server.URL)

	state := "state-1"
	mgr.states[state] = time.Now().Add(time.Minute)
	user, token, expiry, err := mgr.HandleCallback("code-1", state)
	if err != nil {
		t.Fatalf("handle callback failed: %v", err)
	}
	if user.Username != "oauthuser" {
		t.Fatalf("expected oauthuser, got %q", user.Username)
	}
	if token == "" || expiry.Before(time.Now()) {
		t.Fatalf("expected non-empty token and future expiry")
	}
	claims, err := mgr.authenticator.ValidateToken(token)
	if err != nil {
		t.Fatalf("validate callback token: %v", err)
	}
	if claims.Sub == "" {
		t.Fatal("expected callback token to contain a user subject")
	}
	if _, err := mgr.authenticator.GetUserByID(claims.Sub); err != nil {
		t.Fatalf("callback token subject does not resolve to its OAuth user: %v", err)
	}

	existingState := "state-2"
	mgr.states[existingState] = time.Now().Add(time.Minute)
	_, existingToken, _, err := mgr.HandleCallback("code-2", existingState)
	if err != nil {
		t.Fatalf("handle callback for existing OAuth user failed: %v", err)
	}
	existingClaims, err := mgr.authenticator.ValidateToken(existingToken)
	if err != nil {
		t.Fatalf("validate existing-user callback token: %v", err)
	}
	if existingClaims.Sub != claims.Sub {
		t.Fatalf("existing OAuth user subject changed: got %q, want %q", existingClaims.Sub, claims.Sub)
	}

	oauthUser := &User{Username: "oauthuser", Metadata: map[string]string{
		"auth_method":         "oauth",
		"oauth_access_token":  "invalid",
		"oauth_token_expiry":  time.Now().Add(time.Hour).Format(time.RFC3339),
		"oauth_refresh_token": "rt-1",
	}}
	if err := mgr.ValidateOAuthToken(oauthUser); err != nil {
		t.Fatalf("validate oauth token with refresh should succeed: %v", err)
	}
	if oauthUser.Metadata["oauth_access_token"] != "at-2" {
		t.Fatalf("expected refreshed access token at-2, got %q", oauthUser.Metadata["oauth_access_token"])
	}

	oauthUser.Metadata["oauth_access_token"] = "valid"
	oauthUser.Metadata["oauth_token_expiry"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	if err := mgr.ValidateOAuthToken(oauthUser); err != nil {
		t.Fatalf("validate oauth token for valid token should succeed: %v", err)
	}
}

func TestOAuthManager_RefreshAndValidationErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/v1/token":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"invalid_grant"}`))
		case "/oauth2/v1/userinfo":
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer server.Close()

	mgr := newOAuthTestManager(t, server.URL)

	if err := mgr.ValidateOAuthToken(nil); err == nil {
		t.Fatalf("expected nil user validation error")
	}

	if err := mgr.RefreshOAuthToken(&User{}, "rt"); err == nil {
		t.Fatalf("expected refresh failure from non-200 response")
	}

	u := &User{Username: "x", Metadata: map[string]string{
		"auth_method":         "oauth",
		"oauth_access_token":  "bad",
		"oauth_token_expiry":  time.Now().Add(-time.Hour).Format(time.RFC3339),
		"oauth_refresh_token": "rt-bad",
	}}
	if err := mgr.ValidateOAuthToken(u); err == nil {
		t.Fatalf("expected validation failure when expired and refresh fails")
	}
}
