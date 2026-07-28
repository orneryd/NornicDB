package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestOAuthProvider_Authorize(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	tests := []struct {
		name           string
		queryParams    map[string]string
		wantStatus     int
		wantContains   string
		wantNotContain string
	}{
		{
			name: "valid authorization request",
			queryParams: map[string]string{
				"response_type": "code",
				"client_id":     "test-client",
				"redirect_uri":  "http://localhost:3000/callback",
				"state":         "test-state",
			},
			wantStatus:   http.StatusOK,
			wantContains: "Local OAuth Provider",
		},
		{
			name: "invalid response_type",
			queryParams: map[string]string{
				"response_type": "token",
				"client_id":     "test-client",
				"redirect_uri":  "http://localhost:3000/callback",
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "Unsupported response_type",
		},
		{
			name: "invalid client_id",
			queryParams: map[string]string{
				"response_type": "code",
				"client_id":     "wrong-client",
				"redirect_uri":  "http://localhost:3000/callback",
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "Invalid client_id",
		},
		{
			name: "missing redirect_uri",
			queryParams: map[string]string{
				"response_type": "code",
				"client_id":     "test-client",
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "redirect_uri is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/oauth2/v1/authorize", nil)
			q := req.URL.Query()
			for k, v := range tt.queryParams {
				q.Set(k, v)
			}
			req.URL.RawQuery = q.Encode()

			w := httptest.NewRecorder()
			provider.handleAuthorize(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("handleAuthorize() status = %d, want %d", w.Code, tt.wantStatus)
			}

			body := w.Body.String()
			if tt.wantContains != "" && !strings.Contains(body, tt.wantContains) {
				t.Errorf("handleAuthorize() body does not contain %q", tt.wantContains)
			}
			if tt.wantNotContain != "" && strings.Contains(body, tt.wantNotContain) {
				t.Errorf("handleAuthorize() body contains %q", tt.wantNotContain)
			}
		})
	}
}

func TestValidateLocalRedirectURI(t *testing.T) {
	for _, test := range []struct {
		name    string
		uri     string
		wantErr bool
	}{
		{name: "localhost http", uri: "http://localhost:3000/callback"},
		{name: "localhost https", uri: "https://app.localhost/callback"},
		{name: "loopback", uri: "http://127.0.0.1:7474/callback"},
		{name: "external host", uri: "https://example.com/callback", wantErr: true},
		{name: "non HTTP scheme", uri: "javascript:alert(1)", wantErr: true},
		{name: "userinfo", uri: "https://user@localhost/callback", wantErr: true},
		{name: "fragment", uri: "http://localhost/callback#token", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateLocalRedirectURI(test.uri)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateLocalRedirectURI(%q) error = %v, wantErr %t", test.uri, err, test.wantErr)
			}
		})
	}
}

func TestOAuthProvider_ConsentRejectsExternalRedirect(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")
	form := url.Values{
		"client_id":    {"test-client"},
		"redirect_uri": {"https://example.com/callback"},
		"state":        {"test-state"},
		"user_id":      {"user-001"},
	}
	req := httptest.NewRequest(http.MethodPost, "/oauth2/v1/authorize/consent", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()

	provider.handleConsent(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("handleConsent status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if location := w.Header().Get("Location"); location != "" {
		t.Fatalf("unexpected redirect location: %q", location)
	}
}

func TestOAuthProvider_Consent(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	redirectURI := "http://localhost:3000/callback"
	state := "test-state-123"

	// First, get authorization page to ensure we have valid params
	req := httptest.NewRequest(http.MethodGet, "/oauth2/v1/authorize", nil)
	q := req.URL.Query()
	q.Set("response_type", "code")
	q.Set("client_id", "test-client")
	q.Set("redirect_uri", redirectURI)
	q.Set("state", state)
	req.URL.RawQuery = q.Encode()

	w := httptest.NewRecorder()
	provider.handleAuthorize(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Failed to get authorization page: %d", w.Code)
	}

	// Now submit consent
	form := url.Values{}
	form.Set("client_id", "test-client")
	form.Set("redirect_uri", redirectURI)
	form.Set("state", state)
	form.Set("user_id", "user-001")

	req = httptest.NewRequest(http.MethodPost, "/oauth2/v1/authorize/consent", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w = httptest.NewRecorder()
	provider.handleConsent(w, req)

	if w.Code != http.StatusFound {
		t.Errorf("handleConsent() status = %d, want %d", w.Code, http.StatusFound)
	}

	location := w.Header().Get("Location")
	if location == "" {
		t.Fatal("handleConsent() missing Location header")
	}

	redirectURL, err := url.Parse(location)
	if err != nil {
		t.Fatalf("Invalid redirect URL: %v", err)
	}

	code := redirectURL.Query().Get("code")
	if code == "" {
		t.Error("handleConsent() missing authorization code")
	}

	returnedState := redirectURL.Query().Get("state")
	if returnedState != state {
		t.Errorf("handleConsent() state = %q, want %q", returnedState, state)
	}

	// Verify code was stored
	provider.mu.RLock()
	authCode, exists := provider.authCodes[code]
	provider.mu.RUnlock()

	if !exists {
		t.Error("Authorization code not stored")
	}

	if authCode.UserID != "user-001" {
		t.Errorf("AuthCode.UserID = %q, want %q", authCode.UserID, "user-001")
	}

	if authCode.RedirectURI != redirectURI {
		t.Errorf("AuthCode.RedirectURI = %q, want %q", authCode.RedirectURI, redirectURI)
	}
}

func TestOAuthProvider_TokenExchange(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	// Create an authorization code
	code, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("Failed to generate code: %v", err)
	}

	redirectURI := "http://localhost:3000/callback"
	provider.mu.Lock()
	provider.authCodes[code] = &AuthCode{
		ClientID:    "test-client",
		RedirectURI: redirectURI,
		UserID:      "user-001",
		ExpiresAt:   time.Now().Add(10 * time.Minute),
	}
	provider.mu.Unlock()

	// Exchange code for token
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirectURI)
	form.Set("client_id", "test-client")
	form.Set("client_secret", "test-secret")

	req := httptest.NewRequest(http.MethodPost, "/oauth2/v1/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	provider.handleToken(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleToken() status = %d, want %d. Body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var response map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	accessToken, ok := response["access_token"].(string)
	if !ok || accessToken == "" {
		t.Error("Missing or invalid access_token in response")
	}

	if response["token_type"] != "Bearer" {
		t.Errorf("token_type = %v, want Bearer", response["token_type"])
	}

	// Verify code was deleted (one-time use)
	provider.mu.RLock()
	_, exists := provider.authCodes[code]
	provider.mu.RUnlock()

	if exists {
		t.Error("Authorization code should be deleted after use")
	}

	// Verify access token was stored
	provider.mu.RLock()
	tokenData, exists := provider.accessTokens[accessToken]
	provider.mu.RUnlock()

	if !exists {
		t.Error("Access token not stored")
	}

	if tokenData.UserID != "user-001" {
		t.Errorf("TokenData.UserID = %q, want %q", tokenData.UserID, "user-001")
	}
}

func TestOAuthProvider_TokenExchange_InvalidCode(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", "invalid-code")
	form.Set("redirect_uri", "http://localhost:3000/callback")
	form.Set("client_id", "test-client")
	form.Set("client_secret", "test-secret")

	req := httptest.NewRequest(http.MethodPost, "/oauth2/v1/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	provider.handleToken(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("handleToken() status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if response["error"] != "invalid_grant" {
		t.Errorf("Error = %q, want invalid_grant", response["error"])
	}
}

func TestOAuthProvider_TokenExchange_InvalidClient(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	code, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("Failed to generate code: %v", err)
	}

	provider.mu.Lock()
	provider.authCodes[code] = &AuthCode{
		ClientID:    "test-client",
		RedirectURI: "http://localhost:3000/callback",
		UserID:      "user-001",
		ExpiresAt:   time.Now().Add(10 * time.Minute),
	}
	provider.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "http://localhost:3000/callback")
	form.Set("client_id", "wrong-client")
	form.Set("client_secret", "wrong-secret")

	req := httptest.NewRequest(http.MethodPost, "/oauth2/v1/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	provider.handleToken(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("handleToken() status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if response["error"] != "invalid_client" {
		t.Errorf("Error = %q, want invalid_client", response["error"])
	}
}

func TestOAuthProvider_Userinfo(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	// Create an access token
	accessToken, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	provider.mu.Lock()
	provider.accessTokens[accessToken] = &AccessToken{
		UserID:    "user-001",
		ExpiresAt: time.Now().Add(1 * time.Hour),
	}
	provider.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/oauth2/v1/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)

	w := httptest.NewRecorder()
	provider.handleUserinfo(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleUserinfo() status = %d, want %d. Body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var userInfo map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&userInfo); err != nil {
		t.Fatalf("Failed to decode userinfo: %v", err)
	}

	if userInfo["sub"] != "user-001" {
		t.Errorf("userInfo.sub = %v, want user-001", userInfo["sub"])
	}

	if userInfo["email"] != "admin@localhost" {
		t.Errorf("userInfo.email = %v, want admin@localhost", userInfo["email"])
	}

	if userInfo["preferred_username"] != "admin" {
		t.Errorf("userInfo.preferred_username = %v, want admin", userInfo["preferred_username"])
	}

	// Verify password is not included
	if _, ok := userInfo["password"]; ok {
		t.Error("Password should not be included in userinfo")
	}
}

func TestOAuthProvider_Userinfo_InvalidToken(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	req := httptest.NewRequest(http.MethodGet, "/oauth2/v1/userinfo", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")

	w := httptest.NewRecorder()
	provider.handleUserinfo(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("handleUserinfo() status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if response["error"] != "invalid_token" {
		t.Errorf("Error = %q, want invalid_token", response["error"])
	}
}

func TestOAuthProvider_Discovery(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	w := httptest.NewRecorder()

	provider.handleDiscovery(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleDiscovery() status = %d, want %d", w.Code, http.StatusOK)
	}

	var discovery map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&discovery); err != nil {
		t.Fatalf("Failed to decode discovery: %v", err)
	}

	if discovery["issuer"] != "http://localhost:8888" {
		t.Errorf("Discovery.issuer = %v, want http://localhost:8888", discovery["issuer"])
	}

	if discovery["authorization_endpoint"] != "http://localhost:8888/oauth2/v1/authorize" {
		t.Errorf("Discovery.authorization_endpoint = %v, want http://localhost:8888/oauth2/v1/authorize", discovery["authorization_endpoint"])
	}

	if discovery["token_endpoint"] != "http://localhost:8888/oauth2/v1/token" {
		t.Errorf("Discovery.token_endpoint = %v, want http://localhost:8888/oauth2/v1/token", discovery["token_endpoint"])
	}
}

func TestOAuthProvider_Health(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	provider.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("handleHealth() status = %d, want %d", w.Code, http.StatusOK)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&health); err != nil {
		t.Fatalf("Failed to decode health: %v", err)
	}

	if health["status"] != "ok" {
		t.Errorf("Health.status = %v, want ok", health["status"])
	}

	if health["users"] != float64(3) { // JSON numbers are float64
		t.Errorf("Health.users = %v, want 3", health["users"])
	}
}

func TestOAuthProvider_TokenExpiration(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	// Create an expired access token
	accessToken, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("Failed to generate token: %v", err)
	}

	provider.mu.Lock()
	provider.accessTokens[accessToken] = &AccessToken{
		UserID:    "user-001",
		ExpiresAt: time.Now().Add(-1 * time.Hour), // Expired
	}
	provider.mu.Unlock()

	req := httptest.NewRequest(http.MethodGet, "/oauth2/v1/userinfo", nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)

	w := httptest.NewRecorder()
	provider.handleUserinfo(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("handleUserinfo() status = %d, want %d", w.Code, http.StatusUnauthorized)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if response["error"] != "invalid_token" {
		t.Errorf("Error = %q, want invalid_token", response["error"])
	}

	if !strings.Contains(response["error_description"], "expired") {
		t.Errorf("Error description should mention expiration")
	}
}

func TestOAuthProvider_RedirectURIMismatch(t *testing.T) {
	provider := NewOAuthProvider("test-client", "test-secret", "http://localhost:8888")

	code, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("Failed to generate code: %v", err)
	}

	provider.mu.Lock()
	provider.authCodes[code] = &AuthCode{
		ClientID:    "test-client",
		RedirectURI: "http://localhost:3000/callback",
		UserID:      "user-001",
		ExpiresAt:   time.Now().Add(10 * time.Minute),
	}
	provider.mu.Unlock()

	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", "http://localhost:9999/wrong") // Mismatch
	form.Set("client_id", "test-client")
	form.Set("client_secret", "test-secret")

	req := httptest.NewRequest(http.MethodPost, "/oauth2/v1/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	w := httptest.NewRecorder()
	provider.handleToken(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("handleToken() status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if response["error"] != "invalid_grant" {
		t.Errorf("Error = %q, want invalid_grant", response["error"])
	}

	if !strings.Contains(response["error_description"], "redirect_uri mismatch") {
		t.Errorf("Error description should mention redirect_uri mismatch")
	}
}

func TestGenerateRandomToken(t *testing.T) {
	token1, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("generateRandomToken() error = %v", err)
	}

	token2, err := generateRandomToken(32)
	if err != nil {
		t.Fatalf("generateRandomToken() error = %v", err)
	}

	if token1 == token2 {
		t.Error("generateRandomToken() generated duplicate tokens")
	}

	if len(token1) != 64 { // 32 bytes = 64 hex chars
		t.Errorf("Token length = %d, want 64", len(token1))
	}
}
