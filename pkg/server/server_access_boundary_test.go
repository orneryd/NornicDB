package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestCORSWildcardDoesNotSendCredentials(t *testing.T) {
	// SECURITY TEST: When CORS origin is wildcard (*), we must NOT send
	// Access-Control-Allow-Credentials header to prevent CSRF attacks.
	tmpDir, err := os.MkdirTemp("", "nornicdb-cors-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server with wildcard CORS
	serverConfig := DefaultConfig()
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"*"}

	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://evil.com")
	req.Header.Set("Access-Control-Request-Method", "POST")

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// Should have wildcard origin
	if origin := recorder.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("expected wildcard origin, got %s", origin)
	}

	// CRITICAL: Should NOT have credentials header with wildcard
	if creds := recorder.Header().Get("Access-Control-Allow-Credentials"); creds != "" {
		t.Errorf("SECURITY VULNERABILITY: credentials header should NOT be sent with wildcard origin, got %s", creds)
	}
}

func TestCORSSpecificOriginAllowsCredentials(t *testing.T) {
	// When CORS has specific origins (not wildcard), credentials are safe to allow
	tmpDir, err := os.MkdirTemp("", "nornicdb-cors-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server with specific CORS origins
	serverConfig := DefaultConfig()
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"http://trusted.com", "http://localhost:3000"}

	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://trusted.com")
	req.Header.Set("Access-Control-Request-Method", "POST")

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// Should echo back the specific origin
	if origin := recorder.Header().Get("Access-Control-Allow-Origin"); origin != "http://trusted.com" {
		t.Errorf("expected trusted.com origin, got %s", origin)
	}

	// Should allow credentials for specific origins
	if creds := recorder.Header().Get("Access-Control-Allow-Credentials"); creds != "true" {
		t.Errorf("expected credentials=true for specific origin, got %s", creds)
	}
}

func TestCORSDisallowedOriginNoHeaders(t *testing.T) {
	// When origin is not in allowed list, no CORS headers should be sent
	tmpDir, err := os.MkdirTemp("", "nornicdb-cors-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server with specific CORS origins (not including evil.com)
	serverConfig := DefaultConfig()
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"http://trusted.com"}

	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://evil.com")
	req.Header.Set("Access-Control-Request-Method", "POST")

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// Should NOT have origin header for disallowed origins
	if origin := recorder.Header().Get("Access-Control-Allow-Origin"); origin != "" {
		t.Errorf("expected no origin header for disallowed origin, got %s", origin)
	}
}

func TestIPRateLimiter_AllowsWithinLimit(t *testing.T) {
	rl := NewIPRateLimiter(10, 100, 5) // 10/min, 100/hour, burst 5
	defer rl.Stop()

	// Should allow requests within limit
	for i := 0; i < 10; i++ {
		if !rl.Allow("192.168.1.1") {
			t.Errorf("request %d should be allowed within limit", i+1)
		}
	}
}

func TestIPRateLimiter_BlocksExcessRequests(t *testing.T) {
	rl := NewIPRateLimiter(5, 100, 2) // 5/min, 100/hour, burst 2
	defer rl.Stop()

	// Use up the limit
	for i := 0; i < 5; i++ {
		rl.Allow("192.168.1.1")
	}

	// Next request should be blocked
	if rl.Allow("192.168.1.1") {
		t.Error("request exceeding limit should be blocked")
	}
}

func TestIPRateLimiter_DifferentIPsAreSeparate(t *testing.T) {
	rl := NewIPRateLimiter(3, 100, 1) // 3/min
	defer rl.Stop()

	// Use up limit for IP1
	for i := 0; i < 3; i++ {
		rl.Allow("192.168.1.1")
	}

	// IP2 should still be allowed
	if !rl.Allow("192.168.1.2") {
		t.Error("different IP should have separate limit")
	}

	// IP1 should be blocked
	if rl.Allow("192.168.1.1") {
		t.Error("IP1 should be rate limited")
	}
}

func TestRateLimitMiddleware_Returns429WhenLimited(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-ratelimit-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server with rate limiting enabled
	serverConfig := DefaultConfig()
	serverConfig.RateLimitEnabled = true
	serverConfig.RateLimitPerMinute = 2
	serverConfig.RateLimitPerHour = 100
	serverConfig.RateLimitBurst = 1

	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	router := server.buildRouter()

	// First two requests should succeed
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		if recorder.Code == http.StatusTooManyRequests {
			t.Errorf("request %d should not be rate limited", i+1)
		}
	}

	// Third request should be rate limited
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429 Too Many Requests, got %d", recorder.Code)
	}

	// Check Retry-After header
	if retry := recorder.Header().Get("Retry-After"); retry == "" {
		t.Error("expected Retry-After header on rate limited response")
	}
}

func TestRateLimitMiddleware_SkipsHealthEndpoint(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-ratelimit-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server with very strict rate limiting
	serverConfig := DefaultConfig()
	serverConfig.RateLimitEnabled = true
	serverConfig.RateLimitPerMinute = 1
	serverConfig.RateLimitPerHour = 1
	serverConfig.RateLimitBurst = 1

	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	router := server.buildRouter()

	// Exhaust rate limit on regular endpoint
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	// Health endpoint should STILL work (not rate limited)
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/health", nil)
		req.RemoteAddr = "192.168.1.1:12345"
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)

		if recorder.Code == http.StatusTooManyRequests {
			t.Error("health endpoint should not be rate limited")
		}
	}
}

func TestDefaultConfig_SecureDefaults(t *testing.T) {
	config := DefaultConfig()

	// SECURITY: Default should bind to localhost only
	if config.Address != "127.0.0.1" {
		t.Errorf("expected default address 127.0.0.1, got %s", config.Address)
	}

	// SECURITY: Default CORS origins should be asterisk (explicit configuration required)
	if len(config.CORSOrigins) != 1 || config.CORSOrigins[0] != "*" {
		t.Errorf("expected default CORS origins to be [\"*\"], got %v", config.CORSOrigins)
	}

	// SECURITY: CORS should be enabled by default - must be explicitly diabled if not desired
	if config.EnableCORS == false {
		t.Error("expected EnableCORS=true by default")
	}
}

func TestStatusEndpointRequiresAuth(t *testing.T) {
	server, _ := setupTestServer(t)

	// Request without auth should fail
	resp := makeRequest(t, server, "GET", "/status", nil, "")

	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 for /status without auth, got %d", resp.Code)
	}
}

func TestMetricsEndpointRequiresAuth(t *testing.T) {
	server, _ := setupTestServer(t)

	// Request without auth should fail
	resp := makeRequest(t, server, "GET", "/metrics", nil, "")

	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 for /metrics without auth, got %d", resp.Code)
	}
}

func TestStatusEndpointWithAuth(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Request with auth should succeed
	resp := makeRequest(t, server, "GET", "/status", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for /status with auth, got %d", resp.Code)
	}
}

func TestHandleGenerateAPIToken(t *testing.T) {
	server, authenticator := setupTestServer(t)

	makeReq := func(method string, body string, claims *auth.JWTClaims) (*httptest.ResponseRecorder, map[string]interface{}) {
		t.Helper()
		req := httptest.NewRequest(method, "/auth/api-token", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if claims != nil {
			req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, claims))
		}
		rec := httptest.NewRecorder()
		server.handleGenerateAPIToken(rec, req)

		var payload map[string]interface{}
		_ = json.Unmarshal(rec.Body.Bytes(), &payload)
		return rec, payload
	}

	t.Run("requires post", func(t *testing.T) {
		rec, payload := makeReq(http.MethodGet, "", nil)
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		assert.Equal(t, "POST required", payload["message"])
	})

	t.Run("requires configured authenticator", func(t *testing.T) {
		original := server.auth
		server.auth = nil
		defer func() { server.auth = original }()

		rec, payload := makeReq(http.MethodPost, `{}`, nil)
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Equal(t, "authentication not configured", payload["message"])
	})

	t.Run("requires authenticated claims", func(t *testing.T) {
		rec, payload := makeReq(http.MethodPost, `{}`, nil)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Equal(t, "not authenticated", payload["message"])
	})

	// The admin requirement is the route guard's (withAuth with
	// auth.PermAdmin): TestAPITokenNeedsTheAdminEntitlement goes through the
	// router; the handler itself makes no second decision.

	t.Run("validates request body and expiry", func(t *testing.T) {
		adminClaims := &auth.JWTClaims{
			Sub:      "admin-id",
			Username: "admin",
			Email:    "admin@example.com",
			Roles:    []string{string(auth.RoleAdmin)},
		}

		rec, payload := makeReq(http.MethodPost, `{"subject":`, adminClaims)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "invalid request body", payload["message"])

		rec, payload = makeReq(http.MethodPost, `{"expires_in":"xyz"}`, adminClaims)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, payload["message"], "invalid expires_in format")

		rec, payload = makeReq(http.MethodPost, `{"expires_in":"abcd"}`, adminClaims)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Equal(t, "invalid expires_in format", payload["message"])
	})

	t.Run("returns signed token with defaults and day parsing", func(t *testing.T) {
		// The stored admin's own claims: a token's roles are the ones its
		// stored account holds now, so the claims must name that account.
		adminClaims, err := authenticator.ValidateToken(getAuthToken(t, authenticator, "admin"))
		if err != nil {
			t.Fatalf("ValidateToken failed: %v", err)
		}

		rec, payload := makeReq(http.MethodPost, `{"expires_in":"7d"}`, adminClaims)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "api-token", payload["subject"])
		assert.Equal(t, []interface{}{string(auth.RoleAdmin)}, payload["roles"])

		token, _ := payload["token"].(string)
		if token == "" {
			t.Fatal("expected token in response")
		}
		claims, err := authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken failed: %v", err)
		}
		assert.Equal(t, adminClaims.Sub, claims.Sub)
		assert.Equal(t, "admin", claims.Username)
		assert.Contains(t, claims.Roles, string(auth.RoleAdmin))

		expiresIn, ok := payload["expires_in"].(float64)
		if !ok {
			t.Fatal("expected expires_in in response")
		}
		assert.InDelta(t, 7*24*60*60, expiresIn, 2)
		_, ok = payload["expires_at"].(string)
		assert.True(t, ok)
	})

	t.Run("supports never-expiring tokens", func(t *testing.T) {
		adminClaims := &auth.JWTClaims{
			Sub:      "admin-id",
			Username: "admin",
			Email:    "admin@example.com",
			Roles:    []string{string(auth.RoleAdmin)},
		}

		rec, payload := makeReq(http.MethodPost, `{"subject":"mcp","expires_in":"0"}`, adminClaims)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "mcp", payload["subject"])
		if _, ok := payload["expires_in"]; ok {
			t.Fatal("did not expect expires_in for never-expiring token")
		}
		if _, ok := payload["expires_at"]; ok {
			t.Fatal("did not expect expires_at for never-expiring token")
		}
	})
}

func TestMetricsEndpointWithAuth(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Request with auth should succeed
	resp := makeRequest(t, server, "GET", "/metrics", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for /metrics with auth, got %d", resp.Code)
	}
}

func TestAuthConfigAndOAuthNilPaths(t *testing.T) {
	server, _ := setupTestServer(t)

	origProvider := os.Getenv("NORNICDB_AUTH_PROVIDER")
	origIssuer := os.Getenv("NORNICDB_OAUTH_ISSUER")
	defer func() {
		_ = os.Setenv("NORNICDB_AUTH_PROVIDER", origProvider)
		_ = os.Setenv("NORNICDB_OAUTH_ISSUER", origIssuer)
	}()

	_ = os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	_ = os.Setenv("NORNICDB_OAUTH_ISSUER", "https://issuer.example")

	// /auth/config with oauth provider path.
	req := httptest.NewRequest(http.MethodGet, "/auth/config", nil)
	rec := httptest.NewRecorder()
	server.handleAuthConfig(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// OAuth handlers with nil manager.
	rec = makeRequest(t, server, "POST", "/auth/oauth/redirect", nil, "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	rec = makeRequest(t, server, "GET", "/auth/oauth/redirect", nil, "")
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	rec = makeRequest(t, server, "POST", "/auth/oauth/callback", nil, "")
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	rec = makeRequest(t, server, "GET", "/auth/oauth/callback?code=x&state=y", nil, "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAuthProfileAndPasswordHandlers(t *testing.T) {
	server, authenticator := setupTestServer(t)
	user, err := authenticator.GetUser("admin")
	assert.NoError(t, err)

	withClaims := func(r *http.Request, username string, sub string) *http.Request {
		claims := &auth.JWTClaims{Username: username, Sub: sub, Roles: []string{string(auth.RoleAdmin)}}
		return r.WithContext(context.WithValue(r.Context(), contextKeyClaims, claims))
	}

	// change-password happy path
	req := httptest.NewRequest(http.MethodPost, "/auth/change-password", strings.NewReader(`{"old_password":"password123","new_password":"password456!"}`))
	req.Header.Set("Content-Type", "application/json")
	req = withClaims(req, "admin", user.ID)
	rec := httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// old password wrong
	req = httptest.NewRequest(http.MethodPost, "/auth/change-password", strings.NewReader(`{"old_password":"wrong","new_password":"password789!"}`))
	req.Header.Set("Content-Type", "application/json")
	req = withClaims(req, "admin", user.ID)
	rec = httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// update-profile with explicit username
	req = httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader(`{"email":"admin@example.com","metadata":{"k":"v"}}`))
	req.Header.Set("Content-Type", "application/json")
	req = withClaims(req, "admin", user.ID)
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// update-profile invalid JSON
	req = httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader(`{`))
	req.Header.Set("Content-Type", "application/json")
	req = withClaims(req, "admin", user.ID)
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAuthEntitlementsAndRoleStoreUnavailable(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	// method not allowed on entitlements
	rec := makeRequest(t, server, "POST", "/auth/entitlements", nil, "Bearer "+token)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// nil stores -> service unavailable
	server.roleEntitlementsStore = nil
	rec = makeRequest(t, server, "GET", "/auth/role-entitlements", nil, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	server.roleStore = nil
	rec = makeRequest(t, server, "GET", "/auth/roles", nil, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	rec = makeRequest(t, server, "PATCH", "/auth/roles/custom", map[string]any{"name": "new"}, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	server.allowlistStore = nil
	rec = makeRequest(t, server, "GET", "/auth/access/databases", nil, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	server.privilegesStore = nil
	rec = makeRequest(t, server, "GET", "/auth/access/privileges", nil, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestIPRateLimiterCleanup(t *testing.T) {
	rl := &IPRateLimiter{
		counters: map[string]*ipRateLimitCounter{
			"1.1.1.1": {
				hourReset: time.Now().Add(-time.Minute),
			},
			"2.2.2.2": {
				hourReset: time.Now().Add(time.Hour),
			},
		},
	}
	rl.cleanup()
	assert.NotContains(t, rl.counters, "1.1.1.1")
	assert.Contains(t, rl.counters, "2.2.2.2")
}

func TestAuthRoleAndAccessHandlersHappyAndErrors(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	// Canonical entitlement list endpoint.
	rec := makeRequest(t, server, http.MethodGet, "/auth/entitlements", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/entitlements", nil, "Bearer "+token)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Roles CRUD-style endpoints.
	rec = makeRequest(t, server, http.MethodGet, "/auth/roles", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "  "}, "Bearer "+token)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "qa_role"}, "Bearer "+token)
	assert.Equal(t, http.StatusCreated, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "qa_role"}, "Bearer "+token)
	assert.Equal(t, http.StatusConflict, rec.Code)
	rec = makeRequest(t, server, http.MethodPatch, "/auth/roles/qa_role", map[string]any{"name": "qa_role_v2"}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Role entitlements: invalid + valid update, then read.
	rec = makeRequest(t, server, http.MethodPut, "/auth/role-entitlements", map[string]any{
		"role":         "qa_role_v2",
		"entitlements": []string{"not-a-real-entitlement"},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	validEntitlements := auth.GlobalEntitlementIDs()
	assert.NotEmpty(t, validEntitlements)
	rec = makeRequest(t, server, http.MethodPut, "/auth/role-entitlements", map[string]any{
		"role":         "qa_role_v2",
		"entitlements": []string{validEntitlements[0]},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodGet, "/auth/role-entitlements", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Per-database allowlist access matrix.
	rec = makeRequest(t, server, http.MethodPut, "/auth/access/databases", map[string]any{
		"role":      "viewer",
		"databases": []string{"nornic"},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodGet, "/auth/access/databases", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/access/databases", nil, "Bearer "+token)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Per-database read/write privileges matrix.
	rec = makeRequest(t, server, http.MethodPut, "/auth/access/privileges", []map[string]any{
		{"role": "viewer", "database": "nornic", "read": true, "write": false},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodGet, "/auth/access/privileges", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, http.MethodPost, "/auth/access/privileges", nil, "Bearer "+token)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	rec = makeRequest(t, server, http.MethodDelete, "/auth/roles/qa_role_v2", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBasePathAndRBACHelpers(t *testing.T) {
	server, _ := setupTestServer(t)

	origBasePath := server.config.BasePath
	defer func() { server.config.BasePath = origBasePath }()
	server.config.BasePath = "api"

	called := false
	var gotPath string
	var gotOriginal string
	var gotBasePath string
	h := server.basePathMiddleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		gotPath = r.URL.Path
		gotOriginal = r.Header.Get("X-Original-Path")
		gotBasePath = r.Header.Get("X-Base-Path")
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.True(t, called)
	assert.Equal(t, "/health", gotPath)
	assert.Equal(t, "/api/health", gotOriginal)
	assert.Equal(t, "/api", gotBasePath)

	// Forwarded prefix path that doesn't match configured base path.
	called = false
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("X-Forwarded-Prefix", "/proxy")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	assert.True(t, called)
	assert.Equal(t, "/health", gotPath)
	assert.Equal(t, "/proxy", req.Header.Get("X-Base-Path"))

	// RBAC helper context enrichment.
	claims := &auth.JWTClaims{Sub: "admin", Username: "admin", Roles: []string{"admin"}}
	req = httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, claims))
	enriched := server.withBifrostRBAC(req)
	roles := auth.RequestPrincipalRolesFromContext(enriched.Context())
	assert.Equal(t, []string{"admin"}, roles)
	assert.NotNil(t, auth.RequestDatabaseAccessModeFromContext(enriched.Context()))
	resolved := auth.RequestResolvedAccessResolverFromContext(enriched.Context())
	assert.NotNil(t, resolved)

	// Cover public helper wrappers.
	assert.NotNil(t, server.GetDatabaseAccessMode())
	perms := server.GetEffectivePermissions([]string{"admin"})
	assert.NotEmpty(t, perms)
	server.roleEntitlementsStore = nil
	fallbackPerms := server.GetEffectivePermissions([]string{"role_admin", "unknown_role"})
	assert.NotEmpty(t, fallbackPerms)
	assert.Contains(t, fallbackPerms, string(auth.PermRead))
}

func TestAuthMeAndRoleByIDBranches(t *testing.T) {
	server, authn := setupTestServer(t)

	// handleMe method not allowed.
	req := httptest.NewRequest(http.MethodPost, "/auth/me", nil)
	rec := httptest.NewRecorder()
	server.handleMe(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// handleMe with auth disabled returns anonymous.
	origAuth := server.auth
	server.auth = nil
	req = httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	rec = httptest.NewRecorder()
	server.handleMe(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	server.auth = origAuth

	// handleMe no claims.
	req = httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	rec = httptest.NewRecorder()
	server.handleMe(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// handleMe user not found.
	claims := &auth.JWTClaims{Sub: "missing-user", Username: "missing", Roles: []string{"admin"}}
	req = httptest.NewRequest(http.MethodGet, "/auth/me", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, claims))
	rec = httptest.NewRecorder()
	server.handleMe(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// handleMe OAuth metadata path.
	_ = authn.UpdateUser("admin", "admin@example.com", map[string]string{
		"auth_method":         "oauth",
		"oauth_access_token":  "provider-access-secret",
		"oauth_refresh_token": "provider-refresh-secret",
		"display_theme":       "dark",
	})
	adminTokenForMe := getAuthToken(t, authn, "admin")
	claims, userErr := authn.ValidateToken(adminTokenForMe)
	assert.NoError(t, userErr)
	req = httptest.NewRequest(http.MethodGet, "/auth/me", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, claims))
	rec = httptest.NewRecorder()
	origProvider := os.Getenv("NORNICDB_AUTH_PROVIDER")
	origIssuer := os.Getenv("NORNICDB_OAUTH_ISSUER")
	_ = os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	_ = os.Setenv("NORNICDB_OAUTH_ISSUER", "https://issuer.example")
	server.handleMe(rec, req)
	_ = os.Setenv("NORNICDB_AUTH_PROVIDER", origProvider)
	_ = os.Setenv("NORNICDB_OAUTH_ISSUER", origIssuer)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "provider-access-secret")
	assert.NotContains(t, rec.Body.String(), "provider-refresh-secret")
	assert.Contains(t, rec.Body.String(), `"display_theme":"dark"`)

	// RoleByID nil store branch.
	server.roleStore = nil
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/test", strings.NewReader(`{"name":"x"}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Restore stores by rebuilding server.
	server, authn = setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	assert.Equal(t, http.StatusCreated, makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "r1"}, "Bearer "+token).Code)
	assert.Equal(t, http.StatusCreated, makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "r2"}, "Bearer "+token).Code)

	// PATCH invalid JSON.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// PATCH empty new name.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader(`{"name":" "}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// PATCH rename to existing role.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader(`{"name":"r2"}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// PATCH role not found.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/missing", strings.NewReader(`{"name":"new_name"}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// DELETE detaches the role while retaining the user.
	_, _ = authn.CreateUser("role_user", "password123", []auth.Role{"r1"})
	req = httptest.NewRequest(http.MethodDelete, "/auth/roles/r1", nil)
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	roleUser, err := authn.GetUser("role_user")
	assert.NoError(t, err)
	assert.Empty(t, roleUser.Roles)
	assert.False(t, server.roleStore.Exists("r1"))

	// DELETE builtin role branch.
	req = httptest.NewRequest(http.MethodDelete, "/auth/roles/admin", nil)
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.True(t, server.roleStore.Exists("admin"))
}

func TestAudit318ProfileHTTPProduct(t *testing.T) {
	// Independent server and user store for this test; no shared state.
	srv, users := setupTestServer(t)

	// Obtain the admin token BEFORE installing the audit callback so that
	// login/setup traffic cannot contribute captured events.
	token := getAuthToken(t, users, "admin")
	require.NotEmpty(t, token)

	// Resolve the admin's original identity prior to the update.
	originalAdmin, err := users.GetUser("admin")
	require.NoError(t, err)
	require.NotNil(t, originalAdmin)
	verifiedClaims, err := users.ValidateToken(token)
	require.NoError(t, err)
	require.NotNil(t, verifiedClaims)
	require.NotEmpty(t, verifiedClaims.Sub)
	originalID := verifiedClaims.Sub

	const (
		newEmail      = "audit318.admin@example.com"
		metadataKey   = "audit318"
		metadataValue = "profile-update"
	)

	// Collect only user_update events from the auth audit callback.
	var userUpdateEvents []auth.AuditEvent
	users.SetAuditLogger(func(ev auth.AuditEvent) {
		if ev.EventType == "user_update" {
			userUpdateEvents = append(userUpdateEvents, ev)
		}
	})

	// Exercise the real router with the production request shape.
	body := map[string]any{
		"email": newEmail,
		"metadata": map[string]string{
			metadataKey: metadataValue,
		},
	}
	w := makeRequest(t, srv, "PUT", "/auth/profile", body, "Bearer "+token)
	require.Equal(t, http.StatusOK, w.Code)

	// Read back the live domain product; this is not a reopen/durability test.
	updated, err := users.GetUser("admin")
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.Equal(t, newEmail, updated.Email)
	require.NotNil(t, updated.Metadata)
	require.Equal(t, metadataValue, updated.Metadata[metadataKey])

	// Domain event: exactly one user_update, with the admin identity preserved.
	require.Len(t, userUpdateEvents, 1)
	ev := userUpdateEvents[0]
	require.Equal(t, "user_update", ev.EventType)
	require.Equal(t, "admin", ev.Username)
	require.Equal(t, originalID, ev.UserID)
	require.True(t, ev.Success)
	require.Equal(t, "profile updated", ev.Details)
}

func TestAudit321APITokenHTTPProduct(t *testing.T) {
	server, users := setupTestServer(t)

	token := getAuthToken(t, users, "admin")
	originalClaims, err := users.ValidateToken(token)
	require.NoError(t, err)

	var events []auth.AuditEvent
	users.SetAuditLogger(func(e auth.AuditEvent) {
		if e.EventType == "api_token_generated" {
			events = append(events, e)
		}
	})

	rec := makeRequest(t, server, "POST", "/auth/api-token", map[string]any{
		"subject":    "audit321",
		"expires_in": "24h",
	}, "Bearer "+token)

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Token     string     `json:"token"`
		Subject   string     `json:"subject"`
		ExpiresIn int64      `json:"expires_in"`
		Roles     []string   `json:"roles"`
		ExpiresAt *time.Time `json:"expires_at"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

	require.NotEmpty(t, resp.Token)
	require.Equal(t, "audit321", resp.Subject)
	require.Equal(t, int64(86400), resp.ExpiresIn)
	require.NotNil(t, resp.ExpiresAt)
	require.Equal(t, originalClaims.Roles, resp.Roles)

	issued, err := users.ValidateToken(resp.Token)
	require.NoError(t, err)
	require.Equal(t, originalClaims.Sub, issued.Sub)
	require.Equal(t, originalClaims.Username, issued.Username)
	require.Equal(t, originalClaims.Roles, issued.Roles)
	require.Equal(t, int64(86400), issued.Exp-issued.Iat)

	require.Len(t, events, 1)
	require.Equal(t, originalClaims.Username, events[0].Username)
	require.Equal(t, originalClaims.Sub, events[0].UserID)
	require.True(t, events[0].Success)
	require.Equal(t, "API token generated for subject 'audit321', expiry=24h0m0s", events[0].Details)
}
