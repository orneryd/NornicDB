// Package server provides HTTP REST API server tests.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testUIAssetsFS() fs.FS {
	return fstest.MapFS{
		"dist/index.html":    &fstest.MapFile{Data: []byte("<html>ui</html>")},
		"dist/assets/app.js": &fstest.MapFile{Data: []byte("console.log('ui');")},
	}
}

type pressureWarningLifecycleController struct{}

type expiringLifecycleController struct {
	gracefulExpire bool
	hardExpire     bool
}

func (pressureWarningLifecycleController) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	_ = info
	return func() {}
}

func (pressureWarningLifecycleController) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled":                            true,
		"pressure_band":                      string(storage.PressureHigh),
		"mvcc_bytes_pinned_by_oldest_reader": 4096,
		"mvcc_oldest_reader_age_seconds":     12.5,
	}
}

func (pressureWarningLifecycleController) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	return nil
}

func (pressureWarningLifecycleController) PauseLifecycle() {}

func (pressureWarningLifecycleController) ResumeLifecycle() {}

func (pressureWarningLifecycleController) AcquireSnapshotReader(info storage.SnapshotReaderInfo) (func(), error) {
	_ = info
	return func() {}, nil
}

func (pressureWarningLifecycleController) EvaluateSnapshotReader(info storage.SnapshotReaderInfo) (bool, bool) {
	_ = info
	return false, false
}

func (pressureWarningLifecycleController) RunPruneNow(ctx context.Context, opts storage.MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}

func (pressureWarningLifecycleController) StartLifecycle(ctx context.Context) { _ = ctx }

func (pressureWarningLifecycleController) StopLifecycle() {}

func (pressureWarningLifecycleController) IsLifecycleEnabled() bool { return true }

func (pressureWarningLifecycleController) IsLifecycleRunning() bool { return false }

func (pressureWarningLifecycleController) ReaderRegistry() storage.SnapshotReaderRegistry { return nil }

func (c *expiringLifecycleController) RegisterSnapshotReader(info storage.SnapshotReaderInfo) func() {
	_ = info
	return func() {}
}

func (c *expiringLifecycleController) LifecycleStatus() map[string]interface{} {
	return map[string]interface{}{"enabled": true, "pressure_band": string(storage.PressureCritical)}
}

func (c *expiringLifecycleController) TriggerPruneNow(ctx context.Context) error {
	_ = ctx
	return nil
}

func (c *expiringLifecycleController) PauseLifecycle() {}

func (c *expiringLifecycleController) ResumeLifecycle() {}

func (c *expiringLifecycleController) AcquireSnapshotReader(info storage.SnapshotReaderInfo) (func(), error) {
	_ = info
	return func() {}, nil
}

func (c *expiringLifecycleController) EvaluateSnapshotReader(info storage.SnapshotReaderInfo) (bool, bool) {
	_ = info
	return c.gracefulExpire, c.hardExpire
}

func (c *expiringLifecycleController) RunPruneNow(ctx context.Context, opts storage.MVCCPruneOptions) (int64, error) {
	_ = ctx
	_ = opts
	return 0, nil
}

func (c *expiringLifecycleController) StartLifecycle(ctx context.Context) { _ = ctx }

func (c *expiringLifecycleController) StopLifecycle() {}

func (c *expiringLifecycleController) IsLifecycleEnabled() bool { return true }

func (c *expiringLifecycleController) IsLifecycleRunning() bool { return false }

func (c *expiringLifecycleController) ReaderRegistry() storage.SnapshotReaderRegistry { return nil }

func unwrapInnerStorage(engine storage.Engine) storage.Engine {
	for {
		inner, ok := engine.(interface{ GetInnerEngine() storage.Engine })
		if !ok {
			return engine
		}
		next := inner.GetInnerEngine()
		if next == nil {
			return engine
		}
		engine = next
	}
}

func TestServerExtra_CoreBranches_SharedFixture(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	t.Run("backup invalid json", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/admin/backup", strings.NewReader("invalid json"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)

		recorder := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(recorder, req)
		require.Equal(t, http.StatusBadRequest, recorder.Code)
	})

	t.Run("user by id variants", func(t *testing.T) {
		resp := makeRequest(t, server, "GET", "/auth/users/admin", nil, "Bearer "+token)
		if resp.Code != http.StatusOK && resp.Code != http.StatusNotFound {
			t.Fatalf("unexpected status %d", resp.Code)
		}

		_ = makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
			"username": "updatetestuser",
			"password": "password123",
			"roles":    []string{"viewer"},
		}, "Bearer "+token)
		resp = makeRequest(t, server, "PUT", "/auth/users/updatetestuser", map[string]interface{}{
			"roles": []string{"editor"},
		}, "Bearer "+token)
		if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status %d", resp.Code)
		}

		createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
			"username": "disabletestuser",
			"password": "password123",
			"roles":    []string{"viewer"},
		}, "Bearer "+token)
		require.Equal(t, http.StatusCreated, createResp.Code)
		disabled := true
		resp = makeRequest(t, server, "PUT", "/auth/users/disabletestuser", map[string]interface{}{
			"disabled": &disabled,
		}, "Bearer "+token)
		if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status %d", resp.Code)
		}

		createResp = makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
			"username": "enabletestuser",
			"password": "password123",
			"roles":    []string{"viewer"},
		}, "Bearer "+token)
		require.Equal(t, http.StatusCreated, createResp.Code)
		disabled = false
		resp = makeRequest(t, server, "PUT", "/auth/users/enabletestuser", map[string]interface{}{
			"disabled": &disabled,
		}, "Bearer "+token)
		if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status %d", resp.Code)
		}

		req := httptest.NewRequest("PUT", "/auth/users/testuser", strings.NewReader("invalid json"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(recorder, req)
		require.Equal(t, http.StatusBadRequest, recorder.Code)

		createResp = makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
			"username": "deletetestuser",
			"password": "password123",
			"roles":    []string{"viewer"},
		}, "Bearer "+token)
		require.Equal(t, http.StatusCreated, createResp.Code)
		resp = makeRequest(t, server, "DELETE", "/auth/users/deletetestuser", nil, "Bearer "+token)
		if resp.Code != http.StatusOK && resp.Code != http.StatusNotFound {
			t.Fatalf("unexpected status %d", resp.Code)
		}

		resp = makeRequest(t, server, "GET", "/auth/users/", nil, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)

		resp = makeRequest(t, server, "PUT", "/auth/users", nil, "Bearer "+token)
		require.Equal(t, http.StatusMethodNotAllowed, resp.Code)

		resp = makeRequest(t, server, "POST", "/auth/users/admin", nil, "Bearer "+token)
		if resp.Code != http.StatusMethodNotAllowed && resp.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status %d", resp.Code)
		}
	})

	t.Run("transaction error and method branches", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": "INVALID CYPHER SYNTAX HERE"}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)
		var txResp map[string]interface{}
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&txResp))
		errors, ok := txResp["errors"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, errors)

		resp = makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
			"statements": []map[string]interface{}{
				{"statement": "MATCH (n) RETURN count(n)"},
				{"statement": "INVALID SYNTAX"},
			},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, resp.Code)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&txResp))
		errors, ok = txResp["errors"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, errors)

		resp = makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": "INVALID CYPHER"}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusCreated, resp.Code)
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&txResp))
		errors, ok = txResp["errors"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, errors)

		openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
			"statements": []map[string]interface{}{},
		}, "Bearer "+token)
		var openResult map[string]interface{}
		require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
		commitURL := openResult["commit"].(string)
		parts := strings.Split(commitURL, "/")
		txID := parts[len(parts)-2]
		commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": "INVALID SYNTAX"}},
		}, "Bearer "+token)
		require.Equal(t, http.StatusOK, commitResp.Code)
		require.NoError(t, json.NewDecoder(commitResp.Body).Decode(&txResp))
		errors, ok = txResp["errors"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, errors)

		resp = makeRequest(t, server, "GET", "/db/nornic/tx/commit", nil, "Bearer "+token)
		require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
		resp = makeRequest(t, server, "GET", "/db/nornic/tx/123456", nil, "Bearer "+token)
		require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
		resp = makeRequest(t, server, "GET", "/db/nornic/tx/123456/commit", nil, "Bearer "+token)
		require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
	})

	t.Run("auth and misc branches", func(t *testing.T) {
		resp := makeRequest(t, server, "POST", "/auth/token", map[string]interface{}{
			"username":   "admin",
			"password":   "password123",
			"grant_type": "unsupported_type",
		}, "")
		require.Equal(t, http.StatusBadRequest, resp.Code)

		resp = makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
			"username": "admin",
			"password": "password123",
			"roles":    []string{"viewer"},
		}, "Bearer "+token)
		require.Equal(t, http.StatusBadRequest, resp.Code)

		resp = makeRequest(t, server, "PUT", "/auth/users/nonexistentuser", map[string]interface{}{
			"roles": []string{"admin"},
		}, "Bearer "+token)
		require.Equal(t, http.StatusBadRequest, resp.Code)

		resp = makeRequest(t, server, "GET", "/admin/stats", nil, "")
		require.Equal(t, http.StatusUnauthorized, resp.Code)

		req := httptest.NewRequest("OPTIONS", "/", nil)
		req.Header.Set("Origin", "http://localhost:3000")
		req.Header.Set("Access-Control-Request-Method", "POST")
		recorder := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(recorder, req)
		require.NotEmpty(t, recorder.Header().Get("Access-Control-Allow-Origin"))

		makeRequest(t, server, "GET", "/health", nil, "")
		require.GreaterOrEqual(t, server.Stats().RequestCount, int64(1))
	})

	t.Run("server stop without start", func(t *testing.T) {
		s, _ := setupTestServer(t)
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		require.NoError(t, s.Stop(ctx))
	})

	t.Run("server stop twice", func(t *testing.T) {
		s, _ := setupTestServer(t)
		go s.Start()
		time.Sleep(50 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, s.Stop(ctx))
		require.NoError(t, s.Stop(ctx))
	})
}

// =============================================================================
// CORS Security Tests
// =============================================================================

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

// =============================================================================
// Rate Limiter Tests
// =============================================================================

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
	defer server.rateLimiter.Stop()

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
	defer server.rateLimiter.Stop()

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

// =============================================================================
// Secure Default Configuration Tests
// =============================================================================

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

// =============================================================================
// Protected Endpoint Tests
// =============================================================================

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

func TestHealthEndpointMinimalInfo(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/health", nil, "")

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for /health, got %d", resp.Code)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse health response: %v", err)
	}

	// Should only have minimal info
	if _, hasEmbeddings := result["embeddings"]; hasEmbeddings {
		t.Error("health endpoint should not expose embedding details")
	}

	// Should have status
	if status, ok := result["status"].(string); !ok || status != "healthy" {
		t.Errorf("expected status=healthy, got %v", result["status"])
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

	t.Run("requires admin role", func(t *testing.T) {
		rec, payload := makeReq(http.MethodPost, `{}`, &auth.JWTClaims{
			Sub:      "reader-id",
			Username: "reader",
			Roles:    []string{string(auth.RoleViewer)},
		})
		assert.Equal(t, http.StatusForbidden, rec.Code)
		assert.Equal(t, "admin role required to generate API tokens", payload["message"])
	})

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
		adminClaims := &auth.JWTClaims{
			Sub:      "admin-id",
			Username: "admin",
			Email:    "admin@example.com",
			Roles:    []string{string(auth.RoleAdmin)},
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
		assert.Equal(t, "admin-id", claims.Sub)
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

func TestHandleSearchAdditionalErrorCoverage(t *testing.T) {
	server, _ := setupTestServer(t)

	makeSearchReq := func(body string, claims *auth.JWTClaims) (*httptest.ResponseRecorder, map[string]interface{}) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if claims != nil {
			req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, claims))
		}
		rec := httptest.NewRecorder()
		server.handleSearch(rec, req)
		var payload map[string]interface{}
		_ = json.Unmarshal(rec.Body.Bytes(), &payload)
		return rec, payload
	}

	t.Run("forbidden when request has no database access", func(t *testing.T) {
		rec, payload := makeSearchReq(`{"query":"hello"}`, nil)
		assert.Equal(t, http.StatusForbidden, rec.Code)

		errors, ok := payload["errors"].([]interface{})
		if !ok || len(errors) == 0 {
			t.Fatalf("expected Neo4j error payload, got %v", payload)
		}
	})

	t.Run("returns not found for missing database", func(t *testing.T) {
		rec, payload := makeSearchReq(`{"database":"missing-db","query":"hello"}`, &auth.JWTClaims{
			Sub:      "admin-id",
			Username: "admin",
			Roles:    []string{string(auth.RoleAdmin)},
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
		assert.Contains(t, payload["message"], "Database 'missing-db' not found")
	})

	t.Run("returns service unavailable while database search is not ready", func(t *testing.T) {
		requireErr := server.dbManager.CreateDatabase("colddb")
		if requireErr != nil {
			t.Fatalf("CreateDatabase failed: %v", requireErr)
		}

		rec, payload := makeSearchReq(`{"database":"colddb","query":"hello"}`, &auth.JWTClaims{
			Sub:      "admin-id",
			Username: "admin",
			Roles:    []string{string(auth.RoleAdmin)},
		})
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Equal(t, "colddb", payload["database"])
		assert.Equal(t, true, payload["retryable"])
		assert.Equal(t, "search_not_ready", payload["request_status"])
	})
}

func TestNewAdditionalInitializationCoverage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-server-new-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbConfig := nornicdb.DefaultConfig()
	dbConfig.Memory.DecayEnabled = false
	dbConfig.Memory.AutoLinksEnabled = false
	dbConfig.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, dbConfig)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	authConfig := auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}
	authenticator, err := auth.NewAuthenticator(authConfig, storage.NewMemoryEngine())
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}

	t.Run("auth disabled uses full database access", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false

		server, err := New(db, nil, cfg)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if server.oauthManager != nil {
			t.Fatal("oauthManager should be nil without authenticator")
		}
		if server.databaseAccessMode == nil || !server.databaseAccessMode.CanAccessDatabase("nornic") {
			t.Fatal("expected full database access mode when auth disabled")
		}
	})

	t.Run("rate limiter oauth and slow query logger initialize", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.RateLimitEnabled = true
		cfg.RateLimitPerMinute = 5
		cfg.RateLimitPerHour = 10
		cfg.RateLimitBurst = 2
		cfg.SlowQueryEnabled = true
		cfg.Logging.SlowQueryThreshold = 50 * time.Millisecond
		cfg.Logging.SlowQueryLogFile = filepath.Join(t.TempDir(), "slow.log")

		server, err := New(db, authenticator, cfg)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		if server.rateLimiter == nil {
			t.Fatal("expected rate limiter to be initialized")
		}
		defer server.rateLimiter.Stop()
		if server.oauthManager == nil {
			t.Fatal("expected oauthManager with authenticator")
		}
		if server.databaseAccessMode == nil || server.databaseAccessMode.CanAccessDatabase("nornic") {
			t.Fatal("expected deny-all access mode before allowlist resolution when auth enabled")
		}
		if server.slowQueryLogger == nil {
			t.Fatal("expected slow query logger when file configured")
		}
		if server.dbManager == nil || server.dbConfigStore == nil {
			t.Fatal("expected database manager and db config store to be initialized")
		}
	})
}

// TestStripCypherComments tests the stripCypherComments function.
func TestStripCypherComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no comments",
			input:    "MATCH (n) RETURN n",
			expected: "MATCH (n) RETURN n",
		},
		{
			name:     "single-line comment at end",
			input:    "MATCH (n) RETURN n // comment",
			expected: "MATCH (n) RETURN n ",
		},
		{
			name:     "single-line comment on own line",
			input:    "MATCH (n)\n// comment\nRETURN n",
			expected: "MATCH (n)\n\nRETURN n",
		},
		{
			name:     "multi-line comment inline",
			input:    "MATCH (n) /* comment */ RETURN n",
			expected: "MATCH (n)  RETURN n",
		},
		{
			name:     "multi-line comment spanning lines",
			input:    "MATCH (n)\n/* comment\n   more comment */\nRETURN n",
			expected: "MATCH (n)\n\nRETURN n",
		},
		{
			name:     "multiple single-line comments",
			input:    "MATCH (n) // first\nWHERE n.age > 25 // second\nRETURN n // third",
			expected: "MATCH (n) \nWHERE n.age > 25 \nRETURN n ",
		},
		{
			name:     "comment only line",
			input:    "// comment only\nMATCH (n) RETURN n",
			expected: "\nMATCH (n) RETURN n",
		},
		{
			name:     "mixed comments",
			input:    "MATCH (n) /* multi */ // single\nRETURN n",
			expected: "MATCH (n)  \nRETURN n",
		},
		{
			name:     "empty query",
			input:    "",
			expected: "",
		},
		{
			name:     "only comments",
			input:    "// comment\n/* another */",
			expected: "\n",
		},
		{
			name:     "comment with :USE command",
			input:    ":USE test_db\n// comment\nMATCH (n) RETURN n",
			expected: ":USE test_db\n\nMATCH (n) RETURN n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripCypherComments(tt.input)
			if result != tt.expected {
				t.Errorf("stripCypherComments(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
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

func TestDBConfigHandlers(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	rec := makeRequest(t, server, "GET", "/admin/databases/config/keys", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = makeRequest(t, server, "POST", "/admin/databases/config/keys", nil, "Bearer "+token)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// nil store branch
	server.dbConfigStore = nil
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/config", nil, "Bearer "+token)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// configured store branches
	server.dbConfigStore = dbconfig.NewStore(storage.NewMemoryEngine())
	_ = server.dbConfigStore.Load(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/admin/databases//config", nil)
	rec = httptest.NewRecorder()
	server.handleDbConfigPrefix(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/system/config", nil, "Bearer "+token)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/other", nil, "Bearer "+token)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/config", nil, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = makeRequest(t, server, "PUT", "/admin/databases/nornic/config", map[string]any{
		"overrides": map[string]string{"unknown_key": "x"},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = makeRequest(t, server, "PUT", "/admin/databases/nornic/config", map[string]any{
		"overrides": map[string]string{},
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, rec.Code)
	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	_, hasRebuild := putResp["rebuildTriggered"]
	assert.True(t, hasRebuild)
}

func TestDBLifecycleHandlers(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	rec := makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	var status map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, "nornic", status["database"])
	require.Contains(t, status, "enabled")
	require.Contains(t, status, "pressure_band")

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/pause", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/status", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, true, status["paused"])

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/resume", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, false, status["paused"])

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/prune", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "prune triggered")

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/schedule", map[string]any{"interval": "0s"}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, false, status["automatic"])

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/schedule", map[string]any{"interval": "2m"}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	require.Equal(t, true, status["automatic"])
	require.Equal(t, "2m0s", status["cycle_interval"])

	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/schedule", map[string]any{"interval": "-1s"}, "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/debt?limit=1", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	var debtResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &debtResp))
	require.Equal(t, "nornic", debtResp["database"])
	require.Equal(t, float64(1), debtResp["limit"])
	keys, ok := debtResp["keys"].([]any)
	require.True(t, ok)
	require.LessOrEqual(t, len(keys), 1)

	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/debt?limit=1000", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &debtResp))
	require.Equal(t, float64(100), debtResp["limit"])

	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/prune", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/schedule", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	rec = makeRequest(t, server, "POST", "/admin/databases/nornic/mvcc/debt", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/debt?limit=bad", nil, "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/missing_db/mvcc", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/nornic/mvcc/unknown", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, rec.Code)
	rec = makeRequest(t, server, "GET", "/admin/databases/system/mvcc", nil, "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestTransactionResponsesIncludeMVCCPressureWarnings(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	storageEngine, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	storageEngine = unwrapInnerStorage(storageEngine)
	setter, ok := storageEngine.(interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	})
	require.True(t, ok)
	setter.SetLifecycleController(pressureWarningLifecycleController{})

	rec := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]any{
		"statements": []map[string]any{{"statement": "RETURN 1 AS n"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(storage.PressureHigh), rec.Header().Get("X-NornicDB-MVCC-Pressure"))
	require.Contains(t, rec.Header().Values("Warning"), "299 NornicDB \"MVCC lifecycle pressure is high on database 'nornic' (pinned_bytes=4096 oldest_reader_age_seconds=12.500)\"")

	var response TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.NotEmpty(t, response.Notifications)
	require.Equal(t, "NornicDB.MVCC.Pressure", response.Notifications[0].Code)
	require.Contains(t, response.Notifications[0].Description, "database 'nornic'")
	require.Empty(t, response.Errors)

	setter.SetLifecycleController(nil)
}

func TestExplicitTransactionLifecycleExpiration_ReplaysErrorAndAuditsOnce(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	auditCfg := audit.DefaultConfig()
	auditCfg.LogPath = filepath.Join(t.TempDir(), "audit.log")
	logger, err := audit.NewLogger(auditCfg)
	require.NoError(t, err)
	defer logger.Close()
	server.SetAuditLogger(logger)

	storageEngine, err := server.dbManager.GetStorage("nornic")
	require.NoError(t, err)
	storageEngine = unwrapInnerStorage(storageEngine)
	setter, ok := storageEngine.(interface {
		SetLifecycleController(storage.MVCCLifecycleController)
	})
	require.True(t, ok)
	controller := &expiringLifecycleController{}
	setter.SetLifecycleController(controller)
	defer setter.SetLifecycleController(nil)

	openRec := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]any{"statements": []any{}}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openRec.Code)
	var openResp TransactionResponse
	require.NoError(t, json.Unmarshal(openRec.Body.Bytes(), &openResp))
	require.NotEmpty(t, openResp.Commit)
	txPath := strings.TrimSuffix(openResp.Commit, "/commit")

	controller.gracefulExpire = true

	execRec := makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'expired'})"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execRec.Code)
	var execResp TransactionResponse
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))
	require.Len(t, execResp.Errors, 1)
	require.Equal(t, "Neo.TransientError.Transaction.Outdated", execResp.Errors[0].Code)
	require.Contains(t, execResp.Errors[0].Message, storage.ErrMVCCSnapshotGracefulCancel.Error())

	execRec = makeRequest(t, server, "POST", txPath, map[string]any{
		"statements": []map[string]any{{"statement": "CREATE (n:Doc {name:'expired-again'})"}},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execRec.Code)
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))
	require.Len(t, execResp.Errors, 1)
	require.Equal(t, "Neo.TransientError.Transaction.Outdated", execResp.Errors[0].Code)
	require.Contains(t, execResp.Errors[0].Message, storage.ErrMVCCSnapshotGracefulCancel.Error())

	logBytes, err := os.ReadFile(auditCfg.LogPath)
	require.NoError(t, err)
	logText := string(logBytes)
	require.Equal(t, 1, strings.Count(logText, string(audit.EventSnapshotExpired)))
	require.Contains(t, logText, "mvcc_snapshot_expired")
	require.Contains(t, logText, "\"resource_id\":\"nornic\"")
}

func TestHeimdallRouterAndHelpers(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)

	assert.NotEmpty(t, router.DefaultDatabaseName())
	dbName, err := router.ResolveDatabase("")
	assert.NoError(t, err)
	assert.NotEmpty(t, dbName)
	assert.NotEmpty(t, router.ListDatabases())

	rows, err := router.Query(context.Background(), dbName, "RETURN 1 AS n", nil)
	assert.NoError(t, err)
	assert.NotNil(t, rows)

	stats, err := router.Stats(dbName)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, stats.NodeCount, int64(0))

	// discover path with nil db/search unavailable
	routerNoDB := newHeimdallDBRouter(nil, server.dbManager, nil)
	_, err = routerNoDB.Discover(context.Background(), dbName, "x", nil, 1, 1)
	assert.Error(t, err)

	// discover path on initialized router (may return empty results, but should be callable)
	engine, engineErr := server.dbManager.GetStorage(dbName)
	assert.NoError(t, engineErr)
	if engineErr == nil {
		_, _ = engine.CreateNode(&storage.Node{
			ID:         "nornic:doc1",
			Labels:     []string{"Document"},
			Properties: map[string]interface{}{"title": "hello", "content": "world"},
		})
	}
	discoverRes, discoverErr := router.Discover(context.Background(), dbName, "hello", []string{"Document"}, 0, 1)
	if discoverErr == nil {
		assert.NotNil(t, discoverRes)
	}

	// related nodes traversal
	mem := storage.NewMemoryEngine()
	_, err = mem.CreateNode(&storage.Node{ID: "nornic:n1", Labels: []string{"A"}})
	assert.NoError(t, err)
	_, err = mem.CreateNode(&storage.Node{ID: "nornic:n2", Labels: []string{"B"}})
	assert.NoError(t, err)
	err = mem.CreateEdge(&storage.Edge{ID: "nornic:e1", StartNode: "nornic:n1", EndNode: "nornic:n2", Type: "REL"})
	assert.NoError(t, err)
	related := router.getRelatedNodes(mem, "nornic:n1", 1)
	assert.Len(t, related, 1)
	assert.Nil(t, router.getRelatedNodes(mem, "nornic:n1", 0))

	assert.Equal(t, "A", getFirstLabel([]string{"A", "B"}))
	assert.Equal(t, "", getFirstLabel(nil))
	assert.Equal(t, "v", getStringProperty(map[string]interface{}{"k": "v"}, "k"))
	assert.Equal(t, "", getStringProperty(nil, "k"))

	metrics := (&heimdallMetricsReader{}).Runtime()
	assert.Greater(t, metrics.GoroutineCount, 0)
}

func TestUIHelpersAndHeimdallSetter(t *testing.T) {
	server, _ := setupTestServer(t)

	origEnabled := UIEnabled
	origAssets := UIAssets
	defer func() {
		UIEnabled = origEnabled
		UIAssets = origAssets
	}()

	UIEnabled = false
	h, err := newUIHandler()
	assert.NoError(t, err)
	assert.Nil(t, h)

	SetUIAssets(fstest.MapFS{})
	h, err = newUIHandler()
	assert.Error(t, err)
	assert.Nil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "text/html")
	assert.True(t, isUIRequest(req))
	req.Header.Set("Accept", "application/json")
	assert.False(t, isUIRequest(req))

	assert.Equal(t, 4, embeddingCacheMemoryMB(256, 4096))
	server.setHeimdallHandler(nil)
	assert.Nil(t, server.getHeimdallHandler())
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
		stopCleanup: make(chan struct{}),
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

func TestEmbedHandlersAndHelperUnits(t *testing.T) {
	server, _ := setupTestServer(t)

	// Embed endpoints: deterministic method checks + stats endpoint.
	req := httptest.NewRequest(http.MethodGet, "/nornicdb/embed/trigger", nil)
	rec := httptest.NewRecorder()
	server.handleEmbedTrigger(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/nornicdb/embed/stats", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedStats(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedTrigger(rec, req)
	assert.Contains(t, []int{http.StatusOK, http.StatusServiceUnavailable, http.StatusInternalServerError}, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedTrigger(rec, req)
	assert.Contains(t, []int{http.StatusAccepted, http.StatusServiceUnavailable}, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/nornicdb/embed/clear", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedClear(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/clear", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedClear(rec, req)
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, rec.Code)

	// Tiny zero-coverage helper: search service wrapper and permission parser.
	_, err := server.getOrCreateSearchService(server.dbManager.DefaultDatabaseName(), server.db.GetStorage())
	assert.NoError(t, err)

	for _, perm := range []auth.Permission{
		auth.PermRead,
		auth.PermWrite,
		auth.PermCreate,
		auth.PermDelete,
		auth.PermAdmin,
		auth.PermSchema,
		auth.PermUserManage,
	} {
		p, ok := parsePermissionString(string(perm))
		assert.True(t, ok)
		assert.Equal(t, perm, p)
	}
	_, invalidOK := parsePermissionString("definitely-invalid")
	assert.False(t, invalidOK)
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

func TestRecoveryMiddlewareRecoversPanic(t *testing.T) {
	server, _ := setupTestServer(t)
	h := server.recoveryMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	h.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestDatabaseAdapterAndConversionHelpers(t *testing.T) {
	server, _ := setupTestServer(t)
	adapter := &databaseManagerAdapter{manager: server.dbManager, db: server.db, server: server}

	// Alias/lifecycle adapter methods.
	err := adapter.CreateDatabase("db_adapter_a")
	assert.NoError(t, err)
	err = adapter.CreateAlias("alias_a", "db_adapter_a")
	assert.NoError(t, err)
	aliases := adapter.ListAliases("db_adapter_a")
	assert.NotNil(t, aliases)
	resolved, err := adapter.ResolveDatabase("alias_a")
	assert.NoError(t, err)
	assert.Equal(t, "db_adapter_a", resolved)
	err = adapter.DropAlias("alias_a")
	assert.NoError(t, err)

	// Limits adapter methods.
	err = adapter.SetDatabaseLimits("db_adapter_a", "bad-type")
	assert.Error(t, err)
	err = adapter.SetDatabaseLimits("db_adapter_a", &multidb.Limits{})
	assert.NoError(t, err)
	_, err = adapter.GetDatabaseLimits("db_adapter_a")
	assert.NoError(t, err)

	// Composite adapter methods (map conversion + constituent add/remove/list).
	constituents := []interface{}{
		map[string]interface{}{
			"alias":         "main",
			"database_name": "nornic",
			"type":          "remote",
			"access_mode":   "read",
			"uri":           "https://remote-1.example/nornic-db",
			"secret_ref":    "spn-prod-a",
			"auth_mode":     "oidc_forwarding",
		},
	}
	err = adapter.CreateCompositeDatabase("cmp_adapter", constituents)
	assert.NoError(t, err)
	assert.True(t, adapter.IsCompositeDatabase("cmp_adapter"))
	err = adapter.CreateDatabase("db_adapter_b")
	assert.NoError(t, err)
	err = adapter.AddConstituent("cmp_adapter", map[string]interface{}{
		"alias":         "extra",
		"database_name": "db_adapter_b",
		"type":          "remote",
		"access_mode":   "read",
		"uri":           "https://remote-2.example/nornic-db",
		"secret_ref":    "spn-prod-b",
		"auth_mode":     "user_password",
		"user":          "svc-user",
		"password":      "svc-pass",
	})
	assert.NoError(t, err)
	constituentList, err := adapter.GetCompositeConstituents("cmp_adapter")
	assert.NoError(t, err)
	require.Len(t, constituentList, 2)
	first, ok := constituentList[0].(multidb.ConstituentRef)
	require.True(t, ok)
	assert.Equal(t, "https://remote-1.example/nornic-db", first.URI)
	assert.Equal(t, "spn-prod-a", first.SecretRef)
	assert.Equal(t, "oidc_forwarding", first.AuthMode)
	second, ok := constituentList[1].(multidb.ConstituentRef)
	require.True(t, ok)
	assert.Equal(t, "https://remote-2.example/nornic-db", second.URI)
	assert.Equal(t, "spn-prod-b", second.SecretRef)
	assert.Equal(t, "user_password", second.AuthMode)
	assert.Equal(t, "svc-user", second.User)
	err = adapter.RemoveConstituent("cmp_adapter", "extra")
	assert.NoError(t, err)
	err = adapter.AddConstituent("cmp_adapter", 123)
	assert.Error(t, err)
	err = adapter.DropCompositeDatabase("cmp_adapter")
	assert.NoError(t, err)
	err = adapter.DropDatabase("db_adapter_b")
	assert.NoError(t, err)
	err = adapter.DropDatabase("db_adapter_a")
	assert.NoError(t, err)

	// Ensure CreatedAt adapter method is exercised.
	dbInfos := adapter.ListDatabases()
	assert.NotEmpty(t, dbInfos)
	assert.False(t, dbInfos[0].CreatedAt().IsZero())

	// Map-node conversion and recursive conversion helpers.
	converted := server.mapNodeToNeo4jHTTPFormat("n1", map[string]interface{}{
		"labels": []interface{}{"Doc"},
		"title":  "T",
	})
	assert.Equal(t, "4:nornicdb:n1", converted["elementId"])
	assert.Equal(t, []string{"Doc"}, converted["labels"])
	props, ok := converted["properties"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "T", props["title"])

	v := server.convertValueToNeo4jFormat(map[string]interface{}{
		"_nodeId":     "n2",
		"labels":      []interface{}{"L"},
		"name":        "A",
		"_pathResult": "drop-me",
		"nested": []interface{}{
			map[string]interface{}{"id": "n3", "labels": []interface{}{"M"}, "k": "v"},
		},
	})
	vm, ok := v.(map[string]interface{})
	assert.True(t, ok)
	assert.NotContains(t, vm, "_pathResult")

	// responseWriter.Flush and uiHandler.ServeHTTP branches.
	rw := &responseWriter{ResponseWriter: httptest.NewRecorder()}
	rw.Flush()

	assetHandler := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusCreated)
		}),
		indexHTML: []byte("<html>ok</html>"),
	}
	rec := httptest.NewRecorder()
	assetHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	assert.Equal(t, http.StatusCreated, rec.Code)
	rec = httptest.NewRecorder()
	assetHandler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/app/route", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<html>ok</html>")
}

func TestMCPDatabaseScopedExecutorBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	cb := server.mcpDatabaseScopedExecutor()
	exec, getNode, err := cb(server.dbManager.DefaultDatabaseName())
	assert.NoError(t, err)
	assert.NotNil(t, exec)
	assert.NotNil(t, getNode)

	// Not found path.
	_, err = getNode(context.Background(), "non-existent-element-id")
	assert.Error(t, err)

	// Create node, fetch elementId, then resolve through getNode.
	res, err := exec.Execute(context.Background(), "CREATE (n:Doc {title:'t'}) RETURN elementId(n) AS id", nil)
	assert.NoError(t, err)
	if assert.NotEmpty(t, res.Rows) && assert.NotEmpty(t, res.Rows[0]) {
		if id, ok := res.Rows[0][0].(string); ok && id != "" {
			node, getErr := getNode(context.Background(), id)
			assert.NoError(t, getErr)
			assert.NotNil(t, node)
			if node != nil {
				assert.Equal(t, "t", node.Properties["title"])
			}
		}
	}

	_, _, err = cb("does-not-exist-db")
	assert.Error(t, err)
}

func TestSlowQueryLoggingBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Disabled branch.
	server.config.SlowQueryEnabled = false
	server.logSlowQuery("MATCH (n) RETURN n", nil, 2*time.Second, nil)
	assert.Equal(t, int64(0), server.slowQueryCount.Load())

	// Enabled but below threshold.
	server.config.SlowQueryEnabled = true
	server.config.Logging.SlowQueryThreshold = 5 * time.Second
	server.logSlowQuery("MATCH (n) RETURN n", nil, 10*time.Millisecond, nil)
	assert.Equal(t, int64(0), server.slowQueryCount.Load())

	// Above threshold with truncation and params.
	server.config.Logging.SlowQueryThreshold = 1 * time.Millisecond
	longQuery := strings.Repeat("x", 700)
	longParam := strings.Repeat("y", 300)
	server.logSlowQuery(longQuery, map[string]interface{}{"p": longParam}, 100*time.Millisecond, fmt.Errorf("query failed"))
	assert.Equal(t, int64(1), server.slowQueryCount.Load())

	// Logger configured branch.
	server.slowQueryLogger = log.New(io.Discard, "", 0)
	server.logSlowQuery("MATCH (n) RETURN n", map[string]interface{}{"k": "v"}, 100*time.Millisecond, nil)
	assert.Equal(t, int64(2), server.slowQueryCount.Load())
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
	_ = authn.UpdateUser("admin", "admin@example.com", map[string]string{"auth_method": "oauth"})
	adminUser, userErr := authn.GetUser("admin")
	assert.NoError(t, userErr)
	claims = &auth.JWTClaims{Sub: adminUser.ID, Username: "admin", Roles: []string{"admin"}}
	req = httptest.NewRequest(http.MethodGet, "/auth/me", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, claims))
	rec = httptest.NewRecorder()
	origProvider := os.Getenv("NORNICDB_AUTH_PROVIDER")
	origIssuer := os.Getenv("NORNICDB_OAUTH_ISSUER")
	_ = os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	_ = os.Setenv("NORNICDB_OAUTH_ISSUER", "https://issuer.example")
	server.handleMe(rec, req)
	_ = os.Setenv("NORNICDB_AUTH_PROVIDER", origProvider)
	_ = os.Setenv("NORNICDB_OAUTH_ISSUER", origIssuer)
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, rec.Code)

	// RoleByID nil store branch.
	server.roleStore = nil
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/test", strings.NewReader(`{"name":"x"}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// Restore stores by rebuilding server.
	server, authn = setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	assert.Equal(t, http.StatusCreated, makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "r1"}, "Bearer "+token).Code)
	assert.Equal(t, http.StatusCreated, makeRequest(t, server, http.MethodPost, "/auth/roles", map[string]any{"name": "r2"}, "Bearer "+token).Code)

	// PATCH invalid JSON.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// PATCH empty new name.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader(`{"name":" "}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// PATCH rename to existing role.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/r1", strings.NewReader(`{"name":"r2"}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// PATCH role not found.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/missing", strings.NewReader(`{"name":"new_name"}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// DELETE conflict when user has role.
	_, _ = authn.CreateUser("role_user", "password123", []auth.Role{"r1"})
	req = httptest.NewRequest(http.MethodDelete, "/auth/roles/r1", nil)
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// DELETE builtin role branch.
	req = httptest.NewRequest(http.MethodDelete, "/auth/roles/admin", nil)
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestSearchAndRebuildAdditionalBranches(t *testing.T) {
	server, authn := setupTestServer(t)
	adminToken := getAuthToken(t, authn, "admin")
	viewerToken := getAuthToken(t, authn, "reader")

	// Search rebuild method not allowed.
	rec := makeRequest(t, server, http.MethodGet, "/nornicdb/search/rebuild", nil, "Bearer "+adminToken)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Search rebuild with non-existent DB.
	rec = makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{"database": "missing"}, "Bearer "+adminToken)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Search rebuild forbidden for reader.
	rec = makeRequest(t, server, http.MethodPost, "/nornicdb/search/rebuild", map[string]any{"database": server.dbManager.DefaultDatabaseName()}, "Bearer "+viewerToken)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// Search method not allowed.
	rec = makeRequest(t, server, http.MethodGet, "/nornicdb/search", nil, "Bearer "+adminToken)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Search invalid body.
	req := httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader("{"))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)
	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	// Search DB not found.
	rec = makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]any{
		"database": "missing",
		"query":    "hello",
	}, "Bearer "+adminToken)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRouterRegistrationBranches(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	// registerUIRoutes: headless branch and UI init error branch.
	mux := http.NewServeMux()
	origHeadless := server.config.Headless
	origEnabled := UIEnabled
	origAssets := UIAssets
	defer func() {
		server.config.Headless = origHeadless
		UIEnabled = origEnabled
		UIAssets = origAssets
	}()
	server.config.Headless = true
	assert.Nil(t, server.registerUIRoutes(mux))
	server.config.Headless = false
	UIEnabled = true
	UIAssets = fstest.MapFS{}
	assert.Nil(t, server.registerUIRoutes(mux))

	// registerUIRoutes success path using injected UI assets.
	SetUIAssets(testUIAssetsFS())
	mux = http.NewServeMux()
	uih := server.registerUIRoutes(mux)
	assert.NotNil(t, uih)
	req := httptest.NewRequest(http.MethodGet, "/assets/", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// registerNeo4jRoutes: discovery JSON and UI HTML branches.
	mux = http.NewServeMux()
	server.registerNeo4jRoutes(mux, nil)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	mockUI := &uiHandler{
		fileServer: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusCreated) }),
		indexHTML:  []byte("<html>ui</html>"),
	}
	mux = http.NewServeMux()
	server.registerNeo4jRoutes(mux, mockUI)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "text/html")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<html>ui</html>")

	// registerMCPRoutes nil branch and active branch.
	originalMCPServer := server.mcpServer
	server.mcpServer = nil
	mux = http.NewServeMux()
	server.registerMCPRoutes(mux)
	req = httptest.NewRequest(http.MethodGet, "/mcp/health", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	server.mcpServer = originalMCPServer

	mux = http.NewServeMux()
	server.registerMCPRoutes(mux)
	req = httptest.NewRequest(http.MethodGet, "/mcp/health", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// registerHeimdallRoutes branches.
	mux = http.NewServeMux()
	server.registerHeimdallRoutes(mux)
	req = httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	// registerGraphQLRoutes nil branch and active branch.
	originalGraphQLHandler := server.graphqlHandler
	server.graphqlHandler = nil
	mux = http.NewServeMux()
	server.registerGraphQLRoutes(mux)
	req = httptest.NewRequest(http.MethodGet, "/graphql", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	server.graphqlHandler = originalGraphQLHandler

	mux = http.NewServeMux()
	server.registerGraphQLRoutes(mux)
	req = httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{__typename}"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)
}

func TestHeimdallRouterFallbackBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Nil dbManager fallback behavior.
	r := newHeimdallDBRouter(server.db, nil, nil)
	assert.Equal(t, "nornic", r.DefaultDatabaseName())
	dbName, err := r.ResolveDatabase("")
	assert.NoError(t, err)
	assert.Equal(t, "nornic", dbName)
	assert.Equal(t, []string{"nornic"}, r.ListDatabases())

	// storageForDatabase with nil db should error.
	rNoDB := newHeimdallDBRouter(nil, nil, nil)
	_, _, err = rNoDB.storageForDatabase("")
	assert.Error(t, err)

	// system DB discover path.
	_, err = r.Discover(context.Background(), "system", "q", nil, 1, 1)
	assert.Error(t, err)
}

func TestGrantAccessAndRBACResolverBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Grant access no-op when stores missing.
	origAllow := server.allowlistStore
	origPriv := server.privilegesStore
	server.allowlistStore = nil
	server.privilegesStore = nil
	server.grantAccessToNewDatabase(context.Background(), "db_new", nil)
	server.allowlistStore = origAllow
	server.privilegesStore = origPriv

	// Seed explicit allowlists and grant with claims.
	requireAllow := []string{"nornic"}
	_ = server.allowlistStore.SaveRoleDatabases(context.Background(), "admin", requireAllow)
	_ = server.allowlistStore.SaveRoleDatabases(context.Background(), "custom", requireAllow)
	claims := &auth.JWTClaims{Roles: []string{"role_custom", "admin", "role_custom"}}
	server.grantAccessToNewDatabase(context.Background(), "db_new", claims)

	al := server.allowlistStore.Allowlist()
	assert.Contains(t, al["admin"], "db_new")
	assert.Contains(t, al["custom"], "db_new")
	assert.True(t, server.privilegesStore.Resolve([]string{"admin"}, "db_new").Write)
	assert.True(t, server.privilegesStore.Resolve([]string{"custom"}, "db_new").Read)

	// Explicit helper branches.
	server.auth = nil
	assert.True(t, server.GetDatabaseAccessModeForRoles(nil).CanAccessDatabase("anything"))
	server.auth = setupAuthForHelper(t)
	assert.False(t, server.GetDatabaseAccessModeForRoles(nil).CanAccessDatabase("nornic"))
	server.allowlistStore = nil
	assert.False(t, server.GetDatabaseAccessModeForRoles([]string{"admin"}).CanAccessDatabase("nornic"))
	server.allowlistStore = origAllow

	server.privilegesStore = nil
	ra := server.GetResolvedAccessForRoles([]string{"admin"}, "nornic")
	assert.True(t, ra.Read)
	server.privilegesStore = origPriv
}

func setupAuthForHelper(t *testing.T) *auth.Authenticator {
	t.Helper()
	a, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewMemoryEngine())
	assert.NoError(t, err)
	return a
}

func TestGPUHandlersNoManagerBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Status no-manager and wrong method.
	req := httptest.NewRequest(http.MethodPost, "/admin/gpu/status", nil)
	rec := httptest.NewRecorder()
	server.handleGPUStatus(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	req = httptest.NewRequest(http.MethodGet, "/admin/gpu/status", nil)
	rec = httptest.NewRecorder()
	server.handleGPUStatus(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Enable/disable/test with no manager.
	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/enable", nil)
	rec = httptest.NewRecorder()
	server.handleGPUEnable(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/disable", nil)
	rec = httptest.NewRecorder()
	server.handleGPUDisable(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader(`{"node_id":"n1","mode":"cpu"}`))
	rec = httptest.NewRecorder()
	server.handleGPUTest(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestEmbedTriggerAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})

	req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil)
	rec := httptest.NewRecorder()
	server.handleEmbedTrigger(rec, req)
	assert.Contains(t, []int{http.StatusOK, http.StatusInternalServerError}, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil)
	rec = httptest.NewRecorder()
	server.handleEmbedTrigger(rec, req)
	assert.Contains(t, []int{http.StatusAccepted, http.StatusInternalServerError}, rec.Code)
}

func TestAuthChangeUpdateAndOAuthBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Change password method/auth/claims/json branches.
	req := httptest.NewRequest(http.MethodGet, "/auth/password", nil)
	rec := httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	origAuth := server.auth
	server.auth = nil
	req = httptest.NewRequest(http.MethodPost, "/auth/password", nil)
	rec = httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.auth = origAuth

	req = httptest.NewRequest(http.MethodPost, "/auth/password", nil)
	rec = httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/auth/password", strings.NewReader("{"))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Sub: "admin"}))
	rec = httptest.NewRecorder()
	server.handleChangePassword(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Update profile method/auth/claims/json/fallback-notfound branches.
	req = httptest.NewRequest(http.MethodPost, "/auth/profile", nil)
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	server.auth = nil
	req = httptest.NewRequest(http.MethodPut, "/auth/profile", nil)
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.auth = origAuth

	req = httptest.NewRequest(http.MethodPut, "/auth/profile", nil)
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader("{"))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Sub: "admin"}))
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader(`{"email":"x@example.com"}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "", Sub: "missing"}))
	rec = httptest.NewRecorder()
	server.handleUpdateProfile(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// OAuth callback rich error branches with configured manager.
	req = httptest.NewRequest(http.MethodGet, "/auth/oauth/callback?error=access_denied&error_description=nope", nil)
	rec = httptest.NewRecorder()
	server.handleOAuthCallback(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/auth/oauth/callback?state=s", nil)
	rec = httptest.NewRecorder()
	server.handleOAuthCallback(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/auth/oauth/callback?code=c", nil)
	rec = httptest.NewRecorder()
	server.handleOAuthCallback(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestNewFeatureConfigBranches(t *testing.T) {
	db, err := nornicdb.Open("", nornicdb.DefaultConfig())
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer db.Close()

	makeServer := func(cfg *Config) *Server {
		s, newErr := New(db, nil, cfg)
		assert.NoError(t, newErr)
		if s != nil {
			time.Sleep(40 * time.Millisecond) // allow async init branches to run
			_ = s.Stop(context.Background())
		}
		return s
	}

	// Search rerank enabled with missing API URL branch.
	cfg := DefaultConfig()
	cfg.EmbeddingEnabled = false
	cfg.MCPEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		SearchRerankEnabled:  true,
		SearchRerankProvider: "http",
	}
	makeServer(cfg)

	// Search rerank external provider configured branch.
	cfg = DefaultConfig()
	cfg.EmbeddingEnabled = false
	cfg.MCPEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		SearchRerankEnabled:  true,
		SearchRerankProvider: "http",
		SearchRerankAPIURL:   "http://127.0.0.1:1/rerank",
		SearchRerankModel:    "test-reranker",
	}
	makeServer(cfg)

	// Search rerank local provider branch.
	cfg = DefaultConfig()
	cfg.EmbeddingEnabled = false
	cfg.MCPEnabled = false
	cfg.ModelsDir = t.TempDir()
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		SearchRerankEnabled:  true,
		SearchRerankProvider: "local",
		SearchRerankModel:    "missing-local-reranker",
	}
	makeServer(cfg)
}

func TestAccessControlHandlersAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// access/databases: invalid JSON
	req := httptest.NewRequest(http.MethodPut, "/auth/access/databases", strings.NewReader("{"))
	rec := httptest.NewRecorder()
	server.handleAccessDatabases(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// access/databases: missing role
	req = httptest.NewRequest(http.MethodPut, "/auth/access/databases", strings.NewReader(`{"databases":["nornic"]}`))
	rec = httptest.NewRecorder()
	server.handleAccessDatabases(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// access/databases: mappings branch
	req = httptest.NewRequest(http.MethodPut, "/auth/access/databases", strings.NewReader(`{"mappings":[{"role":"reader","databases":["nornic","system"]}]}`))
	rec = httptest.NewRecorder()
	server.handleAccessDatabases(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// access/databases: method not allowed
	req = httptest.NewRequest(http.MethodDelete, "/auth/access/databases", nil)
	rec = httptest.NewRecorder()
	server.handleAccessDatabases(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// access/databases: nil store unavailable
	origAllow := server.allowlistStore
	server.allowlistStore = nil
	req = httptest.NewRequest(http.MethodGet, "/auth/access/databases", nil)
	rec = httptest.NewRecorder()
	server.handleAccessDatabases(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.allowlistStore = origAllow

	// access/privileges: invalid JSON
	req = httptest.NewRequest(http.MethodPut, "/auth/access/privileges", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleAccessPrivileges(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// access/privileges: valid matrix update
	req = httptest.NewRequest(http.MethodPut, "/auth/access/privileges", strings.NewReader(`[{"role":"reader","database":"nornic","read":true,"write":false}]`))
	rec = httptest.NewRecorder()
	server.handleAccessPrivileges(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// access/privileges: method not allowed
	req = httptest.NewRequest(http.MethodPost, "/auth/access/privileges", nil)
	rec = httptest.NewRecorder()
	server.handleAccessPrivileges(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// access/privileges: nil store unavailable
	origPriv := server.privilegesStore
	server.privilegesStore = nil
	req = httptest.NewRequest(http.MethodGet, "/auth/access/privileges", nil)
	rec = httptest.NewRecorder()
	server.handleAccessPrivileges(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.privilegesStore = origPriv
}

func TestClusterAndRollbackForbiddenBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// No claims + auth enabled => deny database access.
	req := httptest.NewRequest(http.MethodGet, "/db/"+dbName+"/cluster", nil)
	rec := httptest.NewRecorder()
	server.handleClusterStatus(rec, req, dbName)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// With claims that can access database => success.
	req = httptest.NewRequest(http.MethodGet, "/db/"+dbName+"/cluster", nil)
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec = httptest.NewRecorder()
	server.handleClusterStatus(rec, req, dbName)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Rollback forbidden branch.
	req = httptest.NewRequest(http.MethodDelete, "/db/"+dbName+"/tx/missing", nil)
	rec = httptest.NewRecorder()
	server.handleRollbackTransaction(rec, req, dbName, "missing")
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// Rollback not found branch with allowed claims.
	req = httptest.NewRequest(http.MethodDelete, "/db/"+dbName+"/tx/missing", nil)
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec = httptest.NewRecorder()
	server.handleRollbackTransaction(rec, req, dbName, "missing")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRouteRegistrationAdvancedBranches(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")

	// Heimdall enabled + handler present path.
	server.config.Features = &nornicConfig.FeatureFlagsConfig{HeimdallEnabled: true}
	router := newHeimdallDBRouter(server.db, server.dbManager, server.config.Features)
	h := heimdall.NewHandler(&heimdall.Manager{}, heimdall.Config{Enabled: true, Model: "test"}, router, &heimdallMetricsReader{})
	server.setHeimdallHandler(h)
	mux := http.NewServeMux()
	server.registerHeimdallRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// GraphQL trace branch and playground endpoint.
	origTrace := os.Getenv("NORNICDB_TRACE_GRAPHQL")
	t.Setenv("NORNICDB_TRACE_GRAPHQL", "1")
	defer os.Setenv("NORNICDB_TRACE_GRAPHQL", origTrace)

	mux = http.NewServeMux()
	server.registerGraphQLRoutes(mux)
	req = httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{__typename}"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/graphql/playground", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotEqual(t, http.StatusNotFound, rec.Code)

	// MCP auth-wrapped endpoint branches.
	mux = http.NewServeMux()
	server.registerMCPRoutes(mux)
	req = httptest.NewRequest(http.MethodGet, "/mcp", nil)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	assert.NotEqual(t, http.StatusUnauthorized, rec.Code)
}

func TestValueConversionAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	assert.Nil(t, server.convertValueToNeo4jFormat(nil))

	node := &storage.Node{
		ID:         "node-x",
		Labels:     []string{"Doc"},
		Properties: map[string]interface{}{"title": "x"},
	}
	edge := &storage.Edge{
		ID:         "edge-x",
		StartNode:  "node-x",
		EndNode:    "node-y",
		Type:       "LINKS",
		Properties: map[string]interface{}{"weight": 1},
	}

	nv := server.convertValueToNeo4jFormat(node)
	ev := server.convertValueToNeo4jFormat(edge)
	nm, ok := nv.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "4:nornicdb:node-x", nm["elementId"])
	em, ok := ev.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "5:nornicdb:edge-x", em["elementId"])

	already := map[string]interface{}{"elementId": "4:nornicdb:keep"}
	assert.Equal(t, already, server.convertValueToNeo4jFormat(already))

	v := server.convertValueToNeo4jFormat(map[string]interface{}{
		"id":     "map-node",
		"labels": []string{"Mapped"},
		"name":   "alice",
		"nested": []interface{}{map[string]interface{}{"x": "y"}},
	})
	vm, ok := v.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "4:nornicdb:map-node", vm["elementId"])

	sliceVal := server.convertValueToNeo4jFormat([]interface{}{
		map[string]interface{}{"id": "n100", "labels": []string{"L"}},
		"text",
	})
	sliceOut, ok := sliceVal.([]interface{})
	assert.True(t, ok)
	assert.Len(t, sliceOut, 2)
}

func TestConstructorAdditionalBranches(t *testing.T) {
	// nil DB branch.
	s, err := New(nil, nil, DefaultConfig())
	assert.Error(t, err)
	assert.Nil(t, s)

	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer db.Close()

	authenticator := setupAuthForHelper(t)

	t.Run("slow query open failure and security mode", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.SlowQueryEnabled = true
		cfg.Logging.SlowQueryLogFile = t.TempDir() // directory path causes open failure branch
		cfg.MCPEnabled = false
		cfg.EmbeddingEnabled = false
		cfg.Features = nil // hit fallback feature resolution

		server, newErr := New(db, authenticator, cfg)
		assert.NoError(t, newErr)
		if server == nil {
			return
		}
		assert.NotNil(t, server.config.Features)
		assert.False(t, server.databaseAccessMode.CanAccessDatabase("nornic"))
		_ = server.Stop(context.Background())
	})

	t.Run("slow query logger success and embeddings retry branch", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.SlowQueryEnabled = true
		cfg.Logging.SlowQueryLogFile = filepath.Join(t.TempDir(), "slow.log")
		cfg.RateLimitEnabled = true
		cfg.MCPEnabled = true
		cfg.EmbeddingEnabled = true
		cfg.EmbeddingProvider = "local"
		cfg.EmbeddingModel = "missing-local-model"
		cfg.ModelsDir = t.TempDir()
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			HeimdallEnabled: false,
		}

		server, newErr := New(db, nil, cfg)
		assert.NoError(t, newErr)
		if server == nil {
			return
		}
		assert.NotNil(t, server.slowQueryLogger)
		assert.NotNil(t, server.rateLimiter)

		// Allow async embed init loop to execute and then stop to hit shutdown path.
		time.Sleep(80 * time.Millisecond)
		_ = server.Stop(context.Background())
	})

	// waitForDurationOrServerClose helper branches.
	assert.True(t, waitForDurationOrServerClose(nil, 0))
	assert.True(t, waitForDurationOrServerClose(nil, 5*time.Millisecond))
	server := &Server{}
	server.closed.Store(true)
	assert.False(t, waitForDurationOrServerClose(server, 25*time.Millisecond))
}

func TestConstructorHeimdallOpenAIAndStartBranches(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.MCPEnabled = true
	cfg.EmbeddingEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		HeimdallEnabled:  true,
		HeimdallProvider: "openai",
		HeimdallAPIKey:   "test-key",
		HeimdallAPIURL:   "http://127.0.0.1:1",
		HeimdallModel:    "gpt-4o-mini",
	}

	server, newErr := New(db, nil, cfg)
	assert.NoError(t, newErr)
	if server == nil {
		return
	}

	// Wait briefly for async Heimdall init to publish handler.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if server.getHeimdallHandler() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.NotNil(t, server.getHeimdallHandler())

	// Addr before start is empty.
	assert.Equal(t, "", server.Addr())

	// Start success path with random port and h2c branch.
	startErr := server.Start()
	assert.NoError(t, startErr)
	if startErr == nil {
		assert.NotEmpty(t, server.Addr())
	}
	_ = server.Stop(context.Background())

	// Start closed-server branch.
	startErr = server.Start()
	assert.ErrorIs(t, startErr, ErrServerClosed)
}

func TestStartQdrantGRPCInvalidPermissionBranch(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	cfg.MCPEnabled = false
	cfg.EmbeddingEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		QdrantGRPCEnabled: true,
		QdrantGRPCMethodPermissions: map[string]string{
			"/qdrant.Points/Upsert": "definitely-not-a-permission",
		},
	}

	server, newErr := New(db, nil, cfg)
	assert.NoError(t, newErr)
	if server == nil {
		return
	}

	startErr := server.Start()
	assert.Error(t, startErr)
	assert.Contains(t, startErr.Error(), "invalid RBAC permission")
}

func TestGPUHandlersInvalidManagerTypeBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	server.db.SetGPUManager("not-a-gpu-manager")

	req := httptest.NewRequest(http.MethodPost, "/admin/gpu/enable", nil)
	rec := httptest.NewRecorder()
	server.handleGPUEnable(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/disable", nil)
	rec = httptest.NewRecorder()
	server.handleGPUDisable(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader(`{"node_id":"n1","mode":"cpu"}`))
	rec = httptest.NewRecorder()
	server.handleGPUTest(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	// Invalid JSON branch.
	server.db.SetGPUManager(nil)
	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleGPUTest(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRoleAndEntitlementHandlersAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// handleRoles: nil store branch.
	origRoleStore := server.roleStore
	server.roleStore = nil
	req := httptest.NewRequest(http.MethodGet, "/auth/roles", nil)
	rec := httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.roleStore = origRoleStore

	// handleRoles: invalid JSON.
	req = httptest.NewRequest(http.MethodPost, "/auth/roles", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// handleRoles: empty name.
	req = httptest.NewRequest(http.MethodPost, "/auth/roles", strings.NewReader(`{"name":"   "}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// handleRoles: builtin conflict.
	req = httptest.NewRequest(http.MethodPost, "/auth/roles", strings.NewReader(`{"name":"admin"}`))
	rec = httptest.NewRecorder()
	server.handleRoles(rec, req)
	assert.Equal(t, http.StatusConflict, rec.Code)

	// handleRoleEntitlements: nil store.
	origEnt := server.roleEntitlementsStore
	server.roleEntitlementsStore = nil
	req = httptest.NewRequest(http.MethodGet, "/auth/role-entitlements", nil)
	rec = httptest.NewRecorder()
	server.handleRoleEntitlements(rec, req)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	server.roleEntitlementsStore = origEnt

	// handleRoleEntitlements: invalid JSON.
	req = httptest.NewRequest(http.MethodPut, "/auth/role-entitlements", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleRoleEntitlements(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// handleRoleEntitlements: invalid entitlement id.
	req = httptest.NewRequest(http.MethodPut, "/auth/role-entitlements", strings.NewReader(`{"role":"viewer","entitlements":["definitely_invalid"]}`))
	rec = httptest.NewRecorder()
	server.handleRoleEntitlements(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// handleRoleEntitlements: body missing role and mappings.
	req = httptest.NewRequest(http.MethodPut, "/auth/role-entitlements", strings.NewReader(`{"entitlements":["read"]}`))
	rec = httptest.NewRecorder()
	server.handleRoleEntitlements(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// handleRoleEntitlements: mappings success path.
	req = httptest.NewRequest(http.MethodPut, "/auth/role-entitlements", strings.NewReader(`{"mappings":[{"role":"viewer","entitlements":["read"]}]}`))
	rec = httptest.NewRecorder()
	server.handleRoleEntitlements(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHeimdallRouterStatsAndDiscoverDeeperBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	engine, err := server.dbManager.GetStorage(dbName)
	assert.NoError(t, err)
	if err != nil {
		return
	}

	_, _ = engine.CreateNode(&storage.Node{
		ID:         "nornic:doc_a",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"title": "alpha title", "content": strings.Repeat("alpha ", 80)},
	})
	_, _ = engine.CreateNode(&storage.Node{
		ID:         "nornic:doc_b",
		Labels:     []string{"Document"},
		Properties: map[string]interface{}{"title": "beta title", "content": "beta"},
	})
	_ = engine.CreateEdge(&storage.Edge{
		ID:        "nornic:edge_ab",
		StartNode: "nornic:doc_a",
		EndNode:   "nornic:doc_b",
		Type:      "REL",
	})

	// Ensure search service is available and indexed for discover success path.
	svc, svcErr := server.db.GetOrCreateSearchService(dbName, engine)
	assert.NoError(t, svcErr)
	if svcErr == nil && svc != nil {
		_ = svc.BuildIndexes(context.Background())
	}

	features := &nornicConfig.FeatureFlagsConfig{
		HeimdallEnabled:                true,
		HeimdallAnomalyDetection:       true,
		HeimdallRuntimeDiagnosis:       true,
		HeimdallMemoryCuration:         true,
		TopologyAutoIntegrationEnabled: true,
		KalmanEnabled:                  true,
	}
	router := newHeimdallDBRouter(server.db, server.dbManager, features)

	// Stats with features and search service path.
	stats, err := router.Stats(dbName)
	assert.NoError(t, err)
	assert.NotNil(t, stats.FeatureFlags)

	// Stats system-db branch (no search stats).
	systemStats, err := router.Stats("system")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, systemStats.NodeCount, int64(0))

	// Stats unknown db error branch.
	_, err = router.Stats("does-not-exist-db")
	assert.Error(t, err)

	// Discover successful branch with traversal and content preview truncation.
	res, err := router.Discover(context.Background(), dbName, "alpha", []string{"Document"}, 2, 2)
	if err == nil {
		assert.NotNil(t, res)
		assert.Equal(t, "keyword", res.Method)
	}
}

func TestPublicHandlersAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// discovery non-root path.
	req := httptest.NewRequest(http.MethodGet, "/not-root", nil)
	rec := httptest.NewRecorder()
	server.handleDiscovery(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// status with canceled context returns early without writing full response.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = httptest.NewRequest(http.MethodGet, "/status", nil).WithContext(ctx)
	rec = httptest.NewRecorder()
	server.handleStatus(rec, req)
	assert.True(t, rec.Code == 0 || rec.Code == http.StatusOK)

	// status normal branch with no dbManager fallback path.
	origManager := server.dbManager
	server.dbManager = nil
	req = httptest.NewRequest(http.MethodGet, "/status", nil)
	rec = httptest.NewRecorder()
	server.handleStatus(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	server.dbManager = origManager

	// NOTE: the handleMetrics slow-query content assertion was removed in
	// Plan 05-04. The rewritten handler calls observability.RenderLegacy
	// against the unified pkg/observability registry; metric content is
	// owned by pkg/observability/legacy_translation_test.go +
	// legacy_snapshot.golden, and server-layer wiring (headers + nil-safety)
	// is owned by pkg/server/server_public_test.go.
}

func TestExecuteTxStatementsAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	tx, err := server.txSessions.Open(context.Background(), dbName)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	defer server.txSessions.RollbackAndDelete(context.Background(), tx)

	// 1) Mutation without claims => forbidden.
	resp := &TransactionResponse{Results: make([]QueryResult, 0), Errors: make([]QueryError, 0)}
	server.executeTxStatements(context.Background(), "", nil, dbName, tx, []StatementRequest{
		{Statement: "CREATE (n:Doc {name:'x'})"},
	}, resp)
	assert.NotEmpty(t, resp.Errors)

	// 2) Claims without write privilege => forbidden.
	resp = &TransactionResponse{Results: make([]QueryResult, 0), Errors: make([]QueryError, 0)}
	server.executeTxStatements(context.Background(), "", &auth.JWTClaims{Roles: []string{"viewer"}}, dbName, tx, []StatementRequest{
		{Statement: "CREATE (n:Doc {name:'y'})"},
	}, resp)
	assert.NotEmpty(t, resp.Errors)

	// 3) Claims with write privilege + syntax error => statement syntax error.
	resp = &TransactionResponse{Results: make([]QueryResult, 0), Errors: make([]QueryError, 0)}
	server.executeTxStatements(context.Background(), "", &auth.JWTClaims{Roles: []string{"admin"}}, dbName, tx, []StatementRequest{
		{Statement: "INVALID CYPHER"},
	}, resp)
	assert.NotEmpty(t, resp.Errors)

	// 4) Read statement success path appends result.
	resp = &TransactionResponse{Results: make([]QueryResult, 0), Errors: make([]QueryError, 0)}
	server.executeTxStatements(context.Background(), "", &auth.JWTClaims{Roles: []string{"viewer"}}, dbName, tx, []StatementRequest{
		{Statement: "RETURN 1 AS n"},
	}, resp)
	assert.NotEmpty(t, resp.Results)
}

func TestBaseURLAndAuditBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// Base URL with explicit host and forwarded proto + base path header.
	req := httptest.NewRequest(http.MethodGet, "/auth/oauth/redirect", nil)
	req.Host = "example.com:8443"
	req.Header.Set("X-Forwarded-Proto", "https, http")
	req.Header.Set("X-Base-Path", "nornic")
	base := server.getBaseURL(req)
	assert.Equal(t, "https://example.com:8443/nornic", base)

	// Base URL fallback host from server config.
	req = httptest.NewRequest(http.MethodGet, "/auth/oauth/redirect", nil)
	req.Host = ""
	server.config.BasePath = "/api"
	base = server.getBaseURL(req)
	assert.Contains(t, base, "/api")

	// logAudit no-op branch and active logger branch.
	server.logAudit(req, "u1", "test_event", true, "ok")
	auditCfg := audit.DefaultConfig()
	auditCfg.LogPath = filepath.Join(t.TempDir(), "audit.log")
	logger, err := audit.NewLogger(auditCfg)
	assert.NoError(t, err)
	if err == nil {
		defer logger.Close()
		server.SetAuditLogger(logger)
		server.logAudit(req, "u2", "security_event", false, "denied")
	}
}

func TestAuthResolverAndRoleByIDAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// getDatabaseAccessMode/getResolvedAccess branches.
	server.auth = nil
	assert.True(t, server.getDatabaseAccessMode(nil).CanAccessDatabase("any"))
	server.auth = setupAuthForHelper(t)
	server.allowlistStore = nil
	assert.False(t, server.getDatabaseAccessMode(&auth.JWTClaims{Roles: []string{"admin"}}).CanAccessDatabase("nornic"))
	assert.Equal(t, auth.ResolvedAccess{}, server.getResolvedAccess(nil, "nornic"))

	// restore stores for role handlers.
	systemStorage, err := server.dbManager.GetStorage("system")
	assert.NoError(t, err)
	if err == nil {
		server.roleStore = auth.NewRoleStore(systemStorage)
		_ = server.roleStore.Load(context.Background())
	}

	// role by id: empty role delegates to handleRoles.
	req := httptest.NewRequest(http.MethodGet, "/auth/roles/", nil)
	rec := httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// role by id: invalid JSON patch.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/missing", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// role by id: empty new name.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/missing", strings.NewReader(`{"name":"   "}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// role by id: rename not found.
	req = httptest.NewRequest(http.MethodPatch, "/auth/roles/missing", strings.NewReader(`{"name":"renamed"}`))
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// role by id: delete builtin hits role store builtin branch (no auth user conflict check).
	server.auth = nil
	req = httptest.NewRequest(http.MethodDelete, "/auth/roles/admin", nil)
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// role by id: method not allowed.
	req = httptest.NewRequest(http.MethodGet, "/auth/roles/admin", nil)
	rec = httptest.NewRecorder()
	server.handleRoleByID(rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestOAuthRedirectInternalErrorBranch(t *testing.T) {
	server, _ := setupTestServer(t)
	server.oauthManager = &auth.OAuthManager{}

	req := httptest.NewRequest(http.MethodGet, "/auth/oauth/redirect", nil)
	rec := httptest.NewRecorder()
	server.handleOAuthRedirect(rec, req)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestDatabaseInfoAndExecuteInTxAdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// handleDatabaseInfo not found.
	req := httptest.NewRequest(http.MethodGet, "/db/missing", nil)
	rec := httptest.NewRecorder()
	server.handleDatabaseInfo(rec, req, "missing")
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// handleDatabaseInfo forbidden with no claims.
	req = httptest.NewRequest(http.MethodGet, "/db/"+dbName, nil)
	rec = httptest.NewRecorder()
	server.handleDatabaseInfo(rec, req, dbName)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// handleExecuteInTransaction forbidden with no claims.
	req = httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/tid", strings.NewReader(`{"statements":[]}`))
	rec = httptest.NewRecorder()
	server.handleExecuteInTransaction(rec, req, dbName, "tid")
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// handleExecuteInTransaction not found tx with claims.
	req = httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/tid", strings.NewReader(`{"statements":[]}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Roles: []string{"admin"}}))
	rec = httptest.NewRecorder()
	server.handleExecuteInTransaction(rec, req, dbName, "tid")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandleSearchDimensionMismatchAndFallbackBranches(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	dbName := server.dbManager.DefaultDatabaseName()

	storageEngine, err := server.dbManager.GetStorage(dbName)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	assert.NoError(t, err)
	if err != nil {
		return
	}

	// Seed vectors so vectorSearchUsable=true path is executed.
	seedVec := make([]float32, 1024)
	seedVec[0] = 1
	assert.NoError(t, searchSvc.IndexNode(&storage.Node{
		ID:              storage.NodeID("seed-vector-search"),
		Labels:          []string{"Seed"},
		Properties:      map[string]interface{}{"title": "seed"},
		ChunkEmbeddings: [][]float32{seedVec},
	}))

	// Short query with alternate dimensions still returns success in current behavior.
	server.db.SetEmbedder(&countingEmbedder{dims: 16})
	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "hello",
		"limit": 5,
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, resp.Code)

	// Short query + embed error (non-dimension mismatch) falls back to BM25 => 200.
	server.db.SetEmbedder(&countingEmbedder{
		dims:             1024,
		failIfLenGreater: 1, // force generic embed error for query "hello"
	})
	resp = makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "hello",
		"limit": 5,
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, resp.Code)

	// Chunked query path with alternate dimensions.
	server.db.SetEmbedder(&countingEmbedder{dims: 16})
	resp = makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": loadLargeDocQuery(t),
		"limit": 8,
	}, "Bearer "+token)
	assert.Equal(t, http.StatusOK, resp.Code)
}
