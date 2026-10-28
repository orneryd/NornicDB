// Package server provides HTTP REST API server tests.
package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Helpers
// =============================================================================

func setupTestServer(t testing.TB) (*Server, *auth.Authenticator) {
	return setupTestServerWithConfig(t, nil)
}

func stopTestServer(t testing.TB, server *Server) {
	t.Helper()
	if server == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = server.Stop(ctx)
}

func setupTestServerWithConfig(t testing.TB, configure func(*Config)) (*Server, *auth.Authenticator) {
	t.Helper()

	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "nornicdb-server-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	// Create database with decay disabled for faster tests
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.SearchCursorMax = 128
	config.Database.AsyncWritesEnabled = false // Disable async writes for predictable test behavior (200 OK vs 202 Accepted)
	// The server test suite exercises temporal reads (graph/temporal, graph/
	// diff) that require historical MVCC versions to still be resolvable at
	// an `as_of` timestamp. The production default is head-only
	// (MaxVersionsPerKey=0) so an operator opts into history explicitly;
	// tests opt in here so prior-version fixtures aren't silently erased
	// by the overwrite-in-place update path.
	config.Database.MVCCRetentionMaxVersions = 100

	// Set encryption key for remote credential tests.
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Share the production system database with the role and privilege stores.
	authConfig := auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}
	systemStorage := storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system")
	authenticator, err := auth.NewAuthenticator(authConfig, systemStorage)
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}

	// Create a test user
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}
	_, err = authenticator.CreateUser("reader", "password123", []auth.Role{auth.RoleViewer})
	if err != nil {
		t.Fatalf("failed to create reader user: %v", err)
	}

	// Create server config
	serverConfig := DefaultConfig()
	serverConfig.Port = 0 // Use random port
	// Keep generic server tests fast and deterministic:
	// avoid async external embedder initialization/retry loops on each setup.
	serverConfig.EmbeddingEnabled = false
	// Enable CORS with wildcard for tests (not recommended for production)
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"*"}
	if configure != nil {
		configure(serverConfig)
	}

	// Create server
	server, err := New(db, authenticator, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	return server, authenticator
}

func getAuthToken(t testing.TB, authenticator *auth.Authenticator, username string) string {
	t.Helper()
	tokenResp, _, err := authenticator.Authenticate(username, "password123", "127.0.0.1", "TestAgent")
	if err != nil {
		t.Fatalf("failed to get auth token: %v", err)
	}
	return tokenResp.AccessToken
}

func makeRequest(t testing.TB, server *Server, method, path string, body interface{}, authHeader string) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("failed to marshal body: %v", err)
		}
		reqBody = bytes.NewReader(jsonBody)
	}

	req := httptest.NewRequest(method, path, reqBody)
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	return recorder
}

func extractCountFromTxResponse(t *testing.T, resp *httptest.ResponseRecorder) int64 {
	t.Helper()
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	results, ok := payload["results"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(results), 0)

	firstResult, ok := results[0].(map[string]interface{})
	require.True(t, ok)

	data, ok := firstResult["data"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(data), 0)

	firstRow, ok := data[0].(map[string]interface{})
	require.True(t, ok)

	row, ok := firstRow["row"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(row), 0)

	rawCount, ok := row[0].(float64)
	require.True(t, ok)
	return int64(rawCount)
}

func extractRowsLenFromTxResponse(t *testing.T, resp *httptest.ResponseRecorder) int {
	t.Helper()
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	results, ok := payload["results"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(results), 0)

	firstResult, ok := results[0].(map[string]interface{})
	require.True(t, ok)

	data, ok := firstResult["data"].([]interface{})
	require.True(t, ok)
	return len(data)
}

func assertTxResponseHasNoErrors(t *testing.T, resp *httptest.ResponseRecorder) {
	t.Helper()
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	errorsRaw, ok := payload["errors"].([]interface{})
	if !ok {
		return
	}
	require.Len(t, errorsRaw, 0, "expected no transaction errors, got: %v", errorsRaw)
}

func loadLargeDocQuery(t *testing.T) string {
	t.Helper()
	path := filepath.Join("..", "..", "docs", "features", "gpu-acceleration.md")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	query := string(data)
	tokens, err := countTestTokens(query)
	require.NoError(t, err)
	require.Greater(t, tokens, 512)
	return query
}

func countTestTokens(text string) (int, error) {
	return len(strings.Fields(text)), nil
}

func chunkTestText(text string, maxTokens, overlap int) ([]string, error) {
	return textchunk.ChunkByTokenCount(text, maxTokens, overlap, countTestTokens)
}

type countingEmbedder struct {
	mu sync.Mutex

	failIfLenGreater    int
	failIfTokensGreater int
	dims                int

	calls     int
	maxLen    int
	maxTokens int
}

func (e *countingEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	e.mu.Lock()
	e.calls++
	if len(text) > e.maxLen {
		e.maxLen = len(text)
	}
	tokens, err := countTestTokens(text)
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	if tokens > e.maxTokens {
		e.maxTokens = tokens
	}
	failByLen := e.failIfLenGreater > 0 && len(text) > e.failIfLenGreater
	failByTokens := e.failIfTokensGreater > 0 && tokens > e.failIfTokensGreater
	fail := failByLen || failByTokens
	dims := e.dims
	e.mu.Unlock()

	if fail {
		return nil, fmt.Errorf("text too long: len=%d tokens=%d", len(text), tokens)
	}
	if dims <= 0 {
		dims = 1024
	}
	vec := make([]float32, dims)
	vec[0] = float32(len(text))
	return vec, nil
}

func (e *countingEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	var firstErr error
	for i, t := range texts {
		v, err := e.Embed(ctx, t)
		if err != nil && firstErr == nil {
			firstErr = err
		}
		out[i] = v
	}
	return out, firstErr
}

func (e *countingEmbedder) Dimensions() int { return e.dims }
func (e *countingEmbedder) Model() string   { return "counting-embedder" }
func (e *countingEmbedder) Backend() string { return "cpu" } // Plan 04-05 D-06
func (e *countingEmbedder) ChunkText(text string, maxTokens, overlap int) ([]string, error) {
	return chunkTestText(text, maxTokens, overlap)
}

// =============================================================================
// Server Creation Tests
// =============================================================================

func TestNew(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name      string
		db        *nornicdb.DB
		auth      *auth.Authenticator
		config    *Config
		wantError bool
	}{
		{
			name:      "valid with defaults",
			db:        db,
			auth:      nil,
			config:    nil,
			wantError: false,
		},
		{
			name:      "valid with custom config",
			db:        db,
			auth:      nil,
			config:    &Config{Port: 8080},
			wantError: false,
		},
		{
			name:      "nil database",
			db:        nil,
			auth:      nil,
			config:    nil,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := New(tt.db, tt.auth, tt.config)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if server == nil {
					t.Error("expected server, got nil")
				} else {
					t.Cleanup(func() { stopTestServer(t, server) })
				}
			}
		})
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	// SECURITY: Default should bind to localhost only (secure default)
	if config.Address != "127.0.0.1" {
		t.Errorf("expected address '127.0.0.1', got %s", config.Address)
	}
	if config.Port != 7474 {
		t.Errorf("expected port 7474, got %d", config.Port)
	}
	if config.ReadTimeout != 30*time.Second {
		t.Errorf("expected read timeout 30s, got %v", config.ReadTimeout)
	}
	if config.MaxRequestSize != 10*1024*1024 {
		t.Errorf("expected max request size 10MB, got %d", config.MaxRequestSize)
	}
	// SECURITY: CORS enabled by default for ease of use
	if config.EnableCORS == false {
		t.Error("expected CORS enabled by default for ease of use")
	}
}

// =============================================================================
// Discovery Endpoint Tests
// =============================================================================

func TestHandleDiscovery(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/", nil, "")

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var discovery map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&discovery); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Check required Neo4j discovery fields
	requiredFields := []string{"bolt_direct", "bolt_routing", "transaction", "neo4j_version", "neo4j_edition"}
	for _, field := range requiredFields {
		if _, ok := discovery[field]; !ok {
			t.Errorf("missing required field: %s", field)
		}
	}

	// Check NornicDB extension: default_database field
	if defaultDB, ok := discovery["default_database"]; !ok {
		t.Error("missing NornicDB extension field: default_database")
	} else {
		// Verify it's a string and matches expected default
		defaultDBStr, ok := defaultDB.(string)
		if !ok {
			t.Errorf("default_database should be a string, got %T", defaultDB)
		} else if defaultDBStr != "nornic" {
			t.Errorf("expected default_database to be 'nornic', got '%s'", defaultDBStr)
		}
	}
}

func TestHandleDiscoveryOmitsBoltEndpointsWhenDisabled(t *testing.T) {
	server, _ := setupTestServerWithConfig(t, func(config *Config) {
		config.BoltEnabled = false
	})

	resp := makeRequest(t, server, http.MethodGet, "/", nil, "")
	require.Equal(t, http.StatusOK, resp.Code)

	var discovery map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&discovery))
	require.NotContains(t, discovery, "bolt_direct")
	require.NotContains(t, discovery, "bolt_routing")
	require.Equal(t, false, discovery["bolt_enabled"])
	require.Contains(t, discovery, "transaction")
}

// =============================================================================
// Health Endpoint Tests
// =============================================================================

func TestHandleHealth(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/health", nil, "")

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var health map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if health["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", health["status"])
	}
}

func TestHandleStatus(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Status endpoint now requires authentication
	resp := makeRequest(t, server, "GET", "/status", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Check that response has the expected structure
	if status["status"] == nil {
		t.Error("missing 'status' field")
	}
	if status["server"] == nil {
		t.Error("missing 'server' field")
	}
	if status["database"] == nil {
		t.Error("missing 'database' field")
	}

	serverStats, ok := status["server"].(map[string]interface{})
	if !ok {
		t.Fatal("'server' field is not a map")
	}
	if serverStats["version"] != buildinfo.Version() {
		t.Fatalf("expected server.version %q, got %v", buildinfo.Version(), serverStats["version"])
	}

	// Verify database stats structure
	dbStats, ok := status["database"].(map[string]interface{})
	if !ok {
		t.Fatal("'database' field is not a map")
	}

	// Check for required fields
	if dbStats["nodes"] == nil {
		t.Error("missing 'nodes' field in database stats")
	}
	if dbStats["edges"] == nil {
		t.Error("missing 'edges' field in database stats")
	}
	if dbStats["databases"] == nil {
		t.Error("missing 'databases' field in database stats")
	}

	// Verify database count is a number
	dbCount, ok := dbStats["databases"].(float64)
	if !ok {
		t.Errorf("'databases' field should be a number, got %T", dbStats["databases"])
	}
	if dbCount < 1 {
		t.Errorf("expected at least 1 database (default + system), got %v", dbCount)
	}

	// Verify node and edge counts are numbers and >= 0
	nodeCount, ok := dbStats["nodes"].(float64)
	if !ok {
		t.Errorf("'nodes' field should be a number, got %T", dbStats["nodes"])
	}
	if nodeCount < 0 {
		t.Errorf("node count should be >= 0, got %v", nodeCount)
	}

	edgeCount, ok := dbStats["edges"].(float64)
	if !ok {
		t.Errorf("'edges' field should be a number, got %T", dbStats["edges"])
	}
	if edgeCount < 0 {
		t.Errorf("edge count should be >= 0, got %v", edgeCount)
	}

	// Verify that system database nodes are NOT included in the count
	// (system database has metadata nodes that shouldn't be counted)
	// The count should only include user databases
}

func TestServerStop_DeadlineExceededForcesClose(t *testing.T) {
	// This test simulates an in-flight handler that takes too long, ensuring
	// Server.Stop returns promptly when the shutdown context is exceeded.
	cfg := DefaultConfig()
	cfg.Headless = true

	db, err := nornicdb.Open("", nornicdb.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s, err := New(db, nil, cfg)
	require.NoError(t, err)

	// Override the router with a handler that blocks past shutdown deadline.
	started := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-started:
		default:
			close(started)
		}
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	s.httpServer = &http.Server{Handler: mux}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.listener = ln
	go func() { _ = s.httpServer.Serve(ln) }()

	// Issue a request that will be in-flight during shutdown.
	go func() {
		req, _ := http.NewRequest("GET", "http://"+ln.Addr().String()+"/status", nil)
		// no auth wrapper; direct handler
		_, _ = http.DefaultClient.Do(req)
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err = s.Stop(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Ensure Stop returns promptly and listener is closed.
	_, dialErr := net.DialTimeout("tcp", ln.Addr().String(), 50*time.Millisecond)
	require.Error(t, dialErr)
}

// TestMetaField_NodeMetadata verifies that the meta field is properly populated
// for nodes and relationships, and null for primitive values (Neo4j compatibility).
func TestMetaField_NodeMetadata(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a node and return it
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Person {name: 'TestPerson'}) RETURN n"},
		},
	}, "Bearer "+token)

	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)

	row := result.Results[0].Data[0]

	// Verify meta field exists and has correct length
	require.NotNil(t, row.Meta, "meta field should always be present")
	require.Len(t, row.Meta, 1, "meta should have one element per column")

	// Verify meta contains node metadata
	metaItem := row.Meta[0]
	require.NotNil(t, metaItem, "meta[0] should not be nil for a node")

	metaMap, ok := metaItem.(map[string]interface{})
	require.True(t, ok, "meta[0] should be a map for a node")

	// Verify required metadata fields
	require.Contains(t, metaMap, "id", "meta should contain 'id' field")
	require.Contains(t, metaMap, "type", "meta should contain 'type' field")
	require.Contains(t, metaMap, "elementId", "meta should contain 'elementId' field")
	require.Contains(t, metaMap, "deleted", "meta should contain 'deleted' field")

	// Verify type is "node"
	require.Equal(t, "node", metaMap["type"], "type should be 'node'")
	require.Equal(t, false, metaMap["deleted"], "deleted should be false")

	// Verify elementId format: the request database is "nornic", so the meta
	// elementId names it (#745 §2).
	elementId, ok := metaMap["elementId"].(string)
	require.True(t, ok, "elementId should be a string")
	require.True(t, strings.HasPrefix(elementId, "4:nornic:"), "elementId should start with '4:nornic:'")
}

// TestMetaField_PrimitiveValues verifies that meta field is null for primitive values.
func TestMetaField_PrimitiveValues(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Return primitive values (like SHOW DATABASES does)
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "RETURN 'test' as name, 123 as count, true as active"},
		},
	}, "Bearer "+token)

	require.Equal(t, http.StatusOK, resp.Code)

	var result TransactionResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	require.Len(t, result.Results, 1)
	require.Len(t, result.Results[0].Data, 1)

	row := result.Results[0].Data[0]

	// Verify meta field exists and has correct length
	require.NotNil(t, row.Meta, "meta field should always be present")
	require.Len(t, row.Meta, 3, "meta should have one element per column")

	// Verify all meta values are null for primitive values
	for i, metaItem := range row.Meta {
		require.Nil(t, metaItem, "meta[%d] should be null for primitive values", i)
	}
}

func TestServerNew_ConfiguresDefaultRemoteEngineFactory(t *testing.T) {
	server, _ := setupTestServer(t)

	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "comp_remote_default", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://127.0.0.1:1",
		},
	}))

	_, err := server.dbManager.GetStorageWithAuth("comp_remote_default", "Bearer token")
	require.NoError(t, err)

	require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "comp_remote_default_basic", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://127.0.0.1:1",
			AuthMode:     "user_password",
			User:         "svc-user",
			Password:     "svc-pass",
		},
	}))
	_, err = server.dbManager.GetStorageWithAuth("comp_remote_default_basic", "Bearer token")
	require.NoError(t, err)
}

func TestRegisterMCPRoutes_AllEndpoints(t *testing.T) {
	server, authn := setupTestServer(t)
	token := getAuthToken(t, authn, "admin")
	mux := http.NewServeMux()
	server.registerMCPRoutes(mux)

	paths := []string{
		"/mcp",
		"/mcp/initialize",
		"/mcp/tools/list",
		"/mcp/tools/call",
		"/mcp/health",
		// Database-pinned variants
		"/mcp/tenant_a",
		"/mcp/tenant_a/initialize",
		"/mcp/tenant_a/tools/list",
		"/mcp/tenant_a/tools/call",
	}

	for _, p := range paths {
		// Unauthorized branch for auth-wrapped endpoints (except health).
		req := httptest.NewRequest(http.MethodPost, p, strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if p == "/mcp/health" {
			require.Equal(t, http.StatusOK, rec.Code)
		} else {
			require.Equal(t, http.StatusUnauthorized, rec.Code)
		}

		// Authorized branch.
		req = httptest.NewRequest(http.MethodPost, p, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec = httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		require.NotEqual(t, http.StatusUnauthorized, rec.Code)
	}
}

func TestNew_AsyncEmbeddingSuccessAndHeimdallMCPBranch(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(tmpDir, cfg)
	require.NoError(t, err)
	defer db.Close()

	// Mock OpenAI embeddings endpoint used by async embedding init health check.
	embedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.1,0.2,0.3,0.4],"index":0}]}`))
	}))
	defer embedServer.Close()

	serverCfg := DefaultConfig()
	serverCfg.Port = 0
	serverCfg.MCPEnabled = true
	serverCfg.EmbeddingEnabled = true
	serverCfg.EmbeddingProvider = "openai"
	serverCfg.EmbeddingAPIURL = embedServer.URL
	serverCfg.EmbeddingAPIKey = "test-key"
	serverCfg.EmbeddingModel = "text-embedding-3-small"
	serverCfg.EmbeddingDimensions = 4
	serverCfg.EmbeddingCacheSize = 8
	serverCfg.Features = &nornicConfig.FeatureFlagsConfig{
		HeimdallEnabled:      true,
		HeimdallProvider:     "openai",
		HeimdallAPIKey:       "test-key",
		HeimdallAPIURL:       "http://127.0.0.1:1",
		HeimdallModel:        "gpt-4o-mini",
		HeimdallMCPEnable:    true,
		HeimdallMCPTools:     []string{"store", "recall"},
		SearchRerankEnabled:  true,
		SearchRerankProvider: "ollama",
		SearchRerankAPIURL:   "http://127.0.0.1:1/rerank",
	}
	serverCfg.PluginsDir = t.TempDir()
	serverCfg.HeimdallPluginsDir = t.TempDir()

	srv, err := New(db, nil, serverCfg)
	require.NoError(t, err)
	require.NotNil(t, srv)
	defer srv.Stop(context.Background())

	// Wait for async embedding init success path to wire embedder into DB.
	require.Eventually(t, func() bool {
		_, e := db.EmbedQuery(context.Background(), "health")
		return e == nil
	}, 2*time.Second, 50*time.Millisecond)

	// Wait for async Heimdall init to set handler.
	require.Eventually(t, func() bool {
		return srv.getHeimdallHandler() != nil
	}, 2*time.Second, 50*time.Millisecond)
}

func TestHeimdallExecutorForDatabase_CacheAndFallbackBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)

	dbName := server.dbManager.DefaultDatabaseName()
	name1, exec1, _, err := router.executorForDatabase(dbName)
	require.NoError(t, err)
	require.Equal(t, dbName, name1)
	require.NotNil(t, exec1)

	// Cached executor fast-path should return same pointer.
	name2, exec2, _, err := router.executorForDatabase(dbName)
	require.NoError(t, err)
	require.Equal(t, dbName, name2)
	require.Equal(t, exec1, exec2)

	// dbManager nil fallback storage path.
	routerNoMgr := newHeimdallDBRouter(server.db, nil, nil)
	_, exec3, _, err := routerNoMgr.executorForDatabase("")
	require.NoError(t, err)
	require.NotNil(t, exec3)
}

// TestHandleMetrics_BranchesWithAndWithoutEmbedQueue was removed in Plan 05-04.
//
// The rewritten handleMetrics calls observability.RenderLegacy against the
// unified pkg/observability registry. The metric-content assertions
// (nornicdb_info, nornicdb_embedding_worker_running) belonged to the old
// hand-built handler; they now live in two dedicated owners:
//
//   - Server-layer wiring (headers, nil-safety, status): pkg/server/server_public_test.go
//     (Test_HandleMetrics_DeprecationHeaders, Test_HandleMetrics_NilRegistry_ReturnsEmptyBodyWithHeaders)
//   - Metric byte-stream content: pkg/observability/legacy_translation_test.go
//     + pkg/observability/legacy_snapshot.golden

func TestNornicDBHandlers_AdditionalCoverage(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	})

	t.Run("embed stats and decay", func(t *testing.T) {
		// Embed stats when auto-embed is disabled (nil stats branch).
		req := as(httptest.NewRequest(http.MethodGet, "/nornicdb/embed/stats", nil), "admin")
		rec := httptest.NewRecorder()
		server.handleEmbedStats(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// Enable embedder then check enabled stats branch.
		server.db.SetEmbedder(&countingEmbedder{dims: 1024})
		req = as(httptest.NewRequest(http.MethodGet, "/nornicdb/embed/stats", nil), "admin")
		rec = httptest.NewRecorder()
		server.handleEmbedStats(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = as(httptest.NewRequest(http.MethodGet, "/nornicdb/decay", nil), "admin")
		rec = httptest.NewRecorder()
		server.handleDecay(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("embed clear and trigger", func(t *testing.T) {
		// embed clear method not allowed
		req := as(httptest.NewRequest(http.MethodGet, "/nornicdb/embed/clear", nil), "admin")
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/nornicdb/embed/clear", nil).WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}}))
		rec = httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// trigger method not allowed
		req = as(httptest.NewRequest(http.MethodGet, "/nornicdb/embed/trigger", nil), "admin")
		rec = httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		// trigger with embed queue not enabled
		serverNoEmbed, _ := setupTestServer(t)
		req = as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil), "admin")
		rec = httptest.NewRecorder()
		serverNoEmbed.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("similar handler branches", func(t *testing.T) {
		// method not allowed
		req := httptest.NewRequest(http.MethodGet, "/nornicdb/similar", nil)
		rec := httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		// invalid json
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader("{"))
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		// forbidden with no claims
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader(`{"node_id":"n1"}`))
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code)

		// db not found
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader(`{"database":"missing","node_id":"n1"}`)).WithContext(adminCtx)
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)

		// node not found
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader(`{"database":"`+dbName+`","node_id":"missing"}`)).WithContext(adminCtx)
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)

		// target node without embedding
		st, err := server.dbManager.GetStorage(dbName)
		require.NoError(t, err)
		_, _ = st.CreateNode(&storage.Node{ID: "no-emb-node", Labels: []string{"Doc"}, Properties: map[string]interface{}{"name": "x"}})
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader(`{"database":"`+dbName+`","node_id":"no-emb-node"}`)).WithContext(adminCtx)
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		// success path with embeddings.
		embA := make([]float32, 16)
		embA[0] = 1
		embB := make([]float32, 16)
		embB[0] = 0.9
		_, _ = st.CreateNode(&storage.Node{ID: "target-emb-node", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{embA}})
		_, _ = st.CreateNode(&storage.Node{ID: "other-emb-node", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{embB}})
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/similar", strings.NewReader(`{"database":"`+dbName+`","node_id":"target-emb-node","limit":1}`)).WithContext(adminCtx)
		rec = httptest.NewRecorder()
		server.handleSimilar(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestGPUHandlers_SuccessPathsWithManager(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// Create a CPU-only manager object to satisfy type checks.
	gm, err := gpu.NewManager(&gpu.Config{Enabled: false, FallbackOnError: true})
	require.NoError(t, err)
	server.db.SetGPUManager(gm)

	// Seed nodes with embeddings so FindSimilar can run.
	st, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	vecA := make([]float32, 8)
	vecA[0] = 1
	vecB := make([]float32, 8)
	vecB[0] = 0.95
	_, _ = st.CreateNode(&storage.Node{ID: "gpu_target", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{vecA}})
	_, _ = st.CreateNode(&storage.Node{ID: "gpu_other", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{vecB}})

	// status happy path with manager set
	req := httptest.NewRequest(http.MethodGet, "/admin/gpu/status", nil)
	rec := httptest.NewRecorder()
	server.handleGPUStatus(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// disable happy path
	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/disable", nil)
	rec = httptest.NewRecorder()
	server.handleGPUControl(rec, req, gpuControlDisable)
	require.Equal(t, http.StatusOK, rec.Code)

	// test handler success with mode auto and cpu
	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader(`{"node_id":"gpu_target","limit":1,"mode":"auto"}`))
	rec = httptest.NewRecorder()
	server.handleGPUTest(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader(`{"node_id":"gpu_target","limit":1,"mode":"cpu"}`))
	rec = httptest.NewRecorder()
	server.handleGPUTest(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

// =============================================================================
// Database Info Endpoint Tests
// =============================================================================

func TestHandleDatabaseInfo(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var info map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &info); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if _, ok := info["nodeStorageBytes"].(float64); !ok {
		t.Fatalf("expected nodeStorageBytes in response, got %T", info["nodeStorageBytes"])
	}
	if _, ok := info["managedEmbeddingBytes"].(float64); !ok {
		t.Fatalf("expected managedEmbeddingBytes in response, got %T", info["managedEmbeddingBytes"])
	}
}

// =============================================================================
// CORS Tests
// =============================================================================

func TestCORSHeaders(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// Should have CORS headers
	if recorder.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("missing Access-Control-Allow-Origin header")
	}
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/db/nornic/tx/commit", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Request.InvalidFormat", response.Errors[0].Code)
}

func TestNotFound(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/nonexistent/endpoint", nil, "")

	if resp.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.Code)
	}
}

// =============================================================================
// Server Lifecycle Tests
// =============================================================================

func TestServerStartStop(t *testing.T) {
	server, _ := setupTestServer(t)

	// Start server
	go func() {
		server.Start()
	}()

	// Give it time to start
	time.Sleep(100 * time.Millisecond)

	// Check stats
	stats := server.Stats()
	if stats.Uptime <= 0 {
		t.Error("expected positive uptime")
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Stop(ctx); err != nil {
		t.Errorf("stop error: %v", err)
	}
}

func TestServerStatsConcurrentWithStart(t *testing.T) {
	server, _ := setupTestServer(t)
	server.config.Port = 0
	start := make(chan struct{})
	started := make(chan error, 1)
	var readers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for iteration := 0; iteration < 10000; iteration++ {
				_ = server.Stats()
			}
		}()
	}
	go func() { <-start; started <- server.Start() }()
	close(start)
	startErr := <-started
	readers.Wait()
	require.NoError(t, startErr)
	require.Positive(t, server.Stats().Uptime)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, server.Stop(ctx))
}

func TestServerStats(t *testing.T) {
	server, _ := setupTestServer(t)

	stats := server.Stats()

	if stats.Uptime < 0 {
		t.Error("expected non-negative uptime")
	}
}

// =============================================================================
// Audit Logger Tests
// =============================================================================

func TestSetAuditLogger(t *testing.T) {
	server, _ := setupTestServer(t)

	// Create audit logger
	auditConfig := audit.Config{
		RetentionDays: 30,
	}
	auditLogger, err := audit.NewLogger(auditConfig)
	if err != nil {
		t.Fatalf("failed to create audit logger: %v", err)
	}
	defer auditLogger.Close()

	// Set it
	server.SetAuditLogger(auditLogger)

	// Make a request that would be audited
	makeRequest(t, server, "GET", "/health", nil, "")

	// No error means success
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestIsMutationQuery(t *testing.T) {
	tests := []struct {
		query    string
		expected bool
	}{
		{"MATCH (n) RETURN n", false},
		{"CREATE (n:Test)", true},
		{"MERGE (n:Test)", true},
		{"DELETE n", true},
		{"SET n.prop = 1", true},
		{"REMOVE n.prop", true},
		{"DROP INDEX", true},
		{"  CREATE (n)", true},
		{"match (n) return n", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := isMutationQuery(tt.query)
			if result != tt.expected {
				t.Errorf("isMutationQuery(%q) = %v, want %v", tt.query, result, tt.expected)
			}
		})
	}
}

func TestParseIntQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		key      string
		def      int
		expected int
	}{
		{"present", "limit=50", "limit", 10, 50},
		{"missing", "", "limit", 10, 10},
		{"invalid", "limit=abc", "limit", 10, 10},
		{"zero", "limit=0", "limit", 10, 10},
		{"negative", "limit=-5", "limit", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tt.query, nil)
			result := parseIntQuery(req, tt.key, tt.def)
			if result != tt.expected {
				t.Errorf("parseIntQuery() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestHasPermission(t *testing.T) {
	tests := []struct {
		name     string
		roles    []string
		perm     auth.Permission
		expected bool
	}{
		{"admin has all", []string{"admin"}, auth.PermAdmin, true},
		{"admin has write", []string{"admin"}, auth.PermWrite, true},
		{"admin perm implies write", []string{"admin"}, auth.PermDelete, true},
		{"viewer has read", []string{"viewer"}, auth.PermRead, true},
		{"viewer no write", []string{"viewer"}, auth.PermWrite, false},
		// A role is not a permission: a role named after one grants nothing
		// (the one entitlement check, auth.EntitlementsAllow, as Bolt).
		{"permission name as role grants nothing", []string{"write"}, auth.PermWrite, false},
		{"perm admin implies delete", []string{"admin"}, auth.PermDelete, true},
		{"role casing normalized", []string{"Admin"}, auth.PermWrite, true},
		{"role_ prefix normalized", []string{"ROLE_ADMIN"}, auth.PermWrite, true},
		{"permission name as role in capitals grants nothing", []string{"WRITE"}, auth.PermWrite, false},
		{"empty roles", []string{}, auth.PermRead, false},
		{"invalid role", []string{"invalid"}, auth.PermRead, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasPermission(nil, tt.roles, tt.perm)
			if result != tt.expected {
				t.Errorf("hasPermission() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetClientIP(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		remote   string
		expected string
	}{
		{
			name:     "X-Forwarded-For",
			headers:  map[string]string{"X-Forwarded-For": "1.2.3.4, 5.6.7.8"},
			remote:   "127.0.0.1:1234",
			expected: "1.2.3.4",
		},
		{
			name:     "X-Real-IP",
			headers:  map[string]string{"X-Real-IP": "1.2.3.4"},
			remote:   "127.0.0.1:1234",
			expected: "1.2.3.4",
		},
		{
			name:     "RemoteAddr fallback",
			headers:  map[string]string{},
			remote:   "192.168.1.1:1234",
			expected: "192.168.1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.RemoteAddr = tt.remote
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result := getClientIP(req)
			if result != tt.expected {
				t.Errorf("getClientIP() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetCookie(t *testing.T) {
	tests := []struct {
		name     string
		cookies  []*http.Cookie
		key      string
		expected string
	}{
		{
			name:     "cookie exists",
			cookies:  []*http.Cookie{{Name: "token", Value: "abc123"}},
			key:      "token",
			expected: "abc123",
		},
		{
			name:     "cookie missing",
			cookies:  []*http.Cookie{},
			key:      "token",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			for _, c := range tt.cookies {
				req.AddCookie(c)
			}

			result := getCookie(req, tt.key)
			if result != tt.expected {
				t.Errorf("getCookie() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	// This tests that panics are recovered
	server, _ := setupTestServer(t)

	// Normal request should work
	resp := makeRequest(t, server, "GET", "/health", nil, "")
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}
}

func TestAddr(t *testing.T) {
	server, _ := setupTestServer(t)

	// Before starting, Addr should be empty or return something
	addr := server.Addr()
	// Just verify it doesn't panic
	_ = addr
}

func TestTokenAuthDisabled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "nornicdb-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create server without authenticator
	serverConfig := DefaultConfig()
	server, err := New(db, nil, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { stopTestServer(t, server) })

	// Token endpoint should fail when auth is not configured
	resp := makeRequest(t, server, "POST", "/auth/token", map[string]interface{}{
		"username": "test",
		"password": "test",
	}, "")

	if resp.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status 503 when auth disabled, got %d", resp.Code)
	}
}

func TestAuthWithNoRequiredPermission(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Health endpoint doesn't require auth
	resp := makeRequest(t, server, "GET", "/health", nil, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}
}

func TestDatabaseUnknownPath(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic/unknown/path", nil, "Bearer "+token)
	if resp.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.Code)
	}
}

func TestDatabaseEmptyName(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/", nil, "Bearer "+token)
	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", resp.Code)
	}
}

func TestInvalidBasicAuthFormat(t *testing.T) {
	server, _ := setupTestServer(t)

	// Invalid base64
	resp := makeRequest(t, server, "GET", "/auth/me", nil, "Basic not-base64!!!")
	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", resp.Code)
	}
}

func TestInvalidBasicAuthNoColon(t *testing.T) {
	server, _ := setupTestServer(t)

	// Valid base64 but no colon separator
	credentials := base64.StdEncoding.EncodeToString([]byte("nocolon"))
	resp := makeRequest(t, server, "GET", "/auth/me", nil, "Basic "+credentials)
	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", resp.Code)
	}
}

func TestIPRateLimiter_ResetAndCleanupLoopBranches(t *testing.T) {
	rl := NewIPRateLimiter(2, 2, 1)
	defer rl.Stop()

	require.True(t, rl.Allow("10.0.0.1"))
	require.True(t, rl.Allow("10.0.0.1"))
	require.False(t, rl.Allow("10.0.0.1"))

	rl.mu.RLock()
	c := rl.counters["10.0.0.1"]
	rl.mu.RUnlock()
	require.NotNil(t, c)

	c.mu.Lock()
	c.minuteCount = 999
	c.hourCount = 1
	c.minuteReset = time.Now().Add(-time.Second)
	c.hourReset = time.Now().Add(time.Hour)
	c.mu.Unlock()
	require.True(t, rl.Allow("10.0.0.1"))

	c.mu.Lock()
	c.hourCount = 999
	c.hourReset = time.Now().Add(-time.Second)
	c.mu.Unlock()
	require.True(t, rl.Allow("10.0.0.1"))

	manual := &IPRateLimiter{
		counters:        map[string]*ipRateLimitCounter{},
		perMinute:       10,
		perHour:         10,
		burst:           1,
		cleanupInterval: 5 * time.Millisecond,
		stopCleanup:     make(chan struct{}),
	}
	manual.counters["stale"] = &ipRateLimitCounter{
		hourReset:   time.Now().Add(-time.Second),
		minuteReset: time.Now().Add(time.Minute),
	}
	go manual.cleanupLoop()
	require.Eventually(t, func() bool {
		manual.mu.RLock()
		defer manual.mu.RUnlock()
		_, ok := manual.counters["stale"]
		return !ok
	}, 250*time.Millisecond, 10*time.Millisecond)
	manual.Stop()
}

func TestDbConfigGPUAndMiddleware_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("db config prefix method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/admin/databases/nornic/config", nil)
		req = req.WithContext(context.WithValue(req.Context(), contextKeyClaims, &auth.JWTClaims{Roles: []string{string(auth.RoleAdmin)}}))
		rec := httptest.NewRecorder()
		server.handleDbConfigPrefix(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("gpu test mode gpu branch", func(t *testing.T) {
		gm, err := gpu.NewManager(&gpu.Config{Enabled: false, FallbackOnError: true})
		require.NoError(t, err)
		server.db.SetGPUManager(gm)

		req := httptest.NewRequest(http.MethodPost, "/admin/gpu/test", strings.NewReader(`{"node_id":"missing","mode":"gpu"}`))
		rec := httptest.NewRecorder()
		server.handleGPUTest(rec, req)
		// Either GPU enable fails (500) or similar lookup fails (500) depending on environment.
		require.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("recovery middleware debug stack path", func(t *testing.T) {
		t.Setenv("NORNICDB_DEBUG", "true")
		h := server.recoveryMiddleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("boom-debug")
		}))
		req := httptest.NewRequest(http.MethodGet, "/panic", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestServer_AdditionalBranches_ReusesFixture(t *testing.T) {
	newServer := func(t *testing.T) *Server {
		t.Helper()
		server, _ := setupTestServer(t)
		return server
	}

	t.Run("embed trigger and rbac helpers", func(t *testing.T) {
		server := newServer(t)
		baseReq := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
		sameReq := server.withBifrostRBAC(baseReq)
		require.Equal(t, baseReq, sameReq)
		require.Nil(t, auth.RequestPrincipalRolesFromContext(sameReq.Context()))

		claimedReq := baseReq.WithContext(context.WithValue(baseReq.Context(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		enriched := server.withBifrostRBAC(claimedReq)
		require.NotNil(t, auth.RequestPrincipalRolesFromContext(enriched.Context()))
		require.NotNil(t, auth.RequestDatabaseAccessModeFromContext(enriched.Context()))
		resolver := auth.RequestResolvedAccessResolverFromContext(enriched.Context())
		require.NotNil(t, resolver)
		_ = resolver(server.dbManager.DefaultDatabaseName())
		databaseScope := auth.RequestDatabaseScopeFromContext(enriched.Context())
		require.NotNil(t, databaseScope)
		resolvedDatabase, ok := databaseScope.Resolve(server.dbManager.DefaultDatabaseName())
		require.True(t, ok)
		require.Equal(t, server.dbManager.DefaultDatabaseName(), resolvedDatabase)

		server.db.SetEmbedder(&countingEmbedder{dims: 1024})

		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil).WithContext(enriched.Context())
		rec := httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code)

		req = as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil), "admin")
		rec = httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/nornicdb/embed/clear", nil).WithContext(enriched.Context())
		rec = httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("rebuild and embed trigger error branches", func(t *testing.T) {
		server := newServer(t)
		rebuildReq := httptest.NewRequest(http.MethodPost, "/nornicdb/search/rebuild", strings.NewReader(`{"database":"nornic"}`))
		rebuildRec := httptest.NewRecorder()
		server.handleSearchRebuild(rebuildRec, rebuildReq)
		require.Equal(t, http.StatusForbidden, rebuildRec.Code)

		adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		})
		cancelCtx, cancelRebuild := context.WithCancel(adminCtx)
		cancelRebuild()
		rebuildReq = httptest.NewRequest(http.MethodPost, "/nornicdb/search/rebuild", strings.NewReader(`{"database":"nornic"}`)).WithContext(cancelCtx)
		rebuildRec = httptest.NewRecorder()
		server.handleSearchRebuild(rebuildRec, rebuildReq)
		require.Equal(t, http.StatusInternalServerError, rebuildRec.Code)

		server.db.SetEmbedder(&countingEmbedder{dims: 1024})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		triggerReq := as(httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil).WithContext(ctx), "admin")
		triggerRec := httptest.NewRecorder()
		server.handleEmbedTrigger(triggerRec, triggerReq)
		require.Equal(t, http.StatusOK, triggerRec.Code)
	})

	t.Run("search build started known dbs includes composite branch", func(t *testing.T) {
		server := newServer(t)
		ref := multidb.ConstituentRef{
			Alias:        server.dbManager.DefaultDatabaseName(),
			DatabaseName: server.dbManager.DefaultDatabaseName(),
			Type:         "local",
			AccessMode:   "read_write",
		}
		require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), "cmp_search_cov", []multidb.ConstituentRef{ref}))
		server.ensureSearchBuildStartedForKnownDatabases()
		require.NoError(t, server.dbManager.DropCompositeDatabase(t.Context(), "cmp_search_cov"))
	})

	t.Run("auth utility minor gaps", func(t *testing.T) {
		server := newServer(t)
		req := httptest.NewRequest(http.MethodGet, "/auth/oauth/callback?code=x&state=y", nil)
		rec := httptest.NewRecorder()
		server.oauthManager = nil
		server.handleOAuthCallback(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		require.False(t, isHTTPSRequest(nil))

		roleEntReq := httptest.NewRequest(http.MethodPut, "/auth/role-entitlements", strings.NewReader(`{"mappings":[{"role":"   ","entitlements":["read"]}]}`))
		roleEntRec := httptest.NewRecorder()
		server.handleRoleEntitlements(roleEntRec, roleEntReq)
		require.Equal(t, http.StatusOK, roleEntRec.Code)
	})
}

func TestUIAndStringHelpers_AdditionalBranches(t *testing.T) {

	origEnabled := UIEnabled
	origAssets := UIAssets
	defer func() {
		UIEnabled = origEnabled
		UIAssets = origAssets
	}()

	UIEnabled = false
	h, err := newUIHandler()
	require.NoError(t, err)
	require.Nil(t, h)

	UIEnabled = true
	UIAssets = nil
	h, err = newUIHandler()
	require.Error(t, err)
	require.Nil(t, h)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept", "application/json")
	require.False(t, isUIRequest(req))
	req.Header.Set("Accept", "text/html,application/xhtml+xml")
	require.True(t, isUIRequest(req))
}

func TestStartupSearchReconcileDatabases(t *testing.T) {
	tests := []struct {
		name         string
		database     string
		composite    string
		requireReady bool
	}{
		{"TestStartupSearchReconcile_InitializesMetadataOnlyDatabase", "animals", "", true},
		{"TestStartupSearchReconcile_SkipsCompositeDatabases", "animals_cov", "animals_cmp_cov", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _ := setupTestServer(t)

			require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: tt.database}))
			if tt.composite != "" {
				require.NoError(t, server.dbManager.CreateCompositeDatabase(t.Context(), tt.composite, []multidb.ConstituentRef{
					{
						Alias:        "a",
						DatabaseName: tt.database,
						Type:         "local",
						AccessMode:   "read_write",
					},
				}))
				t.Cleanup(func() {
					_ = server.dbManager.DropCompositeDatabase(t.Context(), tt.composite)
				})
			}

			// Deterministically run the reconcile pass (the background ticker does this too).
			server.ensureSearchBuildStartedForKnownDatabases()

			require.Eventually(t, func() bool {
				st := server.db.GetDatabaseSearchStatus(tt.database)
				if tt.requireReady {
					return st.Initialized && st.Ready && !st.Building
				}
				return st.Initialized
			}, 3*time.Second, 50*time.Millisecond)

			if tt.composite != "" {
				// Composite DB should not receive its own search-service startup.
				cmpStatus := server.db.GetDatabaseSearchStatus(tt.composite)
				require.False(t, cmpStatus.Initialized)
				require.False(t, cmpStatus.Ready)
			}
		})
	}
}
