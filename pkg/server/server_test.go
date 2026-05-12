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
	"net/url"
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
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/textchunk"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Test Helpers
// =============================================================================

func setupTestServer(t *testing.T) (*Server, *auth.Authenticator) {
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
	config.Database.AsyncWritesEnabled = false // Disable async writes for predictable test behavior (200 OK vs 202 Accepted)
	// The server test suite exercises temporal reads (graph/temporal, graph/
	// diff) that require historical MVCC versions to still be resolvable at
	// an `as_of` timestamp. The production default is head-only
	// (MaxVersionsPerKey=0) so an operator opts into history explicitly;
	// tests opt in here so prior-version fixtures aren't silently erased
	// by the overwrite-in-place update path.
	config.Database.MVCCRetentionMaxVersions = 100

	db, err := nornicdb.Open(tmpDir, config)
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	// Create authenticator with memory storage for testing
	authConfig := auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}
	// Use memory storage for tests
	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(authConfig, memoryStorage)
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

	// Set encryption key for remote credential tests.
	t.Setenv("NORNICDB_REMOTE_CREDENTIALS_KEY", "test-remote-credential-key-32b!")

	// Create server config
	serverConfig := DefaultConfig()
	serverConfig.Port = 0 // Use random port
	// Keep generic server tests fast and deterministic:
	// avoid async external embedder initialization/retry loops on each setup.
	serverConfig.EmbeddingEnabled = false
	// Enable CORS with wildcard for tests (not recommended for production)
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"*"}

	// Create server
	server, err := New(db, authenticator, serverConfig)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	return server, authenticator
}

func getAuthToken(t *testing.T, authenticator *auth.Authenticator, username string) string {
	t.Helper()
	tokenResp, _, err := authenticator.Authenticate(username, "password123", "127.0.0.1", "TestAgent")
	if err != nil {
		t.Fatalf("failed to get auth token: %v", err)
	}
	return tokenResp.AccessToken
}

func makeRequest(t *testing.T, server *Server, method, path string, body interface{}, authHeader string) *httptest.ResponseRecorder {
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

	// Verify elementId format
	elementId, ok := metaMap["elementId"].(string)
	require.True(t, ok, "elementId should be a string")
	require.True(t, strings.HasPrefix(elementId, "4:nornicdb:"), "elementId should start with '4:nornicdb:'")
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

// =============================================================================
// Authentication Tests
// =============================================================================

func TestHandleToken(t *testing.T) {
	server, _ := setupTestServer(t)

	tests := []struct {
		name       string
		body       map[string]string
		wantStatus int
	}{
		{
			name:       "valid credentials",
			body:       map[string]string{"username": "admin", "password": "password123"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid username",
			body:       map[string]string{"username": "invalid", "password": "password123"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid password",
			body:       map[string]string{"username": "admin", "password": "wrongpassword"},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing fields",
			body:       map[string]string{},
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/auth/token", tt.body, "")

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, resp.Code)
			}

			if tt.wantStatus == http.StatusOK {
				var tokenResp map[string]interface{}
				if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if tokenResp["access_token"] == nil {
					t.Error("expected access_token in response")
				}
			}
		})
	}
}

func TestHandleTokenMethodNotAllowed(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "GET", "/auth/token", nil, "")

	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestHandleMe(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	tests := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "valid token",
			authHeader: "Bearer " + token,
			wantStatus: http.StatusOK,
		},
		{
			name:       "no auth",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "invalid token",
			authHeader: "Bearer invalid-token",
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "GET", "/auth/me", nil, tt.authHeader)

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, resp.Code)
			}
		})
	}
}

func TestBasicAuth(t *testing.T) {
	server, _ := setupTestServer(t)

	// Create basic auth header
	credentials := base64.StdEncoding.EncodeToString([]byte("admin:password123"))
	authHeader := "Basic " + credentials

	resp := makeRequest(t, server, "GET", "/auth/me", nil, authHeader)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 with basic auth, got %d", resp.Code)
	}
}

func TestBasicAuthInvalid(t *testing.T) {
	server, _ := setupTestServer(t)

	// Create invalid basic auth header
	credentials := base64.StdEncoding.EncodeToString([]byte("admin:wrongpassword"))
	authHeader := "Basic " + credentials

	resp := makeRequest(t, server, "GET", "/auth/me", nil, authHeader)

	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 with invalid basic auth, got %d", resp.Code)
	}
}

// =============================================================================
// Transaction Endpoint Tests (Neo4j Compatible)
// =============================================================================

func TestHandleImplicitTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{
			name: "valid query",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN n LIMIT 10"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "multiple statements",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN count(n) AS count"},
					{"statement": "MATCH (n) RETURN n LIMIT 5"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty statements",
			body:       map[string]interface{}{"statements": []map[string]interface{}{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", tt.body, "Bearer "+token)

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, resp.Code, resp.Body.String())
			}

			// Check Neo4j response format
			var txResp map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&txResp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}

			if _, ok := txResp["results"]; !ok {
				t.Error("missing 'results' field in response")
			}
			if _, ok := txResp["errors"]; !ok {
				t.Error("missing 'errors' field in response")
			}
		})
	}
}

func TestHandleOpenTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open a new transaction
	resp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if resp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", resp.Code, resp.Body.String())
	}

	// Check that commit URL is returned
	var txResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&txResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if txResp["commit"] == nil {
		t.Error("missing 'commit' URL in response")
	}
}

func TestHandleOpenTransaction_CompositeRemoteOpenSucceeds(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	dbManager, err := multidb.NewDatabaseManager(server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			_ = authToken
			return storage.NewMemoryEngine(), nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateCompositeDatabase("comp_remote", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read_write",
			URI:          "http://remote.example",
		},
	}))

	server.dbManager = dbManager
	server.txSessions = txsession.NewManager(30*time.Second, server.newExecutorForDatabase)

	resp := makeRequest(t, server, "POST", "/db/comp_remote/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	// Composite explicit transactions open a fabric coordinator on BEGIN. Remote
	// participant auth forwarding is exercised in the lower-level multidb/cypher
	// tests; at the HTTP layer we only require the transaction open to succeed.
	require.Equal(t, http.StatusCreated, resp.Code, resp.Body.String())
	var txResp map[string]interface{}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &txResp))
	require.NotNil(t, txResp["commit"])
}

func TestCompositeExplicitTx_SecondWriteShardErrorCode(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	dbManager, err := multidb.NewDatabaseManager(server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateDatabase("shard_a"))
	require.NoError(t, dbManager.CreateDatabase("shard_b"))
	require.NoError(t, dbManager.CreateCompositeDatabase("cmp_tx_code", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "shard_b", Type: "local", AccessMode: "read_write"},
	}))
	server.dbManager = dbManager
	server.txSessions = txsession.NewManager(30*time.Second, server.newExecutorForDatabase)

	// Open explicit transaction.
	openResp := makeRequest(t, server, "POST", "/db/cmp_tx_code/tx", map[string]interface{}{"statements": []map[string]interface{}{}}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())
	var open map[string]interface{}
	require.NoError(t, json.Unmarshal(openResp.Body.Bytes(), &open))
	commitURL, _ := open["commit"].(string)
	require.NotEmpty(t, commitURL)
	txPath := strings.TrimSuffix(strings.TrimPrefix(commitURL, "http://localhost:7474"), "/commit")

	// First write on shard a succeeds.
	firstResp := makeRequest(t, server, "POST", txPath, map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL { USE cmp_tx_code.a CREATE (n:W {id:'1'}) RETURN count(n) AS c } RETURN c"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, firstResp.Code, firstResp.Body.String())

	// Second write on shard b in same tx must fail with Neo4j tx type code.
	secondResp := makeRequest(t, server, "POST", txPath, map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL { USE cmp_tx_code.b CREATE (n:W {id:'2'}) RETURN count(n) AS c } RETURN c"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, secondResp.Code, secondResp.Body.String())

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(secondResp.Body.Bytes(), &body))
	errs, ok := body["errors"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, errs)
	firstErr, ok := errs[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", firstErr["code"])
}

func TestServerNew_ConfiguresDefaultRemoteEngineFactory(t *testing.T) {
	server, _ := setupTestServer(t)

	require.NoError(t, server.dbManager.CreateCompositeDatabase("comp_remote_default", []multidb.ConstituentRef{
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

	require.NoError(t, server.dbManager.CreateCompositeDatabase("comp_remote_default_basic", []multidb.ConstituentRef{
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

func TestGetExecutorForDatabaseWithAuthBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	// No-auth and non-remote path delegates to cached executor behavior.
	execA, err := server.getExecutorForDatabaseWithAuth("nornic", "")
	require.NoError(t, err)
	execB, err := server.getExecutorForDatabaseWithAuth("nornic", "Bearer token")
	require.NoError(t, err)
	require.Equal(t, execA, execB)
	require.False(t, server.databaseHasRemoteConstituent("nornic"))
	require.False(t, server.databaseHasRemoteConstituent("missing_db"))

	var tokenSeen string
	dbManager, err := multidb.NewDatabaseManager(server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			tokenSeen = authToken
			return storage.NewMemoryEngine(), nil
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbManager.CreateCompositeDatabase("comp_remote_exec", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example",
		},
	}))
	server.dbManager = dbManager
	server.executors = make(map[string]*cypher.StorageExecutor)

	require.True(t, server.databaseHasRemoteConstituent("comp_remote_exec"))
	execRemote, err := server.getExecutorForDatabaseWithAuth("comp_remote_exec", "Bearer remote")
	require.NoError(t, err)
	require.NotNil(t, execRemote)
	require.Equal(t, "Bearer remote", tokenSeen)

	// Factory error propagation path.
	badManager, err := multidb.NewDatabaseManager(server.db.GetBaseStorageForManager(), &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(_ multidb.ConstituentRef, _ string) (storage.Engine, error) {
			return nil, fmt.Errorf("remote dial failed")
		},
	})
	require.NoError(t, err)
	require.NoError(t, badManager.CreateCompositeDatabase("comp_remote_bad", []multidb.ConstituentRef{
		{
			Alias:        "r1",
			DatabaseName: "remote_db",
			Type:         "remote",
			AccessMode:   "read",
			URI:          "http://remote.example",
		},
	}))
	server.dbManager = badManager
	_, err = server.getExecutorForDatabaseWithAuth("comp_remote_bad", "Bearer token")
	require.Error(t, err)
	require.Contains(t, err.Error(), "remote dial failed")
}

func TestExplicitTransactionWorkflow(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Step 1: Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Fatalf("failed to open transaction: %d", openResp.Code)
	}

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	// Extract transaction ID from commit URL
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2] // Format: /db/nornic/tx/{txId}/commit

	// Step 2: Execute in transaction
	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) AS count"},
		},
	}, "Bearer "+token)

	if execResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for execute, got %d: %s", execResp.Code, execResp.Body.String())
	}

	// Step 3: Commit transaction
	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if commitResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for commit, got %d: %s", commitResp.Code, commitResp.Body.String())
	}
}

func TestRollbackTransaction(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Fatalf("failed to open transaction: %d", openResp.Code)
	}

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	// Rollback transaction
	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)

	if rollbackResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for rollback, got %d", rollbackResp.Code)
	}
}

func TestExplicitTransactionRollbackRevertsCreate(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {name: 'Transaction Test'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rollbackResp.Code, rollbackResp.Body.String())

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(0), extractCountFromTxResponse(t, verifyResp))
}

func TestExplicitTransactionRollbackRevertsNodeQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Cleanup outside explicit transaction.
	cleanupResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) DETACH DELETE n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, cleanupResp.Code, cleanupResp.Body.String())

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {name: 'Transaction Test'})"},
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	rollbackResp := makeRequest(t, server, "DELETE", fmt.Sprintf("/db/nornic/tx/%s", txID), nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, rollbackResp.Code, rollbackResp.Body.String())

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Test {name: 'Transaction Test'}) RETURN n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, 0, extractRowsLenFromTxResponse(t, verifyResp))
}

func TestImplicitTransactionSingleStatementRollsBackOnError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": `
CREATE (n:ImplicitRollback {id: 1})
WITH n
CREAT (m:ImplicitRollback {id: 2})
RETURN n
`,
			},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	var txResp map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&txResp))
	errorsList, ok := txResp["errors"].([]interface{})
	require.True(t, ok)
	require.Greater(t, len(errorsList), 0)

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:ImplicitRollback) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(0), extractCountFromTxResponse(t, verifyResp))
}

func TestExplicitTransactionCommitPersistsCreate(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusCreated, openResp.Code, openResp.Body.String())

	var openResult map[string]interface{}
	require.NoError(t, json.NewDecoder(openResp.Body).Decode(&openResult))
	commitURL, ok := openResult["commit"].(string)
	require.True(t, ok)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	execResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:TxCommitVerify {name: 'commit survives'})"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, execResp.Code, execResp.Body.String())
	assertTxResponseHasNoErrors(t, execResp)

	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, commitResp.Code, commitResp.Body.String())
	assertTxResponseHasNoErrors(t, commitResp)

	verifyResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:TxCommitVerify {name: 'commit survives'}) RETURN count(n) AS cnt"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, verifyResp.Code, verifyResp.Body.String())
	require.Equal(t, int64(1), extractCountFromTxResponse(t, verifyResp))

	cleanupResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:TxCommitVerify {name: 'commit survives'}) DETACH DELETE n"},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, cleanupResp.Code, cleanupResp.Body.String())
	assertTxResponseHasNoErrors(t, cleanupResp)
}

// =============================================================================
// Query Endpoint Tests
// =============================================================================

func TestHandleQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Use Neo4j-compatible endpoint for queries
	tests := []struct {
		name       string
		body       map[string]interface{}
		wantStatus int
	}{
		{
			name: "valid match query",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{"statement": "MATCH (n) RETURN n LIMIT 10"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "query with parameters",
			body: map[string]interface{}{
				"statements": []map[string]interface{}{
					{
						"statement":  "MATCH (n) WHERE n.name = $name RETURN n",
						"parameters": map[string]interface{}{"name": "test"},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", tt.body, "Bearer "+token)

			if resp.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d: %s", tt.wantStatus, resp.Code, resp.Body.String())
			}
		})
	}
}

// =============================================================================
// Node/Edge via Cypher Tests (Neo4j-compatible approach)
// =============================================================================

func TestNodesCRUDViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a node via Cypher
	createResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Person {name: 'Test User'}) RETURN n"},
		},
	}, "Bearer "+token)

	if createResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for create node, got %d: %s", createResp.Code, createResp.Body.String())
	}

	// Query nodes
	queryResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n:Person) RETURN n"},
		},
	}, "Bearer "+token)

	if queryResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for query, got %d", queryResp.Code)
	}
}

func TestEdgesCRUDViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create two nodes and a relationship via Cypher
	createResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'}) RETURN a, r, b"},
		},
	}, "Bearer "+token)

	if createResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for create, got %d: %s", createResp.Code, createResp.Body.String())
	}

	// Query relationships
	queryResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (a)-[r:KNOWS]->(b) RETURN a.name, r, b.name"},
		},
	}, "Bearer "+token)

	if queryResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for query, got %d", queryResp.Code)
	}
}

// =============================================================================
// Search Tests (NornicDB extension endpoints)
// =============================================================================

func TestHandleSearch(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test search endpoint - should work even with empty database
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": "test query",
		"limit": 10,
	}, "Bearer "+token)

	// Should return 200 (success) - search service caching and index building should handle empty database
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	// Verify response structure
	var results []interface{}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Results should be an array (may be empty if no data)
	if results == nil {
		t.Error("results should be an array, got nil")
	}

	// Test that search service is cached (second request should be faster)
	resp2 := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": "another query",
		"limit": 5,
	}, "Bearer "+token)

	if resp2.Code != http.StatusOK {
		t.Errorf("expected status 200 on second request, got %d: %s", resp2.Code, resp2.Body.String())
	}
}

func TestStartupSearchReconcile_InitializesMetadataOnlyDatabase(t *testing.T) {
	server, _ := setupTestServer(t)

	require.NoError(t, server.dbManager.CreateDatabase("animals"))

	// Deterministically run the reconcile pass (the background ticker does this too).
	server.ensureSearchBuildStartedForKnownDatabases()

	require.Eventually(t, func() bool {
		st := server.db.GetDatabaseSearchStatus("animals")
		return st.Initialized && st.Ready && !st.Building
	}, 3*time.Second, 50*time.Millisecond)
}

func TestStartupSearchReconcile_SkipsCompositeDatabases(t *testing.T) {
	server, _ := setupTestServer(t)

	require.NoError(t, server.dbManager.CreateDatabase("animals_cov"))
	require.NoError(t, server.dbManager.CreateCompositeDatabase("animals_cmp_cov", []multidb.ConstituentRef{
		{
			Alias:        "a",
			DatabaseName: "animals_cov",
			Type:         "local",
			AccessMode:   "read_write",
		},
	}))
	t.Cleanup(func() {
		_ = server.dbManager.DropCompositeDatabase("animals_cmp_cov")
	})

	server.ensureSearchBuildStartedForKnownDatabases()

	require.Eventually(t, func() bool {
		st := server.db.GetDatabaseSearchStatus("animals_cov")
		return st.Initialized
	}, 3*time.Second, 50*time.Millisecond)

	// Composite DB should not receive its own search-service startup.
	cmpStatus := server.db.GetDatabaseSearchStatus("animals_cmp_cov")
	require.False(t, cmpStatus.Initialized)
	require.False(t, cmpStatus.Ready)
}

func TestHandleSearch_ChunksLongQueriesForVectorSearch(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Simulate a local embedder that fails on long inputs (token buffer overflow, etc.).
	// The search endpoint should chunk the query and only embed chunk-sized strings.
	emb := &countingEmbedder{
		failIfTokensGreater: 512,
		dims:                1024,
	}
	server.db.SetEmbedder(emb)

	// Ensure vector search is actually usable; otherwise the handler correctly
	// short-circuits to BM25 and embedding is intentionally skipped.
	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	seedVec := make([]float32, 1024)
	seedVec[0] = 1
	require.NoError(t, searchSvc.IndexNode(&storage.Node{
		ID:              storage.NodeID("seed-vector-node"),
		Labels:          []string{"Seed"},
		Properties:      map[string]interface{}{"name": "seed"},
		ChunkEmbeddings: [][]float32{seedVec},
	}))
	require.Greater(t, searchSvc.EmbeddingCount(), 0)

	longQuery := loadLargeDocQuery(t)
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	emb.mu.Lock()
	calls := emb.calls
	maxTokens := emb.maxTokens
	emb.mu.Unlock()

	require.GreaterOrEqual(t, calls, 2, "expected query embedding to run on multiple chunks")
	require.LessOrEqual(t, maxTokens, 512, "expected no embedding call on chunks > 512 tokens")
}

func TestHandleSearch_SkipsEmbeddingWhenNoVectorsIndexed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	emb := &countingEmbedder{dims: 1024}
	server.db.SetEmbedder(emb)

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
	searchSvc.ClearVectorIndex()
	require.Equal(t, 0, searchSvc.EmbeddingCount())

	longQuery := strings.Repeat("a", 1200)
	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": longQuery,
		"limit": 10,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	emb.mu.Lock()
	calls := emb.calls
	emb.mu.Unlock()
	require.Equal(t, 0, calls, "expected no query embedding calls when vector index is empty")
}

func TestTxCommit_VectorQueryNodes_StringInput_UsesConfiguredEmbedder(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	emb := &countingEmbedder{dims: 1024}
	server.db.SetEmbedder(emb)

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)
	searchSvc, err := server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	seedVec := make([]float32, 1024)
	seedVec[0] = float32(len("seed"))
	require.NoError(t, searchSvc.IndexNode(&storage.Node{
		ID:              storage.NodeID("seed-vector-node"),
		Labels:          []string{"Seed"},
		Properties:      map[string]interface{}{"name": "seed"},
		ChunkEmbeddings: [][]float32{seedVec},
	}))

	resp := makeRequest(t, server, "POST", "/db/"+dbName+"/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": "CALL db.index.vector.queryNodes('node_embedding_index', 5, 'seed') YIELD node, score RETURN node.id AS id, score LIMIT 1",
			},
		},
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	if errs, ok := payload["errors"].([]interface{}); ok {
		require.Len(t, errs, 0, "expected no cypher errors for string vector query")
	}

	emb.mu.Lock()
	calls := emb.calls
	emb.mu.Unlock()
	require.Greater(t, calls, 0, "expected query embedder to be called")
}

func TestHandleSearchRebuild(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test search rebuild endpoint
	resp := makeRequest(t, server, "POST", "/nornicdb/search/rebuild", map[string]interface{}{
		"database": server.dbManager.DefaultDatabaseName(),
	}, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify response structure
	if result["success"] == nil {
		t.Error("missing 'success' field")
	}
	if result["database"] == nil {
		t.Error("missing 'database' field")
	}
	if result["message"] == nil {
		t.Error("missing 'message' field")
	}

	// Verify success is true
	success, ok := result["success"].(bool)
	if !ok {
		t.Errorf("'success' should be a boolean, got %T", result["success"])
	}
	if !success {
		t.Error("expected success to be true")
	}

	// Test rebuild without database parameter (should use default)
	resp2 := makeRequest(t, server, "POST", "/nornicdb/search/rebuild", nil, "Bearer "+token)
	if resp2.Code != http.StatusOK {
		t.Errorf("expected status 200 without database param, got %d: %s", resp2.Code, resp2.Body.String())
	}
}

func TestHandleSearchAdditionalDirectBranches(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Method not allowed.
	req := httptest.NewRequest(http.MethodGet, "/nornicdb/search", nil)
	rec := httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

	// Invalid JSON.
	req = httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	// Forbidden (auth enabled + no claims).
	req = httptest.NewRequest(http.MethodPost, "/nornicdb/search", strings.NewReader(`{"query":"x"}`))
	rec = httptest.NewRecorder()
	server.handleSearch(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// Search-not-ready branch.
	server.db.ResetSearchService(server.dbManager.DefaultDatabaseName())
	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{
		"query": "x",
	}, "Bearer "+token)
	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
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

func TestHandleSearchRebuild_WriteForbiddenBranch(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// Reader can access nornic but should not have write privilege for rebuild.
	req := httptest.NewRequest(http.MethodPost, "/nornicdb/search/rebuild", strings.NewReader(`{"database":"`+dbName+`"}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "reader",
		Roles:    []string{"viewer"},
	}))
	rec := httptest.NewRecorder()
	server.handleSearchRebuild(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestHandleCommitTransaction_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	// Open transaction as admin claim.
	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openResp map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openResp))
	commitURL, _ := openResp["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	// Commit with invalid final statement to exercise rollback-on-errors branch.
	commitReq := httptest.NewRequest(http.MethodPost, commitURL, strings.NewReader(`{"statements":[{"statement":"INVALID CYPHER"}]}`))
	commitReq = commitReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	commitRec := httptest.NewRecorder()
	server.handleCommitTransaction(commitRec, commitReq, dbName, txID)
	require.Equal(t, http.StatusOK, commitRec.Code)

	var commitResp map[string]interface{}
	require.NoError(t, json.NewDecoder(commitRec.Body).Decode(&commitResp))
	if errs, ok := commitResp["errors"].([]interface{}); ok {
		require.NotEmpty(t, errs)
	}
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
		req := httptest.NewRequest(http.MethodGet, "/nornicdb/embed/stats", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedStats(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// Enable embedder then check enabled stats branch.
		server.db.SetEmbedder(&countingEmbedder{dims: 1024})
		req = httptest.NewRequest(http.MethodGet, "/nornicdb/embed/stats", nil)
		rec = httptest.NewRecorder()
		server.handleEmbedStats(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodGet, "/nornicdb/decay", nil)
		rec = httptest.NewRecorder()
		server.handleDecay(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("embed clear and trigger", func(t *testing.T) {
		// embed clear method not allowed
		req := httptest.NewRequest(http.MethodGet, "/nornicdb/embed/clear", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/nornicdb/embed/clear", nil)
		rec = httptest.NewRecorder()
		server.handleEmbedClear(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// trigger method not allowed
		req = httptest.NewRequest(http.MethodGet, "/nornicdb/embed/trigger", nil)
		rec = httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		// trigger with embed queue not enabled
		serverNoEmbed, _ := setupTestServer(t)
		req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil)
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
	server.handleGPUDisable(rec, req)
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

func TestHandleSearch_LongChunkedBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	server.auth = nil // Full DB access for branch-focused direct handler tests

	require.NoError(t, server.dbManager.CreateDatabase("translations"))
	st, err := server.dbManager.GetStorage("translations")
	require.NoError(t, err)

	// Seed vectors so vectorSearchUsable=true.
	seed := make([]float32, 1024)
	seed[0] = 1
	_, _ = st.CreateNode(&storage.Node{
		ID:              "t_seed",
		Labels:          []string{"Doc"},
		Properties:      map[string]interface{}{"title": "seed"},
		ChunkEmbeddings: [][]float32{seed},
	})

	// Ensure search service exists and is ready for the database.
	svc, err := server.db.GetOrCreateSearchService("translations", st)
	require.NoError(t, err)
	require.NotNil(t, svc)
	require.NoError(t, svc.BuildIndexes(context.Background()))

	// Use embedder so chunked vector path is exercised.
	server.db.SetEmbedder(&countingEmbedder{dims: 1024})
	server.config.Features = &nornicConfig.FeatureFlagsConfig{
		SearchRerankEnabled: true,
	}
	t.Setenv("NORNICDB_SEARCH_DIAG_TIMINGS", "true")

	// > 32 chunks to trigger maxQueryChunks cap and high limit to hit perChunkLimit > 100 path.
	longQuery := strings.Repeat("This is a long query block for chunking. ", 1200)
	body := map[string]interface{}{
		"database": "translations",
		"query":    longQuery,
		"labels":   []string{"Doc"},
		"limit":    1000,
	}
	resp := makeRequest(t, server, http.MethodPost, "/nornicdb/search", body, "")
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
}

func TestOAuthRedirectAndCallback_SuccessFlow(t *testing.T) {
	server, _ := setupTestServer(t)

	// Mock OAuth provider.
	oauthSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/oauth2/v1/token":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"oauth-at","token_type":"Bearer","expires_in":3600,"refresh_token":"oauth-rt"}`))
		case "/oauth2/v1/userinfo":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"sub":"oauth-sub","email":"oauth@example.com","preferred_username":"oauthuser","roles":["viewer"]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer oauthSrv.Close()

	t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	t.Setenv("NORNICDB_OAUTH_ISSUER", oauthSrv.URL)
	t.Setenv("NORNICDB_OAUTH_CLIENT_ID", "cid")
	t.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "csecret")
	t.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	// Redirect step generates and stores state.
	req := httptest.NewRequest(http.MethodGet, "/auth/oauth/redirect", nil)
	req.Host = "localhost:7474"
	rec := httptest.NewRecorder()
	server.handleOAuthRedirect(rec, req)
	require.Equal(t, http.StatusFound, rec.Code)

	location := rec.Header().Get("Location")
	require.NotEmpty(t, location)
	parsed, err := url.Parse(location)
	require.NoError(t, err)
	state := parsed.Query().Get("state")
	require.NotEmpty(t, state)

	// Callback step validates state and issues app token/cookie.
	cbReq := httptest.NewRequest(http.MethodGet, "/auth/oauth/callback?code=test-code&state="+url.QueryEscape(state), nil)
	cbRec := httptest.NewRecorder()
	server.handleOAuthCallback(cbRec, cbReq)
	require.Equal(t, http.StatusFound, cbRec.Code)
	require.Equal(t, "/", cbRec.Header().Get("Location"))
}

func TestHandleImplicitTransaction_BranchMatrix(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	t.Run("invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader("{"))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("mutation denied without claims", func(t *testing.T) {
		body := `{"statements":[{"statement":"CREATE (n:Doc {name:'x'})"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("use missing database returns 404", func(t *testing.T) {
		body := `{"statements":[{"statement":":USE missing_db RETURN 1 AS n"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("native USE statement is accepted", func(t *testing.T) {
		body := `{"statements":[{"statement":"USE nornic RETURN 1 AS n"}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("comment-only statement yields empty result and includeStats", func(t *testing.T) {
		body := `{"statements":[{"statement":"// comment only","includeStats":true},{"statement":"RETURN 1 AS n","includeStats":true}]}`
		req := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/commit", strings.NewReader(body))
		req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
			Username: "admin",
			Roles:    []string{"admin"},
		}))
		rec := httptest.NewRecorder()
		server.handleImplicitTransaction(rec, req, dbName)
		require.Equal(t, http.StatusOK, rec.Code)

		var payload map[string]interface{}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
		results, ok := payload["results"].([]interface{})
		require.True(t, ok)
		require.NotEmpty(t, results)
	})
}

func TestHandleImplicitTransaction_AsyncWriteAccepted(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewMemoryEngine())
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'async'})"}]}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "eventual", rec.Header().Get("X-NornicDB-Consistency"))
	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	optimisticRaw, ok := payload["optimistic"].(map[string]interface{})
	require.True(t, ok, "expected optimistic metadata in async response")
	createdIDs, ok := optimisticRaw["createdNodeIds"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, createdIDs)
}

func TestHandleImplicitTransaction_AsyncEnabledSyncFallbackReturnsOK(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = true
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	authn, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewMemoryEngine())
	require.NoError(t, err)
	_, _ = authn.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, authn, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'sync'}) SET n.updatedAt = datetime() RETURN n.name"}]}`))
	req = req.WithContext(context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	}))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")

	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Header().Get("X-NornicDB-Consistency"))

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	_, hasReceipt := payload["receipt"]
	require.True(t, hasReceipt, "sync fallback should expose durable receipt metadata")
}

func TestHandleImplicitTransaction_NoAuth_SkipsRBACWriteCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	dbCfg.Database.AsyncWritesEnabled = false // Deterministic status: isolate RBAC behavior from async 202 semantics.
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", strings.NewReader(`{"statements":[{"statement":"CREATE (n:Doc {name:'open'}) RETURN n.name"}]}`))
	rec := httptest.NewRecorder()
	srv.handleImplicitTransaction(rec, req, "nornic")

	require.Equal(t, http.StatusOK, rec.Code)
	var payload TransactionResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&payload))
	require.Empty(t, payload.Errors, "writes should not be RBAC-forbidden when auth is disabled")
	require.NotEmpty(t, payload.Results)
}

func TestExecuteTxStatements_NoAuth_SkipsRBACWriteCheck(t *testing.T) {
	tmpDir := t.TempDir()
	dbCfg := nornicdb.DefaultConfig()
	dbCfg.Memory.DecayEnabled = false
	db, err := nornicdb.Open(tmpDir, dbCfg)
	require.NoError(t, err)
	defer db.Close()

	cfg := DefaultConfig()
	cfg.Port = 0
	srv, err := New(db, nil, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })

	dbName := srv.dbManager.DefaultDatabaseName()
	tx, err := srv.txSessions.Open(context.Background(), dbName)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.txSessions.RollbackAndDelete(context.Background(), tx) })

	resp := &TransactionResponse{
		Results: make([]QueryResult, 0, 1),
		Errors:  make([]QueryError, 0, 1),
	}
	srv.executeTxStatements(context.Background(), "", nil, dbName, tx, []StatementRequest{
		{Statement: "CREATE (n:Doc {name:'tx-open'}) RETURN n.name"},
	}, resp)

	require.Empty(t, resp.Errors, "writes in tx session should not be RBAC-forbidden when auth is disabled")
	require.NotEmpty(t, resp.Results)
}

func TestHandleSimilar(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Get the default database storage to create test nodes with embeddings
	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err, "should get default database storage")

	// Create a target node with an embedding
	targetEmbedding := make([]float32, 1024)
	for i := range targetEmbedding {
		targetEmbedding[i] = float32(i) / 1024.0
	}
	targetNode := &storage.Node{
		ID:              storage.NodeID("target-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Target Node"},
		ChunkEmbeddings: [][]float32{targetEmbedding},
	}
	_, err = storageEngine.CreateNode(targetNode)
	require.NoError(t, err, "should create target node")

	// Create a similar node with a similar embedding (slight variation)
	similarEmbedding := make([]float32, 1024)
	for i := range similarEmbedding {
		similarEmbedding[i] = float32(i+1) / 1024.0 // Slight variation
	}
	similarNode := &storage.Node{
		ID:              storage.NodeID("similar-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Similar Node"},
		ChunkEmbeddings: [][]float32{similarEmbedding},
	}
	_, err = storageEngine.CreateNode(similarNode)
	require.NoError(t, err, "should create similar node")

	// Create a dissimilar node with a very different embedding
	dissimilarEmbedding := make([]float32, 1024)
	for i := range dissimilarEmbedding {
		dissimilarEmbedding[i] = float32(1024-i) / 1024.0 // Very different
	}
	dissimilarNode := &storage.Node{
		ID:              storage.NodeID("dissimilar-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "Dissimilar Node"},
		ChunkEmbeddings: [][]float32{dissimilarEmbedding},
	}
	_, err = storageEngine.CreateNode(dissimilarNode)
	require.NoError(t, err, "should create dissimilar node")

	// Test the similar endpoint
	resp := makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "target-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusOK, resp.Code, "should return 200 OK")

	// Verify response structure
	var results []map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&results)
	require.NoError(t, err, "should decode response as JSON array")

	// Should find at least the similar node (may also find dissimilar node)
	require.Greater(t, len(results), 0, "should return at least one result")

	// Verify result structure
	for _, result := range results {
		require.Contains(t, result, "node", "result should contain 'node' field")
		require.Contains(t, result, "score", "result should contain 'score' field")
		node, ok := result["node"].(map[string]interface{})
		require.True(t, ok, "node should be a map")
		require.Contains(t, node, "id", "node should have 'id' field")
		score, ok := result["score"].(float64)
		require.True(t, ok, "score should be a number")
		require.GreaterOrEqual(t, score, 0.0, "score should be non-negative")
		require.LessOrEqual(t, score, 1.0, "score should be at most 1.0")
	}

	// Test with non-existent node (should return 404)
	resp = makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "non-existent-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusNotFound, resp.Code, "should return 404 for non-existent node")

	// Test with node without embedding (should return 400)
	nodeWithoutEmbedding := &storage.Node{
		ID:              storage.NodeID("no-embedding-node"),
		Labels:          []string{"Test"},
		Properties:      map[string]interface{}{"name": "No Embedding"},
		ChunkEmbeddings: nil, // No embedding
	}
	_, err = storageEngine.CreateNode(nodeWithoutEmbedding)
	require.NoError(t, err, "should create node without embedding")

	resp = makeRequest(t, server, "POST", "/nornicdb/similar", map[string]interface{}{
		"node_id": "no-embedding-node",
		"limit":   5,
	}, "Bearer "+token)

	require.Equal(t, http.StatusBadRequest, resp.Code, "should return 400 for node without embedding")
}

// =============================================================================
// Schema Endpoint Tests (via Cypher - Neo4j compatible approach)
// =============================================================================

func TestSchemaViaCypher(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Get labels via CALL db.labels()
	labelsResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL db.labels()"},
		},
	}, "Bearer "+token)

	if labelsResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for labels query, got %d", labelsResp.Code)
	}

	// Get relationship types via CALL db.relationshipTypes()
	relTypesResp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CALL db.relationshipTypes()"},
		},
	}, "Bearer "+token)

	if relTypesResp.Code != http.StatusOK {
		t.Errorf("expected status 200 for relationship types query, got %d", relTypesResp.Code)
	}
}

// =============================================================================
// Admin Endpoint Tests
// =============================================================================

func TestHandleAdminStats(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/admin/stats", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var stats map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if stats["server"] == nil {
		t.Error("missing 'server' stats")
	}
	if stats["database"] == nil {
		t.Error("missing 'database' stats")
	}

	// Verify new database stats structure (multi-database aware)
	dbStats, ok := stats["database"].(map[string]interface{})
	if !ok {
		t.Fatal("'database' field is not a map")
	}

	// Check for required fields
	if dbStats["node_count"] == nil {
		t.Error("missing 'node_count' field in database stats")
	}
	if dbStats["edge_count"] == nil {
		t.Error("missing 'edge_count' field in database stats")
	}
	if dbStats["databases"] == nil {
		t.Error("missing 'databases' field in database stats")
	}
	if dbStats["per_database"] == nil {
		t.Error("missing 'per_database' field in database stats")
	}

	// Verify per_database is a map
	perDB, ok := dbStats["per_database"].(map[string]interface{})
	if !ok {
		t.Fatal("'per_database' field is not a map")
	}

	// Verify system database is NOT included in per_database (it's excluded from totals)
	if _, exists := perDB["system"]; exists {
		t.Error("system database should not be included in per_database stats")
	}

	// Verify default database is included
	defaultDB := server.dbManager.DefaultDatabaseName()
	if _, exists := perDB[defaultDB]; !exists {
		t.Errorf("default database '%s' should be included in per_database stats", defaultDB)
	}

	// Verify counts are numbers
	nodeCount, ok := dbStats["node_count"].(float64)
	if !ok {
		t.Errorf("'node_count' should be a number, got %T", dbStats["node_count"])
	}
	edgeCount, ok := dbStats["edge_count"].(float64)
	if !ok {
		t.Errorf("'edge_count' should be a number, got %T", dbStats["edge_count"])
	}

	// Counts should be >= 0
	if nodeCount < 0 {
		t.Errorf("node_count should be >= 0, got %v", nodeCount)
	}
	if edgeCount < 0 {
		t.Errorf("edge_count should be >= 0, got %v", edgeCount)
	}
}

func TestHandleAdminConfig(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/admin/config", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}
}

// =============================================================================
// User Management Tests
// =============================================================================

func TestHandleUsers(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test GET (list users) - using correct endpoint
	resp := makeRequest(t, server, "GET", "/auth/users", nil, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	// Test POST (create user)
	createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "newuser",
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if createResp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", createResp.Code, createResp.Body.String())
	}
}

// =============================================================================
// RBAC Tests
// =============================================================================

func TestRBACWritePermission(t *testing.T) {
	server, auth := setupTestServer(t)
	readerToken := getAuthToken(t, auth, "reader")

	// Reader (viewer role) should not be able to run mutation queries
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "CREATE (n:Test {name: 'test'}) RETURN n"},
		},
	}, "Bearer "+readerToken)

	// The response should have an error about permissions
	var txResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&txResp)
	errors, ok := txResp["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("expected error for viewer running mutation query")
	}
}

func TestRBACMutationQuery(t *testing.T) {
	server, auth := setupTestServer(t)
	readerToken := getAuthToken(t, auth, "reader")

	// Reader (viewer role) should be able to run read queries
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN n LIMIT 10"},
		},
	}, "Bearer "+readerToken)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for read query, got %d", resp.Code)
	}
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

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
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

func TestIsCreateDatabaseStatement(t *testing.T) {
	tests := []struct {
		stmt     string
		expected bool
	}{
		{"CREATE DATABASE foo", true},
		{"CREATE DATABASE foo IF NOT EXISTS", true},
		{"create database bar", true},
		{"CREATE COMPOSITE DATABASE comp ALIAS a FOR DATABASE db1", true},
		{"CREATE (n:Node)", false},
		{"MATCH (n) RETURN n", false},
		{"SHOW DATABASES", false},
		{"  CREATE DATABASE x  ", true},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			got := isCreateDatabaseStatement(tt.stmt)
			if got != tt.expected {
				t.Errorf("isCreateDatabaseStatement(%q) = %v, want %v", tt.stmt, got, tt.expected)
			}
		})
	}
}

func TestParseCreatedDatabaseName(t *testing.T) {
	tests := []struct {
		stmt     string
		wantName string
		wantOk   bool
	}{
		{"CREATE DATABASE foo", "foo", true},
		{"CREATE DATABASE foo IF NOT EXISTS", "foo", true},
		{"create database bar", "bar", true},
		{"CREATE DATABASE `backtick-db`", "backtick-db", true},
		{"CREATE COMPOSITE DATABASE comp ALIAS a FOR DATABASE db1", "comp", true},
		{"CREATE COMPOSITE DATABASE my_comp", "my_comp", true},
		{"CREATE (n:Node)", "", false},
		{"CREATE DATABASE ", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.stmt, func(t *testing.T) {
			gotName, gotOk := parseCreatedDatabaseName(tt.stmt)
			if gotOk != tt.wantOk || gotName != tt.wantName {
				t.Errorf("parseCreatedDatabaseName(%q) = (%q, %v), want (%q, %v)", tt.stmt, gotName, gotOk, tt.wantName, tt.wantOk)
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
		{"perm token allowed", []string{"write"}, auth.PermWrite, true},
		{"perm admin implies delete", []string{"admin"}, auth.PermDelete, true},
		{"role casing normalized", []string{"Admin"}, auth.PermWrite, true},
		{"role_ prefix normalized", []string{"ROLE_ADMIN"}, auth.PermWrite, true},
		{"perm casing normalized", []string{"WRITE"}, auth.PermWrite, true},
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

// =============================================================================
// Decay Endpoint Test (NornicDB extension)
// =============================================================================

func TestHandleDecay(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/nornicdb/decay", nil, "Bearer "+token)

	// Should work or fail gracefully
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

// =============================================================================
// GDPR Endpoint Tests
// =============================================================================

func TestHandleGDPRExport(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/gdpr/export", map[string]interface{}{
		"user_id": "admin",
		"format":  "json",
	}, "Bearer "+token)

	// May succeed or fail depending on implementation
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleGDPRDelete(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Test without confirmation
	resp := makeRequest(t, server, "POST", "/gdpr/delete", map[string]interface{}{
		"user_id": "testuser",
		"confirm": false,
	}, "Bearer "+token)

	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 without confirmation, got %d", resp.Code)
	}
}

func TestHandleBackup(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/admin/backup", map[string]interface{}{
		"path": "/tmp/backup",
	}, "Bearer "+token)

	// May succeed or fail depending on implementation
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError && resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

// =============================================================================
// Additional Coverage Tests
// =============================================================================

func TestHandleLogout(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/auth/logout", nil, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	// Test without auth
	resp2 := makeRequest(t, server, "POST", "/auth/logout", nil, "")
	if resp2.Code != http.StatusOK {
		t.Errorf("expected status 200 even without auth, got %d", resp2.Code)
	}
}

func TestHandleMePUT(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// PUT on /auth/me should fail (method not explicitly handled)
	resp := makeRequest(t, server, "PUT", "/auth/me", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestHandleUserByID(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// First get users to find an ID
	listResp := makeRequest(t, server, "GET", "/auth/users", nil, "Bearer "+token)
	if listResp.Code != http.StatusOK {
		t.Fatalf("failed to list users: %d", listResp.Code)
	}

	var users []map[string]interface{}
	json.NewDecoder(listResp.Body).Decode(&users)

	require.Greater(t, len(users), 0)

	userID := users[0]["id"].(string)

	// Test GET user by ID
	getResp := makeRequest(t, server, "GET", "/auth/users/"+userID, nil, "Bearer "+token)
	if getResp.Code != http.StatusOK && getResp.Code != http.StatusNotFound {
		t.Errorf("expected status 200 or 404, got %d", getResp.Code)
	}
}

func TestClusterStatus(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic/cluster", nil, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}
}

func TestTransactionWithStatements(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction with initial statements
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)

	if openResp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d: %s", openResp.Code, openResp.Body.String())
	}

	// Check that results are included
	var result map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&result)

	results, ok := result["results"].([]interface{})
	if !ok || len(results) == 0 {
		t.Error("expected results from initial statement execution")
	}
}

func TestCommitTransactionWithStatements(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction
	openResp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{},
	}, "Bearer "+token)

	var openResult map[string]interface{}
	json.NewDecoder(openResp.Body).Decode(&openResult)

	commitURL := openResult["commit"].(string)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]

	// Commit with final statements
	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n) as count"},
		},
	}, "Bearer "+token)

	if commitResp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", commitResp.Code, commitResp.Body.String())
	}
}

func TestImplicitTransactionBadJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Send malformed request (bad JSON) - this should give an error
	req := httptest.NewRequest("POST", "/db/nornic/tx/commit", strings.NewReader("not valid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestGDPRExportCSV(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/gdpr/export", map[string]interface{}{
		"user_id": "admin",
		"format":  "csv",
	}, "Bearer "+token)

	// May succeed or fail depending on implementation
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestGDPRDeleteWithConfirmation(t *testing.T) {
	server, authenticator := setupTestServer(t)

	// Create a test user to delete
	_, err := authenticator.CreateUser("deletetest", "password123", []auth.Role{auth.RoleViewer})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	token := getAuthToken(t, authenticator, "admin")

	// Test with confirmation (anonymize mode)
	resp := makeRequest(t, server, "POST", "/gdpr/delete", map[string]interface{}{
		"user_id":   "deletetest",
		"confirm":   true,
		"anonymize": true,
	}, "Bearer "+token)

	// May succeed or fail depending on implementation
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status %d: %s", resp.Code, resp.Body.String())
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

func TestTransactionMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// GET on tx should fail
	resp := makeRequest(t, server, "GET", "/db/nornic/tx", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
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

func TestUsersPostInvalidBody(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Invalid JSON body
	req := httptest.NewRequest("POST", "/auth/users", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", recorder.Code)
	}
}

func TestSearchMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/nornicdb/search", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestSimilarMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/nornicdb/similar", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestBackupMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/admin/backup", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestDecayGET(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// GET on decay endpoint - check it returns OK
	resp := makeRequest(t, server, "GET", "/nornicdb/decay", nil, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}
}

func TestGDPRExportMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/gdpr/export", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestGDPRDeleteMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/gdpr/delete", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

// =============================================================================
// Additional Coverage Tests for 90%+
// =============================================================================

func TestGDPRExportForbidden(t *testing.T) {
	server, auth := setupTestServer(t)
	readerToken := getAuthToken(t, auth, "reader")

	// Reader tries to export someone else's data
	resp := makeRequest(t, server, "POST", "/gdpr/export", map[string]interface{}{
		"user_id": "other-user-id",
		"format":  "json",
	}, "Bearer "+readerToken)

	if resp.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", resp.Code)
	}
}

func TestGDPRDeleteForbidden(t *testing.T) {
	server, auth := setupTestServer(t)
	readerToken := getAuthToken(t, auth, "reader")

	// Reader tries to delete someone else's data
	resp := makeRequest(t, server, "POST", "/gdpr/delete", map[string]interface{}{
		"user_id": "other-user-id",
		"confirm": true,
	}, "Bearer "+readerToken)

	if resp.Code != http.StatusForbidden {
		t.Errorf("expected status 403, got %d", resp.Code)
	}
}

func TestGDPRExportInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/gdpr/export", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestGDPRDeleteInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/gdpr/delete", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestSearchInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/nornicdb/search", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestSimilarInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/nornicdb/similar", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestBuildEmbedConfigFromResolved_Branches(t *testing.T) {
	require.Nil(t, buildEmbedConfigFromResolved(map[string]string{}, nil))

	fallback := DefaultConfig()
	fallback.EmbeddingProvider = ""
	fallback.EmbeddingDimensions = 0
	fallback.ModelsDir = "/tmp/models"

	cfg := buildEmbedConfigFromResolved(map[string]string{}, fallback)
	require.NotNil(t, cfg)
	require.Equal(t, "openai", cfg.Provider)
	require.Equal(t, "/v1/embeddings", cfg.APIPath)
	require.Equal(t, 1024, cfg.Dimensions)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"NORNICDB_EMBEDDING_PROVIDER":   "ollama",
		"NORNICDB_EMBEDDING_DIMENSIONS": "1536",
		"NORNICDB_EMBEDDING_GPU_LAYERS": "12",
	}, fallback)
	require.Equal(t, "ollama", cfg.Provider)
	require.Equal(t, "/api/embeddings", cfg.APIPath)
	require.Equal(t, 1536, cfg.Dimensions)
	require.Equal(t, 12, cfg.GPULayers)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"NORNICDB_EMBEDDING_PROVIDER":   "local",
		"NORNICDB_EMBEDDING_DIMENSIONS": "-1",
		"NORNICDB_EMBEDDING_GPU_LAYERS": "not-an-int",
	}, fallback)
	require.Equal(t, "local", cfg.Provider)
	require.Equal(t, "", cfg.APIPath)
	require.Equal(t, 1024, cfg.Dimensions)
	require.Equal(t, 0, cfg.GPULayers)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"NORNICDB_EMBEDDING_PROVIDER": "custom-provider",
	}, fallback)
	require.Equal(t, "/api/embeddings", cfg.APIPath)
}

func TestEnsureSearchBuildStartedForKnownDatabases_NilAndNoopBranches(t *testing.T) {
	var nilServer *Server
	nilServer.ensureSearchBuildStartedForKnownDatabases()

	server, _ := setupTestServer(t)
	server.ensureSearchBuildStartedForKnownDatabases()

	origDB := server.db
	server.db = nil
	server.ensureSearchBuildStartedForKnownDatabases()
	server.db = origDB

	origMgr := server.dbManager
	server.dbManager = nil
	server.ensureSearchBuildStartedForKnownDatabases()
	server.dbManager = origMgr
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

func TestServerAuthAndAuxHandlers_AdditionalBranches(t *testing.T) {
	server, authenticator := setupTestServer(t)
	_, adminUser, err := authenticator.Authenticate("admin", "password123", "127.0.0.1", "test")
	require.NoError(t, err)
	require.NotNil(t, adminUser)
	require.NotEmpty(t, adminUser.ID)

	withClaims := func(r *http.Request, claims *auth.JWTClaims) *http.Request {
		return r.WithContext(context.WithValue(r.Context(), contextKeyClaims, claims))
	}

	t.Run("handleMe oauth env inference", func(t *testing.T) {
		t.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
		t.Setenv("NORNICDB_OAUTH_ISSUER", "https://issuer.example")
		req := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
		req = withClaims(req, &auth.JWTClaims{
			Sub:      adminUser.ID,
			Username: "admin",
			Roles:    []string{"admin"},
		})
		rec := httptest.NewRecorder()
		server.handleMe(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "\"auth_method\":\"oauth\"")
		require.Contains(t, rec.Body.String(), "\"oauth_provider\":\"https://issuer.example\"")
	})

	t.Run("change password fallback user lookup branches", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/auth/change-password", strings.NewReader(`{"old_password":"password123","new_password":"newPass123!"}`))
		req.Header.Set("Content-Type", "application/json")
		req = withClaims(req, &auth.JWTClaims{
			Sub:      adminUser.ID,
			Username: "",
			Roles:    []string{"admin"},
		})
		rec := httptest.NewRecorder()
		server.handleChangePassword(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodPost, "/auth/change-password", strings.NewReader(`{"old_password":"x","new_password":"y"}`))
		req.Header.Set("Content-Type", "application/json")
		req = withClaims(req, &auth.JWTClaims{
			Sub:      "missing-user-id",
			Username: "",
			Roles:    []string{"admin"},
		})
		rec = httptest.NewRecorder()
		server.handleChangePassword(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("roles and role-by-id success and conflict branches", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/auth/roles", nil)
		rec := httptest.NewRecorder()
		server.handleRoles(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodPost, "/auth/roles", strings.NewReader(`{"name":"analyst"}`))
		rec = httptest.NewRecorder()
		server.handleRoles(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)

		_ = server.auth.UpdateRoles("reader", []auth.Role{auth.Role("analyst")})

		req = httptest.NewRequest(http.MethodDelete, "/auth/roles/analyst", nil)
		rec = httptest.NewRecorder()
		server.handleRoleByID(rec, req)
		require.Equal(t, http.StatusConflict, rec.Code)

		_ = server.auth.UpdateRoles("reader", []auth.Role{auth.RoleViewer})
		req = httptest.NewRequest(http.MethodDelete, "/auth/roles/analyst", nil)
		rec = httptest.NewRecorder()
		server.handleRoleByID(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("oauth redirect bad method and not configured", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/auth/oauth/redirect", nil)
		rec := httptest.NewRecorder()
		server.handleOAuthRedirect(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		orig := server.oauthManager
		server.oauthManager = nil
		req = httptest.NewRequest(http.MethodGet, "/auth/oauth/redirect", nil)
		rec = httptest.NewRecorder()
		server.handleOAuthRedirect(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		server.oauthManager = orig
	})
}

func TestDbConfigGPUAndMiddleware_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	t.Run("db config prefix method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/admin/databases/nornic/config", nil)
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

func TestNeo4jConversionAndTxHelpers_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	require.Nil(t, server.nodeToNeo4jHTTPFormat(nil))
	require.Nil(t, server.edgeToNeo4jHTTPFormat(nil))

	converted := server.convertValueToNeo4jFormat(map[string]interface{}{
		"_pathResult": "drop",
		"node": &storage.Node{
			ID:         "cv-node",
			Labels:     []string{"Doc"},
			Properties: map[string]interface{}{"k": "v"},
		},
		"vals": []interface{}{
			&storage.Edge{ID: "cv-edge", StartNode: "a", EndNode: "b", Type: "REL"},
			"text",
		},
	})
	m, ok := converted.(map[string]interface{})
	require.True(t, ok)
	require.NotContains(t, m, "_pathResult")
	require.Contains(t, m, "node")
	require.Contains(t, m, "vals")

	withProps := server.mapNodeToNeo4jHTTPFormat("n-props", map[string]interface{}{
		"labels":     []string{"Doc"},
		"properties": map[string]interface{}{"title": "t"},
	})
	props, ok := withProps["properties"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "t", props["title"])

	withMixedLabels := server.mapNodeToNeo4jHTTPFormat("n-mixed", map[string]interface{}{
		"labels": []interface{}{"A", 123, "B"},
		"name":   "mixed",
	})
	labels, ok := withMixedLabels["labels"].([]string)
	require.True(t, ok)
	require.Equal(t, "A", labels[0])
	require.Equal(t, "", labels[1])
	require.Equal(t, "B", labels[2])

	server.config.Address = "0.0.0.0"
	commitURL := server.transactionCommitURL("nornic", "tx-1")
	require.Contains(t, commitURL, "http://localhost:")

	resp := &TransactionResponse{Results: make([]QueryResult, 0)}
	server.appendStatementResult(resp, &cypher.ExecuteResult{
		Columns: []string{"n"},
		Rows:    [][]interface{}{{map[string]interface{}{"elementId": "4:nornicdb:abc"}}},
		Metadata: map[string]interface{}{
			"receipt":    map[string]interface{}{"writes": 1},
			"optimistic": map[string]interface{}{"createdNodeIds": []string{"nornic:1"}},
		},
	})
	require.Len(t, resp.Results, 1)
	require.NotNil(t, resp.Receipt)
	require.NotNil(t, resp.Optimistic)

	// Nil result columns must serialize as [] for UI safety.
	resp = &TransactionResponse{Results: make([]QueryResult, 0)}
	server.appendStatementResult(resp, &cypher.ExecuteResult{
		Columns: nil,
		Rows:    [][]interface{}{},
	})
	require.Len(t, resp.Results, 1)
	require.NotNil(t, resp.Results[0].Columns)
	require.Len(t, resp.Results[0].Columns, 0)
}

func TestRequestTimeoutMiddleware_TxRoute_UsesConfigAndOverride(t *testing.T) {
	// By default tx timeout has a safety floor, so a short handler should not time out
	// even when WriteTimeout is configured very small.
	s := &Server{config: &Config{WriteTimeout: 10 * time.Millisecond}}
	slowOK := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(25 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr := httptest.NewRecorder()
	slowOK.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for tx route without override, got %d body=%q", rr.Code, rr.Body.String())
	}

	// Env override should be honored for tx timeout, enabling short deterministic limits in tests.
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "5ms")
	s2 := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
	slowTimeout := s2.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(30 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	req2 := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr2 := httptest.NewRecorder()
	slowTimeout.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when tx timeout override expires, got %d", rr2.Code)
	}
	if !strings.Contains(rr2.Body.String(), "request timeout: transaction busy") {
		t.Fatalf("expected tx timeout body, got %q", rr2.Body.String())
	}
}

func TestRequestTimeoutMiddleware_TxRoute_TracksActiveTxRequests(t *testing.T) {
	s := &Server{config: &Config{WriteTimeout: 10 * time.Second}}
	started := make(chan struct{})
	release := make(chan struct{})
	mw := s.requestTimeoutMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", nil)
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		mw.ServeHTTP(rr, req)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tx handler did not start")
	}
	require.Eventually(t, func() bool { return s.activeTxReqs.Load() == 1 }, time.Second, 10*time.Millisecond)

	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tx handler did not complete")
	}
	require.Equal(t, int64(0), s.activeTxReqs.Load())
}

func TestTransactionHandlers_AdditionalErrorBranches(t *testing.T) {
	server, _ := setupTestServer(t)
	_ = server.dbManager.CreateDatabase("private")

	adminCtx := context.WithValue(context.Background(), contextKeyClaims, &auth.JWTClaims{
		Username: "admin",
		Roles:    []string{"admin"},
	})

	req := httptest.NewRequest(http.MethodPost, "/db/missing_db/tx", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec := httptest.NewRecorder()
	server.handleOpenTransaction(rec, req, "missing_db")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/private/tx", strings.NewReader(`{"statements":[]}`))
	rec = httptest.NewRecorder()
	server.handleOpenTransaction(rec, req, "private")
	require.Equal(t, http.StatusForbidden, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/nornic/tx/nope/commit", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec = httptest.NewRecorder()
	server.handleCommitTransaction(rec, req, "nornic", "nope")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/private/tx/nope/commit", strings.NewReader(`{"statements":[]}`))
	rec = httptest.NewRecorder()
	server.handleCommitTransaction(rec, req, "private", "nope")
	require.Equal(t, http.StatusForbidden, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/db/nornic/tx/nope", strings.NewReader(`{"statements":[]}`)).WithContext(adminCtx)
	rec = httptest.NewRecorder()
	server.handleExecuteInTransaction(rec, req, "nornic", "nope")
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodDelete, "/db/private/tx/nope", nil)
	rec = httptest.NewRecorder()
	server.handleRollbackTransaction(rec, req, "private", "nope")
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestDbConfigStatusAndHeimdallRoutes_AdditionalBranches(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	req := httptest.NewRequest(http.MethodGet, "/admin/databases/config/keys", nil)
	rec := httptest.NewRecorder()
	server.handleDbConfigPrefix(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/admin/databases/nornic/config", strings.NewReader("{"))
	rec = httptest.NewRecorder()
	server.handlePutDbConfig(rec, req, "nornic")
	require.Equal(t, http.StatusBadRequest, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/admin/databases/nornic/config", strings.NewReader(`{}`))
	rec = httptest.NewRecorder()
	server.handlePutDbConfig(rec, req, "nornic")
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, server.dbManager.CreateDatabase("status_db"))
	st, err := server.dbManager.GetStorage("status_db")
	require.NoError(t, err)
	_, _ = st.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Doc"}})
	_, _ = st.CreateNode(&storage.Node{ID: "s2", Labels: []string{"Doc"}})
	_ = st.CreateEdge(&storage.Edge{ID: "e1", StartNode: "s1", EndNode: "s2", Type: "REL"})

	statusReq := httptest.NewRequest(http.MethodGet, "/status", nil)
	statusRec := httptest.NewRecorder()
	server.handleStatus(statusRec, statusReq)
	require.Equal(t, http.StatusOK, statusRec.Code)
	require.Contains(t, statusRec.Body.String(), "\"database\"")

	server.config.Features.HeimdallEnabled = false
	mux := http.NewServeMux()
	server.registerHeimdallRoutes(mux)
	disabledReq := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	disabledReq.Header.Set("Authorization", "Bearer "+token)
	disabledRec := httptest.NewRecorder()
	mux.ServeHTTP(disabledRec, disabledReq)
	require.Equal(t, http.StatusServiceUnavailable, disabledRec.Code)

	server.config.Features.HeimdallEnabled = true
	server.setHeimdallHandler(nil)
	initReq := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
	initReq.Header.Set("Authorization", "Bearer "+token)
	initRec := httptest.NewRecorder()
	mux.ServeHTTP(initRec, initReq)
	require.Equal(t, http.StatusServiceUnavailable, initRec.Code)

	router := newHeimdallDBRouter(server.db, server.dbManager, server.config.Features)
	h := heimdall.NewHandler(&heimdall.Manager{}, heimdall.Config{Enabled: true, Model: "test"}, router, &heimdallMetricsReader{})
	server.setHeimdallHandler(h)
	okReq := httptest.NewRequest(http.MethodGet, "/api/bifrost/autocomplete", nil)
	okReq.Header.Set("Authorization", "Bearer "+token)
	okRec := httptest.NewRecorder()
	mux.ServeHTTP(okRec, okReq)
	require.NotEqual(t, http.StatusServiceUnavailable, okRec.Code)
}

func TestAuthHandlers_BranchExpansion(t *testing.T) {
	server, authenticator := setupTestServer(t)
	adminUser, err := authenticator.GetUser("admin")
	require.NoError(t, err)

	withClaims := func(r *http.Request, claims *auth.JWTClaims) *http.Request {
		return r.WithContext(context.WithValue(r.Context(), contextKeyClaims, claims))
	}

	t.Run("logout with claims", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
		req = withClaims(req, &auth.JWTClaims{Sub: adminUser.ID, Username: "admin", Roles: []string{"admin"}})
		rec := httptest.NewRecorder()
		server.handleLogout(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("token invalid body and unsupported grant type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/auth/token", strings.NewReader("{"))
		rec := httptest.NewRecorder()
		server.handleToken(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		req = httptest.NewRequest(http.MethodPost, "/auth/token", strings.NewReader(`{"username":"admin","password":"password123","grant_type":"client_credentials"}`))
		rec = httptest.NewRecorder()
		server.handleToken(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("token locked account path", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			_, _, _ = server.auth.Authenticate("reader", "bad-pass", "127.0.0.1", "test")
		}
		req := httptest.NewRequest(http.MethodPost, "/auth/token", strings.NewReader(`{"username":"reader","password":"bad-pass","grant_type":"password"}`))
		rec := httptest.NewRecorder()
		server.handleToken(rec, req)
		require.Equal(t, http.StatusTooManyRequests, rec.Code)
	})

	t.Run("change password validation error branch", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/auth/change-password", strings.NewReader(`{"old_password":"password123","new_password":"x"}`))
		req = withClaims(req, &auth.JWTClaims{Sub: adminUser.ID, Username: "admin", Roles: []string{"admin"}})
		rec := httptest.NewRecorder()
		server.handleChangePassword(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("update profile fallback-not-found and update error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader(`{"email":"x@example.com","metadata":{"a":"b"}}`))
		req = withClaims(req, &auth.JWTClaims{Sub: "missing-id", Username: "", Roles: []string{"admin"}})
		rec := httptest.NewRecorder()
		server.handleUpdateProfile(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)

		req = httptest.NewRequest(http.MethodPut, "/auth/profile", strings.NewReader(`{"email":"x@example.com"}`))
		req = withClaims(req, &auth.JWTClaims{Sub: adminUser.ID, Username: "does-not-exist", Roles: []string{"admin"}})
		rec = httptest.NewRecorder()
		server.handleUpdateProfile(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("user by id not found branches", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/auth/users/definitely-missing", nil)
		rec := httptest.NewRecorder()
		server.handleUserByID(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/auth/users/definitely-missing", nil)
		rec = httptest.NewRecorder()
		server.handleUserByID(rec, req)
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("role handlers default and rename propagation branches", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/auth/role-entitlements", nil)
		rec := httptest.NewRecorder()
		server.handleRoleEntitlements(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		req = httptest.NewRequest(http.MethodPost, "/auth/roles", strings.NewReader(`{"name":"role_src"}`))
		rec = httptest.NewRecorder()
		server.handleRoles(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code)

		require.NoError(t, server.auth.UpdateRoles("reader", []auth.Role{auth.Role("role_src")}))

		req = httptest.NewRequest(http.MethodPatch, "/auth/roles/role_src", strings.NewReader(`{"name":"role_dst"}`))
		rec = httptest.NewRecorder()
		server.handleRoleByID(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		user, err := server.auth.GetUser("reader")
		require.NoError(t, err)
		var hasRoleDst bool
		for _, r := range user.Roles {
			if string(r) == "role_dst" {
				hasRoleDst = true
			}
		}
		require.True(t, hasRoleDst)

		req = httptest.NewRequest(http.MethodGet, "/auth/roles/role_dst", nil)
		rec = httptest.NewRecorder()
		server.handleRoleByID(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("access handlers additional invalid/method branches", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/auth/access/databases", nil)
		rec := httptest.NewRecorder()
		server.handleAccessDatabases(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)

		req = httptest.NewRequest(http.MethodPut, "/auth/access/databases", strings.NewReader(`{"databases":["nornic"]}`))
		rec = httptest.NewRecorder()
		server.handleAccessDatabases(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		req = httptest.NewRequest(http.MethodPut, "/auth/access/databases", strings.NewReader(`{"mappings":[{"role":"viewer","databases":["nornic"]}]}`))
		rec = httptest.NewRecorder()
		server.handleAccessDatabases(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodPut, "/auth/access/privileges", strings.NewReader("{"))
		rec = httptest.NewRecorder()
		server.handleAccessPrivileges(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)

		req = httptest.NewRequest(http.MethodPut, "/auth/access/privileges", strings.NewReader(`[{"role":"viewer","database":"nornic","read":true,"write":false}]`))
		rec = httptest.NewRecorder()
		server.handleAccessPrivileges(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/auth/access/privileges", nil)
		rec = httptest.NewRecorder()
		server.handleAccessPrivileges(rec, req)
		require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
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

		server.db.SetEmbedder(&countingEmbedder{dims: 1024})

		req := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger?regenerate=true", nil)
		rec := httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusAccepted, rec.Code)

		req = httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil)
		rec = httptest.NewRecorder()
		server.handleEmbedTrigger(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		req = httptest.NewRequest(http.MethodDelete, "/nornicdb/embed/clear", nil)
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
		triggerReq := httptest.NewRequest(http.MethodPost, "/nornicdb/embed/trigger", nil).WithContext(ctx)
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
		require.NoError(t, server.dbManager.CreateCompositeDatabase("cmp_search_cov", []multidb.ConstituentRef{ref}))
		server.ensureSearchBuildStartedForKnownDatabases()
		require.NoError(t, server.dbManager.DropCompositeDatabase("cmp_search_cov"))
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

func TestNew_SearchRerankProviderBranches(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	require.NoError(t, err)
	defer db.Close()

	t.Run("ollama provider defaults api url and model", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "ollama",
		}
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		require.NotNil(t, s)
	})

	t.Run("external provider with missing api url logs disabled path", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.EmbeddingEnabled = false
		cfg.MCPEnabled = false
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "cohere",
		}
		s, err := New(db, nil, cfg)
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}

func TestUIAndStringHelpers_AdditionalBranches(t *testing.T) {
	require.Equal(t, "value", getString(map[string]interface{}{"k": "value"}, "k"))
	require.Equal(t, "", getString(map[string]interface{}{"k": 42}, "k"))
	require.Equal(t, "", getString(map[string]interface{}{}, "missing"))

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

func TestExplicitTransaction_OwnerIsolationAcrossCallers(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()

	ownerA := &auth.JWTClaims{Username: "admin", Roles: []string{"admin"}}
	ownerB := &auth.JWTClaims{Username: "other-admin", Roles: []string{"admin"}}

	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, ownerA))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openPayload map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openPayload))
	commitURL, _ := openPayload["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	execReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, strings.NewReader(`{"statements":[{"statement":"RETURN 1 AS n"}]}`))
	execReq = execReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, ownerB))
	execRec := httptest.NewRecorder()
	server.handleExecuteInTransaction(execRec, execReq, dbName, txID)
	require.Equal(t, http.StatusNotFound, execRec.Code)
}

func TestExplicitTransaction_UsesEffectiveGraphForAuthorization(t *testing.T) {
	server, _ := setupTestServer(t)
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.dbManager.CreateDatabase("private"))
	require.NotNil(t, server.allowlistStore)
	require.NotNil(t, server.privilegesStore)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, true, false))

	viewerClaims := &auth.JWTClaims{Username: "reader", Roles: []string{"viewer"}}

	openReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx", strings.NewReader(`{"statements":[]}`))
	openReq = openReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, viewerClaims))
	openRec := httptest.NewRecorder()
	server.handleOpenTransaction(openRec, openReq, dbName)
	require.Equal(t, http.StatusCreated, openRec.Code)

	var openPayload map[string]interface{}
	require.NoError(t, json.NewDecoder(openRec.Body).Decode(&openPayload))
	commitURL, _ := openPayload["commit"].(string)
	require.NotEmpty(t, commitURL)
	parts := strings.Split(commitURL, "/")
	txID := parts[len(parts)-2]
	require.NotEmpty(t, txID)

	execReq := httptest.NewRequest(http.MethodPost, "/db/"+dbName+"/tx/"+txID, strings.NewReader(`{"statements":[{"statement":"USE private RETURN 1 AS n"}]}`))
	execReq = execReq.WithContext(context.WithValue(context.Background(), contextKeyClaims, viewerClaims))
	execRec := httptest.NewRecorder()
	server.handleExecuteInTransaction(execRec, execReq, dbName, txID)
	require.Equal(t, http.StatusOK, execRec.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(execRec.Body).Decode(&payload))
	errs, ok := payload["errors"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, errs)
	firstErr, ok := errs[0].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "Neo.ClientError.Security.Forbidden", firstErr["code"])
	require.Contains(t, fmt.Sprint(firstErr["message"]), "private")
}
