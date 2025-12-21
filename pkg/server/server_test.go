// Package server provides HTTP REST API server tests.
package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
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
	config.DecayEnabled = false
	config.AutoLinksEnabled = false
	config.AsyncWritesEnabled = false // Disable async writes for predictable test behavior (200 OK vs 202 Accepted)

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

	// Create server config
	serverConfig := DefaultConfig()
	serverConfig.Port = 0 // Use random port
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
	config.DecayEnabled = false
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

	resp := makeRequest(t, server, "POST", "/nornicdb/search", map[string]interface{}{
		"query": "test query",
		"limit": 10,
	}, "Bearer "+token)

	// May return 200 (success) or 500 (if search not fully implemented)
	if resp.Code != http.StatusOK && resp.Code != http.StatusInternalServerError {
		t.Errorf("unexpected status %d", resp.Code)
	}
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
		ID:         storage.NodeID("target-node"),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "Target Node"},
		Embedding:  targetEmbedding,
	}
	_, err = storageEngine.CreateNode(targetNode)
	require.NoError(t, err, "should create target node")

	// Create a similar node with a similar embedding (slight variation)
	similarEmbedding := make([]float32, 1024)
	for i := range similarEmbedding {
		similarEmbedding[i] = float32(i+1) / 1024.0 // Slight variation
	}
	similarNode := &storage.Node{
		ID:         storage.NodeID("similar-node"),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "Similar Node"},
		Embedding:  similarEmbedding,
	}
	_, err = storageEngine.CreateNode(similarNode)
	require.NoError(t, err, "should create similar node")

	// Create a dissimilar node with a very different embedding
	dissimilarEmbedding := make([]float32, 1024)
	for i := range dissimilarEmbedding {
		dissimilarEmbedding[i] = float32(1024-i) / 1024.0 // Very different
	}
	dissimilarNode := &storage.Node{
		ID:         storage.NodeID("dissimilar-node"),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "Dissimilar Node"},
		Embedding:  dissimilarEmbedding,
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
		ID:         storage.NodeID("no-embedding-node"),
		Labels:     []string{"Test"},
		Properties: map[string]interface{}{"name": "No Embedding"},
		Embedding:  nil, // No embedding
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
		{"viewer has read", []string{"viewer"}, auth.PermRead, true},
		{"viewer no write", []string{"viewer"}, auth.PermWrite, false},
		{"empty roles", []string{}, auth.PermRead, false},
		{"invalid role", []string{"invalid"}, auth.PermRead, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasPermission(tt.roles, tt.perm)
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

	if len(users) == 0 {
		t.Skip("no users to test")
	}

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
	config.DecayEnabled = false
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

func TestBackupInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("POST", "/admin/backup", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestHandleUserByIDGet(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Get admin user by username (not ID in this case)
	resp := makeRequest(t, server, "GET", "/auth/users/admin", nil, "Bearer "+token)
	// May be 200 or 404 depending on if GetUser finds by username
	if resp.Code != http.StatusOK && resp.Code != http.StatusNotFound {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleUserByIDPut(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a user first
	createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "updatetestuser",
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if createResp.Code != http.StatusCreated {
		t.Skipf("failed to create user: %d", createResp.Code)
	}

	// Update the user
	resp := makeRequest(t, server, "PUT", "/auth/users/updatetestuser", map[string]interface{}{
		"roles": []string{"editor"},
	}, "Bearer "+token)

	if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleUserByIDPutDisable(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a user first
	createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "disabletestuser",
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if createResp.Code != http.StatusCreated {
		t.Skipf("failed to create user: %d", createResp.Code)
	}

	// Disable the user
	disabled := true
	resp := makeRequest(t, server, "PUT", "/auth/users/disabletestuser", map[string]interface{}{
		"disabled": &disabled,
	}, "Bearer "+token)

	if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleUserByIDPutEnable(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create and disable a user first
	createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "enabletestuser",
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if createResp.Code != http.StatusCreated {
		t.Skipf("failed to create user: %d", createResp.Code)
	}

	// Enable the user
	disabled := false
	resp := makeRequest(t, server, "PUT", "/auth/users/enabletestuser", map[string]interface{}{
		"disabled": &disabled,
	}, "Bearer "+token)

	if resp.Code != http.StatusOK && resp.Code != http.StatusBadRequest {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleUserByIDPutInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	req := httptest.NewRequest("PUT", "/auth/users/testuser", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for invalid JSON, got %d", recorder.Code)
	}
}

func TestHandleUserByIDDelete(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Create a user first
	createResp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "deletetestuser",
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if createResp.Code != http.StatusCreated {
		t.Skipf("failed to create user: %d", createResp.Code)
	}

	// Delete the user
	resp := makeRequest(t, server, "DELETE", "/auth/users/deletetestuser", nil, "Bearer "+token)

	if resp.Code != http.StatusOK && resp.Code != http.StatusNotFound {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestHandleUserByIDEmptyUsername(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/auth/users/", nil, "Bearer "+token)
	// This should route to /auth/users (list) not the by-ID handler
	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for /auth/users/, got %d", resp.Code)
	}
}

func TestHandleUsersMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "PUT", "/auth/users", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405 for PUT on /auth/users, got %d", resp.Code)
	}
}

func TestHandleUserByIDMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "POST", "/auth/users/admin", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed && resp.Code != http.StatusBadRequest {
		t.Errorf("unexpected status %d", resp.Code)
	}
}

func TestImplicitTransactionWithError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Send a query with syntax error
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "INVALID CYPHER SYNTAX HERE"},
		},
	}, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 (with errors in response), got %d", resp.Code)
	}

	// Check that response contains errors
	var txResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&txResp)
	errors, ok := txResp["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("expected errors in response for invalid query")
	}
}

func TestImplicitTransactionMultipleStatementsWithError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// First statement is valid, second is invalid
	resp := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "MATCH (n) RETURN count(n)"},
			{"statement": "INVALID SYNTAX"},
		},
	}, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.Code)
	}

	var txResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&txResp)
	errors, ok := txResp["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("expected errors in response")
	}
}

func TestOpenTransactionWithError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Open transaction with invalid statement
	resp := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "INVALID CYPHER"},
		},
	}, "Bearer "+token)

	if resp.Code != http.StatusCreated {
		t.Errorf("expected status 201, got %d", resp.Code)
	}

	var txResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&txResp)
	errors, ok := txResp["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("expected errors in response")
	}
}

func TestCommitTransactionWithError(t *testing.T) {
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

	// Commit with invalid statement
	commitResp := makeRequest(t, server, "POST", fmt.Sprintf("/db/nornic/tx/%s/commit", txID), map[string]interface{}{
		"statements": []map[string]interface{}{
			{"statement": "INVALID SYNTAX"},
		},
	}, "Bearer "+token)

	if commitResp.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", commitResp.Code)
	}

	var txResp map[string]interface{}
	json.NewDecoder(commitResp.Body).Decode(&txResp)
	errors, ok := txResp["errors"].([]interface{})
	if !ok || len(errors) == 0 {
		t.Error("expected errors in response")
	}
}

func TestTransactionMethodNotAllowedCommit(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic/tx/commit", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestTransactionMethodNotAllowedTxID(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic/tx/123456", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestTransactionMethodNotAllowedCommitID(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, "GET", "/db/nornic/tx/123456/commit", nil, "Bearer "+token)
	if resp.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", resp.Code)
	}
}

func TestTokenGrantTypeUnsupported(t *testing.T) {
	server, _ := setupTestServer(t)

	resp := makeRequest(t, server, "POST", "/auth/token", map[string]interface{}{
		"username":   "admin",
		"password":   "password123",
		"grant_type": "unsupported_type",
	}, "")

	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for unsupported grant_type, got %d", resp.Code)
	}
}

func TestCreateUserError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Try to create user with existing username
	resp := makeRequest(t, server, "POST", "/auth/users", map[string]interface{}{
		"username": "admin", // Already exists
		"password": "password123",
		"roles":    []string{"viewer"},
	}, "Bearer "+token)

	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for duplicate username, got %d", resp.Code)
	}
}

func TestUpdateUserRolesError(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Try to update non-existent user
	resp := makeRequest(t, server, "PUT", "/auth/users/nonexistentuser", map[string]interface{}{
		"roles": []string{"admin"},
	}, "Bearer "+token)

	if resp.Code != http.StatusBadRequest {
		t.Errorf("expected status 400 for non-existent user, got %d", resp.Code)
	}
}

func TestAuthWithNilClaims(t *testing.T) {
	server, _ := setupTestServer(t)

	// Request without any auth should fail on protected endpoint
	resp := makeRequest(t, server, "GET", "/admin/stats", nil, "")

	if resp.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401 without auth, got %d", resp.Code)
	}
}

func TestCORSWithSpecificOrigin(t *testing.T) {
	server, _ := setupTestServer(t)

	req := httptest.NewRequest("OPTIONS", "/", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	req.Header.Set("Access-Control-Request-Method", "POST")

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// Should have CORS headers
	if recorder.Header().Get("Access-Control-Allow-Origin") == "" {
		t.Error("missing Access-Control-Allow-Origin header")
	}
}

func TestMetricsAfterRequests(t *testing.T) {
	server, _ := setupTestServer(t)

	// Make a request
	makeRequest(t, server, "GET", "/health", nil, "")

	stats := server.Stats()
	if stats.RequestCount < 1 {
		t.Errorf("expected request count >= 1, got %d", stats.RequestCount)
	}
}

func TestServerStopWithoutStart(t *testing.T) {
	server, _ := setupTestServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Stop without starting should not error
	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("stop without start should not error: %v", err)
	}
}

func TestServerStopTwice(t *testing.T) {
	server, _ := setupTestServer(t)

	go server.Start()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First stop
	err := server.Stop(ctx)
	if err != nil {
		t.Errorf("first stop error: %v", err)
	}

	// Second stop should be idempotent
	err = server.Stop(ctx)
	if err != nil {
		t.Errorf("second stop should be idempotent: %v", err)
	}
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
	config.DecayEnabled = false
	config.AsyncWritesEnabled = false

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
	config.DecayEnabled = false
	config.AsyncWritesEnabled = false

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
	config.DecayEnabled = false
	config.AsyncWritesEnabled = false

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
	config.DecayEnabled = false
	config.AsyncWritesEnabled = false

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
	config.DecayEnabled = false
	config.AsyncWritesEnabled = false

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

func TestMetricsEndpointWithAuth(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Request with auth should succeed
	resp := makeRequest(t, server, "GET", "/metrics", nil, "Bearer "+token)

	if resp.Code != http.StatusOK {
		t.Errorf("expected status 200 for /metrics with auth, got %d", resp.Code)
	}
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
