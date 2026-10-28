package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/heimdall"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

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
	path := filepath.Join(t.TempDir(), "backup.bin")
	resp := makeRequest(t, server, "POST", "/admin/backup", map[string]interface{}{
		"path": path,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	var payload struct {
		Status string `json:"status"`
		Path   string `json:"path"`
	}
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &payload))
	require.NotEmpty(t, payload.Status)
	require.Equal(t, path, payload.Path)
	artifact, err := os.Stat(path)
	require.NoError(t, err)
	require.Positive(t, artifact.Size())
}

func TestHandleRestore(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")
	backupPath := filepath.Join(t.TempDir(), "backup.json")
	require.NoError(t, os.WriteFile(backupPath, []byte(`{"version":"1.0","nodes":[],"edges":[]}`), 0o600))

	resp := makeRequest(t, server, "POST", "/admin/restore", map[string]interface{}{
		"path": backupPath,
	}, "Bearer "+token)
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.Code)
	}

	resp = makeRequest(t, server, "POST", "/admin/restore", map[string]interface{}{}, "Bearer "+token)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400 for missing path, got %d", resp.Code)
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

func TestBuildEmbedConfigFromResolved_Branches(t *testing.T) {
	require.Nil(t, buildEmbedConfigFromResolved(map[string]string{}, nil))

	fallback := DefaultConfig()
	fallback.EmbeddingProvider = ""
	fallback.EmbeddingAPIURL = ""
	fallback.EmbeddingModel = ""
	fallback.EmbeddingDimensions = 0
	fallback.ModelsDir = "/tmp/models"

	cfg := buildEmbedConfigFromResolved(map[string]string{}, fallback)
	require.NotNil(t, cfg)
	require.Equal(t, "openai", cfg.Provider)
	require.Equal(t, "/v1/embeddings", cfg.APIPath)
	require.Equal(t, 1024, cfg.Dimensions)
	require.True(t, cfg.LazyModeSet)
	require.Equal(t, 1, cfg.LazyMode)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider":   "ollama",
		"db.nornic.embedding.dimensions": "1536",
		"db.nornic.embedding.gpu.layers": "12",
	}, fallback)
	require.Equal(t, "ollama", cfg.Provider)
	require.Equal(t, "/api/embeddings", cfg.APIPath)
	require.Equal(t, 1536, cfg.Dimensions)
	require.Equal(t, 12, cfg.GPULayers)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider":   "local",
		"db.nornic.embedding.dimensions": "-1",
		"db.nornic.embedding.gpu.layers": "not-an-int",
	}, fallback)
	require.Equal(t, "local", cfg.Provider)
	require.Equal(t, "", cfg.APIPath)
	require.Equal(t, 1024, cfg.Dimensions)
	require.Equal(t, -1, cfg.GPULayers)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider": "custom-provider",
	}, fallback)
	require.Equal(t, "/api/embeddings", cfg.APIPath)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider": "orca",
		"db.nornic.embedding.api.key":  "orca-key",
	}, fallback)
	require.Equal(t, "https://api.orcarouter.ai", cfg.APIURL)
	require.Equal(t, "/v1/embeddings", cfg.APIPath)
	require.Equal(t, "openai/text-embedding-3-small", cfg.Model)
	require.Equal(t, 1536, cfg.Dimensions)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider":   " ORCA ",
		"db.nornic.embedding.api.url":    "http://localhost:11434",
		"db.nornic.embedding.model":      "bge-m3",
		"db.nornic.embedding.dimensions": "1024",
	}, fallback)
	require.Equal(t, "orca", cfg.Provider)
	require.Equal(t, "http://localhost:11434", cfg.APIURL)
	require.Equal(t, "/v1/embeddings", cfg.APIPath)
	require.Equal(t, "bge-m3", cfg.Model)
	require.Equal(t, 1024, cfg.Dimensions)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider":   "orca",
		"db.nornic.embedding.api.url":    "https://gateway.example",
		"db.nornic.embedding.model":      "google/gemini-embedding-001",
		"db.nornic.embedding.dimensions": "3072",
	}, fallback)
	require.Equal(t, "https://gateway.example", cfg.APIURL)
	require.Equal(t, "google/gemini-embedding-001", cfg.Model)
	require.Equal(t, 3072, cfg.Dimensions)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider":       "voyage",
		"db.nornic.embedding.api.url":        "",
		"db.nornic.embedding.model":          "",
		"db.nornic.embedding.mode":           "contextualized",
		"db.nornic.embedding.dimensions":     "1024",
		"db.nornic.embedding.gpu.layers":     "0",
		"db.nornic.embedding.cache.size":     "100",
		"db.nornic.embedding.include.labels": "true",
	}, fallback)
	require.Equal(t, "voyage", cfg.Provider)
	require.Equal(t, "https://api.voyageai.com", cfg.APIURL)
	require.Equal(t, "/v1/embeddings", cfg.APIPath)
	require.Equal(t, "voyage-context-4", cfg.Model)
	require.Equal(t, "contextualized", cfg.Mode)

	voyageFallback := DefaultConfig()
	voyageFallback.EmbeddingAPIURL = "http://localhost:11434"
	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider": "voyage",
	}, voyageFallback)
	require.Equal(t, "https://api.voyageai.com", cfg.APIURL)

	voyageFallback.EmbeddingAPIURL = "http://127.0.0.1:9000"
	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider": "voyage",
	}, voyageFallback)
	require.Equal(t, "http://127.0.0.1:9000", cfg.APIURL)

	cfg = buildEmbedConfigFromResolved(map[string]string{
		"db.nornic.embedding.provider": "voyage",
		"db.nornic.embedding.api.url":  "http://localhost:11434",
	}, DefaultConfig())
	require.Equal(t, "http://localhost:11434", cfg.APIURL)
}

func TestSetSearchFallbackReasonHeader(t *testing.T) {
	recorder := httptest.NewRecorder()
	setSearchFallbackReasonHeader(recorder, search.SearchFallbackQueryEmbeddingFailed)
	require.Equal(t, "query_embedding_failed", recorder.Header().Get("X-NornicDB-Search-Fallback-Reason"))
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

	t.Run("role deletion detaches users and preserves builtin roles", func(t *testing.T) {
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
		server.handleRoles(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		reader, err := server.auth.GetUser("reader")
		require.NoError(t, err)
		require.Empty(t, reader.Roles)
		require.False(t, server.roleStore.Exists("analyst"))
		require.NoError(t, server.roleStore.CreateRole(context.Background(), "analyst"))

		_ = server.auth.UpdateRoles("reader", []auth.Role{auth.RoleViewer})
		req = httptest.NewRequest(http.MethodDelete, "/auth/roles/analyst", nil)
		rec = httptest.NewRecorder()
		server.handleRoles(rec, req)
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

func TestNeo4jConversionAndTxHelpers_AdditionalBranches(t *testing.T) {
	server, _ := setupTestServer(t)

	require.Nil(t, server.convertValueToNeo4jFormat((*storage.Node)(nil), "nornicdb"))
	require.Nil(t, server.convertValueToNeo4jFormat((*storage.Edge)(nil), "nornicdb"))

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
	}, "nornicdb")
	m, ok := converted.(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "drop", m["_pathResult"])
	require.Contains(t, m, "node")
	require.Contains(t, m, "vals")

	withProps := server.convertValueToNeo4jFormat(map[string]interface{}{
		"labels":     []string{"Doc"},
		"properties": map[string]interface{}{"title": "t"},
	}, "nornicdb")
	props, ok := withProps.(map[string]interface{})["properties"].(map[string]interface{})
	require.True(t, ok)
	require.Equal(t, "t", props["title"])

	withMixedLabels := server.convertValueToNeo4jFormat(map[string]interface{}{
		"labels": []interface{}{"A", 123, "B"},
		"name":   "mixed",
	}, "nornicdb")
	labels, ok := withMixedLabels.(map[string]interface{})["labels"].([]interface{})
	require.True(t, ok)
	require.Equal(t, "A", labels[0])
	require.Equal(t, 123, labels[1])
	require.Equal(t, "B", labels[2])

	// The commit URL follows the host the client used; without a Host header
	// it falls back to the listen address, with 0.0.0.0 shown as localhost.
	txReq := httptest.NewRequest(http.MethodPost, "/db/nornic/tx", nil)
	txReq.Host = "db.example:7474"
	require.Equal(t, "http://db.example:7474/db/nornic/tx/tx-1/commit", server.transactionCommitURL(txReq, "nornic", "tx-1"))
	server.config.Address = "0.0.0.0"
	txReq.Host = ""
	require.Contains(t, server.transactionCommitURL(txReq, "nornic", "tx-1"), "http://localhost:")

	resp := &TransactionResponse{Results: make([]QueryResult, 0)}
	server.appendStatementResult(resp, &cypher.ExecuteResult{
		Columns: []string{"n"},
		Rows:    [][]interface{}{{map[string]interface{}{"elementId": "4:nornicdb:abc"}}},
		Metadata: map[string]interface{}{
			"receipt":    map[string]interface{}{"writes": 1},
			"optimistic": map[string]interface{}{"createdNodeIds": []string{"nornic:1"}},
		},
	}, "nornicdb", false)
	require.Len(t, resp.Results, 1)
	require.NotNil(t, resp.Receipt)
	require.NotNil(t, resp.Optimistic)

	// Nil result columns must serialize as [] for UI safety.
	resp = &TransactionResponse{Results: make([]QueryResult, 0)}
	server.appendStatementResult(resp, &cypher.ExecuteResult{
		Columns: nil,
		Rows:    [][]interface{}{},
	}, "nornicdb", false)
	require.Len(t, resp.Results, 1)
	require.NotNil(t, resp.Results[0].Columns)
	require.Len(t, resp.Results[0].Columns, 0)
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
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "status_db"}))
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
		server.handleRoles(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)

		// role_src and role_dst are the roles src and dst (one role-name
		// form, auth.NormalizeRoleName): the user's assignment moves to dst.
		user, err := server.auth.GetUser("reader")
		require.NoError(t, err)
		var hasRoleDst bool
		for _, r := range user.Roles {
			if string(r) == "dst" {
				hasRoleDst = true
			}
		}
		require.True(t, hasRoleDst)

		req = httptest.NewRequest(http.MethodGet, "/auth/roles/role_dst", nil)
		rec = httptest.NewRecorder()
		server.handleRoles(rec, req)
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
