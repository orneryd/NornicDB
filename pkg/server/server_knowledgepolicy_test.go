package server

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupKPTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "nornicdb-kp-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false

	db, err := nornicdb.Open(tmpDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewMemoryEngine())
	require.NoError(t, err)
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false

	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)

	token := getAuthToken(t, authenticator, "admin")
	return server, token
}

func TestKPProfilesEndpoint(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/profiles", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Contains(t, payload, "bundles")
	assert.Contains(t, payload, "bindings")
	assert.Contains(t, payload, "decay_enabled")
}

func TestKPProfilesEndpoint_MethodNotAllowed(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodPost, "/admin/knowledge-policies/profiles", nil, "Bearer "+token)
	require.Equal(t, http.StatusMethodNotAllowed, resp.Code)
}

func TestKPPoliciesEndpoint(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/policies", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Contains(t, payload, "promotion_profiles")
	assert.Contains(t, payload, "promotion_policies")
}

func TestKPResolveEndpoint_Labels(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/resolve?labels=Memory,Concept", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Contains(t, payload, "TargetID")
	assert.Contains(t, payload, "FinalScore")
}

func TestKPResolveEndpoint_EdgeType(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/resolve?edgeType=RELATES_TO", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Contains(t, payload, "TargetID")
}

func TestKPResolveEndpoint_MissingParams(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/resolve", nil, "Bearer "+token)
	require.Equal(t, http.StatusBadRequest, resp.Code)
}

func TestKPDeindexStatusEndpoint(t *testing.T) {
	server, token := setupKPTestServer(t)

	resp := makeRequest(t, server, http.MethodGet, "/admin/knowledge-policies/deindex/status", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)

	var payload map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Contains(t, payload, "pending_count")
	assert.Contains(t, payload, "items")
	assert.Contains(t, payload, "supported")
}

func TestKPEndpoints_RequireAuth(t *testing.T) {
	server, _ := setupKPTestServer(t)

	endpoints := []string{
		"/admin/knowledge-policies/profiles",
		"/admin/knowledge-policies/policies",
		"/admin/knowledge-policies/resolve?labels=Test",
		"/admin/knowledge-policies/deindex/status",
	}

	for _, endpoint := range endpoints {
		t.Run(endpoint, func(t *testing.T) {
			resp := makeRequest(t, server, http.MethodGet, endpoint, nil, "")
			require.Equal(t, http.StatusUnauthorized, resp.Code)
		})
	}
}
