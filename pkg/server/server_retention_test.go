package server

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"encoding/json"
	"net/http"
	"os"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func setupRetentionTestServer(t *testing.T) (*Server, *auth.Authenticator) {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "nornicdb-retention-server-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Database.AsyncWritesEnabled = false
	cfg.Compliance.RetentionEnabled = true
	cfg.Compliance.RetentionPolicyDays = 1
	cfg.Retention.SweepIntervalSeconds = 86400

	db, err := nornicdb.Open(tmpDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	authStorage := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = authStorage.Close() })
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, authStorage)
	require.NoError(t, err)
	_, err = authenticator.CreateUser("admin", "password123", []auth.Role{auth.RoleAdmin})
	require.NoError(t, err)
	_, err = authenticator.CreateUser("subject", "password123", []auth.Role{auth.RoleViewer})
	require.NoError(t, err)

	serverConfig := DefaultConfig()
	serverConfig.Port = 0
	serverConfig.EmbeddingEnabled = false
	serverConfig.EnableCORS = true
	serverConfig.CORSOrigins = []string{"*"}

	server, err := New(db, authenticator, serverConfig)
	require.NoError(t, err)
	t.Cleanup(func() { stopTestServer(t, server) })
	return server, authenticator
}

func TestHandleGDPRDeleteBlockedByLegalHold(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	rm := server.db.GetRetentionManager()
	require.NotNil(t, rm)
	require.NoError(t, rm.PlaceLegalHold(&retention.LegalHold{
		ID:          "hold-1",
		Description: "test hold",
		PlacedBy:    "legal",
		SubjectIDs:  []string{"subject"},
	}))

	resp := makeRequest(t, server, http.MethodPost, "/gdpr/delete", map[string]any{
		"user_id": "subject",
		"confirm": true,
	}, "Bearer "+token)
	require.Equal(t, http.StatusConflict, resp.Code)
	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, true, payload["error"])
	require.Contains(t, payload["message"], "legal hold")
	require.Len(t, server.db.GetRetentionManager().ListErasureRequests(), 0)
}

func TestHandleGDPRDeleteCreatesErasureRequest(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodPost, "/gdpr/delete", map[string]any{
		"user_id":   "subject",
		"confirm":   true,
		"anonymize": true,
	}, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	requests := server.db.GetRetentionManager().ListErasureRequests()
	require.Len(t, requests, 1)
	require.Equal(t, "subject", requests[0].SubjectID)
	require.Equal(t, retention.ErasureStatusPending, requests[0].Status)
	var payload map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, "anonymized", payload["status"])
	require.Equal(t, "subject", payload["user_id"])
}

func TestRetentionStatusEndpoint(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodGet, "/admin/retention/status", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, true, payload["enabled"])
	require.Equal(t, float64(1), payload["policy_count"])
	require.Equal(t, float64(0), payload["hold_count"])
	require.Equal(t, float64(0), payload["erasure_count"])
}

func TestRetentionStatusEndpointDisabled(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	resp := makeRequest(t, server, http.MethodGet, "/admin/retention/status", nil, "Bearer "+token)
	require.Equal(t, http.StatusServiceUnavailable, resp.Code)
	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, true, payload["error"])
	require.Contains(t, payload["message"], "disabled")
}

func TestRetentionPoliciesDefaultsReportsDeterministicCounts(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodPost, "/admin/retention/policies/defaults", nil, "Bearer "+token)
	require.Equal(t, http.StatusOK, resp.Code)
	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, float64(7), payload["loaded"])
	require.Equal(t, float64(0), payload["skipped"])
	require.Empty(t, payload["errors"])
	require.Equal(t, float64(8), payload["total"])
}

func TestRetentionProcessErasureUnknownID(t *testing.T) {
	server, authenticator := setupRetentionTestServer(t)
	token := getAuthToken(t, authenticator, "admin")

	resp := makeRequest(t, server, http.MethodPost, "/admin/retention/erasures/missing/process", nil, "Bearer "+token)
	require.Equal(t, http.StatusNotFound, resp.Code)
	var payload map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	require.Equal(t, true, payload["error"])
	require.Contains(t, payload["message"], "not found")
}
