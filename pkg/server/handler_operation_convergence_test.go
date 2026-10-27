package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/stretchr/testify/require"
)

// The concrete public router must keep each operation's method and manager
// admission order; a helper-only test cannot detect a swapped route purpose.
func TestGPUControlPublicOperationAdmission(t *testing.T) {
	s, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	for _, path := range []string{"/admin/gpu/enable", "/admin/gpu/disable"} {
		s.db.SetGPUManager(nil)
		response := makeRequest(t, s, http.MethodGet, path, nil, token)
		require.Equal(t, http.StatusMethodNotAllowed, response.Code, response.Body.String())
		response = makeRequest(t, s, http.MethodPost, path, nil, token)
		require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
		s.db.SetGPUManager("invalid-manager")
		response = makeRequest(t, s, http.MethodPost, path, nil, token)
		require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	}
	manager, err := gpu.NewManager(&gpu.Config{Enabled: false, FallbackOnError: true})
	require.NoError(t, err)
	s.db.SetGPUManager(manager)
	response := makeRequest(t, s, http.MethodPost, "/admin/gpu/disable", nil, token)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"status":"disabled"`)
	require.False(t, manager.IsEnabled())
}

func TestBackupOperationPublicValidation(t *testing.T) {
	s, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	for _, path := range []string{"/admin/backup", "/admin/restore"} {
		response := makeRequest(t, s, http.MethodGet, path, nil, token)
		require.Equal(t, http.StatusMethodNotAllowed, response.Code, response.Body.String())
		response = makeRequest(t, s, http.MethodPost, path, map[string]any{"path": " \t "}, token)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "backup path is required")
	}
}

func TestEmbeddingFailurePublicOperationValidation(t *testing.T) {
	s, users := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, users, "admin")
	router := s.buildRouter()
	for _, tc := range []struct {
		method, path, body string
		status             int
		message            string
	}{
		{http.MethodPost, "/nornicdb/embed/failures", "", 405, ""},
		{http.MethodGet, "/nornicdb/embed/retry-failures", "", 405, ""},
		{http.MethodGet, "/nornicdb/embed/failures?limit=0", "", 400, "limit must be between 1 and 1000"},
		{http.MethodGet, "/nornicdb/embed/failures?limit=1001", "", 400, "limit must be between 1 and 1000"},
		{http.MethodGet, "/nornicdb/embed/failures?limit=bad", "", 400, "limit must be between 1 and 1000"},
		{http.MethodPost, "/nornicdb/embed/retry-failures", "{", 400, "invalid retry request"},
		// No worker exists in this fixture. Valid list and retry requests must pass
		// validation and reach that exact backend error, including EOF retry-all.
		{http.MethodGet, "/nornicdb/embed/failures?limit=1", "", 500, ""},
		{http.MethodGet, "/nornicdb/embed/failures?limit=1000", "", 500, ""},
		{http.MethodPost, "/nornicdb/embed/retry-failures", "", 500, ""},
		{http.MethodPost, "/nornicdb/embed/retry-failures", `{"node_ids":[]}`, 500, ""},
		{http.MethodPost, "/nornicdb/embed/retry-failures", `{"node_ids":["test-node"]}`, 500, ""},
	} {
		t.Run(tc.method+tc.path+tc.body, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			request.Header.Set("Authorization", token)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			require.Equal(t, tc.status, response.Code, response.Body.String())
			if tc.message != "" {
				require.Contains(t, response.Body.String(), tc.message)
			}
		})
	}
	// Install the existing deterministic provider through the real DB lifecycle.
	// Foreground-yield prevents background processing; these empty durable queues
	// still exercise each public success response rather than only validation.
	s.db.SetEmbedQueueShouldYield(func() bool { return true })
	require.True(t, s.db.SetEmbedder(fixedSearchEmbedder{}))
	for _, tc := range []struct {
		method, path, body string
		status             int
		field              string
	}{
		{http.MethodGet, "/nornicdb/embed/failures", "", http.StatusOK, `"returned":0`},
		{http.MethodPost, "/nornicdb/embed/retry-failures", "", http.StatusAccepted, `"retried":0`},
		{http.MethodPost, "/nornicdb/embed/retry-failures", `{"node_ids":[]}`, http.StatusAccepted, `"retried":0`},
	} {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("Authorization", token)
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		require.Equal(t, tc.status, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), tc.field)
	}

}
