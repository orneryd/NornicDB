// GPU endpoint tests for server package.
package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestHandleGPUStatus(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Initialize GPU manager (will be disabled by default, no GPU)
	gpuManager, err := gpu.NewManager(nil)
	if err != nil {
		t.Fatalf("failed to create GPU manager: %v", err)
	}
	server.db.SetGPUManager(gpuManager)

	req := httptest.NewRequest("GET", "/admin/gpu/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recorder.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := response["enabled"]; !ok {
		t.Error("response missing 'enabled' field")
	}
	if _, ok := response["available"]; !ok {
		t.Error("response missing 'available' field")
	}
}

func TestHandleGPUStatusNoManager(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	// Don't set GPU manager

	req := httptest.NewRequest("GET", "/admin/gpu/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recorder.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if enabled, ok := response["enabled"].(bool); !ok || enabled {
		t.Error("expected enabled=false when manager not initialized")
	}
	if message, ok := response["message"].(string); !ok || message == "" {
		t.Error("expected message explaining manager not initialized")
	}
}

func TestHandleGPUEnable(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, err := gpu.NewManager(nil)
	if err != nil {
		t.Fatalf("failed to create GPU manager: %v", err)
	}
	server.db.SetGPUManager(gpuManager)

	req := httptest.NewRequest("POST", "/admin/gpu/enable", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	// GPU may or may not be available depending on test environment
	// On Apple Silicon: 200 (success)
	// On machines without GPU: 500 (no GPU available)
	if recorder.Code != http.StatusOK && recorder.Code != http.StatusInternalServerError {
		t.Errorf("expected status 200 (GPU available) or 500 (no GPU), got %d", recorder.Code)
	}
}

func TestHandleGPUDisable(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, err := gpu.NewManager(nil)
	if err != nil {
		t.Fatalf("failed to create GPU manager: %v", err)
	}
	server.db.SetGPUManager(gpuManager)

	req := httptest.NewRequest("POST", "/admin/gpu/disable", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", recorder.Code)
	}

	var response map[string]interface{}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if status, ok := response["status"].(string); !ok || status != "disabled" {
		t.Error("expected status=disabled")
	}
}

func TestHandleGPUTest(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, err := gpu.NewManager(nil)
	if err != nil {
		t.Fatalf("failed to create GPU manager: %v", err)
	}
	server.db.SetGPUManager(gpuManager)

	// Create a test node with embedding
	emb := make([]float32, 1024)
	for i := range emb {
		emb[i] = float32(i) / 1024.0
	}
	testNode := &storage.Node{
		ID:              storage.NodeID(uuid.New().String()),
		Labels:          []string{"Memory"},
		Properties:      map[string]interface{}{"content": "Test memory for vector search"},
		ChunkEmbeddings: [][]float32{emb},
	}
	storedID, err := server.db.GetStorage().CreateNode(testNode)
	if err != nil {
		t.Fatalf("failed to store test memory: %v", err)
	}

	reqBody := map[string]interface{}{
		"node_id": string(storedID),
		"limit":   5,
		"mode":    "cpu", // Force CPU mode for testing
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/admin/gpu/test", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", recorder.Code, recorder.Body.String())
	}

	var response map[string]interface{}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if _, ok := response["results"]; !ok {
		t.Error("response missing 'results' field")
	}
	if _, ok := response["performance"]; !ok {
		t.Error("response missing 'performance' field")
	}

	// Check performance info
	if perf, ok := response["performance"].(map[string]interface{}); ok {
		if mode, ok := perf["mode"].(string); !ok || mode != "cpu" {
			t.Errorf("expected mode=cpu, got %v", mode)
		}
		if _, ok := perf["elapsed_ms"]; !ok {
			t.Error("performance missing elapsed_ms")
		}
	} else {
		t.Error("performance is not a map")
	}
}

func TestGPUEndpointsRequireAdmin(t *testing.T) {
	server, authenticator := setupTestServer(t)

	// Create a regular user first
	_, err := authenticator.CreateUser("regularuser", "password123", []auth.Role{auth.RoleViewer})
	if err != nil {
		t.Fatalf("failed to create test user: %v", err)
	}

	token := getAuthToken(t, authenticator, "regularuser") // Regular user

	gpuManager, _ := gpu.NewManager(nil)
	server.db.SetGPUManager(gpuManager)

	endpoints := []string{
		"/admin/gpu/status",
		"/admin/gpu/enable",
		"/admin/gpu/disable",
		"/admin/gpu/test",
	}

	for _, endpoint := range endpoints {
		var req *http.Request
		if endpoint == "/admin/gpu/status" {
			req = httptest.NewRequest("GET", endpoint, nil)
		} else {
			req = httptest.NewRequest("POST", endpoint, nil)
		}
		req.Header.Set("Authorization", "Bearer "+token)

		recorder := httptest.NewRecorder()
		server.buildRouter().ServeHTTP(recorder, req)

		if recorder.Code != http.StatusForbidden {
			t.Errorf("endpoint %s should require admin permission, got %d", endpoint, recorder.Code)
		}
	}
}

func TestGPUStatusMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, _ := gpu.NewManager(nil)
	server.db.SetGPUManager(gpuManager)

	// POST to GET-only endpoint
	req := httptest.NewRequest("POST", "/admin/gpu/status", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", recorder.Code)
	}
}

func TestGPUEnableMethodNotAllowed(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, _ := gpu.NewManager(nil)
	server.db.SetGPUManager(gpuManager)

	// GET to POST-only endpoint
	req := httptest.NewRequest("GET", "/admin/gpu/enable", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", recorder.Code)
	}
}

func TestGPUTestInvalidJSON(t *testing.T) {
	server, auth := setupTestServer(t)
	token := getAuthToken(t, auth, "admin")

	gpuManager, _ := gpu.NewManager(nil)
	server.db.SetGPUManager(gpuManager)

	req := httptest.NewRequest("POST", "/admin/gpu/test", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	recorder := httptest.NewRecorder()
	server.buildRouter().ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", recorder.Code)
	}
}
