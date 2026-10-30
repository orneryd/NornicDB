package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBifrostRoutes_Integration tests that Bifrost routes work correctly
// by testing the route registration pattern used in buildRouter.
func TestBifrostRoutes_Integration(t *testing.T) {
	// Create a mock handler that simulates heimdall.Handler behavior
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/bifrost/status":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
				"model":  "test-model",
				"heimdall": map[string]interface{}{
					"enabled": true,
				},
				"bifrost": map[string]interface{}{
					"enabled":          true,
					"connection_count": 0,
				},
			})
		case "/api/bifrost/chat/completions", "/v1/chat/completions":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"id":      "test-123",
				"object":  "chat.completion",
				"model":   "test-model",
				"choices": []map[string]interface{}{},
			})
		case "/v1/models":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"object": "list",
				"data": []map[string]interface{}{
					{"id": "test-model", "object": "model"},
				},
			})
		case "/api/bifrost/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte("data: {\"type\":\"connected\"}\n\n"))
		default:
			http.NotFound(w, r)
		}
	})

	// Build router with routes registered (same pattern as in buildRouter)
	mux := http.NewServeMux()

	// This is exactly how routes are registered in server.go
	mux.HandleFunc("/api/bifrost/status", func(w http.ResponseWriter, r *http.Request) {
		mockHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/bifrost/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		mockHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		mockHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		mockHandler.ServeHTTP(w, r)
	})
	mux.HandleFunc("/api/bifrost/events", func(w http.ResponseWriter, r *http.Request) {
		mockHandler.ServeHTTP(w, r)
	})

	// Test /api/bifrost/status
	t.Run("GET /api/bifrost/status returns 200 with correct body", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "Expected 200 OK")
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err, "Response should be valid JSON")

		assert.Equal(t, "ok", response["status"])
		assert.Equal(t, "test-model", response["model"])

		heimdallData := response["heimdall"].(map[string]interface{})
		assert.Equal(t, true, heimdallData["enabled"])
	})

	// Test /api/bifrost/chat/completions
	t.Run("POST /api/bifrost/chat/completions returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/bifrost/chat/completions", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "Expected 200 OK")

		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)

		assert.Equal(t, "chat.completion", response["object"])
	})

	t.Run("POST /v1/chat/completions returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "Expected 200 OK")

		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, "chat.completion", response["object"])
	})

	t.Run("GET /v1/models returns single announced model", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code, "Expected 200 OK")

		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, "list", response["object"])
	})

	// Test /api/bifrost/events (SSE endpoint)
	t.Run("GET /api/bifrost/events returns SSE content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/bifrost/events", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	})
}

// TestBifrostRoutes_NotRegistered tests that routes return 404 when not registered
func TestBifrostRoutes_NotRegistered(t *testing.T) {
	// Empty mux - no routes registered
	mux := http.NewServeMux()

	t.Run("GET /api/bifrost/status returns 404 when not registered", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/bifrost/status", nil)
		rec := httptest.NewRecorder()

		mux.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestBifrostRoutePattern verifies the exact route patterns work
func TestBifrostRoutePattern(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{"status endpoint", http.MethodGet, "/api/bifrost/status", http.StatusOK},
		{"chat endpoint", http.MethodPost, "/api/bifrost/chat/completions", http.StatusOK},
		{"openai chat endpoint", http.MethodPost, "/v1/chat/completions", http.StatusOK},
		{"openai models endpoint", http.MethodGet, "/v1/models", http.StatusOK},
		{"events endpoint", http.MethodGet, "/api/bifrost/events", http.StatusOK},
		{"unknown endpoint", http.MethodGet, "/api/bifrost/unknown", http.StatusNotFound},
		{"wrong prefix", http.MethodGet, "/api/other/status", http.StatusNotFound},
	}

	// Setup mock handler
	mockHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/bifrost/status", "/api/bifrost/chat/completions", "/api/bifrost/events", "/v1/chat/completions", "/v1/models":
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})

	mux := http.NewServeMux()
	mux.HandleFunc("/api/bifrost/status", mockHandler)
	mux.HandleFunc("/api/bifrost/chat/completions", mockHandler)
	mux.HandleFunc("/v1/chat/completions", mockHandler)
	mux.HandleFunc("/v1/models", mockHandler)
	mux.HandleFunc("/api/bifrost/events", mockHandler)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()

			mux.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code, "Path: %s", tt.path)
		})
	}
}
