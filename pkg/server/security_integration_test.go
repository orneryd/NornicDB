// Package server provides HTTP REST API server tests.
package server

import (
	"net/http/httptest"
	"os"
	"testing"
) // TestSecurityMiddleware_Integration verifies security middleware blocks attacks at server level
func TestSecurityMiddleware_Integration(t *testing.T) {
	server, authenticator := setupTestServer(t)
	handler := server.buildRouter()

	// Get valid token for authenticated endpoints (uses existing helper)
	validToken := getAuthToken(t, authenticator, "admin")

	tests := []struct {
		name           string
		method         string
		path           string
		headers        map[string]string
		expectedStatus int
		description    string
	}{
		{
			name:   "valid request",
			method: "GET",
			path:   "/health",
			headers: map[string]string{
				"User-Agent": "NornicDB-Client/1.0",
			},
			expectedStatus: 200,
			description:    "Normal requests should pass through",
		},
		{
			name:   "CRLF injection in header",
			method: "GET",
			path:   "/health",
			headers: map[string]string{
				"User-Agent": "Evil\r\nX-Malicious: injected",
			},
			expectedStatus: 400,
			description:    "CRLF injection should be blocked",
		},
		{
			name:   "XSS in authorization token",
			method: "GET",
			path:   "/status",
			headers: map[string]string{
				"Authorization": "Bearer <script>alert('xss')</script>",
			},
			expectedStatus: 401, // Invalid token returns 401 (security middleware validates THEN auth middleware rejects)
			description:    "XSS in token should be blocked",
		},
		{
			name:   "protocol smuggling in callback URL",
			method: "GET",
			path:   "/auth/token?callback=file:///etc/passwd",
			headers: map[string]string{
				"Authorization": "Bearer " + validToken,
			},
			expectedStatus: 400,
			description:    "Protocol smuggling should be blocked",
		},
		{
			name:   "SSRF to private IP",
			method: "GET",
			path:   "/auth/token?redirect_uri=http://192.168.1.1/steal",
			headers: map[string]string{
				"Authorization": "Bearer " + validToken,
			},
			expectedStatus: 400,
			description:    "SSRF to private IPs should be blocked",
		},
		{
			name:   "SSRF to metadata service",
			method: "GET",
			path:   "/auth/token?webhook=http://169.254.169.254/latest/meta-data/",
			headers: map[string]string{
				"Authorization": "Bearer " + validToken,
			},
			expectedStatus: 400,
			description:    "SSRF to cloud metadata should be blocked",
		},
		{
			name:   "null byte in header",
			method: "GET",
			path:   "/health",
			headers: map[string]string{
				"User-Agent": "Valid\x00Evil",
			},
			expectedStatus: 400,
			description:    "Null bytes should be blocked",
		},
		{
			name:   "data URI injection",
			method: "GET",
			path:   "/auth/token?url=data:text/html,<script>alert('xss')</script>",
			headers: map[string]string{
				"Authorization": "Bearer " + validToken,
			},
			expectedStatus: 400,
			description:    "Data URI injection should be blocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)

			// Add headers
			for key, value := range tt.headers {
				req.Header.Set(key, value)
			}

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("%s: got status %d, want %d\nResponse: %s",
					tt.description, w.Code, tt.expectedStatus, w.Body.String())
			}
		})
	}
}

// TestSecurityMiddleware_DevelopmentMode verifies localhost is allowed in development
func TestSecurityMiddleware_DevelopmentMode(t *testing.T) {
	// Set development environment
	oldEnv := os.Getenv("NORNICDB_ENV")
	os.Setenv("NORNICDB_ENV", "development")
	defer os.Setenv("NORNICDB_ENV", oldEnv)

	server, authenticator := setupTestServer(t)
	handler := server.buildRouter()

	validToken := getAuthToken(t, authenticator, "admin")

	tests := []struct {
		name           string
		path           string
		expectedStatus int
		description    string
	}{
		{
			name:           "localhost allowed in dev",
			path:           "/auth/token?callback=http://localhost:3000/callback",
			expectedStatus: 405, // Endpoint requires POST, but middleware passes (no 400)
			description:    "Localhost should be allowed in development mode",
		},
		{
			name:           "127.0.0.1 allowed in dev",
			path:           "/auth/token?redirect_uri=http://127.0.0.1:8080/redirect",
			expectedStatus: 405, // Endpoint requires POST, but middleware passes (no 400)
			description:    "127.0.0.1 should be allowed in development mode",
		},
		{
			name:           "private IP still blocked in dev",
			path:           "/auth/token?callback=http://192.168.1.1/steal",
			expectedStatus: 400, // Middleware blocks BEFORE route handler (SSRF protection)
			description:    "Other private IPs should still be blocked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+validToken)

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("%s: got status %d, want %d\nResponse: %s",
					tt.description, w.Code, tt.expectedStatus, w.Body.String())
			}
		})
	}
}

// TestSecurityMiddleware_Performance benchmarks overhead
func BenchmarkSecurityMiddleware(b *testing.B) {
	server, _ := setupTestServer(&testing.T{})
	handler := server.buildRouter()

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("User-Agent", "Benchmark")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}
