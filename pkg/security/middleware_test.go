// Package security provides HTTP middleware tests.
package security

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSecurityMiddleware_HeaderValidation(t *testing.T) {
	middleware := NewSecurityMiddleware()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	wrappedHandler := middleware.ValidateRequest(handler)

	tests := []struct {
		name          string
		headerName    string
		headerValue   string
		expectBlocked bool
	}{
		{"valid header", "User-Agent", "Mozilla/5.0", false},
		{"CRLF injection", "X-Custom", "value\r\nX-Injected: evil", true},
		{"newline injection", "X-Custom", "value\nX-Injected: evil", true},
		{"null byte", "X-Custom", "value\x00injected", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set(tt.headerName, tt.headerValue)

			rr := httptest.NewRecorder()
			wrappedHandler.ServeHTTP(rr, req)

			if tt.expectBlocked {
				if rr.Code == http.StatusOK {
					t.Errorf("Expected request to be blocked, but got status %d", rr.Code)
				}
			} else {
				if rr.Code != http.StatusOK {
					t.Errorf("Expected status 200, got %d: %s", rr.Code, rr.Body.String())
				}
			}
		})
	}
}

func TestSecurityMiddleware_TokenValidation(t *testing.T) {
	middleware := NewSecurityMiddleware()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware.ValidateRequest(handler)

	tests := []struct {
		name          string
		authHeader    string
		expectBlocked bool
	}{
		{"valid bearer token", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", false},
		{"valid basic auth", "Basic dXNlcjpwYXNz", false},
		{"injection in bearer", "Bearer token\r\nX-Evil: header", true},
		{"XSS in token", "Bearer <script>alert('xss')</script>", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			req.Header.Set("Authorization", tt.authHeader)

			rr := httptest.NewRecorder()
			wrappedHandler.ServeHTTP(rr, req)

			if tt.expectBlocked {
				if rr.Code == http.StatusOK {
					t.Errorf("Expected token to be blocked, but got status %d", rr.Code)
				}
			} else {
				if rr.Code != http.StatusOK {
					t.Errorf("Expected status 200, got %d: %s", rr.Code, rr.Body.String())
				}
			}
		})
	}
}

func TestSecurityMiddleware_URLValidation(t *testing.T) {
	// Set production environment
	oldEnv := os.Getenv("NORNICDB_ENV")
	os.Setenv("NORNICDB_ENV", "production")
	defer os.Setenv("NORNICDB_ENV", oldEnv)

	middleware := NewSecurityMiddleware()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware.ValidateRequest(handler)

	tests := []struct {
		name          string
		urlParam      string
		expectBlocked bool
	}{
		{"valid HTTPS callback", "https://example.com/callback", false},
		{"SSRF to private IP", "https://192.168.1.1/callback", true},
		{"SSRF to metadata service", "http://169.254.169.254/latest/meta-data/", true},
		{"protocol smuggling", "file:///etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test?callback="+tt.urlParam, nil)

			rr := httptest.NewRecorder()
			wrappedHandler.ServeHTTP(rr, req)

			if tt.expectBlocked {
				if rr.Code == http.StatusOK {
					t.Errorf("Expected SSRF to be blocked, but got status %d", rr.Code)
				}
			} else {
				if rr.Code != http.StatusOK {
					t.Errorf("Expected status 200, got %d: %s", rr.Code, rr.Body.String())
				}
			}
		})
	}
}

func TestSecurityMiddleware_DevelopmentMode(t *testing.T) {
	oldEnv := os.Getenv("NORNICDB_ENV")
	os.Setenv("NORNICDB_ENV", "development")
	defer os.Setenv("NORNICDB_ENV", oldEnv)

	middleware := NewSecurityMiddleware()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware.ValidateRequest(handler)

	// Localhost should be allowed in development
	req := httptest.NewRequest("GET", "/test?callback=http://localhost:8080/callback", nil)
	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected localhost to be allowed in development, got status %d: %s", rr.Code, rr.Body.String())
	}
}

func TestSecurityMiddleware_AllowHTTP(t *testing.T) {
	oldEnv := os.Getenv("NORNICDB_ALLOW_HTTP")
	oldNodeEnv := os.Getenv("NORNICDB_ENV")
	os.Setenv("NORNICDB_ALLOW_HTTP", "true")
	os.Setenv("NORNICDB_ENV", "production")
	defer func() {
		os.Setenv("NORNICDB_ALLOW_HTTP", oldEnv)
		os.Setenv("NORNICDB_ENV", oldNodeEnv)
	}()

	middleware := NewSecurityMiddleware()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware.ValidateRequest(handler)

	// HTTP should be allowed when NORNICDB_ALLOW_HTTP=true
	req := httptest.NewRequest("GET", "/test?callback=http://example.com/callback", nil)
	rr := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected HTTP to be allowed with NORNICDB_ALLOW_HTTP=true, got status %d: %s", rr.Code, rr.Body.String())
	}
}
