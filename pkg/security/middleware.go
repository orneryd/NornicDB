// Package security provides HTTP middleware for NornicDB security validation.
package security

import (
	"fmt"
	"net/http"
	"strings"
)

// SecurityMiddleware wraps HTTP handlers with security validations.
type SecurityMiddleware struct {
	isDevelopment bool
	allowHTTP     bool
}

// SecurityConfig holds security middleware configuration.
// This is passed from the main config to avoid direct env var access.
type SecurityConfig struct {
	Environment string // "development", "production"
	AllowHTTP   bool   // Allow non-TLS connections
}

// NewSecurityMiddleware creates a new security middleware instance.
// Use NewSecurityMiddlewareWithConfig for production code.
func NewSecurityMiddleware() *SecurityMiddleware {
	return &SecurityMiddleware{
		isDevelopment: true, // default to development for safety
		allowHTTP:     true,
	}
}

// NewSecurityMiddlewareWithConfig creates a security middleware with explicit config.
func NewSecurityMiddlewareWithConfig(cfg SecurityConfig) *SecurityMiddleware {
	env := strings.ToLower(cfg.Environment)
	isDevelopment := env == "development" || env == "dev" || env == ""

	return &SecurityMiddleware{
		isDevelopment: isDevelopment,
		allowHTTP:     cfg.AllowHTTP,
	}
}

// ValidateRequest performs comprehensive security validation on incoming requests.
func (m *SecurityMiddleware) ValidateRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// Validate all header values for injection attacks
for name, values := range r.Header {
for _, value := range values {
if err := ValidateHeaderValue(value); err != nil {
					http.Error(w, fmt.Sprintf("Invalid header %s: %v", name, err), http.StatusBadRequest)
					return
				}
			}
		}
		
		// Validate Authorization header specifically
		authHeader := r.Header.Get("Authorization")
		if authHeader != "" {
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) == 2 {
				token := strings.TrimSpace(parts[1])
				if err := ValidateToken(token); err != nil {
					http.Error(w, fmt.Sprintf("Invalid authorization token: %v", err), http.StatusUnauthorized)
					return
				}
			}
		}
		
		// Validate query parameter tokens (for SSE/WebSocket)
		if tokenParam := r.URL.Query().Get("token"); tokenParam != "" {
			if err := ValidateToken(tokenParam); err != nil {
				http.Error(w, fmt.Sprintf("Invalid token parameter: %v", err), http.StatusUnauthorized)
				return
			}
		}
		
		// Validate URL parameters
		urlParams := []string{"callback", "redirect", "redirect_uri", "url", "webhook"}
		for _, param := range urlParams {
			if urlValue := r.URL.Query().Get(param); urlValue != "" {
				if err := ValidateURL(urlValue, m.isDevelopment, m.allowHTTP); err != nil {
					http.Error(w, fmt.Sprintf("Invalid %s parameter: %v", param, err), http.StatusBadRequest)
					return
				}
			}
		}
		
		next.ServeHTTP(w, r)
	})
}

// Wrap is a convenience method for wrapping individual handlers.
func (m *SecurityMiddleware) Wrap(handler http.Handler) http.Handler {
	return m.ValidateRequest(handler)
}
