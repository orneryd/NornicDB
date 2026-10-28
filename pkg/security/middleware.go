// Package security provides HTTP middleware for NornicDB security validation.
package security

import (
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"net/http"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// SecurityMiddleware wraps HTTP handlers with security validations.
type SecurityMiddleware struct {
	isDevelopment bool
	allowHTTP     bool
	localizer     *localization.Manager
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

// SetLocalizer sets the immutable message catalog used for HTTP errors.
func (m *SecurityMiddleware) SetLocalizer(manager *localization.Manager) {
	m.localizer = manager
}

// ValidateRequest performs comprehensive security validation on incoming requests.
func (m *SecurityMiddleware) ValidateRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate all header values for injection attacks
		for name, values := range r.Header {
			for _, value := range values {
				if err := ValidateHeaderValue(value); err != nil {
					httpresponse.WriteLocalizedRejection(w, r, m.localizer, localization.InvalidHeader(name, err), http.StatusBadRequest, httpresponse.PlainText)
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
					httpresponse.WriteLocalizedRejection(w, r, m.localizer, localization.InvalidAuthorizationToken(err), http.StatusUnauthorized, httpresponse.PlainText)
					return
				}
			}
		}

		// Validate query parameter tokens (for SSE/WebSocket)
		query := r.URL.Query()
		if tokenParam := query.Get("token"); tokenParam != "" {
			if err := ValidateToken(tokenParam); err != nil {
				httpresponse.WriteLocalizedRejection(w, r, m.localizer, localization.InvalidTokenParameter(err), http.StatusUnauthorized, httpresponse.PlainText)
				return
			}
		}

		// Validate URL parameters
		urlPolicy := URLPolicy{AllowHTTP: m.isDevelopment || m.allowHTTP}
		if m.isDevelopment {
			urlPolicy.HostPolicy = URLDevelopmentHost
		}
		urlParams := []string{"callback", "redirect", "redirect_uri", "url", "webhook"}
		for _, param := range urlParams {
			if urlValue := query.Get(param); urlValue != "" {
				if err := ValidateURL(urlValue, urlPolicy); err != nil {
					httpresponse.WriteLocalizedRejection(w, r, m.localizer, localization.InvalidURLParameter(param, err), http.StatusBadRequest, httpresponse.PlainText)
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
