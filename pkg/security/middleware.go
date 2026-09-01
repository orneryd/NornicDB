// Package security provides HTTP middleware for NornicDB security validation.
package security

import (
	"net/http"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"golang.org/x/text/language"
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

func (m *SecurityMiddleware) writeError(w http.ResponseWriter, r *http.Request, message localization.Message, status int) {
	text := message.Fallback
	tag := language.AmericanEnglish
	if m.localizer != nil {
		ctx := r.Context()
		preferences, _, err := language.ParseAcceptLanguage(r.Header.Get("Accept-Language"))
		if err == nil && len(preferences) > 0 {
			match := m.localizer.Resolve("http", preferences...)
			ctx = localization.WithPreferences(ctx, match.Tag)
		}
		if rendered, resolved, err := m.localizer.Render(ctx, message); err == nil {
			text = rendered
			tag = resolved
		}
	}
	w.Header().Set("Content-Language", tag.String())
	w.Header().Add("Vary", "Accept-Language")
	http.Error(w, text, status)
}

// ValidateRequest performs comprehensive security validation on incoming requests.
func (m *SecurityMiddleware) ValidateRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Validate all header values for injection attacks
		for name, values := range r.Header {
			for _, value := range values {
				if err := ValidateHeaderValue(value); err != nil {
					m.writeError(w, r, localization.InvalidHeader(name, err), http.StatusBadRequest)
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
					m.writeError(w, r, localization.InvalidAuthorizationToken(err), http.StatusUnauthorized)
					return
				}
			}
		}

		// Validate query parameter tokens (for SSE/WebSocket)
		if tokenParam := r.URL.Query().Get("token"); tokenParam != "" {
			if err := ValidateToken(tokenParam); err != nil {
				m.writeError(w, r, localization.InvalidTokenParameter(err), http.StatusUnauthorized)
				return
			}
		}

		// Validate URL parameters
		urlParams := []string{"callback", "redirect", "redirect_uri", "url", "webhook"}
		for _, param := range urlParams {
			if urlValue := r.URL.Query().Get(param); urlValue != "" {
				if err := ValidateURL(urlValue, m.isDevelopment, m.allowHTTP); err != nil {
					m.writeError(w, r, localization.InvalidURLParameter(param, err), http.StatusBadRequest)
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
