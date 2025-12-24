package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// =============================================================================
// Middleware
// =============================================================================

// withAuth wraps a handler with authentication and authorization.
// Supports both Neo4j Basic Auth and Bearer JWT tokens.
func (s *Server) withAuth(handler http.HandlerFunc, requiredPerm auth.Permission) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Check if auth is enabled
		if s.auth == nil || !s.auth.IsSecurityEnabled() {
			// Auth disabled - allow all
			handler(w, r)
			return
		}

		var claims *auth.JWTClaims
		var err error

		// Try Basic Auth first (Neo4j compatibility)
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Basic ") {
			claims, err = s.handleBasicAuth(authHeader, r)
		} else {
			// Try Bearer/JWT token extraction
			// Check both "nornicdb_token" (preferred) and "token" (legacy) cookies
			cookieToken := getCookie(r, "nornicdb_token")
			if cookieToken == "" {
				cookieToken = getCookie(r, "token")
			}
			token := auth.ExtractToken(
				authHeader,
				r.Header.Get("X-API-Key"),
				cookieToken,
				r.URL.Query().Get("token"),
				r.URL.Query().Get("api_key"),
			)

			if token == "" {
				s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", "No authentication provided")
				return
			}

			claims, err = s.auth.ValidateToken(token)
		}

		if err != nil {
			s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", err.Error())
			return
		}

		// Check permission
		if !hasPermission(claims.Roles, requiredPerm) {
			s.logAudit(r, claims.Sub, "access_denied", false,
				fmt.Sprintf("required permission: %s", requiredPerm))
			s.writeNeo4jError(w, http.StatusForbidden, "Neo.ClientError.Security.Forbidden", "insufficient permissions")
			return
		}

		// Add claims to request context
		ctx := context.WithValue(r.Context(), contextKeyClaims, claims)
		handler(w, r.WithContext(ctx))
	}
}

// handleBasicAuth handles Neo4j-compatible Basic authentication.
func (s *Server) handleBasicAuth(authHeader string, r *http.Request) (*auth.JWTClaims, error) {
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid basic auth format")
	}

	username, password := parts[0], parts[1]

	// Authenticate and get token
	_, user, err := s.auth.Authenticate(username, password, getClientIP(r), r.UserAgent())
	if err != nil {
		return nil, err
	}

	// Convert user to claims
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}

	return &auth.JWTClaims{
		Sub:      user.ID,
		Username: user.Username,
		Email:    user.Email,
		Roles:    roles,
	}, nil
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.config.EnableCORS {
			origin := r.Header.Get("Origin")

			// Check if origin is allowed
			allowed := false
			isWildcard := false
			for _, o := range s.config.CORSOrigins {
				if o == "*" {
					allowed = true
					isWildcard = true
					break
				}
				if o == origin {
					allowed = true
					break
				}
			}

			if allowed {
				// SECURITY: Never use wildcard with credentials - this is a CSRF vector
				// When wildcard is configured, don't send credentials header
				if isWildcard {
					w.Header().Set("Access-Control-Allow-Origin", "*")
					// Deliberately NOT setting Allow-Credentials with wildcard
				} else if origin != "" {
					// Specific origin - safe to allow credentials
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-API-Key")
				w.Header().Set("Access-Control-Max-Age", "86400")
			}

			// Handle preflight
			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

// rateLimitMiddleware applies IP-based rate limiting to prevent DoS attacks.
// Returns 429 Too Many Requests when limits are exceeded.
func (s *Server) rateLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip rate limiting if disabled
		if s.rateLimiter == nil {
			next.ServeHTTP(w, r)
			return
		}

		// Skip rate limiting for health checks (k8s probes, load balancers)
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Extract client IP (handle proxies via X-Forwarded-For)
		ip := r.RemoteAddr
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			// Take first IP in chain (original client)
			if idx := strings.Index(forwarded, ","); idx > 0 {
				ip = strings.TrimSpace(forwarded[:idx])
			} else {
				ip = strings.TrimSpace(forwarded)
			}
		} else if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
			ip = realIP
		}

		// Check rate limit
		if !s.rateLimiter.Allow(ip) {
			w.Header().Set("Retry-After", "60")
			s.writeNeo4jError(w, http.StatusTooManyRequests,
				"Neo.ClientError.Request.TooManyRequests",
				"Rate limit exceeded. Please slow down your requests.")
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap response writer to capture status
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		// Log request (skip health checks for noise reduction)
		if r.URL.Path != "/health" {
			duration := time.Since(start)
			s.logRequest(r, wrapped.status, duration)
		}
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Log panic summary to stderr (without stack trace to prevent info exposure)
				// #nosec CWE-209 -- Panic type only logged to stderr, not exposed to clients
				log.Printf("PANIC: %v", err)
				// Stack trace only in debug mode, written to stderr
				if os.Getenv("NORNICDB_DEBUG") == "true" {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					// #nosec CWE-209 -- Debug-only, stderr output, not exposed to clients
					log.Printf("Stack trace:\n%s", buf[:n])
				}

				s.errorCount.Add(1)
				s.writeError(w, http.StatusInternalServerError, "internal server error", ErrInternalError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requestCount.Add(1)
		s.activeRequests.Add(1)
		defer s.activeRequests.Add(-1)

		next.ServeHTTP(w, r)
	})
}
