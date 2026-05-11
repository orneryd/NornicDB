package server

import (
	"context"
	"encoding/base64"
	"fmt"
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
		traceAuth := os.Getenv("NORNICDB_TRACE_AUTH") != ""

		// Check if auth is enabled
		if s.auth == nil || !s.auth.IsSecurityEnabled() {
			// Auth disabled - allow all
			handler(w, r)
			return
		}

		var claims *auth.JWTClaims
		var err error

		authHeader := r.Header.Get("Authorization")

		// Only treat Authorization as a token source when it's a Bearer token.
		// (Basic auth uses the same header but is handled separately.)
		bearerHeader := ""
		if strings.HasPrefix(authHeader, "Bearer ") {
			bearerHeader = authHeader
		}

		// Prefer Bearer/JWT token extraction to avoid doing bcrypt on every request.
		// This is especially important for the UI and any clients that can persist cookies.
		// Check both "nornicdb_token" (preferred) and "token" (legacy) cookies.
		cookieToken := getCookie(r, "nornicdb_token")
		if cookieToken == "" {
			cookieToken = getCookie(r, "token")
		}
		token := auth.ExtractToken(
			bearerHeader,
			r.Header.Get("X-API-Key"),
			cookieToken,
			r.URL.Query().Get("token"),
			r.URL.Query().Get("api_key"),
		)

		if token != "" {
			start := time.Now()
			claims, err = s.auth.ValidateToken(token)
			if traceAuth && r.URL.Path == "/graphql" {
				s.log.Debug("auth", "subsystem", "auth", "method", r.Method, "path", r.URL.Path, "step", "validate_token", "duration", time.Since(start), "error", err)
			}
		} else if strings.HasPrefix(authHeader, "Basic ") {
			start := time.Now()

			// Cache successful Basic auth results so clients that send Basic on every request
			// (Neo4j compatibility) don't pay bcrypt+JWT generation each time.
			if s.basicAuthCache != nil {
				if cached, ok := s.basicAuthCache.GetFromHeader(authHeader); ok {
					claims = cached
					err = nil
					if traceAuth && r.URL.Path == "/graphql" {
						s.log.Debug("auth", "subsystem", "auth", "method", r.Method, "path", r.URL.Path, "step", "basic_auth_cache", "duration", time.Since(start), "error", err)
					}
					goto doneAuth
				}
			}

			var tokenResp *auth.TokenResponse
			tokenResp, claims, err = s.handleBasicAuth(authHeader, r)
			if err == nil && tokenResp != nil && tokenResp.AccessToken != "" {
				// Help browsers / UI by issuing a cookie so future requests can use JWT.
				http.SetCookie(w, &http.Cookie{
					Name:     "nornicdb_token",
					Value:    tokenResp.AccessToken,
					Path:     "/",
					HttpOnly: true,
					Secure:   r.TLS != nil,
					SameSite: http.SameSiteLaxMode,
					MaxAge:   86400 * 7, // 7 days
				})
			}
			if err == nil && s.basicAuthCache != nil && claims != nil {
				s.basicAuthCache.SetFromHeader(authHeader, claims)
			}
			if traceAuth && r.URL.Path == "/graphql" {
				s.log.Debug("auth", "subsystem", "auth", "method", r.Method, "path", r.URL.Path, "step", "basic_auth", "duration", time.Since(start), "error", err)
			}
		} else {
			s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", "No authentication provided")
			return
		}

	doneAuth:

		if err != nil {
			s.writeNeo4jError(w, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", err.Error())
			return
		}

		// Check permission
		if !hasPermission(s, claims.Roles, requiredPerm) {
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
func (s *Server) handleBasicAuth(authHeader string, r *http.Request) (*auth.TokenResponse, *auth.JWTClaims, error) {
	encoded := strings.TrimPrefix(authHeader, "Basic ")
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid basic auth encoding")
	}

	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, nil, fmt.Errorf("invalid basic auth format")
	}

	username, password := parts[0], parts[1]

	// Authenticate and get token
	tokenResp, user, err := s.auth.Authenticate(username, password, getClientIP(r), r.UserAgent())
	if err != nil {
		return nil, nil, err
	}

	// Convert user to claims
	roles := make([]string, len(user.Roles))
	for i, role := range user.Roles {
		roles[i] = string(role)
	}

	claims := &auth.JWTClaims{
		Sub:      user.ID,
		Username: user.Username,
		Email:    user.Email,
		Roles:    roles,
	}

	return tokenResp, claims, nil
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

// requestTimeoutMiddleware bounds handler latency for critical API paths that
// otherwise may appear to hang under lock contention.
func (s *Server) requestTimeoutMiddleware(next http.Handler) http.Handler {
	statusTimeout := 5 * time.Second
	embedStatsTimeout := 2 * time.Second
	searchTimeout := 20 * time.Second
	txTimeout := s.config.WriteTimeout
	if txTimeout <= 0 {
		txTimeout = 300 * time.Second
	}
	// Tx commit routes can include multi-part Fabric fan-out and can legitimately
	// take longer than default request timeouts under load. Keep a hard floor to
	// avoid spurious 503 "transaction busy" failures.
	if txTimeout < 5*time.Minute {
		txTimeout = 5 * time.Minute
	}
	if v := strings.TrimSpace(os.Getenv("NORNICDB_HTTP_TX_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			txTimeout = d
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/status":
			http.TimeoutHandler(next, statusTimeout, "request timeout: status busy").ServeHTTP(w, r)
			return
		case path == "/nornicdb/embed/stats":
			http.TimeoutHandler(next, embedStatsTimeout, "request timeout: embed stats busy").ServeHTTP(w, r)
			return
		case path == "/nornicdb/search":
			http.TimeoutHandler(next, searchTimeout, "request timeout: search busy").ServeHTTP(w, r)
			return
		case strings.HasPrefix(path, "/db/") && strings.Contains(path, "/tx"):
			txWrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.activeTxReqs.Add(1)
				defer s.activeTxReqs.Add(-1)
				next.ServeHTTP(w, r)
			})
			http.TimeoutHandler(txWrapped, txTimeout, "request timeout: transaction busy").ServeHTTP(w, r)
			return
		default:
			next.ServeHTTP(w, r)
		}
	})
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Log panic summary (without stack trace to prevent info exposure)
				// #nosec CWE-209 -- Panic type only logged via slog stack, not exposed to clients
				s.log.Error("panic recovered in HTTP handler", "panic", fmt.Sprintf("%v", err))
				// Stack trace only in debug mode
				if os.Getenv("NORNICDB_DEBUG") == "true" {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					// #nosec CWE-209 -- Debug-only, slog output, not exposed to clients
					s.log.Error("panic stack trace", "stack", string(buf[:n]))
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
