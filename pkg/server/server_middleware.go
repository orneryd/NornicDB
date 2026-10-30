package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/txphase"
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
		if !s.isRBACEnforced() {
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
				s.logEvent(r.Context(), slog.LevelDebug, localization.ServerAuthTraceEvent(r.Method, r.URL.Path, "validate_token", time.Since(start), err))
			}
		} else if strings.HasPrefix(authHeader, "Basic ") {
			start := time.Now()

			// Cache successful Basic auth results so clients that send Basic on every request
			// (Neo4j compatibility) don't pay bcrypt+JWT generation each time. The cache
			// spares the password check only: the roles are the account's current ones,
			// read through the same owner as a Bearer token's (auth.CurrentClaims). A
			// cached account that is gone (deleted, disabled, replaced) is checked again
			// in full.
			if s.basicAuthCache != nil {
				if cached, ok := s.basicAuthCache.GetFromHeader(authHeader); ok {
					if claims, err = s.auth.CurrentClaims(cached); err == nil {
						if traceAuth && r.URL.Path == "/graphql" {
							s.logEvent(r.Context(), slog.LevelDebug, localization.ServerAuthTraceEvent(r.Method, r.URL.Path, "basic_auth_cache", time.Since(start), err))
						}
						goto doneAuth
					}
				}
			}

			generation := s.basicAuthCache.Generation()
			var tokenResp *auth.TokenResponse
			tokenResp, claims, err = s.handleBasicAuth(authHeader, r)
			if err == nil && tokenResp != nil && tokenResp.AccessToken != "" {
				// Help browsers / UI by issuing a cookie so future requests can use JWT.
				http.SetCookie(w, sessionTokenCookie(r, tokenResp.AccessToken, 86400*7))
			}
			if err == nil && s.basicAuthCache != nil && claims != nil {
				s.basicAuthCache.SetFromHeaderAtGeneration(authHeader, claims, generation)
			}
			if traceAuth && r.URL.Path == "/graphql" {
				s.logEvent(r.Context(), slog.LevelDebug, localization.ServerAuthTraceEvent(r.Method, r.URL.Path, "basic_auth", time.Since(start), err))
			}
		} else {
			s.writeNeo4jNoAuthenticationProvided(w, r)
			return
		}

	doneAuth:

		if err != nil {
			s.writeBoundaryNeo4jError(w, r, http.StatusUnauthorized, "Neo.ClientError.Security.Unauthorized", err)
			return
		}

		// Check permission
		if !hasPermission(s, claims.Roles, requiredPerm) {
			s.logAudit(r, claims.Sub, "access_denied", false,
				fmt.Sprintf("required permission: %s", requiredPerm))
			s.writeNeo4jInsufficientPermissions(w, r)
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

	return continuationDeliveryMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case path == "/status":
			s.serveRequestTimeout(next, w, r, statusTimeout, "request timeout: status busy")
			return
		case path == "/nornicdb/embed/stats":
			s.serveRequestTimeout(next, w, r, embedStatsTimeout, "request timeout: embed stats busy")
			return
		case path == "/nornicdb/search":
			s.serveRequestTimeout(next, w, r, searchTimeout, "request timeout: search busy")
			return
		case strings.HasPrefix(path, "/db/") && strings.Contains(path, "/tx"):
			// Keep the execution deadline and retention wrapper, while binding
			// participant completion to server lifetime and admitting terminal
			// commits through the transaction phase owner.
			ctx, cancel := txphase.WithExecutionDeadline(r.Context(), txTimeout)
			if s.shutdown.workContext != nil {
				ctx = fabric.WithTransactionLifetime(ctx, s.shutdown.workContext)
			}
			defer cancel()
			txWrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				s.activeTxReqs.Add(1)
				defer s.activeTxReqs.Add(-1)
				next.ServeHTTP(w, r)
			})
			s.serveTransactionRequest(txWrapped, w, r.WithContext(ctx))
			return
		default:
			next.ServeHTTP(w, r)
		}
	}))
}

func (s *Server) serveRequestTimeout(next http.Handler, w http.ResponseWriter, r *http.Request, timeout time.Duration, message string) {
	// Retain before TimeoutHandler starts its goroutine, even if cancellation
	// wins before the worker is scheduled. The worker releases on return/panic.
	next = s.shutdown.work.RetainHandler(memory.WireRequestFromContext(r.Context()).RetainHandler(next), r)
	http.TimeoutHandler(next, timeout, message).ServeHTTP(w, r)
}

// serveTransactionRequest returns the timeout response while an unadmitted
// transaction handler remains retained. After COMMIT admission, it waits for
// the storage outcome instead of replacing that result with a timeout.
func (s *Server) serveTransactionRequest(next http.Handler, w http.ResponseWriter, r *http.Request) {
	// The phase owner controls when the caller may stop waiting. The shared
	// handler owner retains this real worker before it can be dispatched.
	next = s.shutdown.work.RetainHandler(memory.WireRequestFromContext(r.Context()).RetainHandler(next), r)
	response := &transactionResponseWriter{header: make(http.Header)}
	done := make(chan struct{})
	panicCh := make(chan any, 1)
	go func() {
		defer close(done)
		defer func() {
			if recovered := recover(); recovered != nil {
				panicCh <- recovered
			}
		}()
		next.ServeHTTP(response, r)
	}()

	writeResponse := func() {
		select {
		case recovered := <-panicCh:
			panic(recovered)
		default:
		}
		response.writeTo(w)
	}
	select {
	case recovered := <-panicCh:
		panic(recovered)
	case <-done:
		if r.Context().Err() == context.DeadlineExceeded && !txphase.CommitAdmitted(r.Context()) {
			response.markTimedOut()
			s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.TransientError.General.DatabaseUnavailable", "request timeout: transaction busy")
			return
		}
		writeResponse()
	case <-r.Context().Done():
		if txphase.CommitAdmitted(r.Context()) {
			select {
			case recovered := <-panicCh:
				panic(recovered)
			case <-done:
				writeResponse()
			}
			return
		}
		response.markTimedOut()
		if r.Context().Err() == context.DeadlineExceeded {
			s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.TransientError.General.DatabaseUnavailable", "request timeout: transaction busy")
		}
	}
}

type transactionResponseWriter struct {
	mu        sync.Mutex
	header    http.Header
	body      bytes.Buffer
	status    int
	timedOut  bool
	wroteBody bool
}

func (w *transactionResponseWriter) Header() http.Header { return w.header }

func (w *transactionResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.timedOut && w.status == 0 {
		w.status = status
	}
}

func (w *transactionResponseWriter) Write(body []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timedOut {
		return 0, http.ErrHandlerTimeout
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if len(body) > 0 && !w.wroteBody {
		if w.header.Get("Content-Type") == "" {
			w.header.Set("Content-Type", http.DetectContentType(body))
		}
		w.wroteBody = true
	}
	return w.body.Write(body)
}

func (w *transactionResponseWriter) markTimedOut() {
	w.mu.Lock()
	w.timedOut = true
	w.mu.Unlock()
}

func (w *transactionResponseWriter) writeTo(dst http.ResponseWriter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for key, values := range w.header {
		dst.Header().Del(key)
		for _, value := range values {
			dst.Header().Add(key, value)
		}
	}
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	dst.WriteHeader(status)
	if w.body.Len() > 0 {
		_, _ = dst.Write(w.body.Bytes())
	}
}

func (s *Server) recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Log panic summary (without stack trace to prevent info exposure)
				// #nosec CWE-209 -- Panic type only logged via slog stack, not exposed to clients
				s.logEvent(r.Context(), slog.LevelError, localization.ServerHTTPPanicRecoveredEvent(fmt.Sprintf("%v", err)))
				// Stack trace only in debug mode
				if os.Getenv("NORNICDB_DEBUG") == "true" {
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					// #nosec CWE-209 -- Debug-only, slog output, not exposed to clients
					s.logEvent(r.Context(), slog.LevelError, localization.ServerHTTPPanicStackTraceEvent(string(buf[:n])))
				}

				s.errorCount.Add(1)
				s.writeInternalServerError(w, r)
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
