package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
)

// =============================================================================
// Helper Functions
// =============================================================================

type contextKey string

const contextKeyClaims = contextKey("claims")

func getClaims(r *http.Request) *auth.JWTClaims {
	claims, _ := r.Context().Value(contextKeyClaims).(*auth.JWTClaims)
	return claims
}

func getCookie(r *http.Request, name string) string {
	cookie, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	return cookie.Value
}

func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For first
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	// Check X-Real-IP
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	// Fall back to RemoteAddr
	host, _, _ := net.SplitHostPort(r.RemoteAddr)
	return host
}

func hasPermission(roles []string, required auth.Permission) bool {
	for _, roleStr := range roles {
		role := auth.Role(roleStr)
		perms, ok := auth.RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == required {
				return true
			}
		}
	}
	return false
}

func isMutationQuery(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))
	return strings.HasPrefix(upper, "CREATE") ||
		strings.HasPrefix(upper, "MERGE") ||
		strings.HasPrefix(upper, "DELETE") ||
		strings.HasPrefix(upper, "SET") ||
		strings.HasPrefix(upper, "REMOVE") ||
		strings.HasPrefix(upper, "DROP")
}

func parseIntQuery(r *http.Request, key string, defaultVal int) int {
	valStr := r.URL.Query().Get(key)
	if valStr == "" {
		return defaultVal
	}
	var val int
	fmt.Sscanf(valStr, "%d", &val)
	if val <= 0 {
		return defaultVal
	}
	return val
}

func getMemoryUsageMB() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / 1024 / 1024
}

// responseWriter wraps http.ResponseWriter to capture status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Flush implements http.Flusher interface for SSE streaming.
// This is critical for Bifrost chat streaming to work properly.
func (w *responseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// JSON helpers

func (s *Server) readJSON(r *http.Request, v interface{}) error {
	// Limit body size
	body := io.LimitReader(r.Body, s.config.MaxRequestSize)
	return json.NewDecoder(body).Decode(v)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *Server) writeError(w http.ResponseWriter, status int, message string, err error) {
	s.errorCount.Add(1)

	response := map[string]interface{}{
		"error":   true,
		"message": message,
		"code":    status,
	}

	s.writeJSON(w, status, response)
}

// writeNeo4jError writes an error in Neo4j format.
func (s *Server) writeNeo4jError(w http.ResponseWriter, status int, code, message string) {
	s.errorCount.Add(1)
	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors: []QueryError{{
			Code:    code,
			Message: message,
		}},
	}
	s.writeJSON(w, status, response)
}

// Logging helpers

func (s *Server) logRequest(r *http.Request, status int, duration time.Duration) {
	// Could be enhanced with structured logging
	fmt.Printf("[HTTP] %s %s %d %v\n", r.Method, r.URL.Path, status, duration)
}

// logSlowQuery logs queries that exceed the configured threshold.
// Logged info includes: query text (truncated), duration, parameters, error if any.
func (s *Server) logSlowQuery(query string, params map[string]interface{}, duration time.Duration, err error) {
	if !s.config.SlowQueryEnabled {
		return
	}

	if duration < s.config.SlowQueryThreshold {
		return
	}

	s.slowQueryCount.Add(1)

	// Truncate long queries for logging
	queryLog := query
	if len(queryLog) > 500 {
		queryLog = queryLog[:500] + "..."
	}

	// Build log message
	status := "OK"
	if err != nil {
		status = fmt.Sprintf("ERROR: %v", err)
	}

	// Format parameters (limit to avoid huge logs)
	paramStr := ""
	if len(params) > 0 {
		paramBytes, _ := json.Marshal(params)
		if len(paramBytes) > 200 {
			paramStr = string(paramBytes[:200]) + "..."
		} else {
			paramStr = string(paramBytes)
		}
	}

	logMsg := fmt.Sprintf("[SLOW QUERY] duration=%v status=%s query=%q params=%s",
		duration, status, queryLog, paramStr)

	// Log to slow query logger if configured, otherwise to stderr
	if s.slowQueryLogger != nil {
		s.slowQueryLogger.Println(logMsg)
	} else {
		log.Println(logMsg)
	}
}

func (s *Server) logAudit(r *http.Request, userID, eventType string, success bool, details string) {
	if s.audit == nil {
		return
	}

	s.audit.Log(audit.Event{
		Timestamp:   time.Now(),
		Type:        audit.EventType(eventType),
		UserID:      userID,
		IPAddress:   getClientIP(r),
		UserAgent:   r.UserAgent(),
		Success:     success,
		Reason:      details,
		RequestPath: r.URL.Path,
	})
}
