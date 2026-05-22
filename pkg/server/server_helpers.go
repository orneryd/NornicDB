package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
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

// hostnameFromRequest returns the hostname portion of the request's
// Host header, or empty when the header is missing. Used by the
// discovery handler so the bolt_direct / ws_direct URLs it advertises
// match the hostname the browser is already using — cookies are
// scoped by exact hostname (localhost vs 127.0.0.1 are distinct), so
// returning the configured bind address would silently break the
// cookie-as-implicit-bearer flow on the WS upgrade.
//
// Forwarded headers (X-Forwarded-Host) take precedence so deployments
// behind a reverse proxy advertise the externally-visible hostname.
func hostnameFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	candidates := []string{
		r.Header.Get("X-Forwarded-Host"),
		r.Host,
	}
	for _, raw := range candidates {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		// X-Forwarded-Host may contain multiple comma-separated names; take the first.
		if idx := strings.Index(raw, ","); idx >= 0 {
			raw = strings.TrimSpace(raw[:idx])
		}
		if host, _, err := net.SplitHostPort(raw); err == nil {
			return host
		}
		return raw
	}
	return ""
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

// isRBACEnforced reports whether RBAC checks should be enforced for requests.
// When auth is disabled (e.g. --no-auth), request-level RBAC checks are skipped.
func (s *Server) isRBACEnforced() bool {
	return s.auth != nil && s.auth.IsSecurityEnabled()
}

// getDatabaseAccessMode returns the per-database access mode for the given principal (claims).
// When auth is disabled returns Full; when unauthenticated returns DenyAll;
// when allowlist is loaded returns allowlist-based mode for claims.Roles.
func (s *Server) getDatabaseAccessMode(claims *auth.JWTClaims) auth.DatabaseAccessMode {
	if !s.isRBACEnforced() {
		return auth.FullDatabaseAccessMode
	}
	if claims == nil {
		return auth.DenyAllDatabaseAccessMode
	}
	if s.allowlistStore != nil {
		return auth.NewAllowlistDatabaseAccessMode(s.allowlistStore.Allowlist(), claims.Roles)
	}
	return auth.DenyAllDatabaseAccessMode
}

// GetDatabaseAccessModeForRoles returns the per-database access mode for the given principal roles.
// Used by Bolt when the principal is known (e.g. from HELLO auth). When auth disabled, Bolt should use Full.
func (s *Server) GetDatabaseAccessModeForRoles(roles []string) auth.DatabaseAccessMode {
	if !s.isRBACEnforced() {
		return auth.FullDatabaseAccessMode
	}
	if roles == nil {
		return auth.DenyAllDatabaseAccessMode
	}
	if s.allowlistStore != nil {
		return auth.NewAllowlistDatabaseAccessMode(s.allowlistStore.Allowlist(), roles)
	}
	return auth.DenyAllDatabaseAccessMode
}

// GetDatabaseAccessMode returns the server's per-database access mode for a request with no principal (e.g. unauthenticated).
// Prefer getDatabaseAccessMode(claims) or GetDatabaseAccessModeForRoles(roles) when principal is known.
func (s *Server) GetDatabaseAccessMode() auth.DatabaseAccessMode {
	return s.getDatabaseAccessMode(nil)
}

// GetDatabaseManager returns the server's multi-database manager so external
// protocol frontends can route through the same database-resolution path.
func (s *Server) GetDatabaseManager() *multidb.DatabaseManager {
	if s == nil {
		return nil
	}
	return s.dbManager
}

// getResolvedAccess returns per-DB read/write for (claims, dbName). When privilegesStore is set, uses it; else falls back to global PermRead/PermWrite.
func (s *Server) getResolvedAccess(claims *auth.JWTClaims, dbName string) auth.ResolvedAccess {
	if claims == nil {
		return auth.ResolvedAccess{}
	}
	return s.GetResolvedAccessForRoles(claims.Roles, dbName)
}

// withBifrostRBAC enriches the request context with RBAC (principal roles, DatabaseAccessMode, ResolvedAccess)
// so that Bifrost, GraphQL, and plugins can enforce per-database access. Call before passing r to heimdallHandler or graphqlHandler.
func (s *Server) withBifrostRBAC(r *http.Request) *http.Request {
	claims := getClaims(r)
	if claims == nil {
		return r
	}
	ctx := r.Context()
	ctx = auth.WithRequestPrincipalRoles(ctx, claims.Roles)
	ctx = auth.WithRequestDatabaseAccessMode(ctx, s.getDatabaseAccessMode(claims))
	ctx = auth.WithRequestResolvedAccessResolver(ctx, func(dbName string) auth.ResolvedAccess {
		return s.getResolvedAccess(claims, dbName)
	})
	return r.WithContext(ctx)
}

// GetEffectivePermissions returns the union of global entitlement IDs for the given roles.
// Used by Bolt auth adapter so BoltAuthResult.HasPermission uses stored role entitlements.
func (s *Server) GetEffectivePermissions(roles []string) []string {
	if len(roles) == 0 {
		return nil
	}
	if s.roleEntitlementsStore != nil {
		return auth.PermissionsForRoles(roles, s.roleEntitlementsStore)
	}
	// Fallback: static RolePermissions
	var out []string
	seen := make(map[string]struct{})
	for _, role := range roles {
		role = strings.ToLower(strings.TrimSpace(role))
		role = strings.TrimPrefix(role, "role_")
		perms, ok := auth.RolePermissions[auth.Role(role)]
		if !ok {
			continue
		}
		for _, p := range perms {
			id := string(p)
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				out = append(out, id)
			}
		}
	}
	return out
}

// GetResolvedAccessForRoles returns per-DB read/write for (roles, dbName). Used by Bolt for mutation checks.
func (s *Server) GetResolvedAccessForRoles(roles []string, dbName string) auth.ResolvedAccess {
	if len(roles) == 0 {
		return auth.ResolvedAccess{}
	}
	if s.privilegesStore != nil {
		return s.privilegesStore.Resolve(roles, dbName)
	}
	return auth.ResolvedAccess{
		Read:  hasPermission(s, roles, auth.PermRead),
		Write: hasPermission(s, roles, auth.PermWrite),
	}
}

// isCreateDatabaseStatement returns true if the statement is CREATE DATABASE or CREATE COMPOSITE DATABASE.
// Used to trigger auto-grant of access when a new database is created.
func isCreateDatabaseStatement(statement string) bool {
	t := strings.TrimSpace(statement)
	u := strings.ToUpper(t)
	return strings.HasPrefix(u, "CREATE COMPOSITE DATABASE") || strings.HasPrefix(u, "CREATE DATABASE")
}

// parseCreatedDatabaseName extracts the database name from a CREATE DATABASE or CREATE COMPOSITE DATABASE statement.
// Returns (name, true) when the statement matches and name is non-empty; otherwise ("", false).
// Handles "CREATE DATABASE name [IF NOT EXISTS]" and "CREATE COMPOSITE DATABASE name ...".
func parseCreatedDatabaseName(statement string) (dbName string, ok bool) {
	t := strings.TrimSpace(statement)
	if t == "" {
		return "", false
	}
	u := strings.ToUpper(t)
	if strings.HasPrefix(u, "CREATE COMPOSITE DATABASE") {
		// Skip "CREATE COMPOSITE DATABASE" (case-insensitive, flexible whitespace)
		rest := t[len("CREATE COMPOSITE DATABASE"):]
		rest = strings.TrimLeft(rest, " \t\n\r")
		if rest == "" {
			return "", false
		}
		// Name is first token (until whitespace or newline)
		end := 0
		for end < len(rest) && rest[end] != ' ' && rest[end] != '\t' && rest[end] != '\n' && rest[end] != '\r' {
			end++
		}
		name := strings.TrimSpace(rest[:end])
		return name, name != ""
	}
	if strings.HasPrefix(u, "CREATE DATABASE") {
		// Skip "CREATE DATABASE" (case-insensitive, flexible whitespace)
		rest := t[len("CREATE DATABASE"):]
		rest = strings.TrimLeft(rest, " \t\n\r")
		if rest == "" {
			return "", false
		}
		// Name runs until "IF NOT EXISTS" or end; may be backtick-quoted
		restUpper := strings.ToUpper(rest)
		ifIdx := strings.Index(restUpper, "IF NOT EXISTS")
		if ifIdx > 0 {
			rest = strings.TrimSpace(rest[:ifIdx])
		}
		rest = strings.TrimSpace(rest)
		if rest == "" {
			return "", false
		}
		// Unquote backticks if present
		if len(rest) >= 2 && rest[0] == '`' && rest[len(rest)-1] == '`' {
			rest = rest[1 : len(rest)-1]
		}
		return rest, rest != ""
	}
	return "", false
}

func hasPermission(s *Server, roles []string, required auth.Permission) bool {
	// When role entitlements store is set, use it (includes built-in defaults and stored overrides).
	if s != nil && s.roleEntitlementsStore != nil {
		effective := auth.PermissionsForRoles(roles, s.roleEntitlementsStore)
		for _, p := range effective {
			if p == string(required) || p == string(auth.PermAdmin) {
				return true
			}
		}
		return false
	}
	// Fallback: static RolePermissions (no store)
	for _, roleOrPerm := range roles {
		roleOrPerm = strings.ToLower(strings.TrimSpace(roleOrPerm))
		roleOrPerm = strings.TrimPrefix(roleOrPerm, "role_")

		// Allow tokens that encode permissions directly (defensive for interop).
		if auth.Permission(roleOrPerm) == required {
			return true
		}
		if auth.Permission(roleOrPerm) == auth.PermAdmin {
			return true
		}

		role := auth.Role(roleOrPerm)
		perms, ok := auth.RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == auth.PermAdmin || p == required {
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

// isShowDatabasesQuery returns true if the normalized statement is SHOW DATABASES (flexible whitespace).
// Used to filter SHOW DATABASES results by CanSeeDatabase when per-database RBAC is enabled.
func isShowDatabasesQuery(query string) bool {
	// Normalize: collapse whitespace, trim, uppercase (match cypher executor behavior)
	norm := strings.TrimSpace(query)
	norm = strings.Join(strings.Fields(norm), " ")
	return strings.EqualFold(norm, "SHOW DATABASES")
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

func (s *Server) applyMVCCPressureWarnings(w http.ResponseWriter, dbName string, response *TransactionResponse) {
	if s.dbManager == nil || dbName == "" || response == nil {
		return
	}
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return
	}
	lce, ok := storageEngine.(storage.MVCCLifecycleEngine)
	if !ok {
		return
	}
	status := lce.LifecycleStatus()
	rawBand, ok := status["pressure_band"]
	if !ok {
		return
	}
	band := fmt.Sprint(rawBand)
	if band != string(storage.PressureHigh) && band != string(storage.PressureCritical) {
		return
	}
	pinnedBytes := int64FromStatus(status["mvcc_bytes_pinned_by_oldest_reader"])
	oldestAgeSeconds := float64FromStatus(status["mvcc_oldest_reader_age_seconds"])
	description := fmt.Sprintf("MVCC lifecycle pressure is %s on database '%s' (pinned_bytes=%d oldest_reader_age_seconds=%.3f)", band, dbName, pinnedBytes, oldestAgeSeconds)
	response.Notifications = append(response.Notifications, ServerNotification{
		Code:        "NornicDB.MVCC.Pressure",
		Severity:    "WARNING",
		Title:       fmt.Sprintf("MVCC pressure %s", band),
		Description: description,
	})
	if w != nil {
		w.Header().Set("X-NornicDB-MVCC-Pressure", band)
		w.Header().Add("Warning", fmt.Sprintf("299 NornicDB \"%s\"", description))
	}
}

func int64FromStatus(value interface{}) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}

func float64FromStatus(value interface{}) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int64:
		return float64(typed)
	case int:
		return float64(typed)
	default:
		return 0
	}
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
	// Phase 2 D-13: conventional "http" group for request records.
	s.log.Info("http request",
		"subsystem", "http",
		"method", r.Method,
		"path", r.URL.Path,
		"status", status,
		"duration", duration,
	)
}

// logSlowQuery logs queries that exceed the configured threshold.
// Logged info includes: query text (truncated), duration, parameters, error if any.
func (s *Server) logSlowQuery(query string, params map[string]interface{}, duration time.Duration, err error) {
	if !s.config.SlowQueryEnabled {
		return
	}

	if duration < s.config.Logging.SlowQueryThreshold {
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

	// Log to slow query logger (file-backed *log.Logger) if configured;
	// otherwise emit via the structured slog stack tagged event=slow_query
	// per Phase 2 D-04c. The full slow-query observability pipeline (with
	// AST literal redactor + plan_hash) lands in Plan 02-03 (cypher wave).
	if s.slowQueryLogger != nil {
		s.slowQueryLogger.Println(logMsg)
	} else {
		s.log.Warn("slow query", "event", "slow_query", "msg", logMsg)
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

func (s *Server) logMVCCSnapshotExpiration(session *txsession.Session, err error) {
	if s.audit == nil || session == nil || err == nil {
		return
	}
	expirationKind := "graceful"
	if errors.Is(err, storage.ErrMVCCSnapshotHardExpired) {
		expirationKind = "hard"
	}
	s.audit.Log(audit.Event{
		Timestamp:  time.Now(),
		Type:       audit.EventSnapshotExpired,
		UserID:     session.Owner,
		Resource:   "database",
		ResourceID: session.Database,
		Action:     "mvcc_snapshot_expired",
		Success:    false,
		Reason:     err.Error(),
		SessionID:  session.ID,
		Metadata: map[string]string{
			"expiration_kind": expirationKind,
			"database":        session.Database,
		},
	})
}
