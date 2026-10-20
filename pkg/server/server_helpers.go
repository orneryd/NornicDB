package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/audit"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
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
	raw := requestHost(r)
	if host, _, err := net.SplitHostPort(raw); err == nil {
		return host
	}
	return raw
}

// requestHost is the host (with port, if any) the client used to reach the
// server: the first X-Forwarded-Host when present, else the Host header.
// Forwarded headers only survive from trusted proxies (trustedProxyMiddleware
// strips them otherwise). URLs handed back to clients (discovery, OAuth,
// transaction URLs) are built from it so they work from the client's side.
func requestHost(r *http.Request) string {
	if r == nil {
		return ""
	}
	for _, raw := range []string{r.Header.Get("X-Forwarded-Host"), r.Host} {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		// X-Forwarded-Host may contain multiple comma-separated names; take the first.
		if idx := strings.Index(raw, ","); idx >= 0 {
			raw = strings.TrimSpace(raw[:idx])
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
	return !auth.AuthenticationDisabled(s.auth)
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

// getResolvedAccess is GetResolvedAccessForRoles for claims (none: no access).
func (s *Server) getResolvedAccess(claims *auth.JWTClaims, dbName string) auth.ResolvedAccess {
	if claims == nil {
		return auth.ResolvedAccess{}
	}
	return s.GetResolvedAccessForRoles(claims.Roles, dbName)
}

// databaseReadAccess is the one check of whether the principal may read a
// database through a route that reads it directly (search, similarity, the
// graph routes): accessible is whether the database allowlist lets it use the
// database at all, readable whether it may also read it (the per-database
// read privilege of a signed-in principal; a request without claims passes
// the allowlist only when authentication is disabled).
func (s *Server) databaseReadAccess(claims *auth.JWTClaims, dbName string) (accessible, readable bool) {
	if !s.getDatabaseAccessMode(claims).CanAccessDatabase(dbName) {
		return false, false
	}
	return true, claims == nil || s.getResolvedAccess(claims, dbName).Read
}

// authorizeDatabaseRead refuses, with Neo4j's Forbidden status, a request
// whose principal may not read dbName (databaseReadAccess), and reports
// whether the request may go on.
func (s *Server) authorizeDatabaseRead(w http.ResponseWriter, r *http.Request, dbName string) bool {
	if _, readable := s.databaseReadAccess(getClaims(r), dbName); !readable {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return false
	}
	return true
}

// withBifrostRBAC enriches the request context with RBAC (principal roles, DatabaseAccessMode, ResolvedAccess)
// so that Bifrost, GraphQL, and plugins can enforce per-database access. Call before passing r to heimdallHandler or graphqlHandler.
func (s *Server) withBifrostRBAC(r *http.Request) *http.Request {
	claims := getClaims(r)
	// Native MCP tools also run in process with this context. Keep native
	// identity and continuation ownership with the same request principal.
	ctx := s.withRequestIdentity(r.Context(), r, claims)
	ctx = cypher.WithAuthenticatedPrincipal(ctx, transactionOwnerKey(r, claims))
	if !s.isRBACEnforced() {
		ctx = cypher.WithAuthenticationDisabled(ctx)
	}
	r = r.WithContext(ctx)
	if claims == nil {
		if !s.isRBACEnforced() {
			return r.WithContext(auth.WithRequestAuthenticationDisabled(r.Context()))
		}
		return r
	}
	ctx = r.Context()
	databaseAccessMode := s.getDatabaseAccessMode(claims)
	ctx = auth.WithRequestPrincipalRoles(ctx, claims.Roles)
	ctx = auth.WithRequestDatabaseAccessMode(ctx, databaseAccessMode)
	// The principal's one permission resolver (principalAccess), which
	// HTTP and Bolt use too, and its boosted procedure grants: GraphQL,
	// Heimdall, MCP and plugins authorize through them.
	access := s.principalAccess(claims)
	ctx = auth.WithRequestEntitlementResolver(ctx, func(database string, permission auth.Permission) bool {
		return access.Allows(database, string(permission))
	})
	ctx = cypher.WithProcedureGrantChecker(ctx, s.ProcedureGrantChecker(claims.Roles))
	selections := make(map[string]string)
	defaultDatabase := ""
	if s.dbManager != nil {
		for _, database := range s.dbManager.ListDatabases() {
			if database == nil || database.Type == "system" || !databaseAccessMode.CanAccessDatabase(database.Name) {
				continue
			}
			selections[database.Name] = database.Name
			for _, alias := range database.Aliases {
				selections[alias] = database.Name
			}
		}
		configuredDefault := s.dbManager.DefaultDatabaseName()
		if _, allowed := selections[configuredDefault]; allowed {
			defaultDatabase = configuredDefault
		}
	}
	ctx = auth.WithRequestDatabaseScope(ctx, auth.NewRequestDatabaseScope(defaultDatabase, selections))
	return r.WithContext(ctx)
}

// GetEffectivePermissions returns the union of global entitlement IDs for the given roles.
// Used by Bolt auth adapter so BoltAuthResult.HasPermission uses stored role entitlements.
func (s *Server) GetEffectivePermissions(roles []string) []string {
	if len(roles) == 0 {
		return nil
	}
	// Without a store, the built-in roles' entitlements.
	return auth.PermissionsForRoles(roles, s.roleEntitlementsStore)
}

// GetResolvedAccessForRoles is the principal's read and write on dbName from
// the one resolver (auth.ResolveDatabaseAccess) over the server's privileges
// and role entitlements. HTTP (principalAccess), Bolt and gRPC use it.
func (s *Server) GetResolvedAccessForRoles(roles []string, dbName string) auth.ResolvedAccess {
	return auth.ResolveDatabaseAccess(roles, dbName, s.privilegesStore, s.roleEntitlementsStore)
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

// hasPermission reports a global entitlement of roles, through the one
// entitlement check (auth.EntitlementsAllow): the role entitlements store
// (stored overrides and the built-in roles' defaults), or without one the
// built-in roles' defaults.
func hasPermission(s *Server, roles []string, required auth.Permission) bool {
	var store *auth.RoleEntitlementsStore
	if s != nil {
		store = s.roleEntitlementsStore
	}
	return auth.EntitlementsAllow(auth.PermissionsForRoles(roles, store), string(required))
}

// principalAccess is the principal's permission resolver
// (cypher.PrincipalAccess), the one Bolt builds too: graph access from the
// database allowlist, read and write from the database privileges, and the
// other permissions from the role entitlements. Without RBAC every graph the
// access mode allows is open for every permission.
func (s *Server) principalAccess(claims *auth.JWTClaims) cypher.PrincipalAccess {
	mode := s.getDatabaseAccessMode(claims)
	access := cypher.PrincipalAccess{
		GraphAccess: func(graph string) (string, bool) { return s.graphAccess(mode, graph) },
	}
	if !s.isRBACEnforced() {
		return access
	}
	access.DatabaseAccess = func(database string) auth.ResolvedAccess { return s.getResolvedAccess(claims, database) }
	access.Entitlement = func(permission string) bool {
		return claims != nil && hasPermission(s, claims.Roles, auth.Permission(permission))
	}
	return access
}

// ProcedureGrantChecker reports, for a principal's roles, whether they hold
// EXECUTE BOOSTED PROCEDURE for a procedure; nil when there is no grant
// store. When the grants can't be read it denies, and logs the error.
func (s *Server) ProcedureGrantChecker(roles []string) func(procedure string) bool {
	if s == nil || s.procedurePrivileges == nil || len(roles) == 0 {
		return nil
	}
	store := s.procedurePrivileges
	return func(procedure string) bool {
		allowed, err := store.AllowsBoosted(roles, procedure)
		if err != nil {
			s.logEvent(context.Background(), slog.LevelWarn, localization.ServerRBACPrivilegesLoadFailedEvent(err))
		}
		return allowed
	}
}

func isMutationQuery(query string) bool {
	requirements := cypher.QueryPermissionRequirements(query)
	return requirements.Write || requirements.Schema
}

// canAccessGraph reports whether the principal may use graph, a request's
// database: a database, an alias, or a composite constituent, which needs
// access to its composite and its database (graphAccess), as the executor
// checks a USE of the same name.
func (s *Server) canAccessGraph(claims *auth.JWTClaims, graph string) bool {
	_, allowed := s.graphAccess(s.getDatabaseAccessMode(claims), graph)
	return allowed
}

// graphAccess reports whether mode allows graph and the database its data
// is in (cypher.GraphAccess). A nil mode allows every database.
func (s *Server) graphAccess(mode auth.DatabaseAccessMode, graph string) (dataDatabase string, allowed bool) {
	var manager cypher.DatabaseManagerInterface
	if s.dbManager != nil {
		manager = &databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s}
	}
	var canAccess func(string) bool
	if mode != nil {
		canAccess = mode.CanAccessDatabase
	}
	return cypher.GraphAccess(manager, graph, canAccess)
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
	if stream, ok := w.(*transactionRowStream); ok {
		stream.writeJSON(status, v)
		return
	}
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
	s.logEvent(r.Context(), slog.LevelInfo, localization.ServerHTTPRequestEvent(r.Method, r.URL.Path, status, duration))
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

	// Build log message
	status := "OK"
	if err != nil {
		status = "ERROR"
	}

	logMsg := fmt.Sprintf("[SLOW QUERY] duration=%v status=%s query_len=%d param_count=%d",
		duration, status, len(query), len(params))

	// Log to slow query logger (file-backed *log.Logger) if configured;
	// otherwise emit via the structured slog stack tagged event=slow_query
	// per Phase 2 D-04c. The full slow-query observability pipeline (with
	// AST literal redactor + plan_hash) lands in Plan 02-03 (cypher wave).
	if s.slowQueryLogger != nil {
		s.slowQueryLogger.Println(logMsg)
	} else {
		s.logEvent(context.Background(), slog.LevelWarn, localization.ServerSlowQueryEvent(logMsg))
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
