// Package mcp provides MCP-specific authentication and authorization.
//
// CRITICAL DESIGN CONSTRAINT: This implementation is 100% STATELESS.
// - JWT tokens only - all auth info comes from the token itself
// - No sessions - no server-side session storage
// - No stored API keys - X-API-Key header accepts JWT tokens
// - No cookies for auth - Bearer header only
//
// Security Model:
//   - Every MCP request contains a JWT in the Authorization header
//   - JWT signature is validated using HMAC-SHA256
//   - Roles and permissions are extracted from JWT claims
//   - Rate limits are per-request throttling (NOT session state)
//   - Audit logs are fire-and-forget (NOT session tracking)
//
// Role Hierarchy:
//   - super_admin: Full access, all tools, user management, audit access
//   - org_admin: Organization-level admin, most tools, can manage org users
//   - org_developer: Read/write access, most tools except admin
//   - org_viewer: Read-only access, recall/discover/tasks only
//   - llm_agent: Automated access, all graph tools, rate limited
//   - service_account: System integration, specific tool access
//
// Compliance:
//   - GDPR Art.32: Security of processing (stateless JWT)
//   - HIPAA §164.312: Access controls (RBAC from JWT claims)
//   - SOC 2 CC6.1: Logical access controls (role-based permissions)
//   - FISMA AC-2: Account management (audit logging)
package mcp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// ============================================================================
// MCP Roles and Permissions
// ============================================================================

// MCPRole represents an MCP-specific role with tool access permissions.
type MCPRole string

const (
	// RoleSuperAdmin has full system access including user management and audit
	RoleSuperAdmin MCPRole = "super_admin"
	// RoleOrgAdmin has organization-level admin access
	RoleOrgAdmin MCPRole = "org_admin"
	// RoleOrgDeveloper has read/write access to graph data
	RoleOrgDeveloper MCPRole = "org_developer"
	// RoleOrgViewer has read-only access
	RoleOrgViewer MCPRole = "org_viewer"
	// RoleLLMAgent is for automated LLM/agent access with higher rate limits
	RoleLLMAgent MCPRole = "llm_agent"
	// RoleServiceAccount is for system integrations
	RoleServiceAccount MCPRole = "service_account"
)

// MCPPermission represents a permission for MCP operations.
type MCPPermission string

const (
	// PermissionStore allows storing new memories
	PermissionStore MCPPermission = "store"
	// PermissionRecall allows retrieving memories
	PermissionRecall MCPPermission = "recall"
	// PermissionDiscover allows semantic search
	PermissionDiscover MCPPermission = "discover"
	// PermissionLink allows creating relationships
	PermissionLink MCPPermission = "link"
	// PermissionIndex allows indexing files
	PermissionIndex MCPPermission = "index"
	// PermissionUnindex allows removing indexed files
	PermissionUnindex MCPPermission = "unindex"
	// PermissionTask allows task management
	PermissionTask MCPPermission = "task"
	// PermissionTasks allows listing tasks
	PermissionTasks MCPPermission = "tasks"
	// PermissionAdmin allows admin operations
	PermissionAdmin MCPPermission = "admin"
	// PermissionAudit allows viewing audit logs
	PermissionAudit MCPPermission = "audit"
)

// AllMCPPermissions returns all available MCP permissions.
func AllMCPPermissions() []MCPPermission {
	return []MCPPermission{
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionIndex, PermissionUnindex,
		PermissionTask, PermissionTasks, PermissionAdmin, PermissionAudit,
	}
}

// MCPRolePermissions maps each MCP role to its allowed permissions.
var MCPRolePermissions = map[MCPRole][]MCPPermission{
	RoleSuperAdmin: {
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionIndex, PermissionUnindex,
		PermissionTask, PermissionTasks, PermissionAdmin, PermissionAudit,
	},
	RoleOrgAdmin: {
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionIndex, PermissionUnindex,
		PermissionTask, PermissionTasks, PermissionAdmin,
	},
	RoleOrgDeveloper: {
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionIndex, PermissionUnindex,
		PermissionTask, PermissionTasks,
	},
	RoleOrgViewer: {
		PermissionRecall, PermissionDiscover, PermissionTasks,
	},
	RoleLLMAgent: {
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionTask, PermissionTasks,
	},
	RoleServiceAccount: {
		PermissionStore, PermissionRecall, PermissionDiscover,
		PermissionLink, PermissionIndex, PermissionUnindex,
	},
}

// ToolPermissions maps each MCP tool to its required permission.
var ToolPermissions = map[string]MCPPermission{
	ToolStore:    PermissionStore,
	ToolRecall:   PermissionRecall,
	ToolDiscover: PermissionDiscover,
	ToolLink:     PermissionLink,
	ToolIndex:    PermissionIndex,
	ToolUnindex:  PermissionUnindex,
	ToolTask:     PermissionTask,
	ToolTasks:    PermissionTasks,
}

// RoleFromString converts a string to an MCPRole.
func RoleFromString(s string) (MCPRole, error) {
	switch s {
	case string(RoleSuperAdmin):
		return RoleSuperAdmin, nil
	case string(RoleOrgAdmin):
		return RoleOrgAdmin, nil
	case string(RoleOrgDeveloper):
		return RoleOrgDeveloper, nil
	case string(RoleOrgViewer):
		return RoleOrgViewer, nil
	case string(RoleLLMAgent):
		return RoleLLMAgent, nil
	case string(RoleServiceAccount):
		return RoleServiceAccount, nil
	// Map legacy roles to MCP roles
	case "admin":
		return RoleOrgAdmin, nil
	case "editor":
		return RoleOrgDeveloper, nil
	case "viewer":
		return RoleOrgViewer, nil
	default:
		return "", fmt.Errorf("unknown MCP role: %s", s)
	}
}

// HasPermission checks if a role has a specific permission.
func HasPermission(role MCPRole, perm MCPPermission) bool {
	perms, ok := MCPRolePermissions[role]
	if !ok {
		return false
	}
	for _, p := range perms {
		if p == perm {
			return true
		}
	}
	return false
}

// CanUseTool checks if a role can use a specific MCP tool.
func CanUseTool(role MCPRole, tool string) bool {
	perm, ok := ToolPermissions[tool]
	if !ok {
		return false // Unknown tool
	}
	return HasPermission(role, perm)
}

// ============================================================================
// Rate Limiting (per-request throttling, NOT session state)
// ============================================================================

// RateLimiter provides per-user rate limiting using in-memory counters.
// This is NOT session state - it's per-request throttling to prevent abuse.
type RateLimiter struct {
	mu       sync.RWMutex
	limits   map[MCPRole]RateLimit
	counters map[string]*rateLimitCounter
}

// RateLimit defines rate limit configuration.
type RateLimit struct {
	RequestsPerMinute int
	RequestsPerHour   int
	BurstSize         int
}

// DefaultRateLimits returns default rate limits per role.
var DefaultRateLimits = map[MCPRole]RateLimit{
	RoleSuperAdmin:     {RequestsPerMinute: 1000, RequestsPerHour: 50000, BurstSize: 100},
	RoleOrgAdmin:       {RequestsPerMinute: 500, RequestsPerHour: 25000, BurstSize: 50},
	RoleOrgDeveloper:   {RequestsPerMinute: 200, RequestsPerHour: 10000, BurstSize: 30},
	RoleOrgViewer:      {RequestsPerMinute: 100, RequestsPerHour: 5000, BurstSize: 20},
	RoleLLMAgent:       {RequestsPerMinute: 500, RequestsPerHour: 30000, BurstSize: 50},
	RoleServiceAccount: {RequestsPerMinute: 300, RequestsPerHour: 15000, BurstSize: 40},
}

type rateLimitCounter struct {
	mu          sync.Mutex
	minuteCount int
	hourCount   int
	minuteReset time.Time
	hourReset   time.Time
	lastRequest time.Time
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter() *RateLimiter {
	return &RateLimiter{
		limits:   DefaultRateLimits,
		counters: make(map[string]*rateLimitCounter),
	}
}

// SetLimits sets custom rate limits for a role.
func (r *RateLimiter) SetLimits(role MCPRole, limit RateLimit) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.limits[role] = limit
}

// Allow checks if a request is allowed and increments counters.
func (r *RateLimiter) Allow(userID string, role MCPRole) (bool, error) {
	r.mu.Lock()
	limit, ok := r.limits[role]
	if !ok {
		limit = RateLimit{RequestsPerMinute: 60, RequestsPerHour: 3600, BurstSize: 10}
	}
	r.mu.Unlock()

	r.mu.Lock()
	counter, exists := r.counters[userID]
	if !exists {
		counter = &rateLimitCounter{
			minuteReset: time.Now().Add(time.Minute),
			hourReset:   time.Now().Add(time.Hour),
		}
		r.counters[userID] = counter
	}
	r.mu.Unlock()

	counter.mu.Lock()
	defer counter.mu.Unlock()

	now := time.Now()

	// Reset minute counter if needed
	if now.After(counter.minuteReset) {
		counter.minuteCount = 0
		counter.minuteReset = now.Add(time.Minute)
	}

	// Reset hour counter if needed
	if now.After(counter.hourReset) {
		counter.hourCount = 0
		counter.hourReset = now.Add(time.Hour)
	}

	// Check limits
	if counter.minuteCount >= limit.RequestsPerMinute {
		return false, fmt.Errorf("rate limit exceeded: %d requests per minute", limit.RequestsPerMinute)
	}
	if counter.hourCount >= limit.RequestsPerHour {
		return false, fmt.Errorf("rate limit exceeded: %d requests per hour", limit.RequestsPerHour)
	}

	// Increment counters
	counter.minuteCount++
	counter.hourCount++
	counter.lastRequest = now

	return true, nil
}

// GetStats returns rate limit statistics for a user.
func (r *RateLimiter) GetStats(userID string) map[string]interface{} {
	r.mu.RLock()
	counter, exists := r.counters[userID]
	r.mu.RUnlock()

	if !exists {
		return map[string]interface{}{
			"minute_count": 0,
			"hour_count":   0,
		}
	}

	counter.mu.Lock()
	defer counter.mu.Unlock()

	return map[string]interface{}{
		"minute_count": counter.minuteCount,
		"hour_count":   counter.hourCount,
		"minute_reset": counter.minuteReset,
		"hour_reset":   counter.hourReset,
		"last_request": counter.lastRequest,
	}
}

// ============================================================================
// Audit Logging (fire-and-forget, NOT session tracking)
// ============================================================================

// AuditSink defines an interface for audit log destinations.
type AuditSink interface {
	Log(event MCPAuditEvent) error
}

// MCPAuditEvent represents an MCP-specific audit event.
type MCPAuditEvent struct {
	Timestamp    time.Time              `json:"timestamp"`
	RequestID    string                 `json:"request_id"`
	UserID       string                 `json:"user_id"`
	Username     string                 `json:"username,omitempty"`
	Role         string                 `json:"role"`
	OrgID        string                 `json:"org_id,omitempty"`
	Tool         string                 `json:"tool"`
	Operation    string                 `json:"operation"`
	ResourceType string                 `json:"resource_type,omitempty"`
	ResourceID   string                 `json:"resource_id,omitempty"`
	IPAddress    string                 `json:"ip_address,omitempty"`
	UserAgent    string                 `json:"user_agent,omitempty"`
	Success      bool                   `json:"success"`
	ErrorMessage string                 `json:"error_message,omitempty"`
	Duration     time.Duration          `json:"duration"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// AuditLogger manages multi-sink audit logging.
type AuditLogger struct {
	mu    sync.RWMutex
	sinks []AuditSink
}

// NewAuditLogger creates a new audit logger.
func NewAuditLogger() *AuditLogger {
	return &AuditLogger{
		sinks: make([]AuditSink, 0),
	}
}

// AddSink adds an audit sink.
func (a *AuditLogger) AddSink(sink AuditSink) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.sinks = append(a.sinks, sink)
}

// Log logs an event to all sinks asynchronously (fire-and-forget).
func (a *AuditLogger) Log(event MCPAuditEvent) {
	a.mu.RLock()
	sinks := a.sinks
	a.mu.RUnlock()

	for _, sink := range sinks {
		go func(s AuditSink) {
			_ = s.Log(event) // Best effort, non-blocking
		}(sink)
	}
}

// ConsoleSink is an audit sink that logs to console.
type ConsoleSink struct{}

// Log implements AuditSink.
func (c *ConsoleSink) Log(event MCPAuditEvent) error {
	status := "✓"
	if !event.Success {
		status = "✗"
	}
	fmt.Printf("[AUDIT] %s %s tool=%s user=%s resource=%s duration=%v\n",
		status, event.Operation, event.Tool, event.UserID, event.ResourceID, event.Duration)
	return nil
}

// ============================================================================
// Authentication Middleware (100% STATELESS)
// ============================================================================

// AuthMiddleware provides MCP authentication and authorization.
// This is 100% STATELESS - all auth info comes from JWT claims.
// Uses the existing auth.Authenticator for JWT validation.
type AuthMiddleware struct {
	authenticator *auth.Authenticator
	rateLimiter   *RateLimiter
	auditLogger   *AuditLogger
	config        AuthConfig
}

// AuthConfig holds authentication configuration.
type AuthConfig struct {
	// RequireAuth enables authentication (default: true)
	RequireAuth bool
	// AllowAnonymous allows unauthenticated read-only access
	AllowAnonymous bool
	// SecurityEnabled enables/disables security (false = development mode)
	SecurityEnabled bool
	// AuditEnabled enables audit logging
	AuditEnabled bool
	// RateLimitEnabled enables rate limiting
	RateLimitEnabled bool
}

// DefaultAuthConfig returns default auth configuration.
func DefaultAuthConfig() AuthConfig {
	return AuthConfig{
		RequireAuth:      true,
		AllowAnonymous:   false,
		SecurityEnabled:  true,
		AuditEnabled:     true,
		RateLimitEnabled: true,
	}
}

// NewAuthMiddleware creates a new auth middleware using the existing auth.Authenticator.
// The authenticator handles JWT validation and provides the SecurityEnabled flag.
func NewAuthMiddleware(authenticator *auth.Authenticator, config AuthConfig) *AuthMiddleware {
	return &AuthMiddleware{
		authenticator: authenticator,
		rateLimiter:   NewRateLimiter(),
		auditLogger:   NewAuditLogger(),
		config:        config,
	}
}

// SetAuditLogger sets the audit logger.
func (m *AuthMiddleware) SetAuditLogger(logger *AuditLogger) {
	m.auditLogger = logger
}

// AuthContext holds authentication context for a request.
// This is populated FROM JWT CLAIMS ONLY - nothing is stored server-side.
type AuthContext struct {
	UserID    string
	Username  string
	Email     string
	Roles     []MCPRole
	OrgID     string
	Workspace string
	Claims    *auth.JWTClaims // Uses existing auth package claims
	Timestamp time.Time
}

// contextKey is a custom type for context keys.
type contextKey string

const authContextKey contextKey = "mcp_auth_context"

// GetAuthContext retrieves the auth context from request context.
func GetAuthContext(ctx context.Context) (*AuthContext, bool) {
	ac, ok := ctx.Value(authContextKey).(*AuthContext)
	return ac, ok
}

// isSecurityEnabled checks if security is enabled.
// Respects the existing auth.Authenticator's flag if available.
func (m *AuthMiddleware) isSecurityEnabled() bool {
	// If we have an authenticator, respect its security flag
	if m.authenticator != nil {
		return m.authenticator.IsSecurityEnabled()
	}
	// Otherwise use our own config
	return m.config.SecurityEnabled
}

// Middleware returns an HTTP middleware that authenticates requests.
// This is 100% STATELESS - validates JWT signature, extracts claims.
func (m *AuthMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip auth for health checks
		if r.URL.Path == "/health" || r.URL.Path == "/mcp/health" {
			next.ServeHTTP(w, r)
			return
		}

		// Security disabled mode (development only)
		// Respects the existing auth.Authenticator's SecurityEnabled flag
		if !m.isSecurityEnabled() {
			authCtx := &AuthContext{
				UserID:    "anonymous",
				Username:  "anonymous",
				Roles:     []MCPRole{RoleSuperAdmin}, // Full access when security disabled
				Timestamp: time.Now(),
			}
			ctx := context.WithValue(r.Context(), authContextKey, authCtx)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Extract token from Authorization header (PRIMARY)
		token := m.extractToken(r)

		// Allow anonymous for read-only if configured
		if token == "" && m.config.AllowAnonymous {
			authCtx := &AuthContext{
				UserID:    "anonymous",
				Username:  "anonymous",
				Roles:     []MCPRole{RoleOrgViewer}, // Read-only
				Timestamp: time.Now(),
			}
			ctx := context.WithValue(r.Context(), authContextKey, authCtx)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Token required
		if token == "" && m.config.RequireAuth {
			http.Error(w, `{"error":"authentication required"}`, http.StatusUnauthorized)
			return
		}

		// Validate JWT signature and expiration - NO DATABASE LOOKUP
		claims, err := m.validateJWT(token)
		if err != nil {
			http.Error(w, `{"error":"invalid or expired token"}`, http.StatusUnauthorized)
			return
		}

		// Build auth context FROM JWT CLAIMS ONLY
		authCtx := m.buildAuthContext(claims)

		// Rate limiting (per-request throttling, not session state)
		if m.config.RateLimitEnabled && len(authCtx.Roles) > 0 {
			allowed, err := m.rateLimiter.Allow(authCtx.UserID, authCtx.Roles[0])
			if !allowed {
				http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusTooManyRequests)
				return
			}
		}

		ctx := context.WithValue(r.Context(), authContextKey, authCtx)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// extractToken extracts token from request headers.
// Priority: Authorization Bearer > X-API-Key > Query param (for SSE only)
// NOTE: No cookie extraction - this is stateless.
func (m *AuthMiddleware) extractToken(r *http.Request) string {
	// 1. Authorization: Bearer header (PRIMARY - OAuth 2.0 standard)
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		return strings.TrimPrefix(authHeader, "Bearer ")
	}

	// 2. X-API-Key header (token is still JWT, not a stored key)
	apiKey := r.Header.Get("X-API-Key")
	if apiKey != "" {
		return apiKey
	}

	// 3. Query parameter (ONLY for SSE/WebSocket connections that can't send headers)
	if token := r.URL.Query().Get("token"); token != "" {
		return token
	}

	return ""
}

// validateJWT validates a JWT token using the existing auth.Authenticator.
// This is STATELESS - the authenticator only validates signature and expiration.
func (m *AuthMiddleware) validateJWT(token string) (*auth.JWTClaims, error) {
	if m.authenticator == nil {
		return nil, fmt.Errorf("authenticator not configured")
	}
	return m.authenticator.ValidateToken(token)
}

// buildAuthContext creates auth context FROM JWT CLAIMS ONLY.
// Uses the existing auth.JWTClaims from the authenticator.
func (m *AuthMiddleware) buildAuthContext(claims *auth.JWTClaims) *AuthContext {
	// Map roles from JWT claims to MCPRole
	var mcpRoles []MCPRole
	for _, roleStr := range claims.Roles {
		if role, err := RoleFromString(roleStr); err == nil {
			mcpRoles = append(mcpRoles, role)
		}
	}

	// Default to viewer if no valid roles
	if len(mcpRoles) == 0 {
		mcpRoles = []MCPRole{RoleOrgViewer}
	}

	return &AuthContext{
		UserID:    claims.Sub,
		Username:  claims.Username,
		Email:     claims.Email,
		Roles:     mcpRoles,
		Claims:    claims,
		Timestamp: time.Now(),
	}
}

// CheckToolAccess verifies if the current user can use a tool.
// Uses ONLY data from JWT claims.
func (m *AuthMiddleware) CheckToolAccess(ctx context.Context, tool string) error {
	authCtx, ok := GetAuthContext(ctx)
	if !ok {
		return fmt.Errorf("no authentication context")
	}

	// Check if any role can use the tool
	for _, role := range authCtx.Roles {
		if CanUseTool(role, tool) {
			return nil
		}
	}

	return fmt.Errorf("permission denied: role(s) %v cannot use tool %s", authCtx.Roles, tool)
}

// LogToolCall logs a tool call for audit (fire-and-forget).
func (m *AuthMiddleware) LogToolCall(ctx context.Context, tool string, operation string, resourceType string, resourceID string, success bool, errMsg string, duration time.Duration, metadata map[string]interface{}) {
	if !m.config.AuditEnabled || m.auditLogger == nil {
		return
	}

	authCtx, _ := GetAuthContext(ctx)
	userID := "anonymous"
	username := "anonymous"
	role := "unknown"
	orgID := ""
	if authCtx != nil {
		userID = authCtx.UserID
		username = authCtx.Username
		orgID = authCtx.OrgID
		if len(authCtx.Roles) > 0 {
			role = string(authCtx.Roles[0])
		}
	}

	event := MCPAuditEvent{
		Timestamp:    time.Now(),
		RequestID:    generateRequestID(),
		UserID:       userID,
		Username:     username,
		Role:         role,
		OrgID:        orgID,
		Tool:         tool,
		Operation:    operation,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Success:      success,
		ErrorMessage: errMsg,
		Duration:     duration,
		Metadata:     metadata,
	}

	// Fire-and-forget (async, non-blocking)
	m.auditLogger.Log(event)
}

// generateRequestID generates a unique request ID.
func generateRequestID() string {
	return fmt.Sprintf("req-%d", time.Now().UnixNano())
}
