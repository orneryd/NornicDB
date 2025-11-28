package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// =============================================================================
// Role and Permission Tests
// =============================================================================

func TestRoleFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected MCPRole
		wantErr  bool
	}{
		{"super_admin", RoleSuperAdmin, false},
		{"org_admin", RoleOrgAdmin, false},
		{"org_developer", RoleOrgDeveloper, false},
		{"org_viewer", RoleOrgViewer, false},
		{"llm_agent", RoleLLMAgent, false},
		{"service_account", RoleServiceAccount, false},
		// Legacy mappings
		{"admin", RoleOrgAdmin, false},
		{"editor", RoleOrgDeveloper, false},
		{"viewer", RoleOrgViewer, false},
		// Invalid
		{"invalid_role", "", true},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := RoleFromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("RoleFromString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.expected {
				t.Errorf("RoleFromString(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestHasPermission(t *testing.T) {
	tests := []struct {
		role     MCPRole
		perm     MCPPermission
		expected bool
	}{
		// SuperAdmin has everything
		{RoleSuperAdmin, PermissionStore, true},
		{RoleSuperAdmin, PermissionAudit, true},
		{RoleSuperAdmin, PermissionAdmin, true},

		// OrgAdmin has most but not audit
		{RoleOrgAdmin, PermissionStore, true},
		{RoleOrgAdmin, PermissionAdmin, true},
		{RoleOrgAdmin, PermissionAudit, false},

		// OrgDeveloper has standard ops but not admin
		{RoleOrgDeveloper, PermissionStore, true},
		{RoleOrgDeveloper, PermissionRecall, true},
		{RoleOrgDeveloper, PermissionAdmin, false},
		{RoleOrgDeveloper, PermissionAudit, false},

		// OrgViewer is read-only
		{RoleOrgViewer, PermissionRecall, true},
		{RoleOrgViewer, PermissionDiscover, true},
		{RoleOrgViewer, PermissionTasks, true},
		{RoleOrgViewer, PermissionStore, false},
		{RoleOrgViewer, PermissionLink, false},

		// LLMAgent has graph tools but not file indexing
		{RoleLLMAgent, PermissionStore, true},
		{RoleLLMAgent, PermissionRecall, true},
		{RoleLLMAgent, PermissionIndex, false},
		{RoleLLMAgent, PermissionUnindex, false},

		// ServiceAccount has specific tools
		{RoleServiceAccount, PermissionStore, true},
		{RoleServiceAccount, PermissionIndex, true},
		{RoleServiceAccount, PermissionAdmin, false},

		// Unknown role has nothing
		{"unknown", PermissionStore, false},
	}

	for _, tt := range tests {
		name := string(tt.role) + "_" + string(tt.perm)
		t.Run(name, func(t *testing.T) {
			got := HasPermission(tt.role, tt.perm)
			if got != tt.expected {
				t.Errorf("HasPermission(%q, %q) = %v, want %v", tt.role, tt.perm, got, tt.expected)
			}
		})
	}
}

func TestCanUseTool(t *testing.T) {
	tests := []struct {
		role     MCPRole
		tool     string
		expected bool
	}{
		// SuperAdmin can use all tools
		{RoleSuperAdmin, ToolStore, true},
		{RoleSuperAdmin, ToolRecall, true},
		{RoleSuperAdmin, ToolDiscover, true},
		{RoleSuperAdmin, ToolLink, true},
		{RoleSuperAdmin, ToolIndex, true},
		{RoleSuperAdmin, ToolUnindex, true},
		{RoleSuperAdmin, ToolTask, true},
		{RoleSuperAdmin, ToolTasks, true},

		// OrgViewer can only use read tools
		{RoleOrgViewer, ToolRecall, true},
		{RoleOrgViewer, ToolDiscover, true},
		{RoleOrgViewer, ToolTasks, true},
		{RoleOrgViewer, ToolStore, false},
		{RoleOrgViewer, ToolLink, false},
		{RoleOrgViewer, ToolIndex, false},

		// LLMAgent can't index files
		{RoleLLMAgent, ToolStore, true},
		{RoleLLMAgent, ToolIndex, false},
		{RoleLLMAgent, ToolUnindex, false},

		// Unknown tool returns false
		{RoleSuperAdmin, "unknown_tool", false},
	}

	for _, tt := range tests {
		name := string(tt.role) + "_" + tt.tool
		t.Run(name, func(t *testing.T) {
			got := CanUseTool(tt.role, tt.tool)
			if got != tt.expected {
				t.Errorf("CanUseTool(%q, %q) = %v, want %v", tt.role, tt.tool, got, tt.expected)
			}
		})
	}
}

func TestAllMCPPermissions(t *testing.T) {
	perms := AllMCPPermissions()
	if len(perms) != 10 {
		t.Errorf("AllMCPPermissions() returned %d permissions, want 10", len(perms))
	}

	// Check all expected permissions are present
	expected := map[MCPPermission]bool{
		PermissionStore:    true,
		PermissionRecall:   true,
		PermissionDiscover: true,
		PermissionLink:     true,
		PermissionIndex:    true,
		PermissionUnindex:  true,
		PermissionTask:     true,
		PermissionTasks:    true,
		PermissionAdmin:    true,
		PermissionAudit:    true,
	}

	for _, p := range perms {
		if !expected[p] {
			t.Errorf("Unexpected permission: %s", p)
		}
	}
}

// =============================================================================
// Rate Limiter Tests
// =============================================================================

func TestNewRateLimiter(t *testing.T) {
	rl := NewRateLimiter()
	if rl == nil {
		t.Fatal("NewRateLimiter() returned nil")
	}
	if rl.limits == nil {
		t.Error("RateLimiter.limits is nil")
	}
	if rl.counters == nil {
		t.Error("RateLimiter.counters is nil")
	}
}

func TestRateLimiterAllow(t *testing.T) {
	rl := NewRateLimiter()

	// Should allow first request
	allowed, err := rl.Allow("user1", RoleOrgDeveloper)
	if !allowed || err != nil {
		t.Errorf("First request should be allowed: allowed=%v, err=%v", allowed, err)
	}

	// Should track counter
	stats := rl.GetStats("user1")
	if stats["minute_count"].(int) != 1 {
		t.Errorf("Expected minute_count=1, got %v", stats["minute_count"])
	}
}

func TestRateLimiterExceedsLimit(t *testing.T) {
	rl := NewRateLimiter()

	// Set very low limit
	rl.SetLimits(RoleOrgViewer, RateLimit{
		RequestsPerMinute: 2,
		RequestsPerHour:   10,
		BurstSize:         2,
	})

	// First two requests should pass
	rl.Allow("user1", RoleOrgViewer)
	rl.Allow("user1", RoleOrgViewer)

	// Third should fail
	allowed, err := rl.Allow("user1", RoleOrgViewer)
	if allowed {
		t.Error("Third request should be rate limited")
	}
	if err == nil {
		t.Error("Expected rate limit error")
	}
}

func TestRateLimiterDefaultForUnknownRole(t *testing.T) {
	rl := NewRateLimiter()

	// Unknown role should get default limits
	allowed, err := rl.Allow("user1", "unknown_role")
	if !allowed || err != nil {
		t.Errorf("Unknown role should use defaults: allowed=%v, err=%v", allowed, err)
	}
}

func TestRateLimiterGetStatsForUnknownUser(t *testing.T) {
	rl := NewRateLimiter()

	stats := rl.GetStats("unknown_user")
	if stats["minute_count"].(int) != 0 {
		t.Errorf("Unknown user should have 0 minute_count, got %v", stats["minute_count"])
	}
	if stats["hour_count"].(int) != 0 {
		t.Errorf("Unknown user should have 0 hour_count, got %v", stats["hour_count"])
	}
}

// =============================================================================
// Audit Logger Tests
// =============================================================================

func TestNewAuditLogger(t *testing.T) {
	logger := NewAuditLogger()
	if logger == nil {
		t.Fatal("NewAuditLogger() returned nil")
	}
	if logger.sinks == nil {
		t.Error("AuditLogger.sinks is nil")
	}
}

type mockAuditSink struct {
	events []MCPAuditEvent
}

func (m *mockAuditSink) Log(event MCPAuditEvent) error {
	m.events = append(m.events, event)
	return nil
}

func TestAuditLoggerAddSink(t *testing.T) {
	logger := NewAuditLogger()
	sink := &mockAuditSink{}

	logger.AddSink(sink)

	// Log an event
	logger.Log(MCPAuditEvent{
		Tool:   "store",
		UserID: "user1",
	})

	// Give goroutine time to execute
	time.Sleep(10 * time.Millisecond)

	if len(sink.events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(sink.events))
	}
}

func TestConsoleSink(t *testing.T) {
	sink := &ConsoleSink{}
	err := sink.Log(MCPAuditEvent{
		Tool:      "store",
		Operation: "create",
		UserID:    "user1",
		Success:   true,
	})
	if err != nil {
		t.Errorf("ConsoleSink.Log() error = %v", err)
	}
}

// =============================================================================
// Auth Config Tests
// =============================================================================

func TestDefaultAuthConfig(t *testing.T) {
	cfg := DefaultAuthConfig()

	if !cfg.RequireAuth {
		t.Error("RequireAuth should default to true")
	}
	if cfg.AllowAnonymous {
		t.Error("AllowAnonymous should default to false")
	}
	if !cfg.SecurityEnabled {
		t.Error("SecurityEnabled should default to true")
	}
	if !cfg.AuditEnabled {
		t.Error("AuditEnabled should default to true")
	}
	if !cfg.RateLimitEnabled {
		t.Error("RateLimitEnabled should default to true")
	}
}

// =============================================================================
// Auth Middleware Tests
// =============================================================================

func TestNewAuthMiddleware(t *testing.T) {
	// Create a real authenticator for testing
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, err := auth.NewAuthenticator(authConfig)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	if middleware == nil {
		t.Fatal("NewAuthMiddleware() returned nil")
	}
	if middleware.authenticator == nil {
		t.Error("AuthMiddleware.authenticator is nil")
	}
	if middleware.rateLimiter == nil {
		t.Error("AuthMiddleware.rateLimiter is nil")
	}
	if middleware.auditLogger == nil {
		t.Error("AuthMiddleware.auditLogger is nil")
	}
}

func TestAuthMiddlewareSecurityDisabled(t *testing.T) {
	// Create authenticator with security disabled
	authConfig := auth.AuthConfig{
		SecurityEnabled: false,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	config := DefaultAuthConfig()
	config.SecurityEnabled = false

	middleware := NewAuthMiddleware(authenticator, config)

	// Create test handler
	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		// Check auth context
		authCtx, ok := GetAuthContext(r.Context())
		if !ok {
			t.Error("Expected auth context to be set")
		}
		if authCtx.UserID != "anonymous" {
			t.Errorf("Expected anonymous user, got %s", authCtx.UserID)
		}
		if len(authCtx.Roles) == 0 || authCtx.Roles[0] != RoleSuperAdmin {
			t.Errorf("Expected super_admin role when security disabled")
		}
	})

	// Wrap with middleware
	wrapped := middleware.Middleware(handler)

	// Make request
	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("Handler was not called")
	}
}

func TestAuthMiddlewareHealthEndpoint(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	wrapped := middleware.Middleware(handler)

	// Health endpoints should bypass auth
	for _, path := range []string{"/health", "/mcp/health"} {
		called = false
		req := httptest.NewRequest("GET", path, nil)
		rec := httptest.NewRecorder()
		wrapped.ServeHTTP(rec, req)

		if !called {
			t.Errorf("Handler not called for %s", path)
		}
	}
}

func TestAuthMiddlewareRequiresToken(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called without token")
	})

	wrapped := middleware.Middleware(handler)

	req := httptest.NewRequest("GET", "/api/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddlewareValidToken(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	// Create a user and get a token
	_, _ = authenticator.CreateUser("testuser", "TestPassword123!", []auth.Role{auth.RoleEditor})
	tokenResp, _, _ := authenticator.Authenticate("testuser", "TestPassword123!", "", "")

	config := DefaultAuthConfig()
	config.RateLimitEnabled = false // Disable for this test
	middleware := NewAuthMiddleware(authenticator, config)

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		authCtx, ok := GetAuthContext(r.Context())
		if !ok {
			t.Error("Expected auth context")
		}
		if authCtx.Username != "testuser" {
			t.Errorf("Expected username testuser, got %s", authCtx.Username)
		}
	})

	wrapped := middleware.Middleware(handler)

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.Header.Set("Authorization", "Bearer "+tokenResp.AccessToken)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("Handler was not called")
	}
	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestAuthMiddlewareInvalidToken(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called with invalid token")
	})

	wrapped := middleware.Middleware(handler)

	req := httptest.NewRequest("GET", "/api/test", nil)
	req.Header.Set("Authorization", "Bearer invalid.token.here")
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401, got %d", rec.Code)
	}
}

func TestAuthMiddlewareAllowAnonymous(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	config := DefaultAuthConfig()
	config.AllowAnonymous = true
	middleware := NewAuthMiddleware(authenticator, config)

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		authCtx, ok := GetAuthContext(r.Context())
		if !ok {
			t.Error("Expected auth context")
		}
		if authCtx.UserID != "anonymous" {
			t.Errorf("Expected anonymous user, got %s", authCtx.UserID)
		}
		if len(authCtx.Roles) == 0 || authCtx.Roles[0] != RoleOrgViewer {
			t.Error("Expected viewer role for anonymous")
		}
	})

	wrapped := middleware.Middleware(handler)

	req := httptest.NewRequest("GET", "/api/test", nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("Handler was not called")
	}
}

func TestAuthMiddlewareXAPIKeyHeader(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	_, _ = authenticator.CreateUser("apiuser", "ApiPassword123!", []auth.Role{auth.RoleAdmin})
	tokenResp, _, _ := authenticator.Authenticate("apiuser", "ApiPassword123!", "", "")

	config := DefaultAuthConfig()
	config.RateLimitEnabled = false
	middleware := NewAuthMiddleware(authenticator, config)

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	wrapped := middleware.Middleware(handler)

	// Use X-API-Key header instead of Authorization
	req := httptest.NewRequest("GET", "/api/test", nil)
	req.Header.Set("X-API-Key", tokenResp.AccessToken)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("Handler was not called with X-API-Key")
	}
}

func TestAuthMiddlewareQueryToken(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	_, _ = authenticator.CreateUser("queryuser", "QueryPassword123!", []auth.Role{auth.RoleViewer})
	tokenResp, _, _ := authenticator.Authenticate("queryuser", "QueryPassword123!", "", "")

	config := DefaultAuthConfig()
	config.RateLimitEnabled = false
	middleware := NewAuthMiddleware(authenticator, config)

	called := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})

	wrapped := middleware.Middleware(handler)

	// Use query parameter
	req := httptest.NewRequest("GET", "/api/test?token="+tokenResp.AccessToken, nil)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, req)

	if !called {
		t.Error("Handler was not called with query token")
	}
}

// =============================================================================
// CheckToolAccess Tests
// =============================================================================

func TestCheckToolAccess(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	tests := []struct {
		name    string
		roles   []MCPRole
		tool    string
		wantErr bool
	}{
		{"admin_can_store", []MCPRole{RoleSuperAdmin}, ToolStore, false},
		{"viewer_cannot_store", []MCPRole{RoleOrgViewer}, ToolStore, true},
		{"viewer_can_recall", []MCPRole{RoleOrgViewer}, ToolRecall, false},
		{"no_roles", []MCPRole{}, ToolStore, true},
		{"multi_role_one_allowed", []MCPRole{RoleOrgViewer, RoleOrgDeveloper}, ToolStore, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), authContextKey, &AuthContext{
				UserID: "test",
				Roles:  tt.roles,
			})

			err := middleware.CheckToolAccess(ctx, tt.tool)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckToolAccess() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCheckToolAccessNoContext(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	err := middleware.CheckToolAccess(context.Background(), ToolStore)
	if err == nil {
		t.Error("Expected error when no auth context")
	}
}

// =============================================================================
// LogToolCall Tests
// =============================================================================

func TestLogToolCall(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	config := DefaultAuthConfig()
	config.AuditEnabled = true
	middleware := NewAuthMiddleware(authenticator, config)

	// Add mock sink
	sink := &mockAuditSink{}
	middleware.auditLogger.AddSink(sink)

	ctx := context.WithValue(context.Background(), authContextKey, &AuthContext{
		UserID:   "user1",
		Username: "testuser",
		Roles:    []MCPRole{RoleOrgDeveloper},
		OrgID:    "org1",
	})

	middleware.LogToolCall(ctx, "store", "create", "memory", "node-123", true, "", time.Second, nil)

	// Give goroutine time
	time.Sleep(10 * time.Millisecond)

	if len(sink.events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(sink.events))
	}

	event := sink.events[0]
	if event.Tool != "store" {
		t.Errorf("Expected tool=store, got %s", event.Tool)
	}
	if event.UserID != "user1" {
		t.Errorf("Expected user_id=user1, got %s", event.UserID)
	}
	if event.OrgID != "org1" {
		t.Errorf("Expected org_id=org1, got %s", event.OrgID)
	}
}

func TestLogToolCallDisabled(t *testing.T) {
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)

	config := DefaultAuthConfig()
	config.AuditEnabled = false
	middleware := NewAuthMiddleware(authenticator, config)

	sink := &mockAuditSink{}
	middleware.auditLogger.AddSink(sink)

	middleware.LogToolCall(context.Background(), "store", "create", "", "", true, "", 0, nil)

	time.Sleep(10 * time.Millisecond)

	if len(sink.events) != 0 {
		t.Error("Expected no events when audit disabled")
	}
}

// =============================================================================
// GetAuthContext Tests
// =============================================================================

func TestGetAuthContext(t *testing.T) {
	// No context
	_, ok := GetAuthContext(context.Background())
	if ok {
		t.Error("Expected false for empty context")
	}

	// With context
	authCtx := &AuthContext{UserID: "test"}
	ctx := context.WithValue(context.Background(), authContextKey, authCtx)
	got, ok := GetAuthContext(ctx)
	if !ok {
		t.Error("Expected true for context with auth")
	}
	if got.UserID != "test" {
		t.Errorf("Expected UserID=test, got %s", got.UserID)
	}
}

// =============================================================================
// isSecurityEnabled Tests
// =============================================================================

func TestIsSecurityEnabled(t *testing.T) {
	// With authenticator
	authConfig := auth.AuthConfig{
		JWTSecret:       []byte("test-secret-key-32-chars-minimum!"),
		SecurityEnabled: true,
	}
	authenticator, _ := auth.NewAuthenticator(authConfig)
	middleware := NewAuthMiddleware(authenticator, DefaultAuthConfig())

	if !middleware.isSecurityEnabled() {
		t.Error("Expected security enabled when authenticator has it enabled")
	}

	// With disabled authenticator
	authConfig.SecurityEnabled = false
	authenticator2, _ := auth.NewAuthenticator(authConfig)
	middleware2 := NewAuthMiddleware(authenticator2, DefaultAuthConfig())

	if middleware2.isSecurityEnabled() {
		t.Error("Expected security disabled when authenticator has it disabled")
	}
}
