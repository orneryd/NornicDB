package mcp

import (
	"context"
	"testing"
)

// --- context.go uncovered branches ---

func TestContextWithDatabase_EmptyString(t *testing.T) {
	ctx := context.Background()
	// Empty dbName should return the same context unchanged
	ctx2 := ContextWithDatabase(ctx, "")
	if ctx2 != ctx {
		t.Error("Expected same context returned for empty dbName")
	}
	if db := DatabaseFromContext(ctx2); db != "" {
		t.Errorf("Expected empty string, got %q", db)
	}
}

func TestDatabaseFromContext_Nil(t *testing.T) {
	db := DatabaseFromContext(nil)
	if db != "" {
		t.Errorf("Expected empty string for nil context, got %q", db)
	}
}

// --- auth.go: isSecurityEnabled without authenticator ---

func TestIsSecurityEnabled_NilAuthenticator(t *testing.T) {
	// When authenticator is nil, falls back to config.SecurityEnabled
	m := &AuthMiddleware{
		authenticator: nil,
		config:        AuthConfig{SecurityEnabled: true},
	}
	if !m.isSecurityEnabled() {
		t.Error("Expected security enabled from config")
	}

	m.config.SecurityEnabled = false
	if m.isSecurityEnabled() {
		t.Error("Expected security disabled from config")
	}
}

// --- auth.go: RateLimiter unknown role fallback ---

func TestRateLimiter_UnknownRole(t *testing.T) {
	rl := NewRateLimiter()
	// Unknown role should use fallback limits (60/min, 3600/hr)
	allowed, err := rl.Allow("user1", MCPRole("nonexistent_role"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !allowed {
		t.Error("Expected first request to be allowed")
	}
}

// --- auth.go: RateLimiter minute limit exceeded ---

func TestRateLimiter_MinuteLimitExceeded(t *testing.T) {
	rl := NewRateLimiter()
	// Set a very low limit
	rl.SetLimits(RoleOrgViewer, RateLimit{RequestsPerMinute: 2, RequestsPerHour: 1000, BurstSize: 5})

	rl.Allow("user-min", RoleOrgViewer)
	rl.Allow("user-min", RoleOrgViewer)

	// Third request should be denied
	allowed, err := rl.Allow("user-min", RoleOrgViewer)
	if allowed {
		t.Error("Expected request to be denied after exceeding minute limit")
	}
	if err == nil {
		t.Error("Expected error for rate limit exceeded")
	}
}

// --- auth.go: RateLimiter hour limit exceeded ---

func TestRateLimiter_HourLimitExceeded(t *testing.T) {
	rl := NewRateLimiter()
	rl.SetLimits(RoleOrgViewer, RateLimit{RequestsPerMinute: 1000, RequestsPerHour: 2, BurstSize: 5})

	rl.Allow("user-hr", RoleOrgViewer)
	rl.Allow("user-hr", RoleOrgViewer)

	allowed, err := rl.Allow("user-hr", RoleOrgViewer)
	if allowed {
		t.Error("Expected request to be denied after exceeding hour limit")
	}
	if err == nil {
		t.Error("Expected error for rate limit exceeded")
	}
}

// --- auth.go: ConsoleSink failure event ---

func TestConsoleSink_FailureEvent(t *testing.T) {
	sink := &ConsoleSink{}
	err := sink.Log(MCPAuditEvent{
		Success:   false,
		Operation: "store",
		Tool:      "store",
		UserID:    "test-user",
	})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// --- auth.go: AuditLogger with no sinks ---

func TestAuditLogger_NoSinks(t *testing.T) {
	logger := NewAuditLogger()
	// Should not panic with no sinks
	logger.Log(MCPAuditEvent{Operation: "test"})
}

// --- auth.go: GetAuthContext missing ---

func TestGetAuthContext_Missing(t *testing.T) {
	ctx := context.Background()
	ac, ok := GetAuthContext(ctx)
	if ok {
		t.Error("Expected ok=false for missing auth context")
	}
	if ac != nil {
		t.Error("Expected nil auth context")
	}
}

// --- auth.go: GetStats for unknown user ---

func TestRateLimiter_GetStats_UnknownUser(t *testing.T) {
	rl := NewRateLimiter()
	stats := rl.GetStats("nonexistent")
	if stats["minute_count"] != 0 {
		t.Errorf("Expected 0 minute_count, got %v", stats["minute_count"])
	}
}

// --- auth.go: GetStats for known user ---

func TestRateLimiter_GetStats_KnownUser(t *testing.T) {
	rl := NewRateLimiter()
	rl.Allow("known-user", RoleSuperAdmin)

	stats := rl.GetStats("known-user")
	if stats["minute_count"] != 1 {
		t.Errorf("Expected 1 minute_count, got %v", stats["minute_count"])
	}
	if stats["hour_count"] != 1 {
		t.Errorf("Expected 1 hour_count, got %v", stats["hour_count"])
	}
}
