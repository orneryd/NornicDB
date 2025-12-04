package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
)

func TestAuthenticatorAdapter(t *testing.T) {
	// Create a real authenticator for testing
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	authenticator, err := auth.NewAuthenticator(config)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	// Create test users
	_, err = authenticator.CreateUser("admin", "admin-password", []auth.Role{auth.RoleAdmin})
	if err != nil {
		t.Fatalf("Failed to create admin user: %v", err)
	}

	_, err = authenticator.CreateUser("editor", "editor-password", []auth.Role{auth.RoleEditor})
	if err != nil {
		t.Fatalf("Failed to create editor user: %v", err)
	}

	_, err = authenticator.CreateUser("viewer", "viewer-password", []auth.Role{auth.RoleViewer})
	if err != nil {
		t.Fatalf("Failed to create viewer user: %v", err)
	}

	// Create service account for clustering
	_, err = authenticator.CreateUser("cluster-node-1", "cluster-secret-123", []auth.Role{auth.RoleAdmin})
	if err != nil {
		t.Fatalf("Failed to create cluster service account: %v", err)
	}

	t.Run("basic auth success - admin", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		result, err := adapter.Authenticate("basic", "admin", "admin-password")
		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}
		if !result.Authenticated {
			t.Error("Expected Authenticated=true")
		}
		if result.Username != "admin" {
			t.Errorf("Expected username 'admin', got '%s'", result.Username)
		}
		if !result.HasRole("admin") {
			t.Error("Expected admin role")
		}
		if !result.HasPermission("write") {
			t.Error("Admin should have write permission")
		}
		if !result.HasPermission("schema") {
			t.Error("Admin should have schema permission")
		}
	})

	t.Run("basic auth success - editor", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		result, err := adapter.Authenticate("basic", "editor", "editor-password")
		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}
		if !result.HasRole("editor") {
			t.Error("Expected editor role")
		}
		if !result.HasPermission("write") {
			t.Error("Editor should have write permission")
		}
		if result.HasPermission("schema") {
			t.Error("Editor should NOT have schema permission")
		}
	})

	t.Run("basic auth success - viewer", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		result, err := adapter.Authenticate("basic", "viewer", "viewer-password")
		if err != nil {
			t.Fatalf("Expected success, got error: %v", err)
		}
		if !result.HasRole("viewer") {
			t.Error("Expected viewer role")
		}
		if !result.HasPermission("read") {
			t.Error("Viewer should have read permission")
		}
		if result.HasPermission("write") {
			t.Error("Viewer should NOT have write permission")
		}
	})

	t.Run("service account auth for clustering", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		// Service accounts use the same "basic" auth as regular users
		result, err := adapter.Authenticate("basic", "cluster-node-1", "cluster-secret-123")
		if err != nil {
			t.Fatalf("Expected success for service account, got error: %v", err)
		}
		if !result.Authenticated {
			t.Error("Expected Authenticated=true for service account")
		}
		if result.Username != "cluster-node-1" {
			t.Errorf("Expected username 'cluster-node-1', got '%s'", result.Username)
		}
		if !result.HasRole("admin") {
			t.Error("Service account should have admin role for clustering")
		}
	})

	t.Run("basic auth failure - wrong password", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		_, err := adapter.Authenticate("basic", "admin", "wrong-password")
		if err == nil {
			t.Error("Expected error for wrong password")
		}
	})

	t.Run("basic auth failure - unknown user", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		_, err := adapter.Authenticate("basic", "unknown-user", "any-password")
		if err == nil {
			t.Error("Expected error for unknown user")
		}
	})

	t.Run("anonymous auth - disabled by default", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		_, err := adapter.Authenticate("none", "", "")
		if err == nil {
			t.Error("Expected error for anonymous auth when disabled")
		}
	})

	t.Run("anonymous auth - enabled", func(t *testing.T) {
		adapter := NewAuthenticatorAdapterWithAnonymous(authenticator)
		result, err := adapter.Authenticate("none", "", "")
		if err != nil {
			t.Fatalf("Expected success for anonymous auth when enabled, got: %v", err)
		}
		if result.Username != "anonymous" {
			t.Errorf("Expected username 'anonymous', got '%s'", result.Username)
		}
		if !result.HasRole("viewer") {
			t.Error("Anonymous should have viewer role")
		}
		if !result.HasPermission("read") {
			t.Error("Anonymous should have read permission")
		}
		if result.HasPermission("write") {
			t.Error("Anonymous should NOT have write permission")
		}
	})

	t.Run("anonymous auth - empty scheme treated as none", func(t *testing.T) {
		adapter := NewAuthenticatorAdapterWithAnonymous(authenticator)
		result, err := adapter.Authenticate("", "", "")
		if err != nil {
			t.Fatalf("Expected success for empty scheme when anonymous enabled: %v", err)
		}
		if result.Username != "anonymous" {
			t.Errorf("Expected username 'anonymous', got '%s'", result.Username)
		}
	})

	t.Run("unsupported auth scheme", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)
		_, err := adapter.Authenticate("kerberos", "user", "ticket")
		if err == nil {
			t.Error("Expected error for unsupported auth scheme")
		}
	})

	t.Run("SetAllowAnonymous toggle", func(t *testing.T) {
		adapter := NewAuthenticatorAdapter(authenticator)

		// Initially disabled
		_, err := adapter.Authenticate("none", "", "")
		if err == nil {
			t.Error("Expected error when anonymous disabled")
		}

		// Enable
		adapter.SetAllowAnonymous(true)
		result, err := adapter.Authenticate("none", "", "")
		if err != nil {
			t.Fatalf("Expected success when anonymous enabled: %v", err)
		}
		if result.Username != "anonymous" {
			t.Error("Expected anonymous user")
		}

		// Disable again
		adapter.SetAllowAnonymous(false)
		_, err = adapter.Authenticate("none", "", "")
		if err == nil {
			t.Error("Expected error after disabling anonymous")
		}
	})
}

func TestAuthenticatorAdapterIntegrationWithBoltConfig(t *testing.T) {
	// This test verifies the adapter works correctly with Bolt Config
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	authenticator, err := auth.NewAuthenticator(config)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	_, err = authenticator.CreateUser("testuser", "testpass", []auth.Role{auth.RoleEditor})
	if err != nil {
		t.Fatalf("Failed to create user: %v", err)
	}

	// Create Bolt config with the adapter
	boltConfig := DefaultConfig()
	boltConfig.Authenticator = NewAuthenticatorAdapter(authenticator)
	boltConfig.RequireAuth = true

	// Verify it's properly assigned
	if boltConfig.Authenticator == nil {
		t.Error("Authenticator should be set")
	}

	// Verify auth works through config
	result, err := boltConfig.Authenticator.Authenticate("basic", "testuser", "testpass")
	if err != nil {
		t.Fatalf("Auth through config failed: %v", err)
	}
	if !result.Authenticated {
		t.Error("Expected successful authentication")
	}
}
