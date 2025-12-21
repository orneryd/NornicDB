package bolt

import (
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestAuthenticatorAdapter(t *testing.T) {
	// Create a real authenticator for testing
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
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

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
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

// TestBearerTokenAuthentication tests JWT bearer token authentication
func TestBearerTokenAuthentication(t *testing.T) {
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	adapter := NewAuthenticatorAdapter(authenticator)

	t.Run("bearer scheme with valid cluster token", func(t *testing.T) {
		// Generate a cluster token
		token, err := authenticator.GenerateClusterToken("node-1", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate cluster token: %v", err)
		}

		// Authenticate with bearer scheme
		result, err := adapter.Authenticate("bearer", "", token)
		if err != nil {
			t.Fatalf("Bearer auth failed: %v", err)
		}

		if !result.Authenticated {
			t.Error("Expected Authenticated=true")
		}
		if result.Username != "node-1" {
			t.Errorf("Expected username 'node-1', got '%s'", result.Username)
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

	t.Run("bearer scheme with viewer role token", func(t *testing.T) {
		token, err := authenticator.GenerateClusterToken("read-replica", auth.RoleViewer)
		if err != nil {
			t.Fatalf("Failed to generate cluster token: %v", err)
		}

		result, err := adapter.Authenticate("bearer", "", token)
		if err != nil {
			t.Fatalf("Bearer auth failed: %v", err)
		}

		if result.Username != "read-replica" {
			t.Errorf("Expected username 'read-replica', got '%s'", result.Username)
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

	t.Run("bearer scheme with editor role token", func(t *testing.T) {
		token, err := authenticator.GenerateClusterToken("worker-node", auth.RoleEditor)
		if err != nil {
			t.Fatalf("Failed to generate cluster token: %v", err)
		}

		result, err := adapter.Authenticate("bearer", "", token)
		if err != nil {
			t.Fatalf("Bearer auth failed: %v", err)
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

	t.Run("bearer scheme with empty token fails", func(t *testing.T) {
		_, err := adapter.Authenticate("bearer", "", "")
		if err == nil {
			t.Error("Expected error for empty bearer token")
		}
		if !strings.Contains(err.Error(), "bearer token required") {
			t.Errorf("Expected 'bearer token required' error, got: %v", err)
		}
	})

	t.Run("bearer scheme with invalid token fails", func(t *testing.T) {
		_, err := adapter.Authenticate("bearer", "", "invalid.token.here")
		if err == nil {
			t.Error("Expected error for invalid bearer token")
		}
		if !strings.Contains(err.Error(), "invalid bearer token") {
			t.Errorf("Expected 'invalid bearer token' error, got: %v", err)
		}
	})

	t.Run("bearer scheme with malformed JWT fails", func(t *testing.T) {
		_, err := adapter.Authenticate("bearer", "", "not-a-jwt-at-all")
		if err == nil {
			t.Error("Expected error for malformed JWT")
		}
	})

	t.Run("bearer scheme with wrong signature fails", func(t *testing.T) {
		// Generate token with correct format but tampered signature
		token, _ := authenticator.GenerateClusterToken("node-1", auth.RoleAdmin)
		parts := strings.Split(token, ".")
		if len(parts) == 3 {
			// Tamper with signature
			tamperedToken := parts[0] + "." + parts[1] + ".tampered_signature"
			_, err := adapter.Authenticate("bearer", "", tamperedToken)
			if err == nil {
				t.Error("Expected error for tampered JWT signature")
			}
		}
	})
}

// TestJWTAsBasicAuth tests JWT tokens passed via basic auth with empty username
func TestJWTAsBasicAuth(t *testing.T) {
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	adapter := NewAuthenticatorAdapter(authenticator)

	t.Run("JWT token with empty principal triggers bearer auth", func(t *testing.T) {
		// Generate a cluster token
		token, err := authenticator.GenerateClusterToken("cluster-west", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate cluster token: %v", err)
		}

		// Pass JWT as credentials with empty principal (the Neo4j driver workaround)
		result, err := adapter.Authenticate("basic", "", token)
		if err != nil {
			t.Fatalf("JWT-as-basic auth failed: %v", err)
		}

		if !result.Authenticated {
			t.Error("Expected Authenticated=true")
		}
		if result.Username != "cluster-west" {
			t.Errorf("Expected username 'cluster-west', got '%s'", result.Username)
		}
		if !result.HasRole("admin") {
			t.Error("Expected admin role")
		}
	})

	t.Run("empty principal with non-JWT password fails properly", func(t *testing.T) {
		// This should NOT be treated as JWT because it doesn't look like one
		_, err := adapter.Authenticate("basic", "", "regular-password")
		if err == nil {
			t.Error("Expected error for empty principal with non-JWT password")
		}
	})

	t.Run("empty principal with invalid JWT fails", func(t *testing.T) {
		// Looks like JWT format but invalid
		_, err := adapter.Authenticate("basic", "", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.invalid_sig")
		if err == nil {
			t.Error("Expected error for invalid JWT")
		}
	})
}

// TestGenerateClusterToken tests cluster token generation
func TestGenerateClusterToken(t *testing.T) {
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	t.Run("generates valid admin token", func(t *testing.T) {
		token, err := authenticator.GenerateClusterToken("node-east", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate token: %v", err)
		}

		// Token should be in JWT format
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Errorf("Expected JWT format (3 parts), got %d parts", len(parts))
		}

		// Validate the token
		claims, err := authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("Token validation failed: %v", err)
		}

		if claims.Username != "node-east" {
			t.Errorf("Expected username 'node-east', got '%s'", claims.Username)
		}
		if len(claims.Roles) != 1 || claims.Roles[0] != "admin" {
			t.Errorf("Expected roles ['admin'], got %v", claims.Roles)
		}
		if !strings.HasPrefix(claims.Sub, "cluster-") {
			t.Errorf("Expected Sub to start with 'cluster-', got '%s'", claims.Sub)
		}
	})

	t.Run("generates valid viewer token", func(t *testing.T) {
		token, err := authenticator.GenerateClusterToken("replica-1", auth.RoleViewer)
		if err != nil {
			t.Fatalf("Failed to generate token: %v", err)
		}

		claims, err := authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("Token validation failed: %v", err)
		}

		if len(claims.Roles) != 1 || claims.Roles[0] != "viewer" {
			t.Errorf("Expected roles ['viewer'], got %v", claims.Roles)
		}
	})

	t.Run("different nodes get different tokens", func(t *testing.T) {
		token1, _ := authenticator.GenerateClusterToken("node-1", auth.RoleAdmin)
		token2, _ := authenticator.GenerateClusterToken("node-2", auth.RoleAdmin)

		if token1 == token2 {
			t.Error("Different nodes should get different tokens")
		}
	})
}

// TestGenerateClusterTokenWithExpiry tests token generation with custom expiry
func TestGenerateClusterTokenWithExpiry(t *testing.T) {
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, err := auth.NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("Failed to create authenticator: %v", err)
	}

	t.Run("generates token with expiry", func(t *testing.T) {
		token, err := authenticator.GenerateClusterTokenWithExpiry("node-1", auth.RoleAdmin, 24*time.Hour)
		if err != nil {
			t.Fatalf("Failed to generate token: %v", err)
		}

		claims, err := authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("Token validation failed: %v", err)
		}

		// Check expiry is set
		if claims.Exp == 0 {
			t.Error("Expected Exp to be set")
		}

		// Check expiry is approximately 24 hours from now
		expectedExp := time.Now().Add(24 * time.Hour).Unix()
		diff := claims.Exp - expectedExp
		if diff < -60 || diff > 60 { // Allow 60 second tolerance
			t.Errorf("Expected Exp around %d, got %d (diff: %d)", expectedExp, claims.Exp, diff)
		}
	})

	t.Run("generates token without expiry when 0", func(t *testing.T) {
		token, err := authenticator.GenerateClusterTokenWithExpiry("node-1", auth.RoleAdmin, 0)
		if err != nil {
			t.Fatalf("Failed to generate token: %v", err)
		}

		claims, err := authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("Token validation failed: %v", err)
		}

		if claims.Exp != 0 {
			t.Errorf("Expected Exp to be 0 (never expires), got %d", claims.Exp)
		}
	})

	t.Run("short-lived token expires correctly", func(t *testing.T) {
		// Generate token that expires in 1 second
		token, err := authenticator.GenerateClusterTokenWithExpiry("node-1", auth.RoleAdmin, 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to generate token: %v", err)
		}

		// Should be valid immediately
		_, err = authenticator.ValidateToken(token)
		if err != nil {
			t.Fatalf("Token should be valid immediately: %v", err)
		}

		// Wait for expiry
		time.Sleep(2 * time.Second)

		// Should be expired now
		_, err = authenticator.ValidateToken(token)
		if err == nil {
			t.Error("Expected expired token to fail validation")
		}
	})
}

// TestLooksLikeJWT tests the JWT detection helper
func TestLooksLikeJWT(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid JWT format", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.signature", true},
		{"real JWT token", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U", true},
		{"short token two dots", "a.b.c", false}, // Too short
		{"empty string", "", false},
		{"no dots", "nodots", false},
		{"one dot", "one.dot", false},
		{"two dots but short", "a.b.c", false},
		{"three dots", "a.b.c.d", false},
		{"regular password", "my-secure-password-123", false},
		{"password with special chars", "P@ssw0rd!123#", false},
		{"base64 without dots", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", false},
		{"long string two dots", "aaaaaaaaaaaaaaaaaaaaa.bbbbbbbbbbbbbbbbbbb.cccccccccccccccc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := looksLikeJWT(tt.input)
			if result != tt.expected {
				t.Errorf("looksLikeJWT(%q) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

// TestClusterAuthenticationE2E tests end-to-end cluster authentication flow
func TestClusterAuthenticationE2E(t *testing.T) {
	// Simulate two nodes with the same JWT secret (shared secret cluster)
	sharedSecret := []byte("shared-cluster-secret-32-chars!!")

	// Node 1 (leader)
	config1 := auth.DefaultAuthConfig()
	config1.JWTSecret = sharedSecret
	config1.SecurityEnabled = true
	memoryStorage1 := storage.NewMemoryEngine()
	node1Auth, _ := auth.NewAuthenticator(config1, memoryStorage1)
	node1Adapter := NewAuthenticatorAdapter(node1Auth)

	// Node 2 (follower)
	config2 := auth.DefaultAuthConfig()
	config2.JWTSecret = sharedSecret
	config2.SecurityEnabled = true
	memoryStorage2 := storage.NewMemoryEngine()
	node2Auth, _ := auth.NewAuthenticator(config2, memoryStorage2)
	node2Adapter := NewAuthenticatorAdapter(node2Auth)

	t.Run("token generated on node1 works on node2", func(t *testing.T) {
		// Generate token on node1
		token, err := node1Auth.GenerateClusterToken("follower-node-2", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate token on node1: %v", err)
		}

		// Use token to authenticate on node2 (the leader)
		result, err := node2Adapter.Authenticate("bearer", "", token)
		if err != nil {
			t.Fatalf("Token from node1 failed on node2: %v", err)
		}

		if !result.Authenticated {
			t.Error("Expected successful authentication")
		}
		if result.Username != "follower-node-2" {
			t.Errorf("Expected username 'follower-node-2', got '%s'", result.Username)
		}
	})

	t.Run("token generated on node2 works on node1", func(t *testing.T) {
		// Generate token on node2
		token, err := node2Auth.GenerateClusterToken("leader-node-1", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate token on node2: %v", err)
		}

		// Use token to authenticate on node1
		result, err := node1Adapter.Authenticate("bearer", "", token)
		if err != nil {
			t.Fatalf("Token from node2 failed on node1: %v", err)
		}

		if !result.Authenticated {
			t.Error("Expected successful authentication")
		}
	})

	t.Run("token with different secret fails", func(t *testing.T) {
		// Create a rogue node with different secret
		rogueConfig := auth.DefaultAuthConfig()
		rogueConfig.JWTSecret = []byte("different-secret-not-trusted!!!")
		rogueConfig.SecurityEnabled = true
		memoryStorage := storage.NewMemoryEngine()
		rogueAuth, _ := auth.NewAuthenticator(rogueConfig, memoryStorage)

		// Generate token with rogue secret
		rogueToken, err := rogueAuth.GenerateClusterToken("rogue-node", auth.RoleAdmin)
		if err != nil {
			t.Fatalf("Failed to generate rogue token: %v", err)
		}

		// Try to use on node1 - should fail
		_, err = node1Adapter.Authenticate("bearer", "", rogueToken)
		if err == nil {
			t.Error("Expected rogue token to fail authentication")
		}
	})

	t.Run("JWT via basic auth works across nodes", func(t *testing.T) {
		// This is the Neo4j driver workaround path
		token, _ := node1Auth.GenerateClusterToken("driver-client", auth.RoleEditor)

		// Authenticate using basic auth with empty principal
		result, err := node2Adapter.Authenticate("basic", "", token)
		if err != nil {
			t.Fatalf("JWT via basic auth failed: %v", err)
		}

		if result.Username != "driver-client" {
			t.Errorf("Expected username 'driver-client', got '%s'", result.Username)
		}
		if !result.HasRole("editor") {
			t.Error("Expected editor role")
		}
	})
}

// TestBoltAuthResultPermissions tests all permission combinations
func TestBoltAuthResultPermissions(t *testing.T) {
	tests := []struct {
		name       string
		roles      []string
		permission string
		shouldHave bool
	}{
		// Admin role
		{"admin has read", []string{"admin"}, "read", true},
		{"admin has write", []string{"admin"}, "write", true},
		{"admin has create", []string{"admin"}, "create", true},
		{"admin has delete", []string{"admin"}, "delete", true},
		{"admin has admin", []string{"admin"}, "admin", true},
		{"admin has schema", []string{"admin"}, "schema", true},
		{"admin has user_manage", []string{"admin"}, "user_manage", true},

		// Editor role
		{"editor has read", []string{"editor"}, "read", true},
		{"editor has write", []string{"editor"}, "write", true},
		{"editor has create", []string{"editor"}, "create", true},
		{"editor has delete", []string{"editor"}, "delete", true},
		{"editor no admin", []string{"editor"}, "admin", false},
		{"editor no schema", []string{"editor"}, "schema", false},
		{"editor no user_manage", []string{"editor"}, "user_manage", false},

		// Viewer role
		{"viewer has read", []string{"viewer"}, "read", true},
		{"viewer no write", []string{"viewer"}, "write", false},
		{"viewer no create", []string{"viewer"}, "create", false},
		{"viewer no delete", []string{"viewer"}, "delete", false},
		{"viewer no admin", []string{"viewer"}, "admin", false},
		{"viewer no schema", []string{"viewer"}, "schema", false},

		// None role
		{"none no read", []string{"none"}, "read", false},
		{"none no write", []string{"none"}, "write", false},
		{"none no admin", []string{"none"}, "admin", false},

		// Multiple roles
		{"viewer+editor has read", []string{"viewer", "editor"}, "read", true},
		{"viewer+editor has write", []string{"viewer", "editor"}, "write", true},
		{"viewer+editor no schema", []string{"viewer", "editor"}, "schema", false},

		// Invalid/unknown role
		{"unknown role no perms", []string{"custom"}, "read", false},
		{"unknown role no write", []string{"custom"}, "write", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &BoltAuthResult{
				Authenticated: true,
				Username:      "test",
				Roles:         tt.roles,
			}

			has := result.HasPermission(tt.permission)
			if has != tt.shouldHave {
				t.Errorf("HasPermission(%q) = %v, expected %v for roles %v",
					tt.permission, has, tt.shouldHave, tt.roles)
			}
		})
	}
}

// TestBoltAuthResultHasRole tests role checking
func TestBoltAuthResultHasRole(t *testing.T) {
	tests := []struct {
		name       string
		roles      []string
		checkRole  string
		shouldHave bool
	}{
		{"has single role", []string{"admin"}, "admin", true},
		{"missing single role", []string{"admin"}, "editor", false},
		{"has in multiple roles", []string{"editor", "viewer"}, "viewer", true},
		{"missing in multiple roles", []string{"editor", "viewer"}, "admin", false},
		{"empty roles", []string{}, "admin", false},
		{"nil treated as empty", nil, "admin", false},
		{"case sensitive", []string{"Admin"}, "admin", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &BoltAuthResult{
				Authenticated: true,
				Username:      "test",
				Roles:         tt.roles,
			}

			has := result.HasRole(tt.checkRole)
			if has != tt.shouldHave {
				t.Errorf("HasRole(%q) = %v, expected %v for roles %v",
					tt.checkRole, has, tt.shouldHave, tt.roles)
			}
		})
	}
}

// TestAuthAdapterNilAuthenticator tests behavior with nil authenticator
func TestAuthAdapterNilAuthenticator(t *testing.T) {
	// This is an edge case - what happens if someone passes nil?
	// The adapter should handle this gracefully

	t.Run("NewAuthenticatorAdapter with nil doesn't panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("NewAuthenticatorAdapter(nil) panicked: %v", r)
			}
		}()

		adapter := NewAuthenticatorAdapter(nil)
		if adapter == nil {
			t.Error("Expected non-nil adapter")
		}
	})
}

// TestConcurrentAuthentication tests thread safety
func TestConcurrentAuthentication(t *testing.T) {
	config := auth.DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-key-for-jwt-signing!!")
	config.SecurityEnabled = true

	memoryStorage := storage.NewMemoryEngine()
	authenticator, _ := auth.NewAuthenticator(config, memoryStorage)
	authenticator.CreateUser("testuser", "testpass", []auth.Role{auth.RoleEditor})

	adapter := NewAuthenticatorAdapter(authenticator)

	// Generate a token for concurrent testing
	token, _ := authenticator.GenerateClusterToken("concurrent-node", auth.RoleAdmin)

	t.Run("concurrent basic auth", func(t *testing.T) {
		done := make(chan bool, 100)
		for i := 0; i < 100; i++ {
			go func() {
				result, err := adapter.Authenticate("basic", "testuser", "testpass")
				if err != nil {
					t.Errorf("Concurrent auth failed: %v", err)
				}
				if !result.Authenticated {
					t.Error("Expected authenticated")
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 100; i++ {
			<-done
		}
	})

	t.Run("concurrent bearer auth", func(t *testing.T) {
		done := make(chan bool, 100)
		for i := 0; i < 100; i++ {
			go func() {
				result, err := adapter.Authenticate("bearer", "", token)
				if err != nil {
					t.Errorf("Concurrent bearer auth failed: %v", err)
				}
				if !result.Authenticated {
					t.Error("Expected authenticated")
				}
				done <- true
			}()
		}

		// Wait for all goroutines
		for i := 0; i < 100; i++ {
			<-done
		}
	})

	t.Run("concurrent SetAllowAnonymous toggle", func(t *testing.T) {
		done := make(chan bool, 100)
		for i := 0; i < 100; i++ {
			go func(idx int) {
				adapter.SetAllowAnonymous(idx%2 == 0)
				done <- true
			}(i)
		}

		// Wait for all goroutines
		for i := 0; i < 100; i++ {
			<-done
		}
	})
}
