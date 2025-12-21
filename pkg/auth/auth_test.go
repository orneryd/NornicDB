// Package auth tests for authentication.
package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestNewAuthenticator(t *testing.T) {
	tests := []struct {
		name    string
		config  AuthConfig
		wantErr bool
	}{
		{
			name: "valid config with secret",
			config: AuthConfig{
				SecurityEnabled: true,
				JWTSecret:       []byte("test-secret-at-least-32-bytes!!"),
			},
			wantErr: false,
		},
		{
			name: "security enabled without secret",
			config: AuthConfig{
				SecurityEnabled: true,
				JWTSecret:       nil,
			},
			wantErr: true,
		},
		{
			name: "security disabled without secret OK",
			config: AuthConfig{
				SecurityEnabled: false,
				JWTSecret:       nil,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memoryStorage := storage.NewMemoryEngine()
			_, err := NewAuthenticator(tt.config, memoryStorage)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAuthenticator() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateUser(t *testing.T) {
	auth := newTestAuthenticator(t)

	// Create first user
	user, err := auth.CreateUser("testuser", "password123", []Role{RoleEditor})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if user.Username != "testuser" {
		t.Errorf("expected username 'testuser', got %q", user.Username)
	}
	if !user.HasRole(RoleEditor) {
		t.Error("expected user to have editor role")
	}
	if user.Email != "testuser@localhost" {
		t.Errorf("expected email 'testuser@localhost', got %q", user.Email)
	}

	// Try to create duplicate
	_, err = auth.CreateUser("testuser", "password456", nil)
	if err != ErrUserExists {
		t.Errorf("expected ErrUserExists, got %v", err)
	}

	// Password too short
	_, err = auth.CreateUser("shortpass", "short", nil)
	if err == nil || !strings.Contains(err.Error(), "minimum") {
		t.Errorf("expected password length error, got %v", err)
	}

	// Default role when none specified
	user2, err := auth.CreateUser("defaultrole", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if !user2.HasRole(RoleViewer) {
		t.Error("expected default viewer role")
	}
}

func TestAuthenticate(t *testing.T) {
	auth := newTestAuthenticator(t)

	// Create test user
	_, err := auth.CreateUser("testuser", "password123", []Role{RoleAdmin})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Successful authentication
	token, user, err := auth.Authenticate("testuser", "password123", "127.0.0.1", "TestAgent")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if token.AccessToken == "" {
		t.Error("expected non-empty access token")
	}
	if token.TokenType != "Bearer" {
		t.Errorf("expected token type 'Bearer', got %q", token.TokenType)
	}
	if user.Username != "testuser" {
		t.Errorf("expected username 'testuser', got %q", user.Username)
	}

	// Wrong password
	_, _, err = auth.Authenticate("testuser", "wrongpassword", "127.0.0.1", "TestAgent")
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}

	// Non-existent user
	_, _, err = auth.Authenticate("nonexistent", "password123", "127.0.0.1", "TestAgent")
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}
}

func TestAuthenticateWithExpiry(t *testing.T) {
	config := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-at-least-32-bytes!!"),
		TokenExpiry:     time.Hour,
	}
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}

	_, err = auth.CreateUser("testuser", "password123", []Role{RoleViewer})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	token, _, err := auth.Authenticate("testuser", "password123", "", "")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}

	// Should have expires_in when expiry is configured
	if token.ExpiresIn != 3600 {
		t.Errorf("expected expires_in 3600, got %d", token.ExpiresIn)
	}
}

func TestAccountLockout(t *testing.T) {
	config := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-at-least-32-bytes!!"),
		MaxFailedLogins: 3,
		LockoutDuration: time.Minute,
	}
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}

	_, err = auth.CreateUser("locktest", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Fail 3 times
	for i := 0; i < 3; i++ {
		_, _, err = auth.Authenticate("locktest", "wrongpassword", "", "")
		if err != ErrInvalidCredentials {
			t.Fatalf("attempt %d: expected ErrInvalidCredentials, got %v", i+1, err)
		}
	}

	// 4th attempt should be locked
	_, _, err = auth.Authenticate("locktest", "password123", "", "") // Even correct password
	if err != ErrAccountLocked {
		t.Errorf("expected ErrAccountLocked, got %v", err)
	}

	// Unlock user
	if err := auth.UnlockUser("locktest"); err != nil {
		t.Fatalf("UnlockUser() error = %v", err)
	}

	// Should work now
	_, _, err = auth.Authenticate("locktest", "password123", "", "")
	if err != nil {
		t.Errorf("expected successful auth after unlock, got %v", err)
	}
}

func TestDisabledUser(t *testing.T) {
	auth := newTestAuthenticator(t)

	_, err := auth.CreateUser("disabletest", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Disable user
	if err := auth.DisableUser("disabletest"); err != nil {
		t.Fatalf("DisableUser() error = %v", err)
	}

	// Should not be able to authenticate
	_, _, err = auth.Authenticate("disabletest", "password123", "", "")
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials for disabled user, got %v", err)
	}

	// Re-enable
	if err := auth.EnableUser("disabletest"); err != nil {
		t.Fatalf("EnableUser() error = %v", err)
	}

	// Should work now
	_, _, err = auth.Authenticate("disabletest", "password123", "", "")
	if err != nil {
		t.Errorf("expected successful auth after enable, got %v", err)
	}
}

func TestValidateToken(t *testing.T) {
	auth := newTestAuthenticator(t)

	_, err := auth.CreateUser("tokentest", "password123", []Role{RoleAdmin, RoleEditor})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	tokenResp, _, err := auth.Authenticate("tokentest", "password123", "", "")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}

	// Validate token
	claims, err := auth.ValidateToken(tokenResp.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken() error = %v", err)
	}

	if claims.Username != "tokentest" {
		t.Errorf("expected username 'tokentest', got %q", claims.Username)
	}
	if len(claims.Roles) != 2 {
		t.Errorf("expected 2 roles, got %d", len(claims.Roles))
	}

	// Test with Bearer prefix
	claims2, err := auth.ValidateToken("Bearer " + tokenResp.AccessToken)
	if err != nil {
		t.Fatalf("ValidateToken() with Bearer prefix error = %v", err)
	}
	if claims2.Username != "tokentest" {
		t.Error("expected same claims with Bearer prefix")
	}

	// Invalid token
	_, err = auth.ValidateToken("invalid.token.here")
	if err != ErrInvalidToken {
		t.Errorf("expected ErrInvalidToken, got %v", err)
	}

	// Empty token
	_, err = auth.ValidateToken("")
	if err != ErrNoCredentials {
		t.Errorf("expected ErrNoCredentials, got %v", err)
	}
}

func TestTokenExpiration(t *testing.T) {
	config := AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-at-least-32-bytes!!"),
		TokenExpiry:     time.Second * 2, // 2 seconds for testing
	}
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}

	_, err = auth.CreateUser("expiretest", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	tokenResp, _, err := auth.Authenticate("expiretest", "password123", "", "")
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}

	// Token should be valid initially
	claims, err := auth.ValidateToken(tokenResp.AccessToken)
	if err != nil {
		t.Fatalf("expected valid token initially, got %v", err)
	}

	// Verify expiration is set
	if claims.Exp == 0 {
		t.Fatal("expected exp claim to be set")
	}

	// Calculate actual remaining time
	remaining := claims.Exp - time.Now().Unix()
	t.Logf("Token expires in %d seconds (exp=%d, now=%d)", remaining, claims.Exp, time.Now().Unix())

	// Wait for expiration (3 seconds to be safe beyond 2 second expiry)
	time.Sleep(time.Second * 3)

	// Token should be expired now
	_, err = auth.ValidateToken(tokenResp.AccessToken)
	if err != ErrSessionExpired {
		t.Errorf("expected ErrSessionExpired, got %v (exp was %d, now is %d)", err, claims.Exp, time.Now().Unix())
	}
}

func TestSecurityDisabled(t *testing.T) {
	config := AuthConfig{
		SecurityEnabled: false,
	}
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}

	// Should return admin claims when security is disabled
	claims, err := auth.ValidateToken("any-token")
	if err != nil {
		t.Fatalf("ValidateToken() error = %v", err)
	}
	if claims.Sub != "anonymous" {
		t.Errorf("expected sub 'anonymous', got %q", claims.Sub)
	}
	if len(claims.Roles) == 0 || claims.Roles[0] != string(RoleAdmin) {
		t.Error("expected admin role when security disabled")
	}
}

func TestChangePassword(t *testing.T) {
	auth := newTestAuthenticator(t)

	_, err := auth.CreateUser("passchange", "oldpassword1", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Change password
	err = auth.ChangePassword("passchange", "oldpassword1", "newpassword1")
	if err != nil {
		t.Fatalf("ChangePassword() error = %v", err)
	}

	// Old password should not work
	_, _, err = auth.Authenticate("passchange", "oldpassword1", "", "")
	if err != ErrInvalidCredentials {
		t.Errorf("expected old password to fail, got %v", err)
	}

	// New password should work
	_, _, err = auth.Authenticate("passchange", "newpassword1", "", "")
	if err != nil {
		t.Errorf("expected new password to work, got %v", err)
	}

	// Wrong old password
	err = auth.ChangePassword("passchange", "wrongold", "newnew123")
	if err != ErrInvalidCredentials {
		t.Errorf("expected ErrInvalidCredentials, got %v", err)
	}

	// New password too short
	err = auth.ChangePassword("passchange", "newpassword1", "short")
	if err == nil || !strings.Contains(err.Error(), "minimum") {
		t.Errorf("expected password length error, got %v", err)
	}
}

func TestUpdateRoles(t *testing.T) {
	auth := newTestAuthenticator(t)

	_, err := auth.CreateUser("rolechange", "password123", []Role{RoleViewer})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Update roles
	err = auth.UpdateRoles("rolechange", []Role{RoleAdmin, RoleEditor})
	if err != nil {
		t.Fatalf("UpdateRoles() error = %v", err)
	}

	user, _ := auth.GetUser("rolechange")
	if !user.HasRole(RoleAdmin) || !user.HasRole(RoleEditor) {
		t.Error("expected admin and editor roles")
	}
	if user.HasRole(RoleViewer) {
		t.Error("should not have viewer role anymore")
	}

	// Non-existent user
	err = auth.UpdateRoles("nonexistent", []Role{RoleAdmin})
	if err != ErrUserNotFound {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}
}

func TestDeleteUser(t *testing.T) {
	auth := newTestAuthenticator(t)

	_, err := auth.CreateUser("deletetest", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	// Delete user
	err = auth.DeleteUser("deletetest")
	if err != nil {
		t.Fatalf("DeleteUser() error = %v", err)
	}

	// User should not exist
	_, err = auth.GetUser("deletetest")
	if err != ErrUserNotFound {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}

	// Delete non-existent user
	err = auth.DeleteUser("nonexistent")
	if err != ErrUserNotFound {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}
}

func TestListUsers(t *testing.T) {
	auth := newTestAuthenticator(t)

	// Create multiple users
	_, _ = auth.CreateUser("user1", "password123", []Role{RoleAdmin})
	_, _ = auth.CreateUser("user2", "password123", []Role{RoleEditor})
	_, _ = auth.CreateUser("user3", "password123", []Role{RoleViewer})

	users := auth.ListUsers()
	if len(users) != 3 {
		t.Errorf("expected 3 users, got %d", len(users))
	}

	// Verify no password hashes
	for _, u := range users {
		if u.PasswordHash != "" {
			t.Errorf("user %s should not have password hash exposed", u.Username)
		}
	}
}

func TestGetUserByID(t *testing.T) {
	auth := newTestAuthenticator(t)

	created, err := auth.CreateUser("idtest", "password123", nil)
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}

	found, err := auth.GetUserByID(created.ID)
	if err != nil {
		t.Fatalf("GetUserByID() error = %v", err)
	}
	if found.Username != "idtest" {
		t.Errorf("expected username 'idtest', got %q", found.Username)
	}

	_, err = auth.GetUserByID("nonexistent-id")
	if err != ErrUserNotFound {
		t.Errorf("expected ErrUserNotFound, got %v", err)
	}
}

func TestUserCount(t *testing.T) {
	auth := newTestAuthenticator(t)

	if auth.UserCount() != 0 {
		t.Error("expected 0 users initially")
	}

	_, _ = auth.CreateUser("user1", "password123", nil)
	_, _ = auth.CreateUser("user2", "password123", nil)

	if auth.UserCount() != 2 {
		t.Errorf("expected 2 users, got %d", auth.UserCount())
	}

	_ = auth.DeleteUser("user1")

	if auth.UserCount() != 1 {
		t.Errorf("expected 1 user after delete, got %d", auth.UserCount())
	}
}

func TestHasPermission(t *testing.T) {
	user := &User{
		Roles: []Role{RoleEditor},
	}

	// Editor should have read, write, create, delete
	if !user.HasPermission(PermRead) {
		t.Error("editor should have read permission")
	}
	if !user.HasPermission(PermWrite) {
		t.Error("editor should have write permission")
	}
	if !user.HasPermission(PermCreate) {
		t.Error("editor should have create permission")
	}
	if !user.HasPermission(PermDelete) {
		t.Error("editor should have delete permission")
	}

	// Editor should NOT have admin permissions
	if user.HasPermission(PermAdmin) {
		t.Error("editor should not have admin permission")
	}
	if user.HasPermission(PermUserManage) {
		t.Error("editor should not have user_manage permission")
	}
}

func TestRoleFromString(t *testing.T) {
	tests := []struct {
		input   string
		want    Role
		wantErr bool
	}{
		{"admin", RoleAdmin, false},
		{"editor", RoleEditor, false},
		{"viewer", RoleViewer, false},
		{"none", RoleNone, false},
		{"invalid", RoleNone, true},
		{"ADMIN", RoleNone, true}, // Case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := RoleFromString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("RoleFromString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("RoleFromString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidRole(t *testing.T) {
	tests := []struct {
		role Role
		want bool
	}{
		{RoleAdmin, true},
		{RoleEditor, true},
		{RoleViewer, true},
		{RoleNone, true},
		{Role("invalid"), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			if got := ValidRole(tt.role); got != tt.want {
				t.Errorf("ValidRole() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHasCredentials(t *testing.T) {
	tests := []struct {
		name        string
		authHeader  string
		apiKey      string
		cookie      string
		queryToken  string
		queryAPIKey string
		want        bool
	}{
		{"no credentials", "", "", "", "", "", false},
		{"auth header", "Bearer token", "", "", "", "", true},
		{"api key header", "", "key123", "", "", "", true},
		{"cookie", "", "", "token", "", "", true},
		{"query token", "", "", "", "token", "", true},
		{"query api key", "", "", "", "", "key", true},
		{"multiple sources", "Bearer token", "key", "cookie", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HasCredentials(tt.authHeader, tt.apiKey, tt.cookie, tt.queryToken, tt.queryAPIKey)
			if got != tt.want {
				t.Errorf("HasCredentials() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractToken(t *testing.T) {
	tests := []struct {
		name        string
		authHeader  string
		apiKey      string
		cookie      string
		queryToken  string
		queryAPIKey string
		want        string
	}{
		{"no credentials", "", "", "", "", "", ""},
		{"auth header with Bearer", "Bearer mytoken", "", "", "", "", "mytoken"},
		{"auth header without Bearer", "mytoken", "", "", "", "", "mytoken"},
		{"api key header", "", "apikey123", "", "", "", "apikey123"},
		{"cookie", "", "", "cookietoken", "", "", "cookietoken"},
		{"query token", "", "", "", "querytoken", "", "querytoken"},
		{"query api key", "", "", "", "", "querykey", "querykey"},
		{"priority: auth header wins", "Bearer authtoken", "apikey", "cookie", "query", "querykey", "authtoken"},
		{"priority: api key over cookie", "", "apikey", "cookie", "query", "", "apikey"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractToken(tt.authHeader, tt.apiKey, tt.cookie, tt.queryToken, tt.queryAPIKey)
			if got != tt.want {
				t.Errorf("ExtractToken() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSecureCompare(t *testing.T) {
	if !SecureCompare("test", "test") {
		t.Error("equal strings should return true")
	}
	if SecureCompare("test", "Test") {
		t.Error("different strings should return false")
	}
	if SecureCompare("test", "testing") {
		t.Error("different length strings should return false")
	}
	if SecureCompare("", "a") {
		t.Error("empty vs non-empty should return false")
	}
	if !SecureCompare("", "") {
		t.Error("two empty strings should return true")
	}
}

func TestAuditLogging(t *testing.T) {
	auth := newTestAuthenticator(t)

	var events []AuditEvent
	auth.SetAuditLogger(func(e AuditEvent) {
		events = append(events, e)
	})

	// Create user should log
	_, _ = auth.CreateUser("audituser", "password123", nil)

	// Failed login should log
	_, _, _ = auth.Authenticate("audituser", "wrongpassword", "127.0.0.1", "TestAgent")

	// Successful login should log
	_, _, _ = auth.Authenticate("audituser", "password123", "127.0.0.1", "TestAgent")

	if len(events) < 3 {
		t.Errorf("expected at least 3 audit events, got %d", len(events))
	}

	// Check event types
	foundCreate := false
	foundFailedLogin := false
	foundSuccessLogin := false

	for _, e := range events {
		if e.EventType == "user_create" && e.Success {
			foundCreate = true
		}
		if e.EventType == "login" && !e.Success {
			foundFailedLogin = true
		}
		if e.EventType == "login" && e.Success {
			foundSuccessLogin = true
		}
	}

	if !foundCreate {
		t.Error("expected user_create event")
	}
	if !foundFailedLogin {
		t.Error("expected failed login event")
	}
	if !foundSuccessLogin {
		t.Error("expected successful login event")
	}
}

// Helper to create test authenticator
func newTestAuthenticator(t *testing.T) *Authenticator {
	t.Helper()
	config := AuthConfig{
		SecurityEnabled:   true,
		JWTSecret:         []byte("test-secret-at-least-32-bytes!!"),
		MinPasswordLength: 8,
		MaxFailedLogins:   5,
		LockoutDuration:   15 * time.Minute,
		BcryptCost:        4, // Low cost for faster tests
	}
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(config, memoryStorage)
	if err != nil {
		t.Fatalf("NewAuthenticator() error = %v", err)
	}
	return auth
}

// TestGenerateClusterToken tests cluster token generation
func TestGenerateClusterToken(t *testing.T) {
	auth := newTestAuthenticator(t)

	t.Run("generates valid admin token", func(t *testing.T) {
		token, err := auth.GenerateClusterToken("cluster-node-1", RoleAdmin)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		// Token should be a valid JWT
		parts := strings.Split(token, ".")
		if len(parts) != 3 {
			t.Errorf("expected JWT format (3 parts), got %d", len(parts))
		}

		// Validate the token
		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		if claims.Username != "cluster-node-1" {
			t.Errorf("expected username 'cluster-node-1', got %q", claims.Username)
		}

		if !strings.HasPrefix(claims.Sub, "cluster-") {
			t.Errorf("expected Sub to start with 'cluster-', got %q", claims.Sub)
		}

		if len(claims.Roles) != 1 || claims.Roles[0] != "admin" {
			t.Errorf("expected roles ['admin'], got %v", claims.Roles)
		}
	})

	t.Run("generates valid viewer token", func(t *testing.T) {
		token, err := auth.GenerateClusterToken("read-replica", RoleViewer)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		if len(claims.Roles) != 1 || claims.Roles[0] != "viewer" {
			t.Errorf("expected roles ['viewer'], got %v", claims.Roles)
		}
	})

	t.Run("generates valid editor token", func(t *testing.T) {
		token, err := auth.GenerateClusterToken("worker-node", RoleEditor)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		if len(claims.Roles) != 1 || claims.Roles[0] != "editor" {
			t.Errorf("expected roles ['editor'], got %v", claims.Roles)
		}
	})

	t.Run("different nodes get different tokens", func(t *testing.T) {
		token1, _ := auth.GenerateClusterToken("node-1", RoleAdmin)
		token2, _ := auth.GenerateClusterToken("node-2", RoleAdmin)

		if token1 == token2 {
			t.Error("different nodes should get different tokens")
		}
	})

	t.Run("token without expiry never expires", func(t *testing.T) {
		token, err := auth.GenerateClusterToken("no-expiry-node", RoleAdmin)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		// Default config has no expiry, so Exp should be 0
		if claims.Exp != 0 {
			t.Errorf("expected Exp = 0 (never expires), got %d", claims.Exp)
		}
	})

	t.Run("fails without JWT secret", func(t *testing.T) {
		memoryStorage := storage.NewMemoryEngine()
		noSecretAuth, _ := NewAuthenticator(AuthConfig{
			SecurityEnabled: false,
		}, memoryStorage)

		_, err := noSecretAuth.GenerateClusterToken("node", RoleAdmin)
		if err == nil {
			t.Error("expected error when generating token without secret")
		}
	})

	t.Run("logs audit event", func(t *testing.T) {
		var events []AuditEvent
		auth.SetAuditLogger(func(e AuditEvent) {
			events = append(events, e)
		})

		_, _ = auth.GenerateClusterToken("audited-node", RoleAdmin)

		found := false
		for _, e := range events {
			if e.EventType == "cluster_token_generated" && e.Username == "audited-node" {
				found = true
				if !e.Success {
					t.Error("expected Success=true")
				}
			}
		}

		if !found {
			t.Error("expected cluster_token_generated audit event")
		}
	})
}

// TestGenerateClusterTokenWithExpiry tests cluster token generation with custom expiry
func TestGenerateClusterTokenWithExpiry(t *testing.T) {
	auth := newTestAuthenticator(t)

	t.Run("generates token with 24h expiry", func(t *testing.T) {
		token, err := auth.GenerateClusterTokenWithExpiry("node-1", RoleAdmin, 24*time.Hour)
		if err != nil {
			t.Fatalf("GenerateClusterTokenWithExpiry() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		if claims.Exp == 0 {
			t.Error("expected Exp to be set")
		}

		// Check expiry is approximately 24 hours from now
		expectedExp := time.Now().Add(24 * time.Hour).Unix()
		diff := claims.Exp - expectedExp
		if diff < -60 || diff > 60 {
			t.Errorf("expected Exp around %d, got %d (diff: %d)", expectedExp, claims.Exp, diff)
		}
	})

	t.Run("generates token with short expiry", func(t *testing.T) {
		token, err := auth.GenerateClusterTokenWithExpiry("node-1", RoleAdmin, 1*time.Hour)
		if err != nil {
			t.Fatalf("GenerateClusterTokenWithExpiry() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		expectedExp := time.Now().Add(1 * time.Hour).Unix()
		diff := claims.Exp - expectedExp
		if diff < -60 || diff > 60 {
			t.Errorf("expected Exp around %d, got %d", expectedExp, claims.Exp)
		}
	})

	t.Run("zero expiry means never expires", func(t *testing.T) {
		token, err := auth.GenerateClusterTokenWithExpiry("node-1", RoleAdmin, 0)
		if err != nil {
			t.Fatalf("GenerateClusterTokenWithExpiry() error = %v", err)
		}

		claims, err := auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() error = %v", err)
		}

		if claims.Exp != 0 {
			t.Errorf("expected Exp = 0, got %d", claims.Exp)
		}
	})

	t.Run("short-lived token expires", func(t *testing.T) {
		// Generate token that expires in 1 second
		token, err := auth.GenerateClusterTokenWithExpiry("node-1", RoleAdmin, 1*time.Second)
		if err != nil {
			t.Fatalf("GenerateClusterTokenWithExpiry() error = %v", err)
		}

		// Should be valid immediately
		_, err = auth.ValidateToken(token)
		if err != nil {
			t.Fatalf("token should be valid immediately: %v", err)
		}

		// Wait for expiry
		time.Sleep(2 * time.Second)

		// Should be expired now
		_, err = auth.ValidateToken(token)
		if err == nil {
			t.Error("expected token to be expired")
		}
		if err != ErrSessionExpired {
			t.Errorf("expected ErrSessionExpired, got %v", err)
		}
	})

	t.Run("fails without JWT secret", func(t *testing.T) {
		memoryStorage := storage.NewMemoryEngine()
		noSecretAuth, _ := NewAuthenticator(AuthConfig{
			SecurityEnabled: false,
		}, memoryStorage)

		_, err := noSecretAuth.GenerateClusterTokenWithExpiry("node", RoleAdmin, 1*time.Hour)
		if err == nil {
			t.Error("expected error when generating token without secret")
		}
	})

	t.Run("logs audit event with expiry info", func(t *testing.T) {
		var events []AuditEvent
		auth.SetAuditLogger(func(e AuditEvent) {
			events = append(events, e)
		})

		_, _ = auth.GenerateClusterTokenWithExpiry("expiry-node", RoleAdmin, 7*24*time.Hour)

		found := false
		for _, e := range events {
			if e.EventType == "cluster_token_generated" && e.Username == "expiry-node" {
				found = true
				if !strings.Contains(e.Details, "expiry=") {
					t.Error("expected expiry info in details")
				}
			}
		}

		if !found {
			t.Error("expected cluster_token_generated audit event")
		}
	})
}

// TestClusterTokenCrossValidation tests that tokens generated with one authenticator
// can be validated by another using the same secret
func TestClusterTokenCrossValidation(t *testing.T) {
	sharedSecret := []byte("shared-cluster-secret-32-chars!!")

	memoryStorage1 := storage.NewMemoryEngine()
	auth1, _ := NewAuthenticator(AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       sharedSecret,
	}, memoryStorage1)

	memoryStorage2 := storage.NewMemoryEngine()
	auth2, _ := NewAuthenticator(AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       sharedSecret,
	}, memoryStorage2)

	t.Run("token from auth1 validates on auth2", func(t *testing.T) {
		token, err := auth1.GenerateClusterToken("node-from-auth1", RoleAdmin)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		claims, err := auth2.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() on auth2 failed: %v", err)
		}

		if claims.Username != "node-from-auth1" {
			t.Errorf("expected username 'node-from-auth1', got %q", claims.Username)
		}
	})

	t.Run("token from auth2 validates on auth1", func(t *testing.T) {
		token, err := auth2.GenerateClusterToken("node-from-auth2", RoleViewer)
		if err != nil {
			t.Fatalf("GenerateClusterToken() error = %v", err)
		}

		claims, err := auth1.ValidateToken(token)
		if err != nil {
			t.Fatalf("ValidateToken() on auth1 failed: %v", err)
		}

		if claims.Username != "node-from-auth2" {
			t.Errorf("expected username 'node-from-auth2', got %q", claims.Username)
		}
	})

	t.Run("token with different secret fails", func(t *testing.T) {
		memoryStorage := storage.NewMemoryEngine()
		rogueAuth, _ := NewAuthenticator(AuthConfig{
			SecurityEnabled: true,
			JWTSecret:       []byte("different-secret-not-trusted!!!"),
		}, memoryStorage)

		rogueToken, _ := rogueAuth.GenerateClusterToken("rogue-node", RoleAdmin)

		_, err := auth1.ValidateToken(rogueToken)
		if err == nil {
			t.Error("expected validation to fail with different secret")
		}
		if err != ErrInvalidToken {
			t.Errorf("expected ErrInvalidToken, got %v", err)
		}
	})
}

// TestClusterTokenRoles tests that roles are correctly preserved in tokens
func TestClusterTokenRoles(t *testing.T) {
	auth := newTestAuthenticator(t)

	roles := []Role{RoleAdmin, RoleEditor, RoleViewer, RoleNone}

	for _, role := range roles {
		t.Run(string(role), func(t *testing.T) {
			token, err := auth.GenerateClusterToken("test-node", role)
			if err != nil {
				t.Fatalf("GenerateClusterToken() error = %v", err)
			}

			claims, err := auth.ValidateToken(token)
			if err != nil {
				t.Fatalf("ValidateToken() error = %v", err)
			}

			if len(claims.Roles) != 1 {
				t.Errorf("expected 1 role, got %d", len(claims.Roles))
			}

			if claims.Roles[0] != string(role) {
				t.Errorf("expected role %q, got %q", role, claims.Roles[0])
			}
		})
	}
}
