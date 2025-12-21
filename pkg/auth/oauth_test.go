// Package auth provides tests for OAuth 2.0 authentication.
package auth

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestOAuthConfig tests OAuth configuration loading.
func TestOAuthConfig(t *testing.T) {
	t.Run("not configured", func(t *testing.T) {
		os.Unsetenv("NORNICDB_AUTH_PROVIDER")
		config := GetOAuthConfig()
		require.Nil(t, config)
	})

	t.Run("configured but incomplete", func(t *testing.T) {
		os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
		os.Unsetenv("NORNICDB_OAUTH_ISSUER")
		os.Unsetenv("NORNICDB_OAUTH_CLIENT_ID")
		os.Unsetenv("NORNICDB_OAUTH_CLIENT_SECRET")
		os.Unsetenv("NORNICDB_OAUTH_CALLBACK_URL")

		config := GetOAuthConfig()
		require.NotNil(t, config)
		require.False(t, config.IsConfigured())
	})

	t.Run("fully configured", func(t *testing.T) {
		os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
		os.Setenv("NORNICDB_OAUTH_ISSUER", "http://localhost:8888")
		os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
		os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
		os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

		config := GetOAuthConfig()
		require.NotNil(t, config)
		require.True(t, config.IsConfigured())
		require.Equal(t, "oauth", config.Provider)
		require.Equal(t, "http://localhost:8888", config.Issuer)
		require.Equal(t, "test-client", config.ClientID)
		require.Equal(t, "test-secret", config.ClientSecret)
		require.Equal(t, "http://localhost:7474/auth/oauth/callback", config.CallbackURL)
	})
}

// TestOAuthManager_GenerateAuthURL tests OAuth authorization URL generation.
func TestOAuthManager_GenerateAuthURL(t *testing.T) {
	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	os.Setenv("NORNICDB_OAUTH_ISSUER", "http://localhost:8888")
	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	authConfig := DefaultAuthConfig()
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(authConfig, memoryStorage)
	require.NoError(t, err)

	manager := NewOAuthManager(auth)

	state, authURL, err := manager.GenerateAuthURL("http://localhost:7474")
	require.NoError(t, err)
	require.NotEmpty(t, state)
	require.Contains(t, authURL, "http://localhost:8888/oauth2/v1/authorize")
	require.Contains(t, authURL, "client_id=test-client")
	require.Contains(t, authURL, "state=")
	require.Contains(t, authURL, "redirect_uri=")
}

// TestOAuthManager_ValidateState tests OAuth state validation.
func TestOAuthManager_ValidateState(t *testing.T) {
	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	os.Setenv("NORNICDB_OAUTH_ISSUER", "http://localhost:8888")
	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	authConfig := DefaultAuthConfig()
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(authConfig, memoryStorage)
	require.NoError(t, err)

	manager := NewOAuthManager(auth)

	// Generate a state
	state, _, err := manager.GenerateAuthURL("http://localhost:7474")
	require.NoError(t, err)

	// Validate it
	err = manager.ValidateState(state)
	require.NoError(t, err)

	// Try to validate again (should fail - one-time use)
	err = manager.ValidateState(state)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// Try invalid state
	err = manager.ValidateState("invalid-state")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// TestOAuthManager_ExchangeCode tests OAuth code exchange.
func TestOAuthManager_ExchangeCode(t *testing.T) {
	// Create mock OAuth provider server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/v1/token" {
			// Validate request
			if r.Method != "POST" {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}

			// Return token response
			tokenResp := OAuthTokenData{
				AccessToken:  "test-access-token",
				TokenType:    "Bearer",
				ExpiresIn:    3600,
				Scope:        "openid profile email",
				RefreshToken: "test-refresh-token",
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(tokenResp)
		}
	}))
	defer server.Close()

	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	os.Setenv("NORNICDB_OAUTH_ISSUER", server.URL)
	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	authConfig := DefaultAuthConfig()
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(authConfig, memoryStorage)
	require.NoError(t, err)

	manager := NewOAuthManager(auth)

	tokenData, err := manager.ExchangeCode("test-code")
	require.NoError(t, err)
	require.NotNil(t, tokenData)
	require.Equal(t, "test-access-token", tokenData.AccessToken)
	require.Equal(t, "Bearer", tokenData.TokenType)
	require.Equal(t, int64(3600), tokenData.ExpiresIn)
	require.Equal(t, "test-refresh-token", tokenData.RefreshToken)
}

// TestOAuthManager_GetUserInfo tests OAuth userinfo retrieval.
func TestOAuthManager_GetUserInfo(t *testing.T) {
	// Create mock OAuth provider server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/v1/userinfo" {
			// Validate authorization header
			authHeader := r.Header.Get("Authorization")
			if authHeader != "Bearer test-access-token" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Return userinfo
			userInfo := OAuthUserInfo{
				Sub:               "user-001",
				Email:             "test@example.com",
				PreferredUsername: "testuser",
				Roles:             []string{"admin", "developer"},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(userInfo)
		}
	}))
	defer server.Close()

	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	os.Setenv("NORNICDB_OAUTH_ISSUER", server.URL)
	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	authConfig := DefaultAuthConfig()
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(authConfig, memoryStorage)
	require.NoError(t, err)

	manager := NewOAuthManager(auth)

	userInfo, err := manager.GetUserInfo("test-access-token")
	require.NoError(t, err)
	require.NotNil(t, userInfo)
	require.Equal(t, "user-001", userInfo.Sub)
	require.Equal(t, "test@example.com", userInfo.Email)
	require.Equal(t, "testuser", userInfo.PreferredUsername)
	require.Equal(t, []string{"admin", "developer"}, userInfo.Roles)
}

// TestOAuthManager_ValidateOAuthToken tests OAuth token validation.
func TestOAuthManager_ValidateOAuthToken(t *testing.T) {
	// Create mock OAuth provider server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth2/v1/userinfo" {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "Bearer valid-token" {
				userInfo := OAuthUserInfo{
					Sub:               "user-001",
					Email:             "test@example.com",
					PreferredUsername: "testuser",
					Roles:             []string{"admin"},
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(userInfo)
			} else {
				w.WriteHeader(http.StatusUnauthorized)
			}
		}
	}))
	defer server.Close()

	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
	os.Setenv("NORNICDB_OAUTH_ISSUER", server.URL)
	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "test-client")
	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "test-secret")
	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")

	authConfig := DefaultAuthConfig()
	authConfig.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	memoryStorage := storage.NewMemoryEngine()
	auth, err := NewAuthenticator(authConfig, memoryStorage)
	require.NoError(t, err)

	manager := NewOAuthManager(auth)

	t.Run("valid token", func(t *testing.T) {
		user := &User{
			ID:       "usr-001",
			Username: "testuser",
			Metadata: map[string]string{
				"auth_method":        "oauth",
				"oauth_access_token": "valid-token",
				"oauth_token_expiry": time.Now().Add(1 * time.Hour).Format(time.RFC3339),
			},
		}

		err := manager.ValidateOAuthToken(user)
		require.NoError(t, err)
	})

	t.Run("expired token", func(t *testing.T) {
		user := &User{
			ID:       "usr-001",
			Username: "testuser",
			Metadata: map[string]string{
				"auth_method":        "oauth",
				"oauth_access_token": "valid-token",
				"oauth_token_expiry": time.Now().Add(-1 * time.Hour).Format(time.RFC3339), // Expired
			},
		}

		err := manager.ValidateOAuthToken(user)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expired")
	})

	t.Run("invalid token", func(t *testing.T) {
		user := &User{
			ID:       "usr-001",
			Username: "testuser",
			Metadata: map[string]string{
				"auth_method":        "oauth",
				"oauth_access_token": "invalid-token",
				"oauth_token_expiry": time.Now().Add(1 * time.Hour).Format(time.RFC3339),
			},
		}

		err := manager.ValidateOAuthToken(user)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("non-oauth user", func(t *testing.T) {
		user := &User{
			ID:       "usr-001",
			Username: "testuser",
			Metadata: map[string]string{
				"auth_method": "password",
			},
		}

		err := manager.ValidateOAuthToken(user)
		require.NoError(t, err) // Should skip validation for non-OAuth users
	})
}

// TestConvertOAuthRoles tests OAuth role conversion.
func TestConvertOAuthRoles(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []Role
	}{
		{
			name:     "admin role",
			input:    []string{"admin"},
			expected: []Role{RoleAdmin},
		},
		{
			name:     "developer role",
			input:    []string{"developer"},
			expected: []Role{RoleEditor},
		},
		{
			name:     "editor role",
			input:    []string{"editor"},
			expected: []Role{RoleEditor},
		},
		{
			name:     "viewer role",
			input:    []string{"viewer"},
			expected: []Role{RoleViewer},
		},
		{
			name:     "multiple roles",
			input:    []string{"admin", "developer", "viewer"},
			expected: []Role{RoleAdmin, RoleEditor, RoleViewer},
		},
		{
			name:     "unknown role defaults to viewer",
			input:    []string{"unknown"},
			expected: []Role{RoleViewer},
		},
		{
			name:     "empty roles defaults to viewer",
			input:    []string{},
			expected: []Role{RoleViewer},
		},
		{
			name:     "case insensitive",
			input:    []string{"ADMIN", "Developer", "VIEWER"},
			expected: []Role{RoleAdmin, RoleEditor, RoleViewer},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertOAuthRoles(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
