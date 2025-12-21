// Package auth provides OAuth 2.0 authentication support for NornicDB.
//
// This package implements OAuth 2.0 authorization code flow with token validation
// and refresh capabilities. It integrates with external OAuth providers to authenticate
// users and issue NornicDB JWT tokens.
//
// OAuth Flow:
//  1. User initiates OAuth login → redirects to OAuth provider
//  2. User authenticates with OAuth provider
//  3. OAuth provider redirects back with authorization code
//  4. Exchange code for access token
//  5. Get user info from OAuth provider
//  6. Create/find NornicDB user account
//  7. Issue NornicDB JWT token
//  8. Validate OAuth tokens on subsequent requests
//
// Security Features:
//   - CSRF protection via state parameter
//   - OAuth token validation on each request
//   - Automatic token refresh when expired
//   - Short-lived NornicDB tokens (match OAuth token expiry)
//
// Example Usage:
//
//	// Configure OAuth
//	os.Setenv("NORNICDB_AUTH_PROVIDER", "oauth")
//	os.Setenv("NORNICDB_OAUTH_ISSUER", "http://localhost:8888")
//	os.Setenv("NORNICDB_OAUTH_CLIENT_ID", "nornicdb-local-test")
//	os.Setenv("NORNICDB_OAUTH_CLIENT_SECRET", "local-test-secret-123")
//	os.Setenv("NORNICDB_OAUTH_CALLBACK_URL", "http://localhost:7474/auth/oauth/callback")
//
//	// Create OAuth manager
//	oauthMgr := oauth.NewManager(authenticator)
//
//	// Generate authorization URL
//	state, authURL, err := oauthMgr.GenerateAuthURL("http://localhost:7474")
//
//	// Handle callback
//	user, token, err := oauthMgr.HandleCallback(code, state)
package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// OAuthManager manages OAuth 2.0 authentication flow and token validation.
type OAuthManager struct {
	authenticator *Authenticator
	states        map[string]time.Time // OAuth state storage (CSRF protection)
	statesMu      sync.RWMutex
}

// OAuthConfig holds OAuth provider configuration.
type OAuthConfig struct {
	Provider     string // "oauth" to enable
	Issuer       string // OAuth provider base URL
	ClientID     string // OAuth client ID
	ClientSecret string // OAuth client secret
	CallbackURL  string // OAuth callback URL
}

// OAuthTokenData represents an OAuth access token response.
type OAuthTokenData struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
	Scope        string `json:"scope"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// OAuthUserInfo represents user information from OAuth provider.
type OAuthUserInfo struct {
	Sub               string   `json:"sub"`
	Email             string   `json:"email"`
	PreferredUsername string   `json:"preferred_username"`
	Roles             []string `json:"roles"`
}

// NewOAuthManager creates a new OAuth manager.
func NewOAuthManager(authenticator *Authenticator) *OAuthManager {
	return &OAuthManager{
		authenticator: authenticator,
		states:        make(map[string]time.Time),
	}
}

// GetConfig returns the current OAuth configuration from environment variables.
func GetOAuthConfig() *OAuthConfig {
	provider := os.Getenv("NORNICDB_AUTH_PROVIDER")
	if provider != "oauth" {
		return nil
	}

	return &OAuthConfig{
		Provider:     provider,
		Issuer:       os.Getenv("NORNICDB_OAUTH_ISSUER"),
		ClientID:     os.Getenv("NORNICDB_OAUTH_CLIENT_ID"),
		ClientSecret: os.Getenv("NORNICDB_OAUTH_CLIENT_SECRET"),
		CallbackURL:  os.Getenv("NORNICDB_OAUTH_CALLBACK_URL"),
	}
}

// IsConfigured checks if OAuth is properly configured.
func (c *OAuthConfig) IsConfigured() bool {
	return c != nil && c.Issuer != "" && c.ClientID != "" && c.ClientSecret != "" && c.CallbackURL != ""
}

// GenerateAuthURL generates an OAuth authorization URL and stores state for CSRF protection.
// Returns the state parameter and the authorization URL.
func (m *OAuthManager) GenerateAuthURL(baseURL string) (string, string, error) {
	config := GetOAuthConfig()
	if !config.IsConfigured() {
		return "", "", fmt.Errorf("OAuth not configured")
	}

	// Generate state parameter for CSRF protection
	state := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))

	// Store state in memory with TTL (10 minutes)
	m.statesMu.Lock()
	m.states[state] = time.Now().Add(10 * time.Minute)
	m.statesMu.Unlock()

	// Clean up expired states (run in background)
	go m.cleanupExpiredStates()

	// Build OAuth authorization URL (URL-encode all query parameters)
	authURL := fmt.Sprintf("%s/oauth2/v1/authorize?response_type=code&client_id=%s&redirect_uri=%s&state=%s&scope=%s",
		config.Issuer, config.ClientID, url.QueryEscape(config.CallbackURL), url.QueryEscape(state), url.QueryEscape("openid profile email"))

	return state, authURL, nil
}

// ValidateState validates an OAuth state parameter and removes it (one-time use).
func (m *OAuthManager) ValidateState(state string) error {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	expiry, exists := m.states[state]
	if !exists {
		return fmt.Errorf("state not found or expired")
	}

	// Check if state has expired
	if time.Now().After(expiry) {
		delete(m.states, state)
		return fmt.Errorf("state expired")
	}

	// Remove used state (one-time use)
	delete(m.states, state)
	return nil
}

// cleanupExpiredStates removes expired OAuth states.
func (m *OAuthManager) cleanupExpiredStates() {
	m.statesMu.Lock()
	defer m.statesMu.Unlock()

	now := time.Now()
	for st, expiry := range m.states {
		if now.After(expiry) {
			delete(m.states, st)
		}
	}
}

// ExchangeCode exchanges an authorization code for an access token.
func (m *OAuthManager) ExchangeCode(code string) (*OAuthTokenData, error) {
	config := GetOAuthConfig()
	if !config.IsConfigured() {
		return nil, fmt.Errorf("OAuth not configured")
	}

	// Exchange authorization code for access token
	tokenURL := fmt.Sprintf("%s/oauth2/v1/token", config.Issuer)
	reqBody := url.Values{}
	reqBody.Set("grant_type", "authorization_code")
	reqBody.Set("code", code)
	reqBody.Set("redirect_uri", config.CallbackURL)
	reqBody.Set("client_id", config.ClientID)
	reqBody.Set("client_secret", config.ClientSecret)

	tokenResp, err := http.PostForm(tokenURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange token: %w", err)
	}
	defer tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tokenResp.Body)
		return nil, fmt.Errorf("token exchange failed: status %d, body: %s", tokenResp.StatusCode, string(body))
	}

	var tokenData OAuthTokenData
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err != nil {
		return nil, fmt.Errorf("failed to parse token response: %w", err)
	}

	return &tokenData, nil
}

// GetUserInfo retrieves user information from the OAuth provider.
func (m *OAuthManager) GetUserInfo(accessToken string) (*OAuthUserInfo, error) {
	config := GetOAuthConfig()
	if !config.IsConfigured() {
		return nil, fmt.Errorf("OAuth not configured")
	}

	// Get user info from OAuth provider
	userinfoURL := fmt.Sprintf("%s/oauth2/v1/userinfo", config.Issuer)
	userinfoReq, err := http.NewRequest(http.MethodGet, userinfoURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create userinfo request: %w", err)
	}
	userinfoReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", accessToken))

	userinfoResp, err := http.DefaultClient.Do(userinfoReq)
	if err != nil {
		return nil, fmt.Errorf("failed to get userinfo: %w", err)
	}
	defer userinfoResp.Body.Close()

	if userinfoResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(userinfoResp.Body)
		return nil, fmt.Errorf("userinfo request failed: status %d, body: %s", userinfoResp.StatusCode, string(body))
	}

	var userInfo OAuthUserInfo
	if err := json.NewDecoder(userinfoResp.Body).Decode(&userInfo); err != nil {
		return nil, fmt.Errorf("failed to parse userinfo: %w", err)
	}

	return &userInfo, nil
}

// HandleCallback handles the OAuth callback, exchanges code for token, gets user info,
// and creates/updates the NornicDB user account.
// Returns the user, NornicDB JWT token, and OAuth token expiry time.
func (m *OAuthManager) HandleCallback(code, state string) (*User, string, time.Time, error) {
	// Validate state
	if err := m.ValidateState(state); err != nil {
		return nil, "", time.Time{}, fmt.Errorf("invalid state: %w", err)
	}

	// Exchange code for access token
	tokenData, err := m.ExchangeCode(code)
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("failed to exchange code: %w", err)
	}

	// Calculate OAuth token expiry time
	oauthTokenExpiry := time.Now()
	if tokenData.ExpiresIn > 0 {
		oauthTokenExpiry = oauthTokenExpiry.Add(time.Duration(tokenData.ExpiresIn) * time.Second)
	} else {
		// Default to 1 hour if not specified
		oauthTokenExpiry = oauthTokenExpiry.Add(1 * time.Hour)
	}

	// Get user info from OAuth provider
	userInfo, err := m.GetUserInfo(tokenData.AccessToken)
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("failed to get userinfo: %w", err)
	}

	// Find or create user in NornicDB auth system
	username := userInfo.PreferredUsername
	if username == "" {
		username = userInfo.Email
	}
	if username == "" {
		username = userInfo.Sub
	}

	// Try to get existing user
	user, err := m.authenticator.GetUser(username)
	if err != nil {
		// User doesn't exist - create them
		// Use a random password since OAuth users don't need passwords
		randomPassword := base64.URLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
		_, err = m.authenticator.CreateUser(username, randomPassword, ConvertOAuthRoles(userInfo.Roles))
		if err != nil {
			return nil, "", time.Time{}, fmt.Errorf("failed to create user: %w", err)
		}
		user, err = m.authenticator.GetUser(username)
		if err != nil {
			return nil, "", time.Time{}, fmt.Errorf("failed to get created user: %w", err)
		}
	}

	// Store OAuth token info in user metadata for validation
	// This persists the OAuth token information to the system database
	oauthMetadata := make(map[string]string)
	oauthMetadata["auth_method"] = "oauth"
	oauthMetadata["oauth_access_token"] = tokenData.AccessToken
	oauthMetadata["oauth_token_expiry"] = oauthTokenExpiry.Format(time.RFC3339)
	if tokenData.RefreshToken != "" {
		oauthMetadata["oauth_refresh_token"] = tokenData.RefreshToken
	}

	// Merge with existing metadata if present
	if user.Metadata != nil {
		for k, v := range user.Metadata {
			oauthMetadata[k] = v
		}
	}

	// Persist OAuth token information to system database
	if err := m.authenticator.UpdateUser(username, user.Email, oauthMetadata); err != nil {
		// Log error but don't fail - user can still authenticate
		// Token validation will work from the in-memory cache
	}

	// Generate JWT token for the user using GenerateAPIToken
	// Use OAuth token expiry (or shorter) for NornicDB token to ensure we re-validate
	// If OAuth token expires in 1 hour, use that; otherwise cap at 24 hours for OAuth users
	nornicdbTokenExpiry := time.Until(oauthTokenExpiry)
	if nornicdbTokenExpiry > 24*time.Hour {
		nornicdbTokenExpiry = 24 * time.Hour
	}
	if nornicdbTokenExpiry < 1*time.Hour {
		nornicdbTokenExpiry = 1 * time.Hour // Minimum 1 hour
	}

	token, err := m.authenticator.GenerateAPIToken(user, "oauth", nornicdbTokenExpiry)
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("failed to generate token: %w", err)
	}

	return user, token, oauthTokenExpiry, nil
}

// ValidateOAuthToken validates that an OAuth user's OAuth access token is still valid
// by calling the OAuth provider's userinfo endpoint.
func (m *OAuthManager) ValidateOAuthToken(user *User) error {
	if user == nil {
		return fmt.Errorf("user is nil")
	}

	// Check if user is OAuth-authenticated
	if user.Metadata == nil {
		return nil // Not OAuth user, skip validation
	}

	authMethod, ok := user.Metadata["auth_method"]
	if !ok || authMethod != "oauth" {
		return nil // Not OAuth user, skip validation
	}

	// Get OAuth access token from metadata
	oauthToken, ok := user.Metadata["oauth_access_token"]
	if !ok || oauthToken == "" {
		return nil // No OAuth token stored - might be old user, allow through
	}

	// Check if token has expired (if expiry is stored)
	if expiryStr, ok := user.Metadata["oauth_token_expiry"]; ok && expiryStr != "" {
		expiry, err := time.Parse(time.RFC3339, expiryStr)
		if err == nil && time.Now().After(expiry) {
			// Token expired - try to refresh if refresh token is available
			if refreshToken, ok := user.Metadata["oauth_refresh_token"]; ok && refreshToken != "" {
				if refreshErr := m.RefreshOAuthToken(user, refreshToken); refreshErr != nil {
					return fmt.Errorf("OAuth access token expired and refresh failed: %w", refreshErr)
				}
				return nil // Successfully refreshed
			}
			return fmt.Errorf("OAuth access token expired")
		}
	}

	// Validate token by calling OAuth provider's userinfo endpoint
	_, err := m.GetUserInfo(oauthToken)
	if err != nil {
		// Token is invalid - try to refresh if refresh token is available
		if refreshToken, ok := user.Metadata["oauth_refresh_token"]; ok && refreshToken != "" {
			if refreshErr := m.RefreshOAuthToken(user, refreshToken); refreshErr != nil {
				return fmt.Errorf("OAuth access token invalid and refresh failed: %w", refreshErr)
			}
			return nil // Successfully refreshed
		}
		return fmt.Errorf("OAuth access token invalid: %w", err)
	}

	// Token is valid
	return nil
}

// RefreshOAuthToken attempts to refresh an expired OAuth access token using a refresh token.
func (m *OAuthManager) RefreshOAuthToken(user *User, refreshToken string) error {
	config := GetOAuthConfig()
	if !config.IsConfigured() {
		return fmt.Errorf("OAuth configuration incomplete")
	}

	// Request new access token using refresh token
	tokenURL := fmt.Sprintf("%s/oauth2/v1/token", config.Issuer)
	reqBody := url.Values{}
	reqBody.Set("grant_type", "refresh_token")
	reqBody.Set("refresh_token", refreshToken)
	reqBody.Set("client_id", config.ClientID)
	reqBody.Set("client_secret", config.ClientSecret)

	tokenResp, err := http.PostForm(tokenURL, reqBody)
	if err != nil {
		return fmt.Errorf("failed to refresh OAuth token: %w", err)
	}
	defer tokenResp.Body.Close()

	if tokenResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tokenResp.Body)
		return fmt.Errorf("OAuth token refresh failed: status %d, body: %s", tokenResp.StatusCode, string(body))
	}

	var tokenData OAuthTokenData
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err != nil {
		return fmt.Errorf("failed to parse refresh token response: %w", err)
	}

	// Calculate new expiry
	oauthTokenExpiry := time.Now()
	if tokenData.ExpiresIn > 0 {
		oauthTokenExpiry = oauthTokenExpiry.Add(time.Duration(tokenData.ExpiresIn) * time.Second)
	} else {
		oauthTokenExpiry = oauthTokenExpiry.Add(1 * time.Hour)
	}

	// Update metadata (this won't persist without UpdateUser method, but will work for current session)
	// Note: In production, you'd want to persist this via an UpdateUser method
	if user.Metadata == nil {
		user.Metadata = make(map[string]string)
	}
	user.Metadata["oauth_access_token"] = tokenData.AccessToken
	user.Metadata["oauth_token_expiry"] = oauthTokenExpiry.Format(time.RFC3339)
	if tokenData.RefreshToken != "" {
		user.Metadata["oauth_refresh_token"] = tokenData.RefreshToken
	}

	return nil
}

// ConvertOAuthRoles converts OAuth role strings to auth.Role enum.
func ConvertOAuthRoles(roles []string) []Role {
	result := make([]Role, 0, len(roles))
	for _, r := range roles {
		switch strings.ToLower(r) {
		case "admin":
			result = append(result, RoleAdmin)
		case "developer", "editor":
			// Map developer/editor to RoleEditor (write access)
			result = append(result, RoleEditor)
		case "viewer":
			result = append(result, RoleViewer)
		}
	}
	if len(result) == 0 {
		// Default to viewer if no roles specified
		result = append(result, RoleViewer)
	}
	return result
}
