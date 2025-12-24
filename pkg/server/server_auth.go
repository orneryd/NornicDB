package server

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// =============================================================================
// Authentication Handlers
// =============================================================================

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Parse request body
	var req struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		GrantType string `json:"grant_type"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// Support OAuth 2.0 password grant
	if req.GrantType != "" && req.GrantType != "password" {
		s.writeError(w, http.StatusBadRequest, "unsupported grant_type", ErrBadRequest)
		return
	}

	// Authenticate
	tokenResp, _, err := s.auth.Authenticate(
		req.Username,
		req.Password,
		getClientIP(r),
		r.UserAgent(),
	)

	if err != nil {
		status := http.StatusUnauthorized
		if err == auth.ErrAccountLocked {
			status = http.StatusTooManyRequests
		}
		s.writeError(w, status, err.Error(), ErrUnauthorized)
		return
	}

	// Set HTTP-only cookie for browser sessions (secure auth)
	http.SetCookie(w, &http.Cookie{
		Name:     "nornicdb_token",
		Value:    tokenResp.AccessToken,
		Path:     "/",
		HttpOnly: true,                 // Prevent XSS attacks
		Secure:   r.TLS != nil,         // Secure only over HTTPS
		SameSite: http.SameSiteLaxMode, // Lax allows normal navigation, prevents CSRF on POST
		MaxAge:   86400 * 7,            // 7 days
	})

	s.writeJSON(w, http.StatusOK, tokenResp)
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Clear the auth cookie
	http.SetCookie(w, &http.Cookie{
		Name:     "nornicdb_token",
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		MaxAge:   -1, // Delete cookie
	})

	// Audit the logout event
	claims := getClaims(r)
	if claims != nil {
		s.logAudit(r, claims.Sub, "logout", true, "")
	}

	s.writeJSON(w, http.StatusOK, map[string]string{"status": "logged out"})
}

// handleGenerateAPIToken generates a stateless API token for MCP servers.
// Only admins can generate these tokens. The tokens inherit the user's roles
// and are not stored - they are validated by signature on each request.
func (s *Server) handleGenerateAPIToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Get the authenticated user's claims
	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "not authenticated", ErrUnauthorized)
		return
	}

	// Check if user has admin role
	isAdmin := false
	for _, role := range claims.Roles {
		if role == "admin" {
			isAdmin = true
			break
		}
	}
	if !isAdmin {
		s.writeError(w, http.StatusForbidden, "admin role required to generate API tokens", ErrForbidden)
		return
	}

	// Parse request body
	var req struct {
		Subject   string `json:"subject"`    // Label for the token (e.g., "my-mcp-server")
		ExpiresIn string `json:"expires_in"` // Duration string (e.g., "24h", "7d", "365d", "0" for never)
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	if req.Subject == "" {
		req.Subject = "api-token"
	}

	// Parse expiry duration
	var expiry time.Duration
	if req.ExpiresIn != "" && req.ExpiresIn != "0" && req.ExpiresIn != "never" {
		// Handle special "d" suffix for days
		expiresIn := req.ExpiresIn
		if strings.HasSuffix(expiresIn, "d") {
			days, err := strconv.Atoi(strings.TrimSuffix(expiresIn, "d"))
			if err != nil {
				s.writeError(w, http.StatusBadRequest, "invalid expires_in format", ErrBadRequest)
				return
			}
			expiry = time.Duration(days) * 24 * time.Hour
		} else {
			var err error
			expiry, err = time.ParseDuration(expiresIn)
			if err != nil {
				s.writeError(w, http.StatusBadRequest, "invalid expires_in format (use: 1h, 24h, 7d, 365d, 0 for never)", ErrBadRequest)
				return
			}
		}
	}

	// Create a user object from claims for token generation
	roles := make([]auth.Role, len(claims.Roles))
	for i, r := range claims.Roles {
		roles[i] = auth.Role(r)
	}
	user := &auth.User{
		ID:       claims.Sub,
		Username: claims.Username,
		Email:    claims.Email,
		Roles:    roles,
	}

	// Generate the API token
	token, err := s.auth.GenerateAPIToken(user, req.Subject, expiry)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "failed to generate token", err)
		return
	}

	// Calculate expiration time for response
	var expiresAt *time.Time
	if expiry > 0 {
		t := time.Now().Add(expiry)
		expiresAt = &t
	}

	response := struct {
		Token     string     `json:"token"`
		Subject   string     `json:"subject"`
		ExpiresAt *time.Time `json:"expires_at,omitempty"`
		ExpiresIn int64      `json:"expires_in,omitempty"` // seconds
		Roles     []string   `json:"roles"`
	}{
		Token:     token,
		Subject:   req.Subject,
		ExpiresAt: expiresAt,
		Roles:     claims.Roles,
	}
	if expiry > 0 {
		response.ExpiresIn = int64(expiry.Seconds())
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleAuthConfig returns auth configuration for the UI
func (s *Server) handleAuthConfig(w http.ResponseWriter, r *http.Request) {
	config := struct {
		DevLoginEnabled bool `json:"devLoginEnabled"`
		SecurityEnabled bool `json:"securityEnabled"`
		OAuthProviders  []struct {
			Name        string `json:"name"`
			URL         string `json:"url"`
			DisplayName string `json:"displayName"`
		} `json:"oauthProviders"`
	}{
		DevLoginEnabled: true, // Dev login enabled for development convenience
		SecurityEnabled: s.auth != nil && s.auth.IsSecurityEnabled(),
		OAuthProviders: []struct {
			Name        string `json:"name"`
			URL         string `json:"url"`
			DisplayName string `json:"displayName"`
		}{},
	}

	// Check if OAuth is configured
	authProvider := os.Getenv("NORNICDB_AUTH_PROVIDER")
	if authProvider == "oauth" {
		issuer := os.Getenv("NORNICDB_OAUTH_ISSUER")
		if issuer != "" {
			// Add OAuth provider to the list
			config.OAuthProviders = append(config.OAuthProviders, struct {
				Name        string `json:"name"`
				URL         string `json:"url"`
				DisplayName string `json:"displayName"`
			}{
				Name:        "oauth",
				URL:         fmt.Sprintf("%s/auth/oauth/redirect", s.getBaseURL(r)),
				DisplayName: "OAuth",
			})
		}
	}

	s.writeJSON(w, http.StatusOK, config)
}

// getBaseURL returns the base URL for the server from the request
func (s *Server) getBaseURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	} else if forwardedProto := r.Header.Get("X-Forwarded-Proto"); forwardedProto != "" {
		scheme = forwardedProto
	}

	host := r.Host
	if host == "" {
		host = fmt.Sprintf("%s:%d", s.config.Address, s.config.Port)
	}

	basePath := s.config.BasePath
	if basePath == "" {
		basePath = r.Header.Get("X-Base-Path")
	}
	if basePath != "" && !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}

	return fmt.Sprintf("%s://%s%s", scheme, host, basePath)
}

// handleOAuthRedirect initiates the OAuth 2.0 authorization flow
// GET /auth/oauth/redirect
func (s *Server) handleOAuthRedirect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed", ErrMethodNotAllowed)
		return
	}

	if s.oauthManager == nil {
		s.writeError(w, http.StatusBadRequest, "OAuth not configured", ErrBadRequest)
		return
	}

	baseURL := s.getBaseURL(r)
	state, authURL, err := s.oauthManager.GenerateAuthURL(baseURL)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error(), ErrInternalError)
		return
	}

	log.Printf("OAuth redirect: Stored state in memory: %s (expires in 10 minutes)", state[:16]+"...")

	http.Redirect(w, r, authURL, http.StatusFound)
}

// handleOAuthCallback handles the OAuth 2.0 callback and exchanges code for token
// GET /auth/oauth/callback
func (s *Server) handleOAuthCallback(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed", ErrMethodNotAllowed)
		return
	}

	if s.oauthManager == nil {
		s.writeError(w, http.StatusBadRequest, "OAuth not configured", ErrBadRequest)
		return
	}

	code := r.URL.Query().Get("code")
	state := r.URL.Query().Get("state") // Go automatically URL-decodes query parameters
	errorParam := r.URL.Query().Get("error")

	if errorParam != "" {
		errorDesc := r.URL.Query().Get("error_description")
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("OAuth error: %s - %s", errorParam, errorDesc), ErrBadRequest)
		return
	}

	if code == "" {
		s.writeError(w, http.StatusBadRequest, "missing authorization code", ErrBadRequest)
		return
	}

	if state == "" {
		s.writeError(w, http.StatusBadRequest, "missing state parameter", ErrBadRequest)
		return
	}

	// Handle callback using OAuth manager
	user, token, _, err := s.oauthManager.HandleCallback(code, state)
	if err != nil {
		log.Printf("OAuth callback error: %v", err)
		s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
		return
	}

	tokenResponse := &auth.TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
	}

	// Set HTTP-only cookie for browser sessions
	http.SetCookie(w, &http.Cookie{
		Name:     "nornicdb_token",
		Value:    tokenResponse.AccessToken,
		Path:     "/",
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400 * 7, // 7 days
	})

	log.Printf("OAuth callback: authenticated user %s", user.Username)

	// Redirect to UI
	http.Redirect(w, r, "/", http.StatusFound)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed", ErrMethodNotAllowed)
		return
	}

	// If auth is disabled, return anonymous admin user
	if s.auth == nil || !s.auth.IsSecurityEnabled() {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":       "anonymous",
			"username": "anonymous",
			"roles":    []string{"admin"},
			"enabled":  true,
		})
		return
	}

	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "no user context", ErrUnauthorized)
		return
	}

	user, err := s.auth.GetUserByID(claims.Sub)
	if err != nil {
		s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
		return
	}

	// Enhance response with authentication method info
	response := map[string]interface{}{
		"id":         user.ID,
		"username":   user.Username,
		"email":      user.Email,
		"roles":      user.Roles,
		"created_at": user.CreatedAt,
		"updated_at": user.UpdatedAt,
		"last_login": user.LastLogin,
		"disabled":   user.Disabled,
		"metadata":   user.Metadata,
	}

	// Determine authentication method
	// Check metadata first, then infer from OAuth configuration
	authMethod := "password" // default
	if user.Metadata != nil {
		if method, ok := user.Metadata["auth_method"]; ok {
			authMethod = method
		}
	}

	// If OAuth is configured and user metadata indicates OAuth, or if we can't determine,
	// check if OAuth is the current auth provider
	if authMethod == "oauth" || (authMethod == "password" && os.Getenv("NORNICDB_AUTH_PROVIDER") == "oauth") {
		// If user has metadata indicating OAuth, or if OAuth is the only auth method configured,
		// mark as OAuth (this is a heuristic - in production you'd store this properly)
		if user.Metadata != nil && user.Metadata["auth_method"] == "oauth" {
			authMethod = "oauth"
		} else if os.Getenv("NORNICDB_AUTH_PROVIDER") == "oauth" {
			// If OAuth is the only auth provider, user is likely OAuth-authenticated
			// (This is a simplification - in practice you'd track this properly)
			authMethod = "oauth"
		}
	}

	response["auth_method"] = authMethod
	if authMethod == "oauth" {
		if issuer := os.Getenv("NORNICDB_OAUTH_ISSUER"); issuer != "" {
			response["oauth_provider"] = issuer
		}
	}

	s.writeJSON(w, http.StatusOK, response)
}

// handleChangePassword allows users to change their own password.
func (s *Server) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.writeError(w, http.StatusMethodNotAllowed, "POST required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Get authenticated user
	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "not authenticated", ErrUnauthorized)
		return
	}

	var req struct {
		OldPassword string `json:"old_password"`
		NewPassword string `json:"new_password"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// Get username from claims
	username := claims.Username
	if username == "" {
		// Fallback to subject if username not in claims
		user, err := s.auth.GetUserByID(claims.Sub)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		username = user.Username
	}

	// Change password
	if err := s.auth.ChangePassword(username, req.OldPassword, req.NewPassword); err != nil {
		if err == auth.ErrInvalidCredentials {
			s.writeError(w, http.StatusUnauthorized, "old password incorrect", ErrUnauthorized)
			return
		}
		s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
		return
	}

	s.logAudit(r, claims.Sub, "password_change", true, "user changed own password")
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "password changed"})
}

// handleUpdateProfile allows users to update their own profile information.
func (s *Server) handleUpdateProfile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		s.writeError(w, http.StatusMethodNotAllowed, "PUT required", ErrMethodNotAllowed)
		return
	}

	if s.auth == nil {
		s.writeError(w, http.StatusServiceUnavailable, "authentication not configured", nil)
		return
	}

	// Get authenticated user
	claims := getClaims(r)
	if claims == nil {
		s.writeError(w, http.StatusUnauthorized, "not authenticated", ErrUnauthorized)
		return
	}

	var req struct {
		Email    string            `json:"email,omitempty"`
		Metadata map[string]string `json:"metadata,omitempty"`
	}

	if err := s.readJSON(r, &req); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
		return
	}

	// Get username from claims
	username := claims.Username
	if username == "" {
		// Fallback to subject if username not in claims
		user, err := s.auth.GetUserByID(claims.Sub)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		username = user.Username
	}

	// Update user profile
	if err := s.auth.UpdateUser(username, req.Email, req.Metadata); err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
		return
	}

	s.logAudit(r, claims.Sub, "profile_update", true, "user updated own profile")
	s.writeJSON(w, http.StatusOK, map[string]string{"status": "profile updated"})
}

func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// List users
		users := s.auth.ListUsers()
		s.writeJSON(w, http.StatusOK, users)

	case http.MethodPost:
		// Create user
		var req struct {
			Username string   `json:"username"`
			Password string   `json:"password"`
			Roles    []string `json:"roles"`
		}

		if err := s.readJSON(r, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}

		roles := make([]auth.Role, len(req.Roles))
		for i, r := range req.Roles {
			roles[i] = auth.Role(r)
		}

		user, err := s.auth.CreateUser(req.Username, req.Password, roles)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
			return
		}

		s.writeJSON(w, http.StatusCreated, user)

	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

func (s *Server) handleUserByID(w http.ResponseWriter, r *http.Request) {
	username := strings.TrimPrefix(r.URL.Path, "/auth/users/")
	if username == "" {
		// Empty username - delegate to list users handler
		s.handleUsers(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		user, err := s.auth.GetUser(username)
		if err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		s.writeJSON(w, http.StatusOK, user)

	case http.MethodPut:
		var req struct {
			Roles    []string `json:"roles,omitempty"`
			Disabled *bool    `json:"disabled,omitempty"`
		}

		if err := s.readJSON(r, &req); err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid request body", ErrBadRequest)
			return
		}

		if len(req.Roles) > 0 {
			roles := make([]auth.Role, len(req.Roles))
			for i, r := range req.Roles {
				roles[i] = auth.Role(r)
			}
			if err := s.auth.UpdateRoles(username, roles); err != nil {
				s.writeError(w, http.StatusBadRequest, err.Error(), ErrBadRequest)
				return
			}
		}

		if req.Disabled != nil {
			if *req.Disabled {
				s.auth.DisableUser(username)
			} else {
				s.auth.EnableUser(username)
			}
		}

		s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})

	case http.MethodDelete:
		if err := s.auth.DeleteUser(username); err != nil {
			s.writeError(w, http.StatusNotFound, "user not found", ErrNotFound)
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})

	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET, PUT, or DELETE required", ErrMethodNotAllowed)
	}
}
