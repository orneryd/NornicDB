package server

import (
	"fmt"
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
		Secure:   isHTTPSRequest(r),    // Secure over direct TLS or trusted proxy TLS
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
		Secure:   isHTTPSRequest(r),
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

	// Check if user has admin role (use canonical role name from auth)
	isAdmin := false
	for _, role := range claims.Roles {
		if strings.ToLower(strings.TrimSpace(role)) == string(auth.RoleAdmin) {
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
	if isHTTPSRequest(r) {
		scheme = "https"
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

func isHTTPSRequest(r *http.Request) bool {
	if r == nil {
		return false
	}
	if r.TLS != nil {
		return true
	}

	// Support TLS termination behind reverse proxies.
	// X-Forwarded-Proto can be a comma-separated list; use first hop.
	forwardedProto := r.Header.Get("X-Forwarded-Proto")
	if forwardedProto == "" {
		return false
	}
	firstProto := strings.TrimSpace(strings.Split(forwardedProto, ",")[0])
	return strings.EqualFold(firstProto, "https")
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

	s.log.Info("oauth redirect: stored state in memory (expires in 10 minutes)",
		"subsystem", "oauth", "state_prefix", state[:16])

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
		s.log.Warn("oauth callback error", "subsystem", "oauth", "error", err)
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
		Secure:   isHTTPSRequest(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400 * 7, // 7 days
	})

	s.log.Info("oauth callback: authenticated user", "subsystem", "oauth", "user", user.Username)

	// Redirect to UI
	http.Redirect(w, r, "/", http.StatusFound)
}

func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed", ErrMethodNotAllowed)
		return
	}

	// If auth is disabled, return anonymous admin user (canonical role from auth)
	if s.auth == nil || !s.auth.IsSecurityEnabled() {
		s.writeJSON(w, http.StatusOK, map[string]interface{}{
			"id":       "anonymous",
			"username": "anonymous",
			"roles":    []string{string(auth.RoleAdmin)},
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

// handleEntitlements serves GET /auth/entitlements. Returns the canonical list of entitlements (global + per-database) for UI and docs.
func (s *Server) handleEntitlements(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.writeError(w, http.StatusMethodNotAllowed, "GET required", ErrMethodNotAllowed)
		return
	}
	s.writeJSON(w, http.StatusOK, auth.AllEntitlements())
}

// handleRoleEntitlements serves GET/PUT /auth/role-entitlements (per-role global entitlements, admin only).
// GET returns { role: string, entitlements: string[] }[] (all roles with effective entitlements).
// PUT accepts { role: string, entitlements: string[] } and sets that role's entitlements.
func (s *Server) handleRoleEntitlements(w http.ResponseWriter, r *http.Request) {
	if s.roleEntitlementsStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "Role entitlements are not configured (auth disabled or system DB unavailable).")
		return
	}
	switch r.Method {
	case http.MethodGet:
		var roles []string
		if s.roleStore != nil {
			roles = s.roleStore.AllRoles()
		}
		out := make([]struct {
			Role         string   `json:"role"`
			Entitlements []string `json:"entitlements"`
		}, 0, len(roles))
		for _, role := range roles {
			ent := auth.PermissionsForRole(role, s.roleEntitlementsStore)
			out = append(out, struct {
				Role         string   `json:"role"`
				Entitlements []string `json:"entitlements"`
			}{Role: role, Entitlements: ent})
		}
		s.writeJSON(w, http.StatusOK, out)
	case http.MethodPut:
		var body struct {
			Role         string   `json:"role"`
			Entitlements []string `json:"entitlements"`
			Mappings     []struct {
				Role         string   `json:"role"`
				Entitlements []string `json:"entitlements"`
			} `json:"mappings"`
		}
		if err := s.readJSON(r, &body); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
			return
		}
		validEntitlementIDs := make(map[string]struct{})
		for _, id := range auth.GlobalEntitlementIDs() {
			validEntitlementIDs[id] = struct{}{}
		}
		normalize := func(ids []string) ([]string, error) {
			out := make([]string, 0, len(ids))
			seen := make(map[string]struct{})
			for _, id := range ids {
				id = strings.ToLower(strings.TrimSpace(id))
				if id == "" {
					continue
				}
				if _, valid := validEntitlementIDs[id]; !valid {
					return nil, fmt.Errorf("invalid entitlement id %q; valid: %s", id, strings.Join(auth.GlobalEntitlementIDs(), ", "))
				}
				if _, ok := seen[id]; !ok {
					seen[id] = struct{}{}
					out = append(out, id)
				}
			}
			return out, nil
		}
		if len(body.Mappings) > 0 {
			for _, m := range body.Mappings {
				role := strings.ToLower(strings.TrimSpace(m.Role))
				if role == "" {
					continue
				}
				norm, err := normalize(m.Entitlements)
				if err != nil {
					s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", err.Error())
					return
				}
				if err := s.roleEntitlementsStore.Set(r.Context(), role, norm); err != nil {
					s.log.Warn("role entitlements set failed", "subsystem", "rbac", "role", role, "error", err)
					s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
					return
				}
			}
		} else if body.Role != "" {
			role := strings.ToLower(strings.TrimSpace(body.Role))
			norm, err := normalize(body.Entitlements)
			if err != nil {
				s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", err.Error())
				return
			}
			if err := s.roleEntitlementsStore.Set(r.Context(), role, norm); err != nil {
				s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
				return
			}
		} else {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "body must contain role and entitlements or mappings array")
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or PUT required", ErrMethodNotAllowed)
	}
}

// handleRoles serves GET/POST /auth/roles (user-defined roles, admin only).
// GET returns list of role names (built-in + user-defined). POST creates a user-defined role; body { "name": string }.
func (s *Server) handleRoles(w http.ResponseWriter, r *http.Request) {
	if s.roleStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "Roles are not configured (auth disabled or system DB unavailable).")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, s.roleStore.AllRoles())
	case http.MethodPost:
		var req struct {
			Name string `json:"name"`
		}
		if err := s.readJSON(r, &req); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
			return
		}
		if strings.TrimSpace(req.Name) == "" {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "name is required")
			return
		}
		if err := s.roleStore.CreateRole(r.Context(), req.Name); err != nil {
			if err == auth.ErrRoleExists {
				s.writeNeo4jError(w, http.StatusConflict, "Neo.ClientError.General.SchemaViolation", "role already exists")
				return
			}
			if err == auth.ErrInvalidRoleName {
				s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", err.Error())
				return
			}
			s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
			return
		}
		s.writeJSON(w, http.StatusCreated, map[string]string{"name": strings.ToLower(strings.TrimSpace(req.Name))})
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or POST required", ErrMethodNotAllowed)
	}
}

// handleRoleByID serves PATCH/DELETE /auth/roles/:name (rename or delete user-defined role, admin only).
func (s *Server) handleRoleByID(w http.ResponseWriter, r *http.Request) {
	roleName := strings.TrimPrefix(r.URL.Path, "/auth/roles/")
	roleName = strings.TrimSpace(roleName)
	if roleName == "" {
		s.handleRoles(w, r)
		return
	}
	if s.roleStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "Roles are not configured.")
		return
	}
	switch r.Method {
	case http.MethodPatch:
		var req struct {
			Name string `json:"name"`
		}
		if err := s.readJSON(r, &req); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
			return
		}
		newName := strings.TrimSpace(req.Name)
		if newName == "" {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "name is required")
			return
		}
		if err := s.roleStore.RenameRole(r.Context(), roleName, newName); err != nil {
			if err == auth.ErrCannotDeleteBuiltinRole || err == auth.ErrRoleNotFound {
				s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.SchemaViolation", err.Error())
				return
			}
			if err == auth.ErrRoleExists {
				s.writeNeo4jError(w, http.StatusConflict, "Neo.ClientError.General.SchemaViolation", "new role name already exists")
				return
			}
			s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
			return
		}
		if s.allowlistStore != nil {
			_ = s.allowlistStore.RenameRoleInAllowlist(r.Context(), roleName, newName)
		}
		if s.auth != nil {
			for _, u := range s.auth.ListUsers() {
				var hasOld bool
				for _, ur := range u.Roles {
					if strings.EqualFold(string(ur), roleName) {
						hasOld = true
						break
					}
				}
				if hasOld {
					newRoles := make([]auth.Role, 0, len(u.Roles))
					for _, ur := range u.Roles {
						if strings.EqualFold(string(ur), roleName) {
							newRoles = append(newRoles, auth.Role(newName))
						} else {
							newRoles = append(newRoles, ur)
						}
					}
					_ = s.auth.UpdateRoles(u.Username, newRoles)
				}
			}
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"name": newName})
	case http.MethodDelete:
		// Reject if any user has this role
		if s.auth != nil {
			for _, u := range s.auth.ListUsers() {
				for _, ur := range u.Roles {
					if strings.EqualFold(string(ur), roleName) {
						s.writeNeo4jError(w, http.StatusConflict, "Neo.ClientError.General.SchemaViolation", "cannot delete role: at least one user has this role")
						return
					}
				}
			}
		}
		if err := s.roleStore.DeleteRole(r.Context(), roleName); err != nil {
			if err == auth.ErrCannotDeleteBuiltinRole || err == auth.ErrRoleNotFound {
				s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.General.SchemaViolation", err.Error())
				return
			}
			s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "PATCH or DELETE required", ErrMethodNotAllowed)
	}
}

// handleAccessDatabases serves GET/PUT /auth/access/databases (per-database allowlist, admin only).
// GET returns { role: string, databases: string[] }[].
// PUT accepts { role: string, databases: string[] } or { mappings: [{ role, databases }] } and persists to system DB.
func (s *Server) handleAccessDatabases(w http.ResponseWriter, r *http.Request) {
	if s.allowlistStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "Database access allowlist is not configured (auth disabled or system DB unavailable).")
		return
	}

	switch r.Method {
	case http.MethodGet:
		allowlist := s.allowlistStore.Allowlist()
		out := make([]struct {
			Role      string   `json:"role"`
			Databases []string `json:"databases"`
		}, 0, len(allowlist))
		for role, dbs := range allowlist {
			out = append(out, struct {
				Role      string   `json:"role"`
				Databases []string `json:"databases"`
			}{Role: role, Databases: dbs})
		}
		s.writeJSON(w, http.StatusOK, out)

	case http.MethodPut:
		var req struct {
			Role      string   `json:"role"`
			Databases []string `json:"databases"`
			Mappings  *[]struct {
				Role      string   `json:"role"`
				Databases []string `json:"databases"`
			} `json:"mappings"`
		}
		if err := s.readJSON(r, &req); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
			return
		}
		if req.Mappings != nil {
			for _, m := range *req.Mappings {
				if err := s.allowlistStore.SaveRoleDatabases(r.Context(), m.Role, m.Databases); err != nil {
					s.log.Warn("allowlist save role databases failed", "subsystem", "rbac", "role", m.Role, "error", err)
					s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
					return
				}
			}
		} else {
			if req.Role == "" {
				s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "role is required")
				return
			}
			if err := s.allowlistStore.SaveRoleDatabases(r.Context(), req.Role, req.Databases); err != nil {
				s.log.Warn("allowlist save role databases failed", "subsystem", "rbac", "role", req.Role, "error", err)
				s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
				return
			}
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})

	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or PUT required", ErrMethodNotAllowed)
	}
}

// handleAccessPrivileges serves GET/PUT /auth/access/privileges (per-DB read/write, admin only, Phase 4).
// GET returns [{ role, database, read, write }, ...]. PUT accepts the same array and replaces the matrix.
func (s *Server) handleAccessPrivileges(w http.ResponseWriter, r *http.Request) {
	if s.privilegesStore == nil {
		s.writeNeo4jError(w, http.StatusServiceUnavailable, "Neo.ClientError.General.Unavailable", "Database access privileges are not configured (auth disabled or system DB unavailable).")
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, s.privilegesStore.Matrix())
	case http.MethodPut:
		var entries []struct {
			Role     string `json:"role"`
			Database string `json:"database"`
			Read     bool   `json:"read"`
			Write    bool   `json:"write"`
		}
		if err := s.readJSON(r, &entries); err != nil {
			s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
			return
		}
		if err := s.privilegesStore.PutMatrix(r.Context(), entries); err != nil {
			s.log.Warn("privileges PutMatrix failed", "subsystem", "rbac", "error", err)
			s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.General.UnknownError", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"status": "updated"})
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "GET or PUT required", ErrMethodNotAllowed)
	}
}
