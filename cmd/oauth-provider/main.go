// Package main provides a minimal local OAuth 2.0 provider for testing.
//
// This implements OAuth 2.0 Authorization Code Flow (RFC 6749):
//   - Authorization endpoint (user consent)
//   - Token exchange endpoint
//   - Userinfo endpoint
//   - OAuth 2.0 Discovery endpoint
//
// Usage:
//
//	go run ./cmd/oauth-provider
//	# or
//	go build -o oauth-provider ./cmd/oauth-provider
//	./oauth-provider
//
// The provider runs on http://localhost:8888 by default.
// Use the -port flag to change the port.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Configuration
const (
	defaultPort         = 8888
	defaultClientID     = "nornicdb-local-test"
	defaultClientSecret = "local-test-secret-123"
	defaultIssuer       = "http://localhost:8888"
)

// User represents a test user in the OAuth provider
type User struct {
	Sub               string   `json:"sub"`
	Email             string   `json:"email"`
	PreferredUsername string   `json:"preferred_username"`
	Roles             []string `json:"roles"`
	Password          string   `json:"-"` // Not included in JSON responses
}

// AuthCode represents a temporary authorization code
type AuthCode struct {
	ClientID    string
	RedirectURI string
	UserID      string
	ExpiresAt   time.Time
}

// AccessToken represents an access token
type AccessToken struct {
	UserID    string
	ExpiresAt time.Time
}

// OAuthProvider implements a minimal OAuth 2.0 provider
type OAuthProvider struct {
	mu           sync.RWMutex
	authCodes    map[string]*AuthCode
	accessTokens map[string]*AccessToken
	users        []*User
	clientID     string
	clientSecret string
	issuer       string
}

// NewOAuthProvider creates a new OAuth provider with default test users
func NewOAuthProvider(clientID, clientSecret, issuer string) *OAuthProvider {
	return &OAuthProvider{
		authCodes:    make(map[string]*AuthCode),
		accessTokens: make(map[string]*AccessToken),
		users: []*User{
			{
				Sub:               "user-001",
				Email:             "admin@localhost",
				PreferredUsername: "admin",
				Roles:             []string{"admin", "developer"},
				Password:          "admin123",
			},
			{
				Sub:               "user-002",
				Email:             "developer@localhost",
				PreferredUsername: "developer",
				Roles:             []string{"developer"},
				Password:          "dev123",
			},
			{
				Sub:               "user-003",
				Email:             "viewer@localhost",
				PreferredUsername: "viewer",
				Roles:             []string{"viewer"},
				Password:          "view123",
			},
		},
		clientID:     clientID,
		clientSecret: clientSecret,
		issuer:       issuer,
	}
}

// generateRandomToken generates a cryptographically secure random token
func generateRandomToken(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// handleAuthorize handles the OAuth 2.0 authorization endpoint
// GET /oauth2/v1/authorize
func (p *OAuthProvider) handleAuthorize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	responseType := r.URL.Query().Get("response_type")
	clientID := r.URL.Query().Get("client_id")
	redirectURI := r.URL.Query().Get("redirect_uri")
	state := r.URL.Query().Get("state")
	scope := r.URL.Query().Get("scope")

	log.Printf("[OAuth Provider] Authorization request: response_type=%s client_id=%s has_redirect=%t has_state=%t scope_len=%d",
		responseType, clientID, redirectURI != "", state != "", len(scope))

	if responseType != "code" {
		http.Error(w, "Unsupported response_type. Only \"code\" is supported.", http.StatusBadRequest)
		return
	}

	if clientID != p.clientID {
		http.Error(w, "Invalid client_id", http.StatusBadRequest)
		return
	}

	if redirectURI == "" {
		http.Error(w, "redirect_uri is required", http.StatusBadRequest)
		return
	}

	// Render consent form
	p.renderConsentForm(w, clientID, redirectURI, state, scope)
}

// renderConsentForm renders the HTML consent form
func (p *OAuthProvider) renderConsentForm(w http.ResponseWriter, clientID, redirectURI, state, scope string) {
	tmpl := `<!DOCTYPE html>
<html>
<head>
	<title>Local OAuth Provider - Login</title>
	<style>
		body { font-family: Arial, sans-serif; max-width: 500px; margin: 50px auto; padding: 20px; }
		h2 { color: #333; }
		form { background: #f5f5f5; padding: 20px; border-radius: 8px; }
		label { display: block; margin: 10px 0 5px; font-weight: bold; }
		input, select { width: 100%; padding: 8px; margin-bottom: 15px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
		button { background: #4CAF50; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; width: 100%; }
		button:hover { background: #45a049; }
		.info { background: #e3f2fd; padding: 15px; border-radius: 4px; margin-bottom: 20px; }
		.user-info { font-size: 12px; color: #666; margin-top: 10px; }
	</style>
</head>
<body>
	<h2>🔐 Local OAuth Provider</h2>
	<div class="info">
		<strong>Test OAuth Login</strong><br>
		Application: NornicDB<br>
		Redirect: {{.RedirectURI}}
	</div>
	<form method="POST" action="/oauth2/v1/authorize/consent">
		<input type="hidden" name="client_id" value="{{.ClientID}}">
		<input type="hidden" name="redirect_uri" value="{{.RedirectURI}}">
		<input type="hidden" name="state" value="{{.State}}">
		<input type="hidden" name="scope" value="{{.Scope}}">
		
		<label>Select Test User:</label>
		<select name="user_id" required>
			{{range .Users}}
			<option value="{{.Sub}}">
				{{.PreferredUsername}} ({{.Email}}) - Roles: {{.RolesString}}
			</option>
			{{end}}
		</select>
		
		<div class="user-info">
			<strong>Available users:</strong><br>
			{{range .Users}}
			• {{.PreferredUsername}} - [{{.RolesString}}]<br>
			{{end}}
		</div>
		
		<button type="submit">Authorize & Continue</button>
	</form>
</body>
</html>`

	type UserData struct {
		Sub               string
		PreferredUsername string
		Email             string
		RolesString       string
		Password          string
	}

	type TemplateData struct {
		ClientID    string
		RedirectURI string
		State       string
		Scope       string
		Users       []UserData
	}

	p.mu.RLock()
	users := make([]UserData, len(p.users))
	for i, u := range p.users {
		users[i] = UserData{
			Sub:               u.Sub,
			PreferredUsername: u.PreferredUsername,
			Email:             u.Email,
			RolesString:       strings.Join(u.Roles, ", "),
			Password:          u.Password,
		}
	}
	p.mu.RUnlock()

	data := TemplateData{
		ClientID:    clientID,
		RedirectURI: template.HTMLEscapeString(redirectURI),
		State:       template.HTMLEscapeString(state),
		Scope:       template.HTMLEscapeString(scope),
		Users:       users,
	}

	t, err := template.New("consent").Parse(tmpl)
	if err != nil {
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.Execute(w, data); err != nil {
		log.Printf("Error rendering template: %v", err)
		http.Error(w, "Failed to render template", http.StatusInternalServerError)
	}
}

// handleConsent handles user consent and generates authorization code
// POST /oauth2/v1/authorize/consent
func (p *OAuthProvider) handleConsent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	clientID := r.Form.Get("client_id")
	redirectURI := r.Form.Get("redirect_uri")
	state := r.Form.Get("state")
	userID := r.Form.Get("user_id")

	log.Printf("[OAuth Provider] User consent received: client_id=%s has_redirect=%t", clientID, redirectURI != "")

	// Generate authorization code
	authCode, err := generateRandomToken(32)
	if err != nil {
		http.Error(w, "Failed to generate authorization code", http.StatusInternalServerError)
		return
	}

	expiresAt := time.Now().Add(10 * time.Minute)

	p.mu.Lock()
	p.authCodes[authCode] = &AuthCode{
		ClientID:    clientID,
		RedirectURI: redirectURI,
		UserID:      userID,
		ExpiresAt:   expiresAt,
	}
	p.mu.Unlock()

	log.Printf("[OAuth Provider] Generated authorization code")

	// Redirect back to app with code
	redirectURL, err := url.Parse(redirectURI)
	if err != nil {
		http.Error(w, "Invalid redirect_uri", http.StatusBadRequest)
		return
	}

	redirectURL.RawQuery = url.Values{
		"code":  []string{authCode},
		"state": []string{state},
	}.Encode()

	// Validate redirect URL to prevent open redirect attacks
	// Only allow http://localhost and http://127.0.0.1 for local testing
	redirectHost := redirectURL.Hostname()
	if redirectHost != "localhost" && redirectHost != "127.0.0.1" && !strings.HasSuffix(redirectHost, ".localhost") {
		http.Error(w, "Invalid redirect_uri: only localhost is allowed for testing", http.StatusBadRequest)
		return
	}

	log.Printf("[OAuth Provider] Redirecting to validated localhost callback host=%s", redirectHost)
	http.Redirect(w, r, redirectURL.String(), http.StatusFound)
}

// handleToken handles the OAuth 2.0 token exchange endpoint
// POST /oauth2/v1/token
func (p *OAuthProvider) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Support both form-encoded and JSON
	var grantType, code, redirectURI, clientID, clientSecret string

	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var req struct {
			GrantType    string `json:"grant_type"`
			Code         string `json:"code"`
			RedirectURI  string `json:"redirect_uri"`
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		grantType = req.GrantType
		code = req.Code
		redirectURI = req.RedirectURI
		clientID = req.ClientID
		clientSecret = req.ClientSecret
	} else {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Invalid form data", http.StatusBadRequest)
			return
		}
		grantType = r.Form.Get("grant_type")
		code = r.Form.Get("code")
		redirectURI = r.Form.Get("redirect_uri")
		clientID = r.Form.Get("client_id")
		clientSecret = r.Form.Get("client_secret")
	}

	// Sanitize code in logs (never log full authorization codes or tokens)
	log.Printf("[OAuth Provider] Token request: grant_type=%s client_id=%s has_code=%t", grantType, clientID, code != "")

	if grantType != "authorization_code" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "unsupported_grant_type",
			"error_description": "Only authorization_code grant type is supported",
		})
		return
	}

	if clientID != p.clientID || clientSecret != p.clientSecret {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_client",
			"error_description": "Invalid client credentials",
		})
		return
	}

	p.mu.Lock()
	authData, exists := p.authCodes[code]
	if exists {
		delete(p.authCodes, code) // One-time use
	}
	p.mu.Unlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "Invalid or expired authorization code",
		})
		return
	}

	if time.Now().After(authData.ExpiresAt) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "Authorization code expired",
		})
		return
	}

	if authData.RedirectURI != redirectURI {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_grant",
			"error_description": "redirect_uri mismatch",
		})
		return
	}

	// Generate access token
	accessToken, err := generateRandomToken(32)
	if err != nil {
		http.Error(w, "Failed to generate access token", http.StatusInternalServerError)
		return
	}

	expiresIn := int64(3600) // 1 hour
	p.mu.Lock()
	p.accessTokens[accessToken] = &AccessToken{
		UserID:    authData.UserID,
		ExpiresAt: time.Now().Add(time.Duration(expiresIn) * time.Second),
	}
	p.mu.Unlock()

	log.Printf("[OAuth Provider] Issued access token for user: %s", authData.UserID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   expiresIn,
		"scope":        "openid profile email",
	})
}

// handleUserinfo handles the OAuth 2.0 userinfo endpoint
// GET /oauth2/v1/userinfo
func (p *OAuthProvider) handleUserinfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_token",
			"error_description": "Missing or invalid authorization header",
		})
		return
	}

	token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))

	p.mu.RLock()
	tokenData, exists := p.accessTokens[token]
	p.mu.RUnlock()

	if !exists {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_token",
			"error_description": "Invalid access token",
		})
		return
	}

	if time.Now().After(tokenData.ExpiresAt) {
		p.mu.Lock()
		delete(p.accessTokens, token)
		p.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "invalid_token",
			"error_description": "Access token expired",
		})
		return
	}

	p.mu.RLock()
	var user *User
	for _, u := range p.users {
		if u.Sub == tokenData.UserID {
			user = u
			break
		}
	}
	p.mu.RUnlock()

	if user == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error":             "server_error",
			"error_description": "User not found",
		})
		return
	}

	log.Printf("[OAuth Provider] Userinfo request for: %s", user.Email)

	// Return user profile without password
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"sub":                user.Sub,
		"email":              user.Email,
		"preferred_username": user.PreferredUsername,
		"roles":              user.Roles,
	})
}

// handleDiscovery handles the OAuth 2.0 Discovery endpoint
// GET /.well-known/oauth-authorization-server
func (p *OAuthProvider) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"issuer":                                p.issuer,
		"authorization_endpoint":                fmt.Sprintf("%s/oauth2/v1/authorize", p.issuer),
		"token_endpoint":                        fmt.Sprintf("%s/oauth2/v1/token", p.issuer),
		"userinfo_endpoint":                     fmt.Sprintf("%s/oauth2/v1/userinfo", p.issuer),
		"response_types_supported":              []string{"code"},
		"grant_types_supported":                 []string{"authorization_code"},
		"token_endpoint_auth_methods_supported": []string{"client_secret_post"},
	})
}

// handleHealth handles the health check endpoint
// GET /health
func (p *OAuthProvider) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	p.mu.RLock()
	userCount := len(p.users)
	p.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"users":  userCount,
	})
}

// cleanupExpiredTokens periodically removes expired tokens and codes
func (p *OAuthProvider) cleanupExpiredTokens() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		p.mu.Lock()
		for code, authCode := range p.authCodes {
			if now.After(authCode.ExpiresAt) {
				delete(p.authCodes, code)
			}
		}
		for token, accessToken := range p.accessTokens {
			if now.After(accessToken.ExpiresAt) {
				delete(p.accessTokens, token)
			}
		}
		p.mu.Unlock()
	}
}

func main() {
	port := flag.Int("port", defaultPort, "Port to listen on")
	clientID := flag.String("client-id", defaultClientID, "OAuth client ID")
	clientSecret := flag.String("client-secret", defaultClientSecret, "OAuth client secret")
	issuer := flag.String("issuer", defaultIssuer, "OAuth issuer URL")
	flag.Parse()

	provider := NewOAuthProvider(*clientID, *clientSecret, *issuer)

	// Start cleanup goroutine
	go provider.cleanupExpiredTokens()

	// Register routes
	http.HandleFunc("/oauth2/v1/authorize", provider.handleAuthorize)
	http.HandleFunc("/oauth2/v1/authorize/consent", provider.handleConsent)
	http.HandleFunc("/oauth2/v1/token", provider.handleToken)
	http.HandleFunc("/oauth2/v1/userinfo", provider.handleUserinfo)
	http.HandleFunc("/.well-known/oauth-authorization-server", provider.handleDiscovery)
	http.HandleFunc("/health", provider.handleHealth)

	addr := fmt.Sprintf(":%d", *port)

	// Build user list for output
	provider.mu.RLock()
	userList := make([]string, len(provider.users))
	for i, u := range provider.users {
		userList[i] = fmt.Sprintf("   • %s / %s - [%s]", u.PreferredUsername, u.Password, strings.Join(u.Roles, ", "))
	}
	provider.mu.RUnlock()

	// Output as single clean block for easy copy-paste
	output := fmt.Sprintf(`
🚀 Local OAuth Provider running on http://localhost%s

📋 To start NornicDB with OAuth, copy-paste the following:

   NORNICDB_AUTH_PROVIDER=oauth \
   NORNICDB_OAUTH_CLIENT_ID=%s \
   NORNICDB_OAUTH_CLIENT_SECRET=%s \
   NORNICDB_OAUTH_ISSUER=%s \
   NORNICDB_OAUTH_CALLBACK_URL=http://localhost:7474/auth/oauth/callback \
   ./bin/nornicdb serve

👥 Test Users:
%s

✅ Ready to accept OAuth requests!
`, addr, *clientID, "<REDACTED>", *issuer, strings.Join(userList, "\n"))

	fmt.Print(output)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
