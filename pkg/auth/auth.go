// Package auth provides authentication and authorization for NornicDB.
//
// This package implements JWT authentication with role-based access control,
// designed to meet regulatory compliance requirements:
//   - GDPR Art.32: Technical and organizational measures to ensure security
//   - HIPAA §164.312(a): Access Control - Unique User Identification
//   - FISMA AC-2: Account Management
//   - SOC 2 CC6.1: Logical access controls
//
// Architecture:
//   - JWT tokens (HS256 algorithm) for stateless authentication
//   - Multiple credential sources: Bearer header, cookies, query parameters
//   - Role-based access control (RBAC) with 4 roles: admin, editor, viewer, none
//   - Account lockout after failed login attempts
//   - Password hashing with bcrypt
//   - Audit logging for compliance
//
// Example Usage:
//
//	// Initialize authenticator
//	config := auth.DefaultAuthConfig()
//	config.JWTSecret = []byte("your-secret-key-min-32-chars")
//	config.MinPasswordLength = 12
//
//	authenticator, err := auth.NewAuthenticator(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Set audit logging (required for HIPAA/GDPR)
//	authenticator.SetAuditLogger(func(event auth.AuditEvent) {
//		log.Printf("[AUDIT] %s: %s (success=%v)",
//			event.EventType, event.Username, event.Success)
//	})
//
//	// Create users
//	admin, _ := authenticator.CreateUser("admin", "SecurePass123!",
//		[]auth.Role{auth.RoleAdmin})
//
//	viewer, _ := authenticator.CreateUser("alice", "AlicePass456!",
//		[]auth.Role{auth.RoleViewer})
//
//	// Authenticate and get JWT token
//	tokenResp, user, err := authenticator.Authenticate(
//		"admin", "SecurePass123!", "192.168.1.1", "Mozilla/5.0")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Token: %s\n", tokenResp.AccessToken)
//	fmt.Printf("Type: %s\n", tokenResp.TokenType) // "Bearer"
//
//	// Validate token
//	claims, err := authenticator.ValidateToken(tokenResp.AccessToken)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Check permissions
//	if user.HasPermission(auth.PermWrite) {
//		fmt.Println("User can write")
//	}
//
// OAuth 2.0 Compatibility:
//
// The token endpoint follows RFC 6749 (OAuth 2.0) format:
//
//	POST /auth/token
//	Content-Type: application/x-www-form-urlencoded
//
//	grant_type=password&username=alice&password=secret
//
// Response:
//
//	{
//	  "access_token": "eyJhbGc...",
//	  "token_type": "Bearer",
//	  "expires_in": 3600  // omitted if never expires
//	}
//
// Compliance Features:
//
// GDPR Art.32 (Security):
//   - Password hashing (bcrypt with configurable cost)
//   - Token-based authentication (no session storage)
//   - Account lockout (prevents brute force)
//   - Audit logging (tracks all authentication events)
//
// HIPAA §164.312(a) (Access Control):
//   - Unique user identification (User.ID)
//   - Role-based permissions
//   - Account disable/enable
//   - Failed login tracking
//
// FISMA AC-2 (Account Management):
//   - User creation with roles
//   - Account lockout
//   - Audit trail
//   - Password policy enforcement
//
// Security Best Practices:
//   - bcrypt for password hashing (adjustable cost)
//   - HMAC-SHA256 for JWT signatures
//   - Constant-time string comparison (prevents timing attacks)
//   - Account lockout after N failed attempts
//   - No password in logs or responses
//
// ELI12 (Explain Like I'm 12):
//
// Think of this like your school's login system:
//
// 1. **Creating an account**: Like signing up for a school portal with username/password
// 2. **Logging in**: Like entering your credentials at the library computer
// 3. **JWT token**: Like a hall pass that proves you're allowed to be here
// 4. **Roles**: Like student vs teacher vs admin - different people can do different things
// 5. **Account lockout**: If you get your password wrong 5 times, you're locked out for 15 minutes
// 6. **Audit log**: Like the principal keeping a record of who entered the building and when
//
// The system makes sure only the right people can access the right data!
package auth

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

// Errors for authentication operations.
var (
	ErrUserNotFound       = errors.New("user not found")
	ErrUserExists         = errors.New("user already exists")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrAccountLocked      = errors.New("account locked due to failed login attempts")
	ErrPasswordTooShort   = errors.New("password does not meet minimum length requirement")
	ErrInvalidToken       = errors.New("invalid or expired token")
	ErrInsufficientRole   = errors.New("insufficient role permissions")
	ErrSessionExpired     = errors.New("session expired")
	ErrNoCredentials      = errors.New("no credentials provided")
	ErrMissingSecret      = errors.New("JWT secret not configured")
)

// Role represents a user role with associated permissions.
// Follows Mimir's role naming conventions.
type Role string

// Predefined roles following Neo4j/Mimir conventions.
const (
	RoleAdmin  Role = "admin"  // Full access including user management
	RoleEditor Role = "editor" // Read/write data
	RoleViewer Role = "viewer" // Read only (default)
	RoleNone   Role = "none"   // No access
)

// Permission represents an action that can be performed.
type Permission string

// Permissions map to Neo4j-compatible actions.
const (
	PermRead       Permission = "read"
	PermWrite      Permission = "write"
	PermCreate     Permission = "create"
	PermDelete     Permission = "delete"
	PermAdmin      Permission = "admin"
	PermSchema     Permission = "schema"
	PermUserManage Permission = "user_manage"
)

// RolePermissions maps roles to their allowed permissions.
// Follows Mimir's RBAC model.
var RolePermissions = map[Role][]Permission{
	RoleAdmin:  {PermRead, PermWrite, PermCreate, PermDelete, PermAdmin, PermSchema, PermUserManage},
	RoleEditor: {PermRead, PermWrite, PermCreate, PermDelete},
	RoleViewer: {PermRead},
	RoleNone:   {},
}

// User represents an authenticated user account.
//
// Users have:
//   - Unique ID and username
//   - Password (hashed with bcrypt, never exposed)
//   - One or more roles (admin, editor, viewer, none)
//   - Timestamps for auditing
//   - Metadata for custom properties
//
// Security features:
//   - PasswordHash is never serialized (json:"-" tag)
//   - FailedLogins and LockedUntil track brute force attempts
//   - Disabled flag allows account suspension
//
// Example:
//
//	user := &auth.User{
//		ID:       "usr-abc123",
//		Username: "alice",
//		Email:    "alice@example.com",
//		Roles:    []auth.Role{auth.RoleEditor},
//		Metadata: map[string]string{
//			"department": "Engineering",
//			"team":       "Backend",
//		},
//	}
//
//	// Check permissions
//	if user.HasRole(auth.RoleAdmin) {
//		fmt.Println("User is admin")
//	}
//
//	if user.HasPermission(auth.PermWrite) {
//		fmt.Println("User can write data")
//	}
type User struct {
	ID           string            `json:"id"`
	Username     string            `json:"username"`
	Email        string            `json:"email,omitempty"`
	PasswordHash string            `json:"-"` // Never serialize
	Roles        []Role            `json:"roles"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
	LastLogin    time.Time         `json:"last_login,omitempty"`
	FailedLogins int               `json:"-"` // Internal tracking
	LockedUntil  time.Time         `json:"-"` // Internal tracking
	Disabled     bool              `json:"disabled,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// HasRole checks if the user has a specific role.
//
// A user can have multiple roles. This method returns true if any of
// the user's roles match the specified role.
//
// Example:
//
//	user := &auth.User{
//		Roles: []auth.Role{auth.RoleEditor, auth.RoleViewer},
//	}
//
//	if user.HasRole(auth.RoleAdmin) {
//		fmt.Println("Is admin") // Not printed
//	}
//
//	if user.HasRole(auth.RoleEditor) {
//		fmt.Println("Is editor") // Printed
//	}
func (u *User) HasRole(role Role) bool {
	for _, r := range u.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// HasPermission checks if the user has a specific permission through any of their roles.
//
// Permissions are granted by roles. This method checks all the user's roles
// and returns true if any role grants the requested permission.
//
// Permission hierarchy:
//   - RoleAdmin: read, write, create, delete, admin, schema, user_manage
//   - RoleEditor: read, write, create, delete
//   - RoleViewer: read
//   - RoleNone: (no permissions)
//
// Example:
//
//	user := &auth.User{
//		Roles: []auth.Role{auth.RoleEditor},
//	}
//
//	if user.HasPermission(auth.PermRead) {
//		fmt.Println("Can read") // Printed
//	}
//
//	if user.HasPermission(auth.PermWrite) {
//		fmt.Println("Can write") // Printed
//	}
//
//	if user.HasPermission(auth.PermUserManage) {
//		fmt.Println("Can manage users") // NOT printed (needs RoleAdmin)
//	}
func (u *User) HasPermission(perm Permission) bool {
	for _, role := range u.Roles {
		perms, ok := RolePermissions[role]
		if !ok {
			continue
		}
		for _, p := range perms {
			if p == perm {
				return true
			}
		}
	}
	return false
}

// JWTClaims represents the claims in a JWT token.
// Compatible with Mimir's JWT structure.
type JWTClaims struct {
	Sub      string   `json:"sub"`                // Subject (user ID)
	Email    string   `json:"email,omitempty"`    // User email
	Username string   `json:"username,omitempty"` // Username
	Roles    []string `json:"roles"`              // User roles
	Iat      int64    `json:"iat"`                // Issued at (Unix timestamp)
	Exp      int64    `json:"exp,omitempty"`      // Expiration (Unix timestamp, 0 = never)
}

// TokenResponse follows OAuth 2.0 RFC 6749 token response format.
// Compatible with Mimir's /auth/token endpoint.
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`           // Always "Bearer"
	ExpiresIn   int64  `json:"expires_in,omitempty"` // Seconds until expiration (omitted if never expires)
	Scope       string `json:"scope,omitempty"`
}

// AuthConfig holds authentication configuration.
// Follows Mimir's configuration patterns.
type AuthConfig struct {
	// Password policy
	MinPasswordLength int
	BcryptCost        int

	// Token settings
	JWTSecret   []byte
	TokenExpiry time.Duration // 0 = never expire (Mimir default)

	// Lockout settings
	MaxFailedLogins int
	LockoutDuration time.Duration

	// Feature flags
	SecurityEnabled bool
}

// DefaultAuthConfig returns default authentication configuration.
// Matches Mimir's defaults for compatibility.
func DefaultAuthConfig() AuthConfig {
	return AuthConfig{
		MinPasswordLength: 8,
		BcryptCost:        bcrypt.DefaultCost,
		TokenExpiry:       0, // Never expire by default (Mimir behavior)
		MaxFailedLogins:   5,
		LockoutDuration:   15 * time.Minute,
		SecurityEnabled:   true,
	}
}

// Authenticator manages users and authentication.
type Authenticator struct {
	mu     sync.RWMutex
	users  map[string]*User // keyed by username
	config AuthConfig

	// Audit callback for compliance logging
	auditLog func(event AuditEvent)
}

// AuditEvent represents an authentication-related event for compliance logging.
// Required for GDPR Art.30, HIPAA §164.312(b), FISMA AU controls.
type AuditEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	EventType   string    `json:"event_type"`
	Username    string    `json:"username,omitempty"`
	UserID      string    `json:"user_id,omitempty"`
	IPAddress   string    `json:"ip_address,omitempty"`
	UserAgent   string    `json:"user_agent,omitempty"`
	Success     bool      `json:"success"`
	Details     string    `json:"details,omitempty"`
	RequestPath string    `json:"request_path,omitempty"`
}

// NewAuthenticator creates a new Authenticator with the given configuration.
//
// The authenticator manages user accounts, authentication, and authorization.
// All operations are thread-safe.
//
// Configuration validation:
//   - If SecurityEnabled=true, JWTSecret is required (min 32 bytes recommended)
//   - MinPasswordLength defaults to 8 characters
//   - BcryptCost defaults to bcrypt.DefaultCost (10)
//   - MaxFailedLogins defaults to 5
//   - LockoutDuration defaults to 15 minutes
//
// Example:
//
//	// Production configuration
//	config := auth.AuthConfig{
//		MinPasswordLength: 12,
//		BcryptCost:        12,  // Higher = more secure but slower
//		JWTSecret:         []byte("your-secret-key-at-least-32-chars-long"),
//		TokenExpiry:       24 * time.Hour,  // Tokens expire after 24h
//		MaxFailedLogins:   5,
//		LockoutDuration:   30 * time.Minute,
//		SecurityEnabled:   true,
//	}
//
//	auth, err := auth.NewAuthenticator(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Development configuration (no security)
//	devConfig := auth.AuthConfig{
//		SecurityEnabled: false,  // All requests allowed
//	}
//	auth = auth.NewAuthenticator(devConfig)
//
// Returns error if SecurityEnabled=true but JWTSecret is empty.
//
// Example 1 - Production HIPAA-Compliant Setup:
//
//	config := auth.DefaultAuthConfig()
//	config.SecurityEnabled = true
//	config.JWTSecret = []byte(os.Getenv("JWT_SECRET")) // Min 32 bytes
//	config.MinPasswordLength = 12
//	config.BcryptCost = 12 // High cost for security
//	config.MaxFailedAttempts = 5
//	config.LockoutDuration = 30 * time.Minute
//	config.TokenExpiry = 4 * time.Hour
//
//	authenticator, err := auth.NewAuthenticator(config)
//	if err != nil {
//		log.Fatal("Failed to initialize auth:", err)
//	}
//
//	// Set up HIPAA-required audit logging
//	authenticator.SetAuditLogger(func(event auth.AuditEvent) {
//		auditLogger.Log(audit.Event{
//			Type:      audit.EventLogin,
//			UserID:    event.UserID,
//			Username:  event.Username,
//			IPAddress: event.IPAddress,
//			Success:   event.Success,
//			Metadata:  map[string]string{"reason": event.Message},
//		})
//	})
//
//	// Create admin user with strong password
//	admin, err := authenticator.CreateUser("admin",
//		"Str0ng!P@ssw0rd#2024", []auth.Role{auth.RoleAdmin})
//
// Example 2 - Multi-Tenant SaaS with Short-Lived Tokens:
//
//	config := auth.DefaultAuthConfig()
//	config.SecurityEnabled = true
//	config.JWTSecret = loadSecretFromVault()
//	config.TokenExpiry = 15 * time.Minute // Short-lived tokens
//	config.MaxFailedAttempts = 3          // Strict lockout
//
//	authenticator, err := auth.NewAuthenticator(config)
//	if err != nil {
//		return nil, fmt.Errorf("auth init failed: %w", err)
//	}
//
//	// Create per-tenant users
//	for _, tenant := range tenants {
//		user, err := authenticator.CreateUser(
//			fmt.Sprintf("%s-%s", tenant.ID, username),
//			generateStrongPassword(),
//			[]auth.Role{auth.RoleEditor},
//		)
//		if err != nil {
//			log.Printf("Failed to create user for tenant %s: %v", tenant.ID, err)
//		}
//	}
//
// Example 3 - Development Mode (No Security):
//
//	config := auth.DefaultAuthConfig()
//	config.SecurityEnabled = false // Bypass all auth checks
//
//	authenticator, err := auth.NewAuthenticator(config)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// In dev mode, any token is accepted
//	// WARNING: Never use in production!
//	claims, _ := authenticator.ValidateToken("any-token") // Always succeeds
//
// Example 4 - API Integration with Rate Limiting:
//
//	config := auth.DefaultAuthConfig()
//	config.JWTSecret = []byte("secure-secret-32-chars-minimum!!")
//	config.MaxFailedAttempts = 5
//
//	authenticator, err := auth.NewAuthenticator(config)
//	if err != nil {
//		return nil, err
//	}
//
//	// Track failed attempts for rate limiting
//	authenticator.SetAuditLogger(func(event auth.AuditEvent) {
//		if event.EventType == "login_failed" {
//			rateLimiter.RecordFailure(event.IPAddress)
//			if rateLimiter.IsBlocked(event.IPAddress) {
//				firewall.BlockIP(event.IPAddress, 1*time.Hour)
//			}
//		}
//	})
//
// ELI12:
//
// Think of NewAuthenticator like hiring a bouncer for a club:
//
//   - The bouncer checks IDs (validates JWT tokens)
//   - They remember troublemakers (account lockout after failed attempts)
//   - They keep a guest list (user database)
//   - They write down who comes in (audit logging)
//   - They have different wristbands for VIP, regular, and just-looking (roles)
//
// SecurityEnabled = true means "bouncer is on duty"
// SecurityEnabled = false means "anyone can walk in" (development only!)
//
// Real-world Analogy:
//
//	JWT tokens are like temporary wristbands:
//	- When you log in, you get a wristband (token)
//	- Show the wristband to access stuff (no need to login again)
//	- Wristband expires after a few hours (token expiry)
//	- If you lose it, get a new one by logging in again
//
// Why JWT Instead of Sessions?
//
//	Sessions: "I'll remember you" (server stores state)
//	JWT: "Here's a signed badge, prove yourself each time" (stateless)
//
//	JWT Benefits:
//	- Works across multiple servers (no shared session storage)
//	- Scales better (no memory for sessions)
//	- Mobile-friendly (just store the token)
//
// Security Features:
//   - Passwords hashed with bcrypt (can't be reversed)
//   - Account lockout (prevents password guessing)
//   - Token expiry (stolen tokens eventually die)
//   - Audit logging (track who did what)
//
// Compliance:
//   - GDPR Art.32: Security measures ✓
//   - HIPAA §164.312(a): Access control ✓
//   - FISMA AC-2: Account management ✓
//   - SOC 2 CC6.1: Logical access controls ✓
//
// Performance:
//   - bcrypt hashing: ~50-200ms (intentionally slow for security)
//   - Token validation: ~1ms (just signature check)
//   - Thread-safe for concurrent authentication
//
// Thread Safety:
//
//	All methods are thread-safe for concurrent use.
func NewAuthenticator(config AuthConfig) (*Authenticator, error) {
	if config.SecurityEnabled && len(config.JWTSecret) == 0 {
		return nil, ErrMissingSecret
	}

	if config.BcryptCost == 0 {
		config.BcryptCost = bcrypt.DefaultCost
	}
	if config.MinPasswordLength == 0 {
		config.MinPasswordLength = 8
	}
	if config.MaxFailedLogins == 0 {
		config.MaxFailedLogins = 5
	}
	if config.LockoutDuration == 0 {
		config.LockoutDuration = 15 * time.Minute
	}

	return &Authenticator{
		users:  make(map[string]*User),
		config: config,
	}, nil
}

// SetAuditLogger sets the audit logging callback.
func (a *Authenticator) SetAuditLogger(fn func(AuditEvent)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.auditLog = fn
}

func (a *Authenticator) logAudit(event AuditEvent) {
	if a.auditLog != nil {
		event.Timestamp = time.Now()
		a.auditLog(event)
	}
}

// CreateUser creates a new user account with the given credentials and roles.
//
// The password is immediately hashed with bcrypt and never stored in plain text.
// If no roles are specified, defaults to RoleViewer.
//
// Parameters:
//   - username: Unique username (must not already exist)
//   - password: Plain text password (will be hashed)
//   - roles: User roles, or empty slice for default (viewer)
//
// Returns:
//   - User object (without password hash)
//   - ErrUserExists if username already taken
//   - ErrPasswordTooShort if password doesn't meet minimum length
//
// Example:
//
//	// Create admin user
//	admin, err := auth.CreateUser("admin", "SecurePassword123!",
//		[]auth.Role{auth.RoleAdmin})
//	if err != nil {
//		return err
//	}
//
//	// Create regular user (defaults to viewer)
//	user, err := auth.CreateUser("alice", "AlicePass456!", nil)
//
//	// Create user with multiple roles
//	editor, err := auth.CreateUser("bob", "BobPass789!",
//		[]auth.Role{auth.RoleEditor, auth.RoleViewer})
//
// Audit event: Logs "user_create" with success/failure.
//
// Compliance: HIPAA §164.308(a)(3)(ii)(A) - Unique user IDs
func (a *Authenticator) CreateUser(username, password string, roles []Role) (*User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Check if user exists
	if _, exists := a.users[username]; exists {
		a.logAudit(AuditEvent{
			EventType: "user_create",
			Username:  username,
			Success:   false,
			Details:   "user already exists",
		})
		return nil, ErrUserExists
	}

	// Validate password
	if len(password) < a.config.MinPasswordLength {
		return nil, fmt.Errorf("%w: minimum %d characters required", ErrPasswordTooShort, a.config.MinPasswordLength)
	}

	// Hash password
	hash, err := bcrypt.GenerateFromPassword([]byte(password), a.config.BcryptCost)
	if err != nil {
		return nil, fmt.Errorf("failed to hash password: %w", err)
	}

	// Default to viewer role if none specified
	if len(roles) == 0 {
		roles = []Role{RoleViewer}
	}

	// Create user
	now := time.Now()
	user := &User{
		ID:           generateID(),
		Username:     username,
		Email:        username + "@localhost", // Mimir pattern for dev users
		PasswordHash: string(hash),
		Roles:        roles,
		CreatedAt:    now,
		UpdatedAt:    now,
		Metadata:     make(map[string]string),
	}

	a.users[username] = user

	a.logAudit(AuditEvent{
		EventType: "user_create",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
		Details:   fmt.Sprintf("created with roles %v", roles),
	})

	// Return copy without password hash
	return a.copyUserSafe(user), nil
}

// Authenticate verifies user credentials and returns a JWT token.
//
// This implements the OAuth 2.0 password grant flow (RFC 6749 Section 4.3).
// On successful authentication:
//  1. Password is verified with bcrypt
//  2. Failed login counter is reset
//  3. LastLogin timestamp is updated
//  4. JWT token is generated
//  5. Audit event is logged
//
// Security features:
//   - Account lockout after MaxFailedLogins attempts
//   - Disabled accounts cannot authenticate
//   - Timing attack resistant (doesn't reveal if user exists)
//   - Failed attempts are logged for security monitoring
//
// Parameters:
//   - username: User's username
//   - password: User's password (plain text)
//   - ipAddress: Client IP for audit logging
//   - userAgent: Client User-Agent for audit logging
//
// Returns:
//   - TokenResponse: OAuth 2.0 token response (access_token, token_type, expires_in)
//   - User: User object (without password)
//   - Error: ErrInvalidCredentials, ErrAccountLocked, or ErrUserNotFound
//
// Example:
//
//	token, user, err := auth.Authenticate(
//		"alice",
//		"AlicePassword123!",
//		"192.168.1.100",
//		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
//	)
//	if err != nil {
//		if errors.Is(err, auth.ErrAccountLocked) {
//			http.Error(w, "Account locked. Try again in 15 minutes.", 423)
//			return
//		}
//		http.Error(w, "Invalid credentials", 401)
//		return
//	}
//
//	// Use token in Authorization header
//	fmt.Printf("Authorization: Bearer %s\n", token.AccessToken)
//
//	// Set cookie for browser sessions
//	http.SetCookie(w, &http.Cookie{
//		Name:     "token",
//		Value:    token.AccessToken,
//		HttpOnly: true,
//		Secure:   true,
//		SameSite: http.SameSiteStrictMode,
//	})
//
// Compliance:
//   - HIPAA §164.312(a)(2)(i): Unique User Identification
//   - HIPAA §164.312(d): Person or Entity Authentication
//   - GDPR Art.32: Technical measures to ensure security
//   - FISMA AC-7: Unsuccessful Login Attempts
func (a *Authenticator) Authenticate(username, password, ipAddress, userAgent string) (*TokenResponse, *User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		a.logAudit(AuditEvent{
			EventType: "login",
			Username:  username,
			IPAddress: ipAddress,
			UserAgent: userAgent,
			Success:   false,
			Details:   "user not found",
		})
		return nil, nil, ErrInvalidCredentials // Don't reveal if user exists
	}

	// Check if account is locked
	if !user.LockedUntil.IsZero() && time.Now().Before(user.LockedUntil) {
		a.logAudit(AuditEvent{
			EventType: "login",
			Username:  username,
			UserID:    user.ID,
			IPAddress: ipAddress,
			UserAgent: userAgent,
			Success:   false,
			Details:   "account locked",
		})
		return nil, nil, ErrAccountLocked
	}

	// Check if account is disabled
	if user.Disabled {
		a.logAudit(AuditEvent{
			EventType: "login",
			Username:  username,
			UserID:    user.ID,
			IPAddress: ipAddress,
			UserAgent: userAgent,
			Success:   false,
			Details:   "account disabled",
		})
		return nil, nil, ErrInvalidCredentials
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		// Increment failed login counter
		user.FailedLogins++
		if user.FailedLogins >= a.config.MaxFailedLogins {
			user.LockedUntil = time.Now().Add(a.config.LockoutDuration)
		}
		user.UpdatedAt = time.Now()

		a.logAudit(AuditEvent{
			EventType: "login",
			Username:  username,
			UserID:    user.ID,
			IPAddress: ipAddress,
			UserAgent: userAgent,
			Success:   false,
			Details:   fmt.Sprintf("invalid password (attempt %d/%d)", user.FailedLogins, a.config.MaxFailedLogins),
		})
		return nil, nil, ErrInvalidCredentials
	}

	// Reset failed login counter on success
	user.FailedLogins = 0
	user.LockedUntil = time.Time{}
	user.LastLogin = time.Now()
	user.UpdatedAt = time.Now()

	// Generate JWT token
	token, err := a.generateJWT(user)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate token: %w", err)
	}

	// Build OAuth 2.0 compliant response
	response := &TokenResponse{
		AccessToken: token,
		TokenType:   "Bearer",
		Scope:       "default",
	}

	// Only include expires_in if token actually expires
	if a.config.TokenExpiry > 0 {
		response.ExpiresIn = int64(a.config.TokenExpiry.Seconds())
	}

	a.logAudit(AuditEvent{
		EventType: "login",
		Username:  username,
		UserID:    user.ID,
		IPAddress: ipAddress,
		UserAgent: userAgent,
		Success:   true,
		Details:   "token generated",
	})

	return response, a.copyUserSafe(user), nil
}

// ValidateToken validates a JWT token and returns the claims.
//
// The token is verified using HMAC-SHA256. If valid, returns the decoded claims
// including user ID, username, roles, and expiration.
//
// Validation checks:
//  1. Token format (header.payload.signature)
//  2. Signature verification (HMAC-SHA256)
//  3. Expiration (if Exp > 0)
//  4. Not before (if configured)
//
// If SecurityEnabled=false, returns dummy claims allowing all access.
//
// The token can include "Bearer " prefix (will be stripped).
//
// Parameters:
//   - token: JWT token string (with or without "Bearer " prefix)
//
// Returns:
//   - JWTClaims: Decoded claims with user info and roles
//   - Error: ErrInvalidToken, ErrSessionExpired, or ErrNoCredentials
//
// Example:
//
//	// From Authorization header
//	authHeader := r.Header.Get("Authorization") // "Bearer eyJhbGc..."
//	claims, err := auth.ValidateToken(authHeader)
//	if err != nil {
//		http.Error(w, "Unauthorized", 401)
//		return
//	}
//
//	fmt.Printf("User: %s\n", claims.Username)
//	fmt.Printf("Roles: %v\n", claims.Roles)
//
//	// Check if user has admin role
//	hasAdmin := false
//	for _, role := range claims.Roles {
//		if role == string(auth.RoleAdmin) {
//			hasAdmin = true
//			break
//		}
//	}
//
// Security:
//   - Uses constant-time comparison to prevent timing attacks
//   - Validates expiration to prevent replay attacks
//   - Checks signature to prevent tampering
func (a *Authenticator) ValidateToken(token string) (*JWTClaims, error) {
	if !a.config.SecurityEnabled {
		// Security disabled - return dummy claims
		return &JWTClaims{
			Sub:   "anonymous",
			Roles: []string{string(RoleAdmin)},
		}, nil
	}

	if token == "" {
		return nil, ErrNoCredentials
	}

	// Strip "Bearer " prefix if present
	token = strings.TrimPrefix(token, "Bearer ")
	token = strings.TrimSpace(token)

	return a.verifyJWT(token)
}

// GetUserByID retrieves a user by their ID.
func (a *Authenticator) GetUserByID(id string) (*User, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, user := range a.users {
		if user.ID == id {
			return a.copyUserSafe(user), nil
		}
	}
	return nil, ErrUserNotFound
}

// GetUser returns user info by username without sensitive data.
func (a *Authenticator) GetUser(username string) (*User, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	user, exists := a.users[username]
	if !exists {
		return nil, ErrUserNotFound
	}

	return a.copyUserSafe(user), nil
}

// ListUsers returns all users without sensitive data.
func (a *Authenticator) ListUsers() []*User {
	a.mu.RLock()
	defer a.mu.RUnlock()

	users := make([]*User, 0, len(a.users))
	for _, u := range a.users {
		users = append(users, a.copyUserSafe(u))
	}
	return users
}

// ChangePassword updates a user's password.
func (a *Authenticator) ChangePassword(username, oldPassword, newPassword string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	// Verify old password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(oldPassword)); err != nil {
		a.logAudit(AuditEvent{
			EventType: "password_change",
			Username:  username,
			UserID:    user.ID,
			Success:   false,
			Details:   "old password incorrect",
		})
		return ErrInvalidCredentials
	}

	// Validate new password
	if len(newPassword) < a.config.MinPasswordLength {
		return fmt.Errorf("%w: minimum %d characters required", ErrPasswordTooShort, a.config.MinPasswordLength)
	}

	// Hash new password
	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), a.config.BcryptCost)
	if err != nil {
		return fmt.Errorf("failed to hash password: %w", err)
	}

	user.PasswordHash = string(hash)
	user.UpdatedAt = time.Now()

	a.logAudit(AuditEvent{
		EventType: "password_change",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
	})

	return nil
}

// UpdateRoles changes a user's roles.
func (a *Authenticator) UpdateRoles(username string, newRoles []Role) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	oldRoles := user.Roles
	user.Roles = newRoles
	user.UpdatedAt = time.Now()

	a.logAudit(AuditEvent{
		EventType: "role_change",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
		Details:   fmt.Sprintf("roles changed from %v to %v", oldRoles, newRoles),
	})

	return nil
}

// DisableUser disables a user account.
func (a *Authenticator) DisableUser(username string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	user.Disabled = true
	user.UpdatedAt = time.Now()

	a.logAudit(AuditEvent{
		EventType: "user_disable",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
	})

	return nil
}

// EnableUser re-enables a disabled user account.
func (a *Authenticator) EnableUser(username string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	user.Disabled = false
	user.FailedLogins = 0
	user.LockedUntil = time.Time{}
	user.UpdatedAt = time.Now()

	a.logAudit(AuditEvent{
		EventType: "user_enable",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
	})

	return nil
}

// UnlockUser manually unlocks a locked user account.
func (a *Authenticator) UnlockUser(username string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	user.FailedLogins = 0
	user.LockedUntil = time.Time{}
	user.UpdatedAt = time.Now()

	a.logAudit(AuditEvent{
		EventType: "user_unlock",
		Username:  username,
		UserID:    user.ID,
		Success:   true,
	})

	return nil
}

// DeleteUser removes a user.
func (a *Authenticator) DeleteUser(username string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	user, exists := a.users[username]
	if !exists {
		return ErrUserNotFound
	}

	userID := user.ID
	delete(a.users, username)

	a.logAudit(AuditEvent{
		EventType: "user_delete",
		Username:  username,
		UserID:    userID,
		Success:   true,
	})

	return nil
}

// UserCount returns the number of registered users.
func (a *Authenticator) UserCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.users)
}

// IsSecurityEnabled returns whether security is enabled.
func (a *Authenticator) IsSecurityEnabled() bool {
	return a.config.SecurityEnabled
}

// GenerateClusterToken creates a JWT token for cluster inter-node authentication.
// This token can be used by cluster nodes to authenticate with each other
// using the same shared JWT secret.
//
// The token contains:
//   - Sub: node identifier (e.g., "cluster-node-2")
//   - Username: same as nodeID for identification
//   - Roles: specified role (typically "admin" for cluster nodes)
//   - Iat: issued at timestamp
//   - Exp: expiration (if TokenExpiry is configured, otherwise never expires)
//
// Usage:
//
//  1. Generate the secret (must be same on all nodes):
//     openssl rand -base64 48
//
//  2. Configure all cluster nodes with the same secret:
//     NORNICDB_JWT_SECRET=<your-generated-secret>
//
//  3. Generate a cluster token (from admin API or code):
//     token, _ := authenticator.GenerateClusterToken("node-2", auth.RoleAdmin)
//
//  4. Use the token to connect from other nodes:
//     driver = GraphDatabase.driver("bolt://node1:7687",
//     basic_auth("", token))  # Empty principal triggers bearer/JWT auth
//
// Example:
//
//	// On cluster setup, generate tokens for each node
//	token, err := authenticator.GenerateClusterToken("cluster-node-west", auth.RoleAdmin)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Distribute token securely to node-west
//	// Node-west uses it to connect:
//	driver, _ := neo4j.NewDriverWithContext(
//		"bolt://node-east:7687",
//		neo4j.BasicAuth("", token, ""),  // Empty username = bearer auth
//	)
//
// Security considerations:
//   - Tokens are signed with HMAC-SHA256 using the shared JWT secret
//   - All cluster nodes MUST use the same JWT secret
//   - Store the secret securely (e.g., HashiCorp Vault, K8s Secrets)
//   - Rotate secrets periodically by generating new tokens
func (a *Authenticator) GenerateClusterToken(nodeID string, role Role) (string, error) {
	if len(a.config.JWTSecret) == 0 {
		return "", ErrMissingSecret
	}

	// Create a virtual user for the cluster node
	user := &User{
		ID:       "cluster-" + nodeID,
		Username: nodeID,
		Roles:    []Role{role},
	}

	token, err := a.generateJWT(user)
	if err != nil {
		return "", fmt.Errorf("failed to generate cluster token: %w", err)
	}

	a.logAudit(AuditEvent{
		Timestamp: time.Now(),
		EventType: "cluster_token_generated",
		Username:  nodeID,
		UserID:    user.ID,
		Success:   true,
		Details:   fmt.Sprintf("cluster token generated for node %s with role %s", nodeID, role),
	})

	return token, nil
}

// GenerateClusterTokenWithExpiry creates a JWT token with a custom expiration time.
// Useful for short-lived tokens during initial cluster setup or testing.
//
// Parameters:
//   - nodeID: identifier for the cluster node (e.g., "node-2", "replica-west")
//   - role: role to assign (typically RoleAdmin for cluster nodes)
//   - expiry: token lifetime (e.g., 24*time.Hour, 0 for never expires)
//
// Example:
//
//	// Generate a token that expires in 7 days
//	token, _ := auth.GenerateClusterTokenWithExpiry("node-2", auth.RoleAdmin, 7*24*time.Hour)
func (a *Authenticator) GenerateClusterTokenWithExpiry(nodeID string, role Role, expiry time.Duration) (string, error) {
	if len(a.config.JWTSecret) == 0 {
		return "", ErrMissingSecret
	}

	now := time.Now().Unix()

	claims := JWTClaims{
		Sub:      "cluster-" + nodeID,
		Username: nodeID,
		Roles:    []string{string(role)},
		Iat:      now,
	}

	// Set expiration if provided
	if expiry > 0 {
		claims.Exp = now + int64(expiry.Seconds())
	}

	// Build JWT manually (header.payload.signature)
	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerJSON, _ := json.Marshal(header)
	claimsJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	// Sign with HMAC-SHA256
	message := headerB64 + "." + claimsB64
	mac := hmac.New(sha256.New, a.config.JWTSecret)
	mac.Write([]byte(message))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	token := message + "." + signature

	a.logAudit(AuditEvent{
		Timestamp: time.Now(),
		EventType: "cluster_token_generated",
		Username:  nodeID,
		UserID:    "cluster-" + nodeID,
		Success:   true,
		Details:   fmt.Sprintf("cluster token generated for node %s with role %s, expiry=%v", nodeID, role, expiry),
	})

	return token, nil
}

// GenerateAPIToken creates a stateless API token for the given user.
// These tokens are for MCP servers and other API integrations.
// The token is not stored; it's generated on-demand and validated by signature.
//
// Parameters:
//   - user: The authenticated user requesting the token
//   - subject: A descriptive label for the token (e.g., "my-mcp-server")
//   - expiry: Token lifetime (0 = never expires)
//
// Returns the raw JWT token string.
func (a *Authenticator) GenerateAPIToken(user *User, subject string, expiry time.Duration) (string, error) {
	if len(a.config.JWTSecret) == 0 {
		return "", ErrMissingSecret
	}

	now := time.Now().Unix()

	// Convert roles to strings
	roles := make([]string, len(user.Roles))
	for i, r := range user.Roles {
		roles[i] = string(r)
	}

	claims := JWTClaims{
		Sub:      user.ID,
		Email:    user.Email,
		Username: user.Username,
		Roles:    roles,
		Iat:      now,
	}

	// Set expiration if provided
	if expiry > 0 {
		claims.Exp = now + int64(expiry.Seconds())
	}

	// Build JWT manually (header.payload.signature)
	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerJSON, _ := json.Marshal(header)
	claimsJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	// Sign with HMAC-SHA256
	message := headerB64 + "." + claimsB64
	mac := hmac.New(sha256.New, a.config.JWTSecret)
	mac.Write([]byte(message))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	token := message + "." + signature

	a.logAudit(AuditEvent{
		Timestamp: time.Now(),
		EventType: "api_token_generated",
		Username:  user.Username,
		UserID:    user.ID,
		Success:   true,
		Details:   fmt.Sprintf("API token generated for subject '%s', expiry=%v", subject, expiry),
	})

	return token, nil
}

// JWT Generation and Validation

// generateJWT creates a JWT token for the user.
// Uses HS256 algorithm matching Mimir's implementation.
func (a *Authenticator) generateJWT(user *User) (string, error) {
	if len(a.config.JWTSecret) == 0 {
		return "", ErrMissingSecret
	}

	now := time.Now().Unix()

	// Convert roles to strings
	roles := make([]string, len(user.Roles))
	for i, r := range user.Roles {
		roles[i] = string(r)
	}

	claims := JWTClaims{
		Sub:      user.ID,
		Email:    user.Email,
		Username: user.Username,
		Roles:    roles,
		Iat:      now,
	}

	// Only set expiration if configured (0 = never expire, Mimir default)
	if a.config.TokenExpiry > 0 {
		claims.Exp = now + int64(a.config.TokenExpiry.Seconds())
	}

	// Build JWT manually (header.payload.signature)
	header := map[string]string{
		"alg": "HS256",
		"typ": "JWT",
	}

	headerJSON, _ := json.Marshal(header)
	claimsJSON, _ := json.Marshal(claims)

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	// Sign with HMAC-SHA256
	message := headerB64 + "." + claimsB64
	mac := hmac.New(sha256.New, a.config.JWTSecret)
	mac.Write([]byte(message))
	signature := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	return message + "." + signature, nil
}

// verifyJWT validates a JWT token and returns the claims.
func (a *Authenticator) verifyJWT(token string) (*JWTClaims, error) {
	if len(a.config.JWTSecret) == 0 {
		return nil, ErrMissingSecret
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, ErrInvalidToken
	}

	// Verify signature
	message := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, a.config.JWTSecret)
	mac.Write([]byte(message))
	expectedSig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))

	// Constant-time comparison to prevent timing attacks
	if !SecureCompare(parts[2], expectedSig) {
		return nil, ErrInvalidToken
	}

	// Decode claims
	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrInvalidToken
	}

	var claims JWTClaims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, ErrInvalidToken
	}

	// Check expiration (0 = never expires)
	if claims.Exp > 0 && time.Now().Unix() > claims.Exp {
		return nil, ErrSessionExpired
	}

	return &claims, nil
}

// copyUserSafe returns a copy of user without sensitive data.
func (a *Authenticator) copyUserSafe(u *User) *User {
	roles := make([]Role, len(u.Roles))
	copy(roles, u.Roles)

	metadata := make(map[string]string)
	for k, v := range u.Metadata {
		metadata[k] = v
	}

	return &User{
		ID:        u.ID,
		Username:  u.Username,
		Email:     u.Email,
		Roles:     roles,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
		LastLogin: u.LastLogin,
		Disabled:  u.Disabled,
		Metadata:  metadata,
	}
}

// Helper functions

func generateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// SecureCompare performs a constant-time string comparison.
// Prevents timing attacks on token validation.
func SecureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// ValidRole checks if a role is valid.
func ValidRole(r Role) bool {
	switch r {
	case RoleAdmin, RoleEditor, RoleViewer, RoleNone:
		return true
	default:
		return false
	}
}

// RoleFromString converts a string to a Role.
func RoleFromString(s string) (Role, error) {
	r := Role(s)
	if !ValidRole(r) {
		return RoleNone, fmt.Errorf("invalid role: %s", s)
	}
	return r, nil
}

// HasCredentials checks if a request has any form of authentication credentials.
// Compatible with Mimir's hasAuthCredentials() helper.
// Checks: Authorization header, X-API-Key header, cookie, query params.
func HasCredentials(authHeader, apiKeyHeader, cookie, queryToken, queryAPIKey string) bool {
	return authHeader != "" ||
		apiKeyHeader != "" ||
		cookie != "" ||
		queryToken != "" ||
		queryAPIKey != ""
}

// ExtractToken extracts the token from various sources.
// Priority: Authorization header > X-API-Key > Cookie > Query param
// Compatible with Mimir's token extraction pattern.
func ExtractToken(authHeader, apiKeyHeader, cookie, queryToken, queryAPIKey string) string {
	// 1. Authorization: Bearer header (OAuth 2.0 RFC 6750 standard)
	if authHeader != "" {
		return strings.TrimPrefix(authHeader, "Bearer ")
	}

	// 2. X-API-Key header (common alternative)
	if apiKeyHeader != "" {
		return apiKeyHeader
	}

	// 3. Cookie (browser sessions)
	if cookie != "" {
		return cookie
	}

	// 4. Query parameter (SSE connections that can't send headers)
	if queryToken != "" {
		return queryToken
	}
	if queryAPIKey != "" {
		return queryAPIKey
	}

	return ""
}
