// Package bolt provides authentication adapters for integrating with NornicDB's auth package.
package bolt

import (
	"fmt"
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// AuthenticatorAdapter wraps auth.Authenticator to implement BoltAuthenticator.
// This allows the Bolt server to use the same authentication system as the HTTP server,
// service accounts, and the UI.
//
// The adapter translates Neo4j-style Bolt authentication (scheme, principal, credentials)
// to NornicDB's auth.Authenticator (username, password or JWT token).
//
// Supported authentication schemes:
//   - "basic": Username/password authentication (same as HTTP basic auth)
//   - "bearer": JWT token authentication (for cluster inter-node auth)
//   - "none": Anonymous access (if enabled, grants viewer role)
//
// # Cluster Authentication with Shared JWT
//
// For cluster deployments where all nodes need to authenticate with each other,
// use bearer token authentication with a shared JWT secret:
//
//  1. Configure all nodes with the same JWT secret:
//     NORNICDB_JWT_SECRET=your-shared-secret-min-32-bytes
//
//  2. Generate a cluster token on any node:
//     POST /api/v1/auth/cluster-token
//     {"node_id": "node-2", "role": "admin"}
//
//  3. Connect from other nodes using the bearer scheme:
//     driver = GraphDatabase.driver("bolt://node1:7687",
//     auth=("", token))  # Empty username triggers bearer auth
//
// Example:
//
//	// Create the shared authenticator
//	authConfig := auth.DefaultAuthConfig()
//	authConfig.JWTSecret = []byte("your-secret-key-shared-across-cluster")
//	authenticator, _ := auth.NewAuthenticator(authConfig)
//
//	// Create service accounts for server-to-server communication
//	authenticator.CreateUser("cluster-node-1", "secure-password", []auth.Role{auth.RoleAdmin})
//	authenticator.CreateUser("backup-service", "backup-password", []auth.Role{auth.RoleViewer})
//
//	// Create Bolt server with shared auth
//	boltConfig := bolt.DefaultConfig()
//	boltConfig.Authenticator = bolt.NewAuthenticatorAdapter(authenticator)
//	boltConfig.RequireAuth = true
//
//	boltServer := bolt.New(boltConfig, executor)
type AuthenticatorAdapter struct {
	auth                    *auth.Authenticator
	allowAnonymous          atomic.Bool
	basicAuthCache          *auth.BasicAuthCache
	getEffectivePermissions func(roles []string) []string // optional; when set, BoltAuthResult.Permissions is filled
}

// NewAuthenticatorAdapter creates a new BoltAuthenticator that wraps auth.Authenticator.
// This enables the Bolt server to use the same user database and authentication
// as the HTTP server, ensuring consistent auth across all protocols.
//
// Parameters:
//   - authenticator: The shared auth.Authenticator instance
//
// Example:
//
//	authenticator, _ := auth.NewAuthenticator(auth.DefaultAuthConfig())
//	boltAuth := bolt.NewAuthenticatorAdapter(authenticator)
//
//	config := bolt.DefaultConfig()
//	config.Authenticator = boltAuth
//	config.RequireAuth = true
func NewAuthenticatorAdapter(authenticator *auth.Authenticator) *AuthenticatorAdapter {
	adapter := &AuthenticatorAdapter{
		auth:           authenticator,
		basicAuthCache: auth.NewBasicAuthCache(auth.DefaultAuthCacheEntries, auth.DefaultAuthCacheTTL),
	}
	adapter.allowAnonymous.Store(false)
	return adapter
}

// NewAuthenticatorAdapterWithAnonymous creates an adapter that allows anonymous connections.
// Anonymous users receive "viewer" role (read-only access).
//
// Use with caution - this allows unauthenticated connections.
func NewAuthenticatorAdapterWithAnonymous(authenticator *auth.Authenticator) *AuthenticatorAdapter {
	adapter := &AuthenticatorAdapter{
		auth:           authenticator,
		basicAuthCache: auth.NewBasicAuthCache(auth.DefaultAuthCacheEntries, auth.DefaultAuthCacheTTL),
	}
	adapter.allowAnonymous.Store(true)
	return adapter
}

// SetGetEffectivePermissions sets the callback used to resolve roles to effective permission IDs.
// When set, BoltAuthResult.Permissions is filled so HasPermission uses stored role entitlements.
func (a *AuthenticatorAdapter) SetGetEffectivePermissions(fn func(roles []string) []string) {
	a.getEffectivePermissions = fn
}

// Authenticate validates credentials from the Bolt HELLO message.
// This method implements the BoltAuthenticator interface.
//
// Supported schemes:
//   - "basic": Username/password authentication (same as HTTP basic auth)
//   - "bearer": JWT token authentication (credentials contains JWT, principal is ignored)
//   - "none": Anonymous access (if enabled, grants viewer role)
//
// # Cluster Authentication
//
// For server-to-server clustering, you have two options:
//
// Option 1: Service accounts with "basic" scheme
//
//	authenticator.CreateUser("cluster-node-west", "secure-password-123",
//		[]auth.Role{auth.RoleAdmin})
//	driver = GraphDatabase.driver("bolt://node-east:7687",
//		basic_auth("cluster-node-west", "secure-password-123"))
//
// Option 2: JWT tokens with "bearer" scheme (recommended for clusters)
//
//	# Generate token via API:
//	curl -X POST http://node:7474/api/v1/auth/cluster-token \
//	  -H "Authorization: Bearer $ADMIN_TOKEN" \
//	  -d '{"node_id": "node-2", "role": "admin"}'
//
//	# Connect with bearer token:
//	driver = GraphDatabase.driver("bolt://node:7687",
//		basic_auth("", token))  # Empty username = bearer auth
func (a *AuthenticatorAdapter) Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error) {
	// Handle anonymous authentication (canonical role from auth)
	if scheme == "none" || scheme == "" {
		if !a.allowAnonymous.Load() {
			return nil, fmt.Errorf("anonymous authentication not allowed")
		}
		roles := []string{string(auth.RoleViewer)}
		result := &BoltAuthResult{Authenticated: true, Username: "anonymous", Roles: roles}
		if a.getEffectivePermissions != nil {
			result.Permissions = a.getEffectivePermissions(roles)
		}
		return result, nil
	}

	// Handle bearer token authentication (JWT)
	// This is the recommended method for cluster inter-node authentication
	if scheme == "bearer" {
		if credentials == "" {
			return nil, fmt.Errorf("bearer token required")
		}

		// Validate the JWT token using the shared authenticator
		// The token was generated with the same JWT secret
		claims, err := a.auth.ValidateToken(credentials)
		if err != nil {
			return nil, fmt.Errorf("invalid bearer token: %w", err)
		}

		result := &BoltAuthResult{Authenticated: true, Username: claims.Username, Roles: claims.Roles}
		if a.getEffectivePermissions != nil {
			result.Permissions = a.getEffectivePermissions(claims.Roles)
		}
		return result, nil
	}

	// Handle basic auth - check if it's actually a bearer token in disguise
	// This supports Neo4j drivers that only support basic auth:
	// When principal is empty and credentials looks like a JWT, treat as bearer
	if scheme == "basic" && principal == "" && looksLikeJWT(credentials) {
		claims, err := a.auth.ValidateToken(credentials)
		if err != nil {
			return nil, fmt.Errorf("invalid bearer token: %w", err)
		}

		result := &BoltAuthResult{Authenticated: true, Username: claims.Username, Roles: claims.Roles}
		if a.getEffectivePermissions != nil {
			result.Permissions = a.getEffectivePermissions(claims.Roles)
		}
		return result, nil
	}

	// Only "basic" scheme supported for username/password authentication
	if scheme != "basic" {
		return nil, fmt.Errorf("unsupported authentication scheme: %s (supported: 'basic', 'bearer', 'none')", scheme)
	}

	if a.basicAuthCache != nil {
		if cached, ok := a.basicAuthCache.Get(principal, credentials); ok {
			result := &BoltAuthResult{Authenticated: true, Username: cached.Username, Roles: cached.Roles}
			if a.getEffectivePermissions != nil {
				result.Permissions = a.getEffectivePermissions(cached.Roles)
			}
			return result, nil
		}
	}

	// Validate credentials using the shared authenticator
	// The Authenticate method handles:
	// - Password verification (bcrypt)
	// - Account lockout (after failed attempts)
	// - Account disabled status
	// - Audit logging
	_, user, err := a.auth.Authenticate(principal, credentials, "bolt", "Bolt/4.4")
	if err != nil {
		return nil, err
	}

	// Convert auth.Role to string roles
	roles := make([]string, len(user.Roles))
	for i, r := range user.Roles {
		roles[i] = string(r)
	}

	if a.basicAuthCache != nil {
		a.basicAuthCache.Set(principal, credentials, &auth.JWTClaims{
			Sub:      user.ID,
			Username: user.Username,
			Email:    user.Email,
			Roles:    roles,
		})
	}

	result := &BoltAuthResult{Authenticated: true, Username: user.Username, Roles: roles}
	if a.getEffectivePermissions != nil {
		result.Permissions = a.getEffectivePermissions(roles)
	}
	return result, nil
}

// SetAllowAnonymous enables or disables anonymous authentication.
func (a *AuthenticatorAdapter) SetAllowAnonymous(allow bool) {
	a.allowAnonymous.Store(allow)
}

// looksLikeJWT checks if a string appears to be a JWT token.
// JWTs have the format: header.payload.signature (3 base64url parts separated by dots)
func looksLikeJWT(s string) bool {
	if len(s) < 20 {
		return false
	}
	dots := 0
	for _, c := range s {
		if c == '.' {
			dots++
		}
	}
	return dots == 2
}
