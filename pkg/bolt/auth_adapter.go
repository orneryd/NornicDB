// Package bolt provides authentication adapters for integrating with NornicDB's auth package.
package bolt

import (
	"fmt"

	"github.com/orneryd/nornicdb/pkg/auth"
)

// AuthenticatorAdapter wraps auth.Authenticator to implement BoltAuthenticator.
// This allows the Bolt server to use the same authentication system as the HTTP server,
// service accounts, and the UI.
//
// The adapter translates Neo4j-style Bolt authentication (scheme, principal, credentials)
// to NornicDB's auth.Authenticator (username, password).
//
// Example:
//
//	// Create the shared authenticator
//	authConfig := auth.DefaultAuthConfig()
//	authConfig.JWTSecret = []byte("your-secret-key")
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
	auth           *auth.Authenticator
	allowAnonymous bool
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
	return &AuthenticatorAdapter{
		auth:           authenticator,
		allowAnonymous: false,
	}
}

// NewAuthenticatorAdapterWithAnonymous creates an adapter that allows anonymous connections.
// Anonymous users receive "viewer" role (read-only access).
//
// Use with caution - this allows unauthenticated connections.
func NewAuthenticatorAdapterWithAnonymous(authenticator *auth.Authenticator) *AuthenticatorAdapter {
	return &AuthenticatorAdapter{
		auth:           authenticator,
		allowAnonymous: true,
	}
}

// Authenticate validates credentials from the Bolt HELLO message.
// This method implements the BoltAuthenticator interface.
//
// Supported schemes:
//   - "basic": Username/password authentication (same as HTTP basic auth)
//   - "none": Anonymous access (if enabled, grants viewer role)
//
// For server-to-server clustering, use service accounts with "basic" scheme.
// Service accounts are regular users created via auth.CreateUser().
//
// Example service account setup:
//
//	// Create service account for cluster node
//	authenticator.CreateUser("cluster-node-west", "secure-password-123",
//		[]auth.Role{auth.RoleAdmin})
//
//	// Connect from another node using Neo4j driver
//	driver = GraphDatabase.driver("bolt://node-east:7687",
//		basic_auth("cluster-node-west", "secure-password-123"))
func (a *AuthenticatorAdapter) Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error) {
	// Handle anonymous authentication
	if scheme == "none" || scheme == "" {
		if !a.allowAnonymous {
			return nil, fmt.Errorf("anonymous authentication not allowed")
		}
		return &BoltAuthResult{
			Authenticated: true,
			Username:      "anonymous",
			Roles:         []string{"viewer"},
		}, nil
	}

	// Only "basic" scheme supported for authenticated connections
	if scheme != "basic" {
		return nil, fmt.Errorf("unsupported authentication scheme: %s (only 'basic' and 'none' supported)", scheme)
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

	return &BoltAuthResult{
		Authenticated: true,
		Username:      user.Username,
		Roles:         roles,
	}, nil
}

// SetAllowAnonymous enables or disables anonymous authentication.
func (a *AuthenticatorAdapter) SetAllowAnonymous(allow bool) {
	a.allowAnonymous = allow
}
