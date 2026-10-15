package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A Bolt session's roles are read again before each statement: once the
// role its user authenticated with is deleted, the session's next
// statement runs without the role's permissions, as Neo4j applies a role
// change to open sessions at once.
func TestBoltSessionLosesDeletedRole(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	// The system database, as the server gives it to the auth stores.
	engine := storage.NewNamespacedEngine(base, "system")
	config := auth.DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := auth.NewAuthenticator(config, engine)
	require.NoError(t, err)
	roles := auth.NewRoleStore(engine)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	entitlements := auth.NewRoleEntitlementsStore(engine)
	require.NoError(t, entitlements.Set(ctx, "ops", []string{string(auth.PermRead), string(auth.PermAdmin)}))
	_, err = authenticator.CreateUser("bob", "password123", []auth.Role{"ops"})
	require.NoError(t, err)

	adapter := NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(func(roles []string) []string {
		return auth.PermissionsForRoles(roles, entitlements)
	})
	result, err := adapter.Authenticate("basic", "bob", "password123")
	require.NoError(t, err)
	require.Equal(t, []string{"ops"}, result.Roles)

	serverConfig := DefaultConfig()
	serverConfig.Authenticator = adapter
	server := &Server{config: serverConfig, sessions: map[string]*Session{}}
	run := func() (bool, string) {
		return runOnBoltSession(t, server, result, "CALL dbms.listConfig()", func(context.Context, string, map[string]any) error { return nil })
	}
	executed, code := run()
	require.True(t, executed, code)
	require.Empty(t, code)

	require.NoError(t, roles.DeleteRoleWithStores(ctx, "ops", auth.RoleMutationStores{Users: authenticator, Entitlements: entitlements}))
	executed, code = run()
	require.False(t, executed, "the statement ran with the deleted role's permissions")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
}

// A Bolt session reads "authentication disabled" from the one definition
// HTTP uses (auth.AuthenticationDisabled): no authenticator, or one whose
// user store has security disabled.
func TestBoltSessionAuthenticationDisabled(t *testing.T) {
	session := &Session{}
	require.True(t, session.authenticationDisabled())
	session.server = &Server{config: DefaultConfig(), sessions: map[string]*Session{}}
	require.True(t, session.authenticationDisabled())
	for _, enabled := range []bool{false, true} {
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { _ = engine.Close() })
		config := auth.DefaultAuthConfig()
		config.SecurityEnabled = enabled
		config.DefaultAdminUsername = ""
		config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
		authenticator, err := auth.NewAuthenticator(config, engine)
		require.NoError(t, err)
		session.server.config.Authenticator = NewAuthenticatorAdapter(authenticator)
		require.Equal(t, !enabled, session.authenticationDisabled())
	}
}

// CurrentAuthResult leaves an identity the user store doesn't keep as it
// is.
func TestCurrentAuthResultKeepsUnstoredIdentity(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	config := auth.DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := auth.NewAuthenticator(config, engine)
	require.NoError(t, err)
	adapter := NewAuthenticatorAdapter(authenticator)
	for _, result := range []*BoltAuthResult{
		nil,
		{Authenticated: true, Username: "", Roles: []string{"viewer"}},
		{Authenticated: true, Username: "cluster-node-2", PrincipalID: "sub:cluster-node-2", Roles: []string{"admin"}},
	} {
		require.Equal(t, result, adapter.CurrentAuthResult(result))
	}
}
