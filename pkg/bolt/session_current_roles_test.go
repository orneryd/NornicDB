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

// boltStores are the system database's auth stores a Bolt server reads
// through the production resolvers.
type boltStores struct {
	roles        *auth.RoleStore
	privileges   *auth.PrivilegesStore
	entitlements *auth.RoleEntitlementsStore
}

// boltAccountFixture is an authenticator over a system database, its adapter
// with the server's permission resolver, and a Bolt server using it, wired
// as the production server wires it (cmd/nornicdb): the effective
// entitlements are auth.PermissionsForRoles over the role entitlements, and
// the database read / write resolver is auth.ResolveDatabaseAccess over the
// privileges and the entitlements (Server.GetResolvedAccessForRoles).
func boltAccountFixture(t *testing.T) (*auth.Authenticator, *AuthenticatorAdapter, *Server) {
	authenticator, adapter, server, _ := boltProductionFixture(t)
	return authenticator, adapter, server
}

func boltProductionFixture(t *testing.T) (*auth.Authenticator, *AuthenticatorAdapter, *Server, boltStores) {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "system")
	config := auth.DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := auth.NewAuthenticator(config, engine)
	require.NoError(t, err)
	stores := boltStores{
		roles:        auth.NewRoleStore(engine),
		privileges:   auth.NewPrivilegesStore(engine),
		entitlements: auth.NewRoleEntitlementsStore(engine),
	}
	adapter := NewAuthenticatorAdapter(authenticator)
	adapter.SetGetEffectivePermissions(func(roles []string) []string { return auth.PermissionsForRoles(roles, stores.entitlements) })
	serverConfig := DefaultConfig()
	serverConfig.Authenticator = adapter
	server := &Server{config: serverConfig, sessions: map[string]*Session{}}
	server.SetResolvedAccessResolver(func(roles []string, database string) auth.ResolvedAccess {
		return auth.ResolveDatabaseAccess(roles, database, stores.privileges, stores.entitlements)
	})
	return authenticator, adapter, server, stores
}

// A Bolt session of an account that is deleted, disabled, or deleted and
// created again under the same name runs nothing more: its next statement is
// refused, and the account's cached password no longer authenticates.
func TestBoltSessionOfRemovedAccountRunsNothing(t *testing.T) {
	for _, change := range []string{"deleted", "disabled", "deleted and recreated"} {
		t.Run(change, func(t *testing.T) {
			authenticator, adapter, server := boltAccountFixture(t)
			_, err := authenticator.CreateUser("bob", "password123", []auth.Role{auth.RoleAdmin})
			require.NoError(t, err)
			result, err := adapter.Authenticate("basic", "bob", "password123")
			require.NoError(t, err)
			run := func() (bool, string) {
				return runOnBoltSession(t, server, result, "CALL dbms.listConfig()", func(context.Context, string, map[string]any) error { return nil })
			}
			executed, code := run()
			require.True(t, executed, code)

			switch change {
			case "deleted":
				require.NoError(t, authenticator.DeleteUser("bob"))
			case "disabled":
				require.NoError(t, authenticator.DisableUser("bob"))
			case "deleted and recreated":
				require.NoError(t, authenticator.DeleteUser("bob"))
				_, err = authenticator.CreateUser("bob", "password123", []auth.Role{auth.RoleAdmin})
				require.NoError(t, err)
			}
			executed, code = run()
			require.False(t, executed, "the statement ran for a removed account")
			require.Equal(t, "Neo.ClientError.Security.Forbidden", code)
			fresh, err := adapter.Authenticate("basic", "bob", "password123")
			if change == "deleted and recreated" {
				require.NoError(t, err, "the new account's password is checked in full")
				executed, code = runOnBoltSession(t, server, fresh, "CALL dbms.listConfig()", func(context.Context, string, map[string]any) error { return nil })
				require.True(t, executed, code)
			} else {
				require.Error(t, err, "the cached password authenticates no more")
			}
		})
	}
}

// The identities that are not user accounts keep their roles: the anonymous
// principal while the adapter allows anonymous access (and only then), and a
// cluster node's token.
func TestCurrentAuthResultNonAccountIdentities(t *testing.T) {
	authenticator, adapter, _ := boltAccountFixture(t)
	require.Nil(t, adapter.CurrentAuthResult(nil))

	adapter.SetAllowAnonymous(true)
	anonymous, err := adapter.Authenticate("none", "", "")
	require.NoError(t, err)
	require.Same(t, anonymous, adapter.CurrentAuthResult(anonymous))
	adapter.SetAllowAnonymous(false)
	require.Empty(t, adapter.CurrentAuthResult(anonymous).Roles)

	token, err := authenticator.GenerateClusterToken("node-2", auth.RoleAdmin)
	require.NoError(t, err)
	cluster, err := adapter.Authenticate("bearer", "", token)
	require.NoError(t, err)
	require.Same(t, cluster, adapter.CurrentAuthResult(cluster))
	require.Equal(t, []string{"admin"}, cluster.Roles)
}
