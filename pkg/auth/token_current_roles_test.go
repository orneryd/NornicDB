package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func currentRolesAuthenticator(t testing.TB) (*Authenticator, *RoleStore) {
	t.Helper()
	base := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	engine := storage.NewNamespacedEngine(base, "system")
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	return authenticator, NewRoleStore(engine)
}

// A token's roles are the roles its user holds now: after the user loses a
// role, directly or because the role was deleted, a token issued before no
// longer carries it, as in Neo4j. A role created again under the old name
// doesn't reach the old token either.
func TestTokenRolesFollowTheUsersCurrentRoles(t *testing.T) {
	ctx := context.Background()
	for _, change := range []string{"role deleted", "role removed from user"} {
		t.Run(change, func(t *testing.T) {
			authenticator, roles := currentRolesAuthenticator(t)
			require.NoError(t, roles.CreateRole(ctx, "ops"))
			_, err := authenticator.CreateUser("bob", "password123", []Role{"ops", RoleViewer})
			require.NoError(t, err)
			token, _, err := authenticator.Authenticate("bob", "password123", "", "")
			require.NoError(t, err)
			claims, err := authenticator.ValidateToken(token.AccessToken)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"ops", "viewer"}, claims.Roles)

			want := []string{"viewer"}
			switch change {
			case "role deleted":
				require.NoError(t, roles.DeleteRoleWithStores(ctx, "ops", RoleMutationStores{Users: authenticator}))
				require.NoError(t, roles.CreateRole(ctx, "ops"))
			case "role removed from user":
				require.NoError(t, authenticator.UpdateRoles("bob", []Role{RoleViewer}))
			}
			claims, err = authenticator.ValidateToken(token.AccessToken)
			require.NoError(t, err)
			require.ElementsMatch(t, want, claims.Roles)
			// Twice: the cached claims are not changed by the first check.
			claims, err = authenticator.ValidateToken(token.AccessToken)
			require.NoError(t, err)
			require.ElementsMatch(t, want, claims.Roles)
		})
	}
}

// AuthenticationDisabled is true without an authenticator or with security
// disabled, and false otherwise.
func TestAuthenticationDisabledDefinition(t *testing.T) {
	require.True(t, AuthenticationDisabled(nil))
	enabled, _ := currentRolesAuthenticator(t)
	require.False(t, AuthenticationDisabled(enabled))
	config := DefaultAuthConfig()
	config.SecurityEnabled = false
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	engine := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = engine.Close() })
	disabled, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	require.True(t, AuthenticationDisabled(disabled))
}

// BenchmarkValidateTokenCurrentRoles is the cost of a token check on a
// cached token, with its roles read from the user store, next to the bare
// cache lookup it adds to.
func BenchmarkValidateTokenCurrentRoles(b *testing.B) {
	authenticator, _ := currentRolesAuthenticator(b)
	_, err := authenticator.CreateUser("bob", "password123", []Role{RoleEditor, RoleViewer})
	require.NoError(b, err)
	token, _, err := authenticator.Authenticate("bob", "password123", "", "")
	require.NoError(b, err)
	_, err = authenticator.ValidateToken(token.AccessToken)
	require.NoError(b, err)
	b.Run("ValidateToken", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := authenticator.ValidateToken(token.AccessToken); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cache lookup only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, ok := authenticator.tokenCache.get(token.AccessToken); !ok {
				b.Fatal("not cached")
			}
		}
	})
}

// A deleted or disabled user's token, and the token of an account deleted
// and created again under the same name, no longer authorize anything:
// ValidateToken refuses them, and a fresh login of the new account works.
func TestTokenOfRemovedAccountAuthorizesNothing(t *testing.T) {
	for _, change := range []string{"deleted", "disabled", "deleted and recreated"} {
		t.Run(change, func(t *testing.T) {
			authenticator, _ := currentRolesAuthenticator(t)
			_, err := authenticator.CreateUser("carol", "password123", []Role{RoleAdmin})
			require.NoError(t, err)
			token, _, err := authenticator.Authenticate("carol", "password123", "", "")
			require.NoError(t, err)
			_, err = authenticator.ValidateToken(token.AccessToken)
			require.NoError(t, err)
			switch change {
			case "deleted":
				require.NoError(t, authenticator.DeleteUser("carol"))
			case "disabled":
				require.NoError(t, authenticator.ChangeUserState("carol", UserDisable))
			case "deleted and recreated":
				require.NoError(t, authenticator.DeleteUser("carol"))
				_, err = authenticator.CreateUser("carol", "password123", []Role{RoleAdmin})
				require.NoError(t, err)
			}
			_, err = authenticator.ValidateToken(token.AccessToken)
			require.ErrorIs(t, err, ErrInvalidToken)
			// Twice: the first refusal leaves the cached token as it was.
			_, err = authenticator.ValidateToken(token.AccessToken)
			require.ErrorIs(t, err, ErrInvalidToken)
			if change == "deleted and recreated" {
				fresh, _, err := authenticator.Authenticate("carol", "password123", "", "")
				require.NoError(t, err)
				claims, err := authenticator.ValidateToken(fresh.AccessToken)
				require.NoError(t, err)
				require.Equal(t, []string{"admin"}, claims.Roles)
			}
		})
	}
}

// CurrentClaims refuses no claims and claims naming no stored account, and
// keeps a cluster node's token, the one identity that is not a user
// account, with its own roles.
func TestCurrentClaimsIdentities(t *testing.T) {
	authenticator, _ := currentRolesAuthenticator(t)
	_, err := authenticator.CurrentClaims(nil)
	require.ErrorIs(t, err, ErrInvalidToken)
	_, err = authenticator.CurrentClaims(&JWTClaims{Sub: "service", Roles: []string{"admin"}})
	require.ErrorIs(t, err, ErrInvalidToken)

	token, err := authenticator.GenerateClusterToken("node-2", RoleAdmin)
	require.NoError(t, err)
	claims, err := authenticator.ValidateToken(token)
	require.NoError(t, err)
	require.Equal(t, []string{"admin"}, claims.Roles)
	token, err = authenticator.GenerateClusterTokenWithExpiry("node-3", RoleViewer, 0)
	require.NoError(t, err)
	claims, err = authenticator.ValidateToken(token)
	require.NoError(t, err)
	require.Equal(t, []string{"viewer"}, claims.Roles)
}
