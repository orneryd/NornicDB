package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func currentRolesAuthenticator(t testing.TB) (*Authenticator, *RoleStore) {
	t.Helper()
	engine := newSystemTestEngine(t)
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
	for _, change := range []string{"role deleted", "role removed from user", "user recreated"} {
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
			case "user recreated":
				require.NoError(t, authenticator.DeleteUser("bob"))
				_, err := authenticator.CreateUser("bob", "password123", []Role{"ops", RoleViewer})
				require.NoError(t, err)
				want = []string{}
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
	engine := storage.NewMemoryEngine()
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

// Claims without a username (or none at all) are returned as they are.
func TestCurrentRoleClaimsWithoutUsername(t *testing.T) {
	authenticator, _ := currentRolesAuthenticator(t)
	require.Nil(t, authenticator.currentRoleClaims(nil))
	claims := &JWTClaims{Sub: "service", Roles: []string{"viewer"}}
	require.Same(t, claims, authenticator.currentRoleClaims(claims))
}
