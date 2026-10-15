package auth

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A changed password stops working at once: every Basic credentials cache
// registered with OnPasswordChange forgets the user's entries, and only
// that user's.
func TestPasswordChangeForgetsCachedCredentials(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := NewAuthenticator(config, engine)
	require.NoError(t, err)
	for _, user := range []string{"bob", "carol"} {
		_, err := authenticator.CreateUser(user, "password123", []Role{RoleViewer})
		require.NoError(t, err)
	}
	cache := NewBasicAuthCache(DefaultAuthCacheEntries, DefaultAuthCacheTTL)
	authenticator.OnPasswordChange(cache.ForgetUser)
	authenticator.OnPasswordChange(nil)
	(*Authenticator)(nil).OnPasswordChange(cache.ForgetUser)
	cache.Set("bob", "password123", &JWTClaims{Username: "bob"})
	cache.SetFromHeader("Basic Ym9iOnBhc3N3b3JkMTIz", &JWTClaims{Username: "bob"})
	cache.Set("carol", "password123", &JWTClaims{Username: "carol"})

	require.Error(t, authenticator.ChangePassword("bob", "wrong-password", "newPassword456"))
	_, cached := cache.Get("bob", "password123")
	require.True(t, cached, "a refused change forgets nothing")

	require.NoError(t, authenticator.ChangePassword("bob", "password123", "newPassword456"))
	_, cached = cache.Get("bob", "password123")
	require.False(t, cached)
	_, cached = cache.GetFromHeader("Basic Ym9iOnBhc3N3b3JkMTIz")
	require.False(t, cached)
	_, cached = cache.Get("carol", "password123")
	require.True(t, cached, "another user's entry stays")
	(*BasicAuthCache)(nil).ForgetUser("carol")
}
