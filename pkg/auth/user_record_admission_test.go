package auth

import (
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestUserRecordWritesCarryTheSystemStoreAdmission drives the public paths that
// write a user record (account creation, successful and failed logins) on the
// namespaced system store: no storage owner has to admit the work itself.
func TestUserRecordWritesCarryTheSystemStoreAdmission(t *testing.T) {
	config := DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-that-is-long-enough-32chars")
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	authenticator, err := NewAuthenticator(config, storage.NewNamespacedEngine(engine, "system"))
	require.NoError(t, err)

	counts := func() map[string]int64 {
		out := map[string]int64{}
		for _, fallback := range storage.VectorWorkFallbacks() {
			out[fallback.Site] = fallback.Count
		}
		return out
	}
	before := counts()
	_, err = authenticator.CreateUser("record-admission", "a-long-enough-password", []Role{RoleViewer})
	require.NoError(t, err)
	_, _, err = authenticator.Authenticate("record-admission", "a-long-enough-password", "127.0.0.1", "test")
	require.NoError(t, err)
	_, _, err = authenticator.Authenticate("record-admission", "the-wrong-password", "127.0.0.1", "test")
	require.ErrorIs(t, err, ErrInvalidCredentials)

	fired := map[string]int64{}
	for site, count := range counts() {
		if delta := count - before[site]; delta != 0 {
			fired[site] = delta
		}
	}
	require.Empty(t, fired, "a user record write reached a storage owner without its admission")
}
