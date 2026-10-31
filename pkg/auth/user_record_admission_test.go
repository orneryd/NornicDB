package auth

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestUserRecordWritesCarryTheSystemStoreAdmission drives the public paths that
// write a user record (account creation, successful and failed logins) on the
// namespaced system store: no storage owner has to admit the work itself, and
// each publishes under the system-store admission.
func TestUserRecordWritesCarryTheSystemStoreAdmission(t *testing.T) {
	config := DefaultAuthConfig()
	config.JWTSecret = []byte("test-secret-that-is-long-enough-32chars")
	engine := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	authenticator, err := NewAuthenticator(config, storage.NewNamespacedEngine(engine, "system"))
	require.NoError(t, err)

	var published sync.Map // operation type -> a wrong origin, if one was published
	var seen atomic.Int64
	require.NoError(t, storage.SubscribeEvents(engine, storage.EventHandlers{EntityCommitted: func(event storage.EntityCommitEvent) {
		seen.Add(1)
		if event.VectorWorkOrigin != storage.SystemStoreAdmission {
			published.Store(event.Operation.Type, event.VectorWorkOrigin)
		}
	}}))
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
	require.Eventually(t, func() bool { return seen.Load() >= 3 }, 5*time.Second, 5*time.Millisecond, "the create and both logins publish")
	published.Range(func(kind, origin any) bool {
		t.Errorf("%v published under %v, not the system-store admission", kind, origin)
		return true
	})
}
