package auth

import (
	"path/filepath"
	"testing"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestFineGrainedDirectoryFollowsLiveRestore(t *testing.T) {
	ctx := t.Context()
	base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	system := storage.NewNamespacedEngine(base, "system")
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("fgac-restore-test-signing-key-32b")
	users, err := NewAuthenticator(config, system)
	require.NoError(t, err)
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "operator"))
	_, err = users.CreateUser(ctx, "sam", "password123", []Role{"operator"})
	require.NoError(t, err)
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "reports-id"}}},
		Assignments: map[string][]string{"operator": {"retry"}},
	}
	require.NoError(t, roles.ReplaceFineGrainedCatalog(ctx, "", fgacTestActions, catalog))
	runtime := NewFineGrainedRuntime(roles, fgacTestActions, func(string) string { return "reports-id" }, func() string { return "reports" })
	runtime.SetSecurityDirectory(users, RoleMutationStores{Users: users})
	token, _, err := users.Authenticate(ctx, "sam", "password123", "", "")
	require.NoError(t, err)

	backup := filepath.Join(t.TempDir(), "system.badger")
	require.NoError(t, base.Backup(backup))
	require.NoError(t, runtime.RenameRole(ctx, "operator", "renamed"))
	claims, err := users.ValidateToken(token.AccessToken)
	require.NoError(t, err)
	require.Equal(t, []string{"renamed"}, claims.Roles)

	requestIdentity := RequestIdentity{ID: PrincipalID(claims), Username: claims.Username}
	admitted, err := runtime.Admit(WithRequestIdentity(ctx, requestIdentity), claims.Roles)
	require.NoError(t, err)
	require.True(t, admitted.AllowsDatabase("embedding.failures.retry", "reports", ""))
	require.NoError(t, base.Restore(backup))

	afterRestore, err := users.ValidateToken(token.AccessToken)
	require.NoError(t, err)
	require.Equal(t, []string{"operator"}, afterRestore.Roles)
	next, err := runtime.Admit(WithRequestIdentity(ctx, requestIdentity), afterRestore.Roles)
	require.NoError(t, err)
	require.True(t, next.AllowsDatabase("embedding.failures.retry", "reports", ""))
	require.ErrorIs(t, admitted.Revalidate(ctx, requestIdentity), ErrFineGrainedForbidden)
}

func TestFineGrainedDirectoryFollowsReplicatedPolicy(t *testing.T) {
	ctx := t.Context()
	leader := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	follower := runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	leaderLog, err := replication.NewStorageAdapterWithWAL(leader, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, leaderLog.Close()) })
	followerLog, err := replication.NewStorageAdapterWithWAL(follower, t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, followerLog.Close()) })
	primary := replication.NewStandaloneReplicator(replication.DefaultConfig(), leaderLog)
	require.NoError(t, primary.Start(ctx))
	t.Cleanup(func() { require.NoError(t, primary.Shutdown()) })
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("fgac-replay-test-signing-key-32b")
	open := func(system storage.Engine) (*Authenticator, *FineGrainedRuntime) {
		users, err := NewAuthenticator(config, system)
		require.NoError(t, err)
		roles := NewRoleStore(system)
		require.NoError(t, roles.Load(ctx))
		runtime := NewFineGrainedRuntime(roles, fgacTestActions, func(string) string { return "reports-id" }, func() string { return "reports" })
		runtime.SetSecurityDirectory(users, RoleMutationStores{Users: users})
		return users, runtime
	}
	leaderUsers, leaderPolicy := open(storage.NewNamespacedEngine(replication.NewReplicatedEngine(leader, primary, 0), "system"))
	require.NoError(t, leaderPolicy.CreateRole(ctx, "operator"))
	user, err := leaderUsers.CreateUser(ctx, "sam", "password123", []Role{"operator"})
	require.NoError(t, err)
	catalog := FineGrainedCatalog{
		Definitions: []FineGrainedEntitlement{{ID: "retry", Action: "embedding.failures.retry",
			Scope: FineGrainedScope{Kind: FineGrainedDatabase, DatabaseID: "reports-id"}}},
		Assignments: map[string][]string{"operator": {"retry"}},
	}
	require.NoError(t, leaderPolicy.ReplaceCatalog(ctx, "", catalog))

	var last uint64
	syncFollower := func() {
		t.Helper()
		require.NoError(t, leaderLog.FlushWAL())
		entries, err := leaderLog.GetWALEntries(last, 20)
		require.NoError(t, err)
		require.NotEmpty(t, entries)
		last, err = replication.NewWALApplier(followerLog).ApplyBatch(ctx, entries)
		require.NoError(t, err)
	}
	syncFollower()
	followerUsers, followerPolicy := open(storage.NewNamespacedEngine(replication.NewReplicatedEngine(follower, nil, 0), "system"))
	initial := &JWTClaims{Sub: user.ID, Username: user.Username}
	identity := RequestIdentity{ID: PrincipalID(initial), Username: initial.Username}
	ctx = WithRequestIdentity(ctx, identity)
	claims, err := followerUsers.CurrentClaims(initial)
	require.NoError(t, err)
	require.Equal(t, []string{"operator"}, claims.Roles)
	request, err := followerPolicy.Admit(ctx, claims.Roles)
	require.NoError(t, err)
	require.True(t, request.AllowsDatabase("embedding.failures.retry", "reports", ""))

	require.NoError(t, leaderPolicy.RenameRole(ctx, "operator", "renamed"))
	syncFollower()
	claims, err = followerUsers.CurrentClaims(initial)
	require.NoError(t, err)
	require.Equal(t, []string{"renamed"}, claims.Roles)
	current, err := followerPolicy.Admit(ctx, claims.Roles)
	require.NoError(t, err)
	require.True(t, current.AllowsDatabase("embedding.failures.retry", "reports", ""))
	require.Contains(t, followerPolicy.RoleNames(), "renamed")
	require.NotContains(t, followerPolicy.RoleNames(), "operator")
	require.ErrorIs(t, request.Revalidate(ctx, identity), ErrFineGrainedForbidden)
}
