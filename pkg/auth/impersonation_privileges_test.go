package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newImpersonationSystem(t *testing.T) storage.Engine {
	t.Helper()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	return storage.NewNamespacedEngine(base, "system")
}

// A principal may impersonate a user granted to one of its roles by name or
// by *, or any user as an administrator; a DENY to any of its roles, by
// name or by *, wins over every grant and over administration.
func TestImpersonatePrivilegeDecision(t *testing.T) {
	ctx := context.Background()
	store := NewImpersonatePrivilegeStore(newImpersonationSystem(t))
	allows := func(roles []string, admin bool, user string) bool {
		t.Helper()
		allowed, err := store.AllowsImpersonation(roles, admin, user)
		require.NoError(t, err)
		return allowed
	}
	require.False(t, allows([]string{"svc"}, false, "alice"))
	require.True(t, allows([]string{"svc"}, true, "alice"), "administrators hold IMPERSONATE (*)")
	require.False(t, allows([]string{"svc"}, true, ""))

	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("alice"), false, false))
	require.True(t, allows([]string{"svc"}, false, "alice"))
	require.True(t, allows([]string{"ROLE_SVC"}, false, "alice"), "role names are normalized")
	require.False(t, allows([]string{"svc"}, false, "Alice"), "user names are case-sensitive")
	require.False(t, allows([]string{"svc"}, false, "bob"))
	require.False(t, allows([]string{"other"}, false, "alice"))

	require.NoError(t, store.Set(ctx, "all", ImpersonateAllUsers, false, false))
	require.True(t, allows([]string{"all"}, false, "bob"))

	require.NoError(t, store.Set(ctx, "blocked", ImpersonateUser("bob"), true, false))
	require.False(t, allows([]string{"all", "blocked"}, false, "bob"), "a DENY on another role wins")
	require.True(t, allows([]string{"all", "blocked"}, false, "carol"))
	require.False(t, allows([]string{"blocked"}, true, "bob"), "a DENY wins over administration")
	require.NoError(t, store.Set(ctx, "nobody", ImpersonateAllUsers, true, false))
	require.False(t, allows([]string{"svc", "nobody"}, false, "alice"), "DENY (*) wins over a named GRANT")
}

// Setting a privilege again changes nothing except making it immutable;
// REVOKE IMMUTABLE removes only an immutable one; List orders by role, user,
// DENY first.
func TestImpersonatePrivilegeStoreRecords(t *testing.T) {
	ctx := context.Background()
	store := NewImpersonatePrivilegeStore(newImpersonationSystem(t))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("alice"), false, false))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("alice"), false, false))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("alice"), true, false))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("bob"), false, true))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("bob"), false, false), "a regular GRANT leaves an immutable one immutable")
	require.NoError(t, store.Set(ctx, "a:b", ImpersonateUser("c"), false, false))
	require.NoError(t, store.Set(ctx, "a", ImpersonateUser("b:c"), false, false))
	list, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ImpersonatePrivilege{
		{Role: "a", User: "b:c"},
		{Role: "a:b", User: "c"},
		{Role: "svc", User: "alice", Denied: true},
		{Role: "svc", User: "alice"},
		{Role: "svc", User: "bob", Immutable: true},
	}, list)

	removed, err := store.Remove(ctx, "svc", ImpersonateUser("alice"), false, true)
	require.NoError(t, err)
	require.False(t, removed, "REVOKE IMMUTABLE leaves a regular privilege")
	removed, err = store.Remove(ctx, "svc", ImpersonateUser("alice"), false, false)
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = store.Remove(ctx, "svc", ImpersonateUser("bob"), false, true)
	require.NoError(t, err)
	require.True(t, removed)
	_, found, err := store.Lookup(ctx, "svc", ImpersonateUser("alice"), true)
	require.NoError(t, err)
	require.True(t, found, "removing the GRANT keeps the DENY")
}

// A node made any other way than through the store is never a privilege.
func TestImpersonatePrivilegeIgnoresForeignNodes(t *testing.T) {
	system := newImpersonationSystem(t)
	_, err := system.CreateNode(&storage.Node{
		ID:         "forged",
		Labels:     []string{impersonatePrivilegeLabel},
		Properties: map[string]any{"role": "svc", "user": "*", "denied": false},
	})
	require.NoError(t, err)
	allowed, err := NewImpersonatePrivilegeStore(system).AllowsImpersonation([]string{"svc"}, false, "alice")
	require.NoError(t, err)
	require.False(t, allowed)
}

// When the privileges can't be read, impersonation is denied with the error.
func TestImpersonatePrivilegeReadFailureDenies(t *testing.T) {
	system := newImpersonationSystem(t)
	require.NoError(t, NewImpersonatePrivilegeStore(system).Set(context.Background(), "svc", ImpersonateAllUsers, false, false))
	failing := &labelReadFailingEngine{Engine: system}
	allowed, err := NewImpersonatePrivilegeStore(failing).AllowsImpersonation([]string{"svc"}, true, "alice")
	require.Error(t, err)
	require.False(t, allowed, "a failed read denies even an administrator")
}

type labelReadFailingEngine struct{ storage.Engine }

func (e *labelReadFailingEngine) GetNodesByLabel(string) ([]*storage.Node, error) {
	return nil, errors.New("read failed")
}

// Renaming a role moves its impersonation privileges; deleting it removes
// them (the one role-owned record enumerator).
func TestImpersonatePrivilegesFollowRoleRenameAndDelete(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	store := NewImpersonatePrivilegeStore(system)
	require.NoError(t, store.Set(ctx, "ops", ImpersonateUser("alice"), false, false))
	require.NoError(t, store.Set(ctx, "ops", ImpersonateUser("bob"), true, true))

	require.NoError(t, roles.RenameRole(ctx, "ops", "renamed"))
	list, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ImpersonatePrivilege{
		{Role: "renamed", User: "alice"},
		{Role: "renamed", User: "bob", Denied: true, Immutable: true},
	}, list)

	require.NoError(t, roles.DeleteRole(ctx, "renamed"))
	list, err = store.List(ctx)
	require.NoError(t, err)
	require.Empty(t, list)
}

// ImpersonationClaims is an existing, enabled account's current identity.
func TestImpersonationClaims(t *testing.T) {
	config := DefaultAuthConfig()
	config.DefaultAdminUsername = ""
	config.JWTSecret = []byte("test-secret-key-for-testing-only-32b")
	authenticator, err := NewAuthenticator(config, newImpersonationSystem(t))
	require.NoError(t, err)
	_, err = authenticator.CreateUser("alice", "password123", []Role{RoleViewer})
	require.NoError(t, err)
	claims, err := authenticator.ImpersonationClaims("alice")
	require.NoError(t, err)
	require.Equal(t, "alice", claims.Username)
	require.Equal(t, []string{string(RoleViewer)}, claims.Roles)
	require.NotEmpty(t, claims.Sub)

	_, err = authenticator.ImpersonationClaims("nobody")
	require.ErrorIs(t, err, ErrUserNotFound)
	require.NoError(t, authenticator.DisableUser("alice"))
	_, err = authenticator.ImpersonationClaims("alice")
	require.ErrorIs(t, err, ErrUserNotFound)
}

// A privilege for the user named * is a separate record from IMPERSONATE (*)
// and lets its role impersonate only that user.
func TestImpersonatePrivilegeUserNamedStarIsNotEveryUser(t *testing.T) {
	ctx := context.Background()
	store := NewImpersonatePrivilegeStore(newImpersonationSystem(t))
	require.NoError(t, store.Set(ctx, "svc", ImpersonateUser("*"), false, false))
	allowed, err := store.AllowsImpersonation([]string{"svc"}, false, "admin")
	require.NoError(t, err)
	require.False(t, allowed)
	allowed, err = store.AllowsImpersonation([]string{"svc"}, false, "*")
	require.NoError(t, err)
	require.True(t, allowed)
	_, found, err := store.Lookup(ctx, "svc", ImpersonateAllUsers, false)
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, store.Set(ctx, "svc", ImpersonateAllUsers, false, false))
	list, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ImpersonatePrivilege{
		{Role: "svc", AllUsers: true},
		{Role: "svc", User: "*"},
	}, list)
	removed, err := store.Remove(ctx, "svc", ImpersonateUser("*"), false, false)
	require.NoError(t, err)
	require.True(t, removed)
	list, err = store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ImpersonatePrivilege{{Role: "svc", AllUsers: true}}, list)
	require.Error(t, store.Set(ctx, "svc", ImpersonateUser(" "), false, false))
}
