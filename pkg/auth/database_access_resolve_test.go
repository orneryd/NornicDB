package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// grantedAccess is the access the privileges matrix records for roles on
// database (PrivilegesStore.Granted), zero when it records none.
func grantedAccess(store *PrivilegesStore, roles []string, database string) ResolvedAccess {
	access, _ := store.Granted(roles, database)
	return access
}

// A principal's read and write on a database come from one resolver: the
// global entitlements an administrator stored for its roles (or the built-in
// roles' defaults) gate them, the privileges matrix decides within them when
// a role has an entry for the database, and the entitlements alone decide
// otherwise. The built-in role table is never consulted over a stored
// override.
func TestResolveDatabaseAccess(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	// The system database, as the server gives it to the auth stores.
	engine := storage.NewNamespacedEngine(base, "system")
	roles := NewRoleStore(engine)
	for _, role := range []string{"plain", "analyst", "loader"} {
		require.NoError(t, roles.CreateRole(ctx, role))
	}
	privileges := NewPrivilegesStore(engine)
	entitlements := NewRoleEntitlementsStore(engine)
	require.NoError(t, privileges.SavePrivilege(ctx, "plain", "nornic", true, true))
	require.NoError(t, privileges.SavePrivilege(ctx, "loader", "nornic", false, true))
	require.NoError(t, privileges.SavePrivilege(ctx, "viewer", "locked", false, false))
	require.NoError(t, entitlements.Set(ctx, "analyst", []string{"read"}))
	require.NoError(t, entitlements.Set(ctx, "loader", []string{"read", "write"}))

	for _, check := range []struct {
		name     string
		roles    []string
		database string
		want     ResolvedAccess
	}{
		{"built-in editor, no entry", []string{"editor"}, "nornic", ResolvedAccess{Read: true, Write: true}},
		{"built-in viewer, no entry", []string{"viewer"}, "nornic", ResolvedAccess{Read: true}},
		{"entry denies within the entitlements", []string{"viewer"}, "locked", ResolvedAccess{}},
		{"entry without any entitlement", []string{"plain"}, "nornic", ResolvedAccess{}},
		{"custom role with read, no entry", []string{"analyst"}, "nornic", ResolvedAccess{Read: true}},
		{"write-only entry", []string{"loader"}, "nornic", ResolvedAccess{Write: true}},
		{"entitlements of one role, entry of another", []string{"plain", "viewer"}, "nornic", ResolvedAccess{Read: true}},
		{"admin entitlement holds read and write", []string{"admin"}, "nornic", ResolvedAccess{Read: true, Write: true}},
		{"no roles", nil, "nornic", ResolvedAccess{}},
	} {
		require.Equal(t, check.want, ResolveDatabaseAccess(check.roles, check.database, privileges, entitlements), check.name)
		require.Equal(t, check.want.Read, DatabaseReadAllowed(check.roles, check.database, privileges, entitlements), check.name)
	}

	// An editor demoted to read keeps no write anywhere, with or without an
	// entry.
	require.NoError(t, privileges.SavePrivilege(ctx, "editor", "granted", true, true))
	require.NoError(t, entitlements.Set(ctx, "editor", []string{"read"}))
	require.Equal(t, ResolvedAccess{Read: true}, ResolveDatabaseAccess([]string{"editor"}, "nornic", privileges, entitlements))
	require.Equal(t, ResolvedAccess{Read: true}, ResolveDatabaseAccess([]string{"editor"}, "granted", privileges, entitlements))

	// Without stores: the built-in roles' defaults.
	require.Equal(t, ResolvedAccess{Read: true, Write: true}, ResolveDatabaseAccess([]string{"editor"}, "nornic", nil, nil))
	require.Equal(t, ResolvedAccess{}, ResolveDatabaseAccess([]string{"analyst"}, "nornic", nil, nil))
	access, found := (*PrivilegesStore)(nil).Granted([]string{"editor"}, "nornic")
	require.False(t, found)
	require.Equal(t, ResolvedAccess{}, access)
}
