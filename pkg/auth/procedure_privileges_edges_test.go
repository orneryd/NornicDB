package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"

	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var errPrivilegeStorage = errors.New("storage unavailable")

// The store's edges: a nil store or storage, other system nodes, storage
// errors on grant, revoke and list, list order among one role's grants, and
// globs ending in stars. The shared storage fault fixture selects the one
// operation that fails.
func TestProcedurePrivilegeStoreEdges(t *testing.T) {
	ctx := context.Background()
	var nilStore *ProcedurePrivilegeStore
	grants, err := nilStore.List(ctx)
	require.NoError(t, err)
	require.Nil(t, grants)
	allowed, err := nilStore.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.False(t, allowed)
	grants, err = NewProcedurePrivilegeStore(nil).List(ctx)
	require.NoError(t, err)
	require.Nil(t, grants)

	system := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = system.Close() })
	_, err = system.CreateNode(&storage.Node{ID: "user:x", Labels: []string{"_User", "_System"}, Properties: map[string]any{"name": "x"}})
	require.NoError(t, err)
	faults := storagefault.NewMutationFailureEngine(system, "", errPrivilegeStorage, 0, WithStoreTransaction)
	store := NewProcedurePrivilegeStore(faults)

	// One role's grants order by procedure, then a plain grant before a
	// boosted one; other system nodes are not grants.
	require.NoError(t, store.Grant(ctx, "ops", "db.b", true, false))
	require.NoError(t, store.Grant(ctx, "ops", "db.a", true, false))
	require.NoError(t, store.Grant(ctx, "ops", "db.a", false, false))
	grants, err = store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ProcedurePrivilege{
		{Role: "ops", Procedure: "db.a", Boosted: false},
		{Role: "ops", Procedure: "db.a", Boosted: true},
		{Role: "ops", Procedure: "db.b", Boosted: true},
	}, grants)

	faults.Select(storagefault.MutationCreate, "", 0)
	require.ErrorIs(t, store.Grant(ctx, "ops", "db.c", true, false), errPrivilegeStorage)

	faults.Select(storagefault.MutationDelete, "", 0)
	revoked, err := store.Revoke(ctx, "ops", "db.b", true, false)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, revoked)

	// A read that fails is an error, never "no such grant" or "already
	// granted".
	faults.Select(storagefault.MutationGet, "", 0)
	require.ErrorIs(t, store.Grant(ctx, "ops", "db.c", true, false), errPrivilegeStorage)
	revoked, err = store.Revoke(ctx, "ops", "db.b", true, false)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, revoked)
	faults.Select("", "", 0)
	allowed, err = store.AllowsBoosted([]string{"ops"}, "db.b")
	require.NoError(t, err)
	require.True(t, allowed)

	// A list that fails is an error.
	faults.Select(storagefault.MutationScan, "", 0)
	_, err = store.List(ctx)
	require.ErrorIs(t, err, errPrivilegeStorage)
	faults.Select("", "", 0)

	require.True(t, ProcedureGlobMatches("db.cdc.**", "db.cdc.query"))
	require.True(t, ProcedureGlobMatches("db.cdc.query*", "db.cdc.query"))
	require.False(t, ProcedureGlobMatches("db.cdc.query*x", "db.cdc.query"))
}

// When the grants can't be read, AllowsBoosted denies and returns the
// error: a grant revoked by another store is never answered from a copy
// read before.
func TestProcedurePrivilegeStoreDeniesWhenUnreadable(t *testing.T) {
	ctx := context.Background()
	system := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = system.Close() })
	writer := NewProcedurePrivilegeStore(system)
	require.NoError(t, writer.Grant(ctx, "ops", "db.cdc.*", true, false))
	faults := storagefault.NewMutationFailureEngine(system, "", errPrivilegeStorage, 0, WithStoreTransaction)
	reader := NewProcedurePrivilegeStore(faults)
	allowed, err := reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed)

	revoked, err := writer.Revoke(ctx, "ops", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.True(t, revoked)
	faults.Select(storagefault.MutationScan, "", 0)
	allowed, err = reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, allowed)

	// Unreadable before the revoke too: still denied.
	faults.Select("", "", 0)
	require.NoError(t, writer.Grant(ctx, "ops", "db.cdc.*", true, false))
	faults.Select(storagefault.MutationScan, "", 0)
	allowed, err = reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, allowed)
}

// A role whose grants can't be read or removed isn't deleted: deleting it
// would leave its grants for a role created later under the same name.
func TestDeleteRoleKeepsRoleWhenGrantsCannotBeRemoved(t *testing.T) {
	ctx := context.Background()
	system := storage.NewNamespacedEngine(runtimeFixtureOwner.Own(t, runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "system")
	t.Cleanup(func() { _ = system.Close() })
	faults := storagefault.NewMutationFailureEngine(system, "", errPrivilegeStorage, 0, WithStoreTransaction)
	roles := NewRoleStore(faults)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	require.NoError(t, NewProcedurePrivilegeStore(faults).Grant(ctx, "ops", "db.cdc.*", true, false))

	faults.Select(storagefault.MutationScan, "", 0)
	require.ErrorIs(t, roles.DeleteRole(ctx, "ops"), errPrivilegeStorage)
	faults.Select(storagefault.MutationDelete, "", 0)
	require.ErrorIs(t, roles.DeleteRole(ctx, "ops"), errPrivilegeStorage)
	faults.Select("", "", 0)
	require.True(t, roles.Exists("ops"))
}
