package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeletedRoleStartsEmptyAfterRecreation(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	roles := seedRoleRename(t, system)
	require.NoError(t, (&Authenticator{storage: system}).saveUser(context.Background(), &User{Username: "solo", Roles: []Role{"ops"}}, nil))
	require.NoError(t, roles.DeleteRole(ctx, "ops"))
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants, err := NewProcedurePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)
	privileges := NewPrivilegesStore(system)
	require.NoError(t, privileges.Load(ctx))
	require.Empty(t, privileges.Matrix())
	require.Equal(t, ResolvedAccess{}, grantedAccess(privileges, []string{"ops"}, "nornic"))
	allowlist := NewAllowlistStore(system)
	require.NoError(t, allowlist.Load(ctx))
	require.Empty(t, allowlist.Allowlist())
	entitlements := NewRoleEntitlementsStore(system)
	require.NoError(t, entitlements.Load(ctx))
	require.Empty(t, ReadRoleCatalog(nil, entitlements).EntitlementOverrides)
	user, err := system.GetNode("user:worker")
	require.NoError(t, err)
	require.Equal(t, `["viewer"]`, user.Properties["roles"])
	solo, err := system.GetNode("user:solo")
	require.NoError(t, err)
	require.Equal(t, `[]`, solo.Properties["roles"])
}

func TestDeleteRoleOwnedRecordFailureChangesNothing(t *testing.T) {
	for _, test := range []struct {
		op storagefault.MutationOperation
		id storage.NodeID
	}{
		{storagefault.MutationDelete, "proc_priv:execute:ops:db.a"},
		{storagefault.MutationDelete, "db_priv:ops:nornic"},
		{storagefault.MutationDelete, "role_db_access:ops"},
		{storagefault.MutationDelete, "role_entitlement:ops"},
		{storagefault.MutationUpdate, "user:worker"},
		{storagefault.MutationDelete, "role:ops"},
	} {
		t.Run(string(test.op)+string(test.id), func(t *testing.T) {
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
			roles := seedRoleRename(t, fault)
			before, err := system.AllNodes()
			require.NoError(t, err)
			fault.Operation, fault.ID = test.op, test.id
			require.ErrorIs(t, roles.DeleteRole(context.Background(), "ops"), errRoleMutation)
			after, err := system.AllNodes()
			require.NoError(t, err)
			require.ElementsMatch(t, before, after)
			require.True(t, roles.Exists("ops"))
		})
	}
}

func TestDeleteRolePublishesCachesAfterCommit(t *testing.T) {
	for _, stage := range []string{"success", "user update", "role delete", "cache load"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
			roles := seedRoleRename(t, fault)
			stores := RoleMutationStores{Allowlist: NewAllowlistStore(fault), Privileges: NewPrivilegesStore(fault), Entitlements: NewRoleEntitlementsStore(fault), Users: &Authenticator{storage: fault, users: map[string]*User{}}}
			require.NoError(t, stores.Allowlist.Load(ctx))
			require.NoError(t, stores.Privileges.Load(ctx))
			require.NoError(t, stores.Entitlements.Load(ctx))
			require.NoError(t, stores.Users.loadUsers())
			before, err := system.AllNodes()
			require.NoError(t, err)
			switch stage {
			case "user update":
				fault.Operation, fault.ID = storagefault.MutationUpdate, "user:worker"
			case "role delete":
				fault.Operation, fault.ID = storagefault.MutationDelete, "role:ops"
			case "cache load":
				fault.Operation, fault.Nth = storagefault.MutationList, 4
			}
			err = roles.DeleteRoleWithStores(ctx, "ops", stores)
			if stage == "success" {
				require.NoError(t, err)
				require.False(t, roles.Exists("ops"))
				require.Empty(t, stores.Allowlist.Allowlist())
				require.Empty(t, stores.Privileges.Matrix())
				require.Empty(t, ReadRoleCatalog(nil, stores.Entitlements).EntitlementOverrides)
				require.Equal(t, []Role{RoleViewer}, stores.Users.users["worker"].Roles)
			} else {
				require.ErrorIs(t, err, errRoleMutation)
				after, err := system.AllNodes()
				require.NoError(t, err)
				require.ElementsMatch(t, before, after)
				require.True(t, roles.Exists("ops"))
				require.Equal(t, map[string][]string{"ops": {"nornic"}}, stores.Allowlist.Allowlist())
				require.Equal(t, ResolvedAccess{Read: true, Write: true}, grantedAccess(stores.Privileges, []string{"ops"}, "nornic"))
				require.Equal(t, []string{"read", "write"}, stores.Entitlements.Get("ops"))
				require.Equal(t, []Role{"ops", RoleViewer}, stores.Users.users["worker"].Roles)
			}
		})
	}
}

func TestRoleChangesPreserveOtherRoles(t *testing.T) {
	for _, remove := range []bool{true, false} {
		t.Run(map[bool]string{true: "delete", false: "rename"}[remove], func(t *testing.T) {
			ctx := context.Background()
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			roles := seedRoleRename(t, system)
			require.NoError(t, roles.CreateRole(ctx, "other"))
			require.NoError(t, NewProcedurePrivilegeStore(system).Grant(ctx, "other", "db.other", true, true))
			require.NoError(t, NewPrivilegesStore(system).SavePrivilege(ctx, "other", "nornic", true, false))
			require.NoError(t, NewAllowlistStore(system).SaveRoleDatabases(ctx, "other", []string{"nornic"}))
			require.NoError(t, NewRoleEntitlementsStore(system).Set(ctx, "other", []string{"read"}))
			require.NoError(t, (&Authenticator{storage: system}).saveUser(context.Background(), &User{Username: "other", Roles: []Role{"other"}}, nil))
			var before []*storage.Node
			for _, id := range []storage.NodeID{"role:other", "proc_priv:boosted:other:db.other", "db_priv:other:nornic", "role_db_access:other", "role_entitlement:other", "user:other"} {
				node, err := system.GetNode(id)
				require.NoError(t, err)
				before = append(before, node)
			}
			if remove {
				require.NoError(t, roles.DeleteRole(ctx, "ops"))
			} else {
				require.NoError(t, roles.RenameRole(ctx, "ops", "new"))
			}
			require.True(t, roles.Exists("other"))
			for _, original := range before {
				node, err := system.GetNode(original.ID)
				require.NoError(t, err)
				require.Equal(t, original, node)
			}
		})
	}
}
