package auth

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"

	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func seedRoleRename(t *testing.T, engine storage.Engine) *RoleStore {
	t.Helper()
	ctx := context.Background()
	roles := NewRoleStore(engine)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants := NewProcedurePrivilegeStore(engine)
	require.NoError(t, grants.Grant(ctx, "ops", "db.cdc.*", true, true))
	require.NoError(t, grants.Grant(ctx, "ops", "db.a", false, false))
	require.NoError(t, NewPrivilegesStore(engine).SavePrivilege(ctx, "ops", "nornic", true, true))
	require.NoError(t, NewAllowlistStore(engine).SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
	require.NoError(t, NewRoleEntitlementsStore(engine).Set(ctx, "ops", []string{"read", "write"}))
	users := &Authenticator{storage: engine}
	require.NoError(t, users.saveUser(&User{Username: "worker", Roles: []Role{"ops", RoleViewer}}, nil))
	return roles
}

func TestRenamedRoleKeepsItsGrants(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	roles := seedRoleRename(t, system)
	require.NoError(t, roles.RenameRole(ctx, "ops", "renamed"))
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants := NewProcedurePrivilegeStore(system)
	allowed, err := grants.AllowsBoosted([]string{"renamed"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed, "a renamed role keeps its grants")
	allowed, err = grants.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.False(t, allowed, "a recreated role starts without the old grants")
	grant, found, err := grants.Lookup(ctx, "renamed", "db.cdc.*", true)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, grant.Immutable)
	privileges := NewPrivilegesStore(system)
	require.NoError(t, privileges.Load(ctx))
	require.Equal(t, ResolvedAccess{Read: true, Write: true}, grantedAccess(privileges, []string{"renamed"}, "nornic"))
	require.Equal(t, ResolvedAccess{}, grantedAccess(privileges, []string{"ops"}, "nornic"))
	allowlist := NewAllowlistStore(system)
	require.NoError(t, allowlist.Load(ctx))
	require.Equal(t, map[string][]string{"renamed": {"nornic"}}, allowlist.Allowlist())
	entitlements := NewRoleEntitlementsStore(system)
	require.NoError(t, entitlements.Load(ctx))
	require.Equal(t, []string{"read", "write"}, entitlements.Get("renamed"))
	require.Empty(t, entitlements.Get("ops"))
	user, err := system.GetNode("user:worker")
	require.NoError(t, err)
	require.Equal(t, `["renamed","viewer"]`, user.Properties["roles"])
}

func TestRoleRenameRecordFailureChangesNothing(t *testing.T) {
	for _, test := range []struct {
		op storagefault.MutationOperation
		id storage.NodeID
	}{
		{storagefault.MutationCreate, "proc_priv:boosted:renamed:db.cdc.*"},
		{storagefault.MutationDelete, "proc_priv:execute:ops:db.a"},
		{storagefault.MutationCreate, "db_priv:renamed:nornic"},
		{storagefault.MutationDelete, "role_db_access:ops"},
		{storagefault.MutationCreate, "role_entitlement:renamed"},
		{storagefault.MutationUpdate, "user:worker"},
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
			require.ErrorIs(t, roles.RenameRole(context.Background(), "ops", "renamed"), errRoleMutation)
			after, err := system.AllNodes()
			require.NoError(t, err)
			require.ElementsMatch(t, before, after)
			require.True(t, roles.Exists("ops"))
			require.False(t, roles.Exists("renamed"))
		})
	}
}

func TestRoleRenamePublishesCachesAfterCommit(t *testing.T) {
	for _, fail := range []bool{true, false} {
		t.Run(map[bool]string{true: "failure", false: "success"}[fail], func(t *testing.T) {
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
			if fail {
				fault.Operation, fault.ID = storagefault.MutationUpdate, "user:worker"
				require.ErrorIs(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", stores), errRoleMutation)
				after, err := system.AllNodes()
				require.NoError(t, err)
				require.ElementsMatch(t, before, after)
			} else {
				require.NoError(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", stores))
			}
			name := "renamed"
			if fail {
				name = "ops"
			}
			require.Equal(t, map[string][]string{name: {"nornic"}}, stores.Allowlist.Allowlist())
			require.Equal(t, ResolvedAccess{Read: true, Write: true}, grantedAccess(stores.Privileges, []string{name}, "nornic"))
			require.Equal(t, []string{"read", "write"}, stores.Entitlements.Get(name))
			require.Equal(t, []Role{Role(name), RoleViewer}, stores.Users.users["worker"].Roles)
		})
	}
}

func TestRoleRenameReadAndCacheFailuresChangeNothing(t *testing.T) {
	for _, test := range []struct {
		op  storagefault.MutationOperation
		id  storage.NodeID
		nth int
	}{
		{storagefault.MutationScan, "", 1}, {storagefault.MutationScan, "", 2}, {storagefault.MutationScan, "", 3}, {storagefault.MutationScan, "", 4}, {storagefault.MutationScan, "", 5}, {storagefault.MutationScan, "", 6},
		{storagefault.MutationList, "", 2}, {storagefault.MutationList, "", 3}, {storagefault.MutationList, "", 4},
		{storagefault.MutationGet, "proc_priv:boosted:renamed:db.cdc.*", 1},
	} {
		t.Run(string(test.op)+string(test.id)+string(rune('0'+test.nth)), func(t *testing.T) {
			ctx := context.Background()
			base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
			roles := seedRoleRename(t, fault)
			stores := RoleMutationStores{Allowlist: NewAllowlistStore(fault), Privileges: NewPrivilegesStore(fault), Entitlements: NewRoleEntitlementsStore(fault)}
			require.NoError(t, stores.Allowlist.Load(ctx))
			require.NoError(t, stores.Privileges.Load(ctx))
			require.NoError(t, stores.Entitlements.Load(ctx))
			before, err := system.AllNodes()
			require.NoError(t, err)
			fault.Operation, fault.ID, fault.Nth = test.op, test.id, test.nth
			require.ErrorIs(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", stores), errRoleMutation)
			after, err := system.AllNodes()
			require.NoError(t, err)
			require.ElementsMatch(t, before, after)
			require.True(t, roles.Exists("ops"))
			require.Equal(t, map[string][]string{"ops": {"nornic"}}, stores.Allowlist.Allowlist())
			require.Equal(t, ResolvedAccess{Read: true, Write: true}, grantedAccess(stores.Privileges, []string{"ops"}, "nornic"))
			require.Equal(t, []string{"read", "write"}, stores.Entitlements.Get("ops"))
		})
	}
}

func TestRoleRenameRejectsDifferentCacheStores(t *testing.T) {
	ctx := context.Background()
	base, other := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed), runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close(); _ = other.Close() })
	roles := seedRoleRename(t, storage.NewNamespacedEngine(base, "system"))
	for _, stores := range []RoleMutationStores{
		{Privileges: NewPrivilegesStore(other)}, {Entitlements: NewRoleEntitlementsStore(other)},
	} {
		require.ErrorIs(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", stores), storage.ErrCrossNamespaceTransaction)
		require.True(t, roles.Exists("ops"))
		require.False(t, roles.Exists("renamed"))
	}
}

func TestAllowlistRenameReadFailureChangesNothing(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	fault := storagefault.NewMutationFailureEngine(storage.NewNamespacedEngine(base, "system"), "", errRoleMutation, 0, WithStoreTransaction)
	roles := NewRoleStore(fault)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	allowlist := NewAllowlistStore(fault)
	require.NoError(t, allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
	before, err := base.AllNodes()
	require.NoError(t, err)
	fault.Operation, fault.ID = storagefault.MutationGet, "role_db_access:renamed"
	require.ErrorIs(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", RoleMutationStores{Allowlist: allowlist}), errRoleMutation)
	after, err := base.AllNodes()
	require.NoError(t, err)
	require.ElementsMatch(t, before, after)
	require.Equal(t, map[string][]string{"ops": {"nornic"}}, allowlist.Allowlist())
}

func TestRoleRenamePreservesUnrelatedRecordsAndRejectsOccupiedKeys(t *testing.T) {
	ctx := context.Background()
	base := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	roles := seedRoleRename(t, system)
	for _, node := range []*storage.Node{
		{ID: "proc_priv:unrelated", Labels: []string{procedurePrivilegeLabel}, Properties: map[string]any{"role": "ops", "procedure": "db.a"}},
		{ID: "db_priv:other:nornic", Labels: []string{dbPrivLabel}, Properties: map[string]any{}},
		{ID: "role_db_access:other", Labels: []string{roleDbAccessLabel}, Properties: map[string]any{}},
		{ID: "role_entitlement:other", Labels: []string{roleEntitlementLabel}, Properties: map[string]any{}},
		{ID: "user:invalid", Labels: []string{"_User"}, Properties: map[string]any{"username": "different", "roles": `["ops"]`}},
		{ID: "user:other", Labels: []string{"_User"}, Properties: map[string]any{"username": "other", "roles": `["viewer"]`}},
	} {
		_, err := system.CreateNode(node)
		require.NoError(t, err)
	}
	// An existing destination record (left by an earlier version, since
	// role-record writes now require the role) is not silently replaced by
	// a rename.
	_, err := system.CreateNode(&storage.Node{ID: "role_entitlement:renamed", Labels: []string{roleEntitlementLabel, roleEntitlementSystems},
		Properties: map[string]any{"role": "renamed", "entitlements": `["read"]`}})
	require.NoError(t, err)
	before, err := system.AllNodes()
	require.NoError(t, err)
	require.ErrorIs(t, roles.RenameRole(ctx, "ops", "renamed"), ErrRoleExists)
	after, err := system.AllNodes()
	require.NoError(t, err)
	require.ElementsMatch(t, before, after)
	require.NoError(t, system.DeleteNode("role_entitlement:renamed"))
	stores := RoleMutationStores{Users: &Authenticator{storage: system, users: map[string]*User{}}}
	require.NoError(t, roles.RenameRoleWithStores(ctx, "ops", "renamed", stores))
	require.Equal(t, []Role{"renamed", RoleViewer}, stores.Users.users["worker"].Roles)
	for _, id := range []storage.NodeID{"proc_priv:unrelated", "db_priv:other:nornic", "role_db_access:other", "role_entitlement:other", "user:invalid", "user:other"} {
		_, err := system.GetNode(id)
		require.NoError(t, err)
	}
}
