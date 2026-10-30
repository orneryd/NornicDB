package auth

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// newSystemEngine is a system database as the server gives the auth stores
// one: a namespaced engine, in which a role and its records share one
// namespace, so one transaction can change them together.
func newSystemEngine(t testing.TB) storage.Engine {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return storage.NewNamespacedEngine(base, "system")
}

func TestRoleAndAllowlistRenameFailureChangesNothing(t *testing.T) {
	ctx := context.Background()
	base := newSystemEngine(t)
	fault := storagefault.NewMutationFailureEngine(base, "", errRoleMutation, 0, WithStoreTransaction)
	roles := NewRoleStore(fault)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	allowlist := NewAllowlistStore(fault)
	require.NoError(t, allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
	fault.Operation, fault.ID = storagefault.MutationDelete, "role:ops"
	require.ErrorIs(t, roles.RenameRole(ctx, "ops", "new"), errRoleMutation)
	require.True(t, roles.Exists("ops"))
	require.False(t, roles.Exists("new"))
	_, err := base.GetNode("role:new")
	require.ErrorIs(t, err, storage.ErrNotFound)
	fault.ID = "role_db_access:ops"
	require.ErrorIs(t, roles.RenameRoleWithStores(ctx, "ops", "new", RoleMutationStores{Allowlist: allowlist}), errRoleMutation)
	require.Equal(t, map[string][]string{"ops": {"nornic"}}, allowlist.Allowlist())
	_, err = base.GetNode("role_db_access:new")
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestPrivilegeMatrixFailureChangesNothing(t *testing.T) {
	for _, operation := range []storagefault.MutationOperation{storagefault.MutationScan, storagefault.MutationDelete, storagefault.MutationCreate} {
		t.Run(string(operation), func(t *testing.T) {
			ctx := context.Background()
			system := newSystemEngine(t)
			fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
			require.NoError(t, NewRoleStore(system).CreateRole(ctx, "ops"))
			require.NoError(t, NewRoleStore(system).CreateRole(ctx, "new"))
			privileges := NewPrivilegesStore(fault)
			require.NoError(t, privileges.SavePrivilege(ctx, "ops", "a", true, false))
			require.NoError(t, privileges.SavePrivilege(ctx, "ops", "b", false, true))
			before := privileges.Matrix()
			fault.Operation, fault.Nth = operation, 2
			if operation == storagefault.MutationScan {
				fault.Nth = 1
			}
			entries := []struct {
				Role     string `json:"role"`
				Database string `json:"database"`
				Read     bool   `json:"read"`
				Write    bool   `json:"write"`
			}{{"new", "c", true, true}, {"new", "d", true, true}}
			require.ErrorIs(t, privileges.PutMatrix(ctx, entries), errRoleMutation)
			require.ElementsMatch(t, before, privileges.Matrix())
			reloaded := NewPrivilegesStore(system)
			require.NoError(t, reloaded.Load(ctx))
			require.ElementsMatch(t, before, reloaded.Matrix())
		})
	}
}

func TestAllowlistBatchFailureChangesNothing(t *testing.T) {
	system := newSystemEngine(t)
	for _, role := range []string{"a", "b"} {
		require.NoError(t, NewRoleStore(system).CreateRole(context.Background(), role))
	}
	fault := storagefault.NewMutationFailureEngine(system, storagefault.MutationCreate, errRoleMutation, 2, WithStoreTransaction)
	allowlist := NewAllowlistStore(fault)
	require.ErrorIs(t, allowlist.SaveMappings(context.Background(), map[string][]string{"a": {"nornic"}, "b": {"system"}}), errRoleMutation)
	require.Empty(t, allowlist.Allowlist())
	nodes, err := system.GetNodesByLabel(roleDbAccessLabel)
	require.NoError(t, err)
	require.Empty(t, nodes)
}

func TestUserMutationFailureChangesNothing(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	fault := storagefault.NewMutationFailureEngine(base, "", errRoleMutation, 0, WithStoreTransaction)
	users := newMutationTestAuthenticator(t, fault)
	fault.Operation, fault.ID = storagefault.MutationCreate, "user:new"
	_, err := users.CreateUser("new", "password123", []Role{RoleViewer})
	require.ErrorIs(t, err, errRoleMutation)
	require.NotContains(t, users.users, "new")
	_, err = base.GetNode("user:new")
	require.ErrorIs(t, err, storage.ErrNotFound)
	fault.Operation = ""
	_, err = users.CreateUser("worker", "password123", []Role{RoleViewer})
	require.NoError(t, err)
	for _, test := range []struct {
		op     storagefault.MutationOperation
		mutate func() error
	}{
		{storagefault.MutationUpdate, func() error { return users.UpdateRoles("worker", []Role{RoleAdmin}) }},
		{storagefault.MutationUpdate, func() error { return users.ChangeUserState("worker", UserDisable) }},
		{storagefault.MutationUpdate, func() error { return users.ChangeUserState("worker", UserEnable) }},
		{storagefault.MutationUpdate, func() error { return users.ChangeUserState("worker", UserUnlock) }},
		{storagefault.MutationUpdate, func() error { return users.UpdateUser("worker", "new@example.com", map[string]string{"key": "new"}) }},
		{storagefault.MutationUpdate, func() error { return users.ChangePassword("worker", "password123", "newpassword123") }},
		{storagefault.MutationDelete, func() error { return users.DeleteUser("worker") }},
	} {
		before := cloneStoredUser(users.users["worker"])
		beforeNode, err := base.GetNode("user:worker")
		require.NoError(t, err)
		fault.ID, fault.Operation = "user:worker", test.op
		require.ErrorIs(t, test.mutate(), errRoleMutation)
		require.Equal(t, before, users.users["worker"])
		node, err := base.GetNode("user:worker")
		require.NoError(t, err)
		require.Equal(t, beforeNode.Properties, node.Properties)
	}
}

func TestCoordinatedRoleRenameFailureChangesNothing(t *testing.T) {
	ctx := context.Background()
	base := newSystemEngine(t)
	fault := storagefault.NewMutationFailureEngine(base, "", errRoleMutation, 0, WithStoreTransaction)
	roles := NewRoleStore(fault)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	allowlist := NewAllowlistStore(fault)
	require.NoError(t, allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
	users := &Authenticator{storage: fault, users: map[string]*User{}}
	for _, name := range []string{"alice", "bob"} {
		user := &User{Username: name, Roles: []Role{"ops"}}
		require.NoError(t, users.saveUser(user, nil))
		users.users[name] = user
	}
	fault.Operation, fault.Nth = storagefault.MutationUpdate, 2
	require.ErrorIs(t, roles.RenameRoleWithUsers(ctx, "ops", "new", allowlist, users), errRoleMutation)
	require.True(t, roles.Exists("ops"))
	require.False(t, roles.Exists("new"))
	require.Equal(t, map[string][]string{"ops": {"nornic"}}, allowlist.Allowlist())
	for _, user := range users.users {
		require.Equal(t, []Role{"ops"}, user.Roles)
		node, err := base.GetNode(storage.NodeID("user:" + user.Username))
		require.NoError(t, err)
		require.Equal(t, `["ops"]`, node.Properties["roles"])
	}
	fault.Operation = ""
	require.NoError(t, roles.RenameRoleWithUsers(ctx, "ops", "new", allowlist, users))
	require.True(t, roles.Exists("new"))
	for _, user := range users.users {
		require.Equal(t, []Role{"new"}, user.Roles)
	}
}

func TestWithStoreTransactionNamespaceAndFailures(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants := NewProcedurePrivilegeStore(system)
	require.NoError(t, grants.Grant(ctx, "ops", "db.a", true, true))
	require.NoError(t, roles.DeleteRole(ctx, "ops"))
	listed, err := grants.List(ctx)
	require.NoError(t, err)
	require.Empty(t, listed)
	_, err = base.GetNode("system:role:ops")
	require.ErrorIs(t, err, storage.ErrNotFound)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, WithStoreTransaction(canceled, base, func(view storage.Engine) error {
		_, err := view.CreateNode(&storage.Node{ID: "role:cancel", Properties: map[string]any{}})
		return err
	}), context.Canceled)
	_, err = base.GetNode("role:cancel")
	require.ErrorIs(t, err, storage.ErrNotFound)
	require.ErrorIs(t, WithStoreTransaction(ctx, nil, func(storage.Engine) error { return nil }), storage.ErrNotImplemented)
	closed := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	require.NoError(t, closed.Close())
	require.Error(t, WithStoreTransaction(ctx, closed, func(storage.Engine) error { return nil }))
}

type transactionStartEngine struct {
	*storage.MemoryEngine
	failFlush       bool
	pinnedNamespace string
}

func (e *transactionStartEngine) FlushWriteBehind() error {
	if e.failFlush {
		return errRoleMutation
	}
	return e.MemoryEngine.FlushWriteBehind()
}

func (e *transactionStartEngine) BeginTransaction() (*storage.BadgerTransaction, error) {
	tx, err := e.MemoryEngine.BeginTransaction()
	if err == nil && e.pinnedNamespace != "" {
		err = tx.SetNamespace(e.pinnedNamespace)
	}
	return tx, err
}

func TestStoreTransactionReadsItsOwnChangesAndStartFailures(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	require.NoError(t, WithStoreTransaction(ctx, base, func(view storage.Engine) error {
		_, err := view.CreateNode(&storage.Node{ID: "role:ops", Labels: []string{roleLabel}, Properties: map[string]any{"name": "ops"}})
		require.NoError(t, err)
		require.Len(t, view.GetAllNodes(), 1)
		projected := 0
		err = view.(*storeTransaction).StreamNodesWithOptions(ctx, storage.StreamNodesOptions{}, func(*storage.Node) error { projected++; return nil })
		require.NoError(t, err)
		require.Equal(t, 1, projected)
		return WithStoreTransaction(ctx, view, func(joined storage.Engine) error {
			require.Same(t, view, joined)
			return nil
		})
	}))
	beginner := &transactionStartEngine{MemoryEngine: base, failFlush: true}
	require.ErrorIs(t, WithStoreTransaction(ctx, beginner, func(storage.Engine) error { return nil }), errRoleMutation)
	beginner.failFlush = false
	beginner.pinnedNamespace = "other"
	require.ErrorIs(t, WithStoreTransaction(ctx, storage.NewNamespacedEngine(beginner, "system"), func(storage.Engine) error { return nil }), storage.ErrCrossNamespaceTransaction)
}

func TestCoordinatedRoleRenameRejectsDistinctStores(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	system := storage.NewNamespacedEngine(base, "system")
	other := storage.NewNamespacedEngine(base, "other")
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	allowlist := NewAllowlistStore(other)
	require.ErrorIs(t, roles.RenameRoleWithUsers(ctx, "ops", "new", allowlist, nil), storage.ErrCrossNamespaceTransaction)
	users := &Authenticator{storage: other, users: map[string]*User{"worker": {Username: "worker", Roles: []Role{"ops"}}}}
	require.ErrorIs(t, roles.RenameRoleWithUsers(ctx, "ops", "new", nil, users), storage.ErrCrossNamespaceTransaction)
	require.True(t, roles.Exists("ops"))
	_, err := system.GetNode("role:new")
	require.ErrorIs(t, err, storage.ErrNotFound)
	separate := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = separate.Close() })
	require.False(t, sameSystemStore(system, separate))
}

func TestCoordinatedRoleRenameStagedFailuresChangeNothing(t *testing.T) {
	for _, test := range []struct {
		op  storagefault.MutationOperation
		id  storage.NodeID
		nth int
	}{
		{storagefault.MutationList, "", 1}, {storagefault.MutationList, "", 2},
		{storagefault.MutationCreate, "role:new", 1}, {storagefault.MutationDelete, "role:ops", 1},
		{storagefault.MutationCreate, "role_db_access:new", 1}, {storagefault.MutationDelete, "role_db_access:ops", 1},
		{storagefault.MutationGet, "user:worker", 1}, {storagefault.MutationUpdate, "user:worker", 1},
	} {
		t.Run(string(test.op)+string(test.id), func(t *testing.T) {
			ctx := context.Background()
			base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = base.Close() })
			system := storage.NewNamespacedEngine(base, "system")
			fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
			roles := NewRoleStore(fault)
			require.NoError(t, roles.CreateRole(ctx, "ops"))
			allowlist := NewAllowlistStore(fault)
			require.NoError(t, allowlist.SaveRoleDatabases(ctx, "ops", []string{"nornic"}))
			users := &Authenticator{storage: fault, users: map[string]*User{"worker": {Username: "worker", Roles: []Role{"ops"}}}}
			require.NoError(t, users.saveUser(users.users["worker"], nil))
			before, err := system.AllNodes()
			require.NoError(t, err)
			fault.Operation, fault.ID, fault.Nth = test.op, test.id, test.nth
			require.ErrorIs(t, roles.RenameRoleWithUsers(ctx, "ops", "new", allowlist, users), errRoleMutation)
			after, err := system.AllNodes()
			require.NoError(t, err)
			require.ElementsMatch(t, before, after)
			require.True(t, roles.Exists("ops"))
			require.Equal(t, []Role{"ops"}, users.users["worker"].Roles)
			require.Equal(t, map[string][]string{"ops": {"nornic"}}, allowlist.Allowlist())
		})
	}
}

func TestEntitlementDeletionFailurePreservesState(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	fault := storagefault.NewMutationFailureEngine(system, "", errRoleMutation, 0, WithStoreTransaction)
	require.NoError(t, NewRoleStore(system).CreateRole(ctx, "ops"))
	store := NewRoleEntitlementsStore(fault)
	require.NoError(t, store.Set(ctx, "ops", []string{"read"}))
	fault.Operation = storagefault.MutationDelete
	require.ErrorIs(t, store.Set(ctx, "ops", nil), errRoleMutation)
	require.Equal(t, []string{"read"}, store.Get("ops"))
	_, err := system.GetNode(storage.NodeID(roleEntitlementPrefix + "ops"))
	require.NoError(t, err)
}

func TestRoleGrantValidationUsesTheTransactionRecord(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { _ = base.Close() })
	fault := storagefault.NewMutationFailureEngine(storage.NewNamespacedEngine(base, "system"), "", errRoleMutation, 0, WithStoreTransaction)
	roles := NewRoleStore(fault)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	for _, name := range []string{"viewer", "ops", "missing"} {
		require.NoError(t, WithStoreTransaction(ctx, fault, func(view storage.Engine) error {
			found, err := NewRoleStore(view).LockRoleForGrant(ctx, name)
			require.NoError(t, err)
			require.Equal(t, name != "missing", found)
			return nil
		}))
	}
	for _, operation := range []storagefault.MutationOperation{storagefault.MutationGet, storagefault.MutationUpdate} {
		fault.Operation = operation
		require.ErrorIs(t, WithStoreTransaction(ctx, fault, func(view storage.Engine) error {
			_, err := NewRoleStore(view).LockRoleForGrant(ctx, "ops")
			return err
		}), errRoleMutation)
	}
	fault.Operation = ""
	_, err := fault.CreateNode(&storage.Node{ID: "role:other", Labels: []string{"_User"}, Properties: map[string]any{}})
	require.NoError(t, err)
	found, err := roles.LockRoleForGrant(ctx, "other")
	require.NoError(t, err)
	require.False(t, found)
}
