package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// failingPrivilegeEngine fails the reads and writes a test switches on.
type failingPrivilegeEngine struct {
	storage.Engine
	failScan, failGet, failCreate, failDelete bool
}

var errPrivilegeStorage = errors.New("storage unavailable")

func (e *failingPrivilegeEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.failScan {
		return nil, errPrivilegeStorage
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *failingPrivilegeEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.failGet {
		return nil, errPrivilegeStorage
	}
	return e.Engine.GetNode(id)
}

func (e *failingPrivilegeEngine) CreateNode(node *storage.Node) (storage.NodeID, error) {
	if e.failCreate {
		return "", errPrivilegeStorage
	}
	return e.Engine.CreateNode(node)
}

func (e *failingPrivilegeEngine) DeleteNode(id storage.NodeID) error {
	if e.failDelete {
		return errPrivilegeStorage
	}
	return e.Engine.DeleteNode(id)
}

// The store's edges: a nil store or storage, other system nodes, storage
// errors on grant, revoke and list, list order among one role's grants, and
// globs ending in stars.
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

	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	_, err = system.CreateNode(&storage.Node{ID: "user:x", Labels: []string{"_User", "_System"}, Properties: map[string]any{"name": "x"}})
	require.NoError(t, err)
	engine := &failingPrivilegeEngine{Engine: system}
	store := NewProcedurePrivilegeStore(engine)

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

	engine.failCreate = true
	require.ErrorIs(t, store.Grant(ctx, "ops", "db.c", true, false), errPrivilegeStorage)
	engine.failCreate = false

	engine.failDelete = true
	revoked, err := store.Revoke(ctx, "ops", "db.b", true, false)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, revoked)
	engine.failDelete = false

	// A read that fails is an error, never "no such grant" or "already
	// granted".
	engine.failGet = true
	require.ErrorIs(t, store.Grant(ctx, "ops", "db.c", true, false), errPrivilegeStorage)
	revoked, err = store.Revoke(ctx, "ops", "db.b", true, false)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, revoked)
	engine.failGet = false
	allowed, err = store.AllowsBoosted([]string{"ops"}, "db.b")
	require.NoError(t, err)
	require.True(t, allowed)

	// A list that fails is an error.
	engine.failScan = true
	_, err = store.List(ctx)
	require.ErrorIs(t, err, errPrivilegeStorage)
	engine.failScan = false

	require.True(t, ProcedureGlobMatches("db.cdc.**", "db.cdc.query"))
	require.True(t, ProcedureGlobMatches("db.cdc.query*", "db.cdc.query"))
	require.False(t, ProcedureGlobMatches("db.cdc.query*x", "db.cdc.query"))
}

// When the grants can't be read, AllowsBoosted denies and returns the
// error: a grant revoked by another store is never answered from a copy
// read before.
func TestProcedurePrivilegeStoreDeniesWhenUnreadable(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	writer := NewProcedurePrivilegeStore(system)
	require.NoError(t, writer.Grant(ctx, "ops", "db.cdc.*", true, false))
	engine := &failingPrivilegeEngine{Engine: system}
	reader := NewProcedurePrivilegeStore(engine)
	allowed, err := reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed)

	revoked, err := writer.Revoke(ctx, "ops", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.True(t, revoked)
	engine.failScan = true
	allowed, err = reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, allowed)

	// Unreadable before the revoke too: still denied.
	engine.failScan = false
	require.NoError(t, writer.Grant(ctx, "ops", "db.cdc.*", true, false))
	engine.failScan = true
	allowed, err = reader.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, allowed)
}

// A role whose grants can't be read or removed isn't deleted: deleting it
// would leave its grants for a role created later under the same name.
func TestDeleteRoleKeepsRoleWhenGrantsCannotBeRemoved(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	engine := &failingPrivilegeEngine{Engine: system}
	roles := NewRoleStore(engine)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	require.NoError(t, NewProcedurePrivilegeStore(engine).Grant(ctx, "ops", "db.cdc.*", true, false))

	engine.failScan = true
	require.ErrorIs(t, roles.DeleteRole(ctx, "ops"), errPrivilegeStorage)
	engine.failScan = false
	engine.failDelete = true
	require.ErrorIs(t, roles.DeleteRole(ctx, "ops"), errPrivilegeStorage)
	engine.failDelete = false
	require.True(t, roles.Exists("ops"))
}

func (e *failingPrivilegeEngine) WithStoreTransaction(ctx context.Context, mutate func(storage.Engine) error) error {
	return WithStoreTransaction(ctx, e.Engine, func(view storage.Engine) error {
		staged := *e
		staged.Engine = view
		return mutate(&staged)
	})
}
