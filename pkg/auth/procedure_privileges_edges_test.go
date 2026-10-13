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
	failScan, failCreate, failDelete bool
}

var errPrivilegeStorage = errors.New("storage unavailable")

func (e *failingPrivilegeEngine) AllNodes() ([]*storage.Node, error) {
	if e.failScan {
		return nil, errPrivilegeStorage
	}
	return e.Engine.AllNodes()
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
// errors on grant, revoke and load (a failed reload keeps the grants it had),
// list order among one role's grants, and globs ending in stars.
func TestProcedurePrivilegeStoreEdges(t *testing.T) {
	ctx := context.Background()
	var nilStore *ProcedurePrivilegeStore
	require.Nil(t, nilStore.List())
	require.NoError(t, nilStore.Load(ctx))
	require.NoError(t, NewProcedurePrivilegeStore(nil).Load(ctx))

	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	_, err := system.CreateNode(&storage.Node{ID: "user:x", Labels: []string{"_User", "_System"}, Properties: map[string]any{"name": "x"}})
	require.NoError(t, err)
	engine := &failingPrivilegeEngine{Engine: system}
	store := NewProcedurePrivilegeStore(engine)

	// One role's grants order by procedure, then a plain grant before a
	// boosted one; other system nodes are not grants.
	require.NoError(t, store.Grant(ctx, "ops", "db.b", true))
	require.NoError(t, store.Grant(ctx, "ops", "db.a", true))
	require.NoError(t, store.Grant(ctx, "ops", "db.a", false))
	require.Equal(t, []ProcedurePrivilege{
		{Role: "ops", Procedure: "db.a", Boosted: false},
		{Role: "ops", Procedure: "db.a", Boosted: true},
		{Role: "ops", Procedure: "db.b", Boosted: true},
	}, store.List())

	engine.failCreate = true
	require.ErrorIs(t, store.Grant(ctx, "ops", "db.c", true), errPrivilegeStorage)
	engine.failCreate = false

	engine.failDelete = true
	revoked, err := store.Revoke(ctx, "ops", "db.b", true)
	require.ErrorIs(t, err, errPrivilegeStorage)
	require.False(t, revoked)
	engine.failDelete = false

	// A failed reload keeps the grants the store had.
	engine.failScan = true
	require.ErrorIs(t, NewProcedurePrivilegeStore(engine).Load(ctx), errPrivilegeStorage)
	other := NewProcedurePrivilegeStore(system)
	require.NoError(t, other.Grant(ctx, "ops", "db.d", true)) // another store's change: reload due
	require.Len(t, store.List(), 3)
	engine.failScan = false
	require.Len(t, store.List(), 4)

	require.True(t, ProcedureGlobMatches("db.cdc.**", "db.cdc.query"))
	require.True(t, ProcedureGlobMatches("db.cdc.query*", "db.cdc.query"))
	require.False(t, ProcedureGlobMatches("db.cdc.query*x", "db.cdc.query"))
}
