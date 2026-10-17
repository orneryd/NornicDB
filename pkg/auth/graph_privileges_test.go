package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nodeReadFailingEngine fails every node read by ID, so a store can't tell
// whether a record exists.
type nodeReadFailingEngine struct{ storage.Engine }

func (e *nodeReadFailingEngine) GetNode(storage.NodeID) (*storage.Node, error) {
	return nil, errors.New("read failed")
}

// Graph privileges are stored normalized and once, listed in role, graph,
// grant-before-deny, segment order, and selected per role and database
// (named, or through *); a node that isn't this store's record is ignored.
func TestGraphPrivilegeStore(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)
	store := NewGraphPrivilegeStore(system)
	for _, privilege := range []GraphPrivilege{
		{Role: " Agent ", Graph: " LIB ", Segment: " NODES Document "},
		{Role: "agent", Graph: "lib", Segment: "NODES Document"}, // the same, again
		{Role: "agent", Graph: "lib", Denied: true, Segment: "NODES Secret"},
		{Role: "agent", Graph: "*", Segment: "NODES Definition"},
		{Role: "agent", Graph: "other", Segment: "NODES Document"},
		{Role: "editor", Graph: "lib", Segment: "NODES *"},
	} {
		require.NoError(t, store.Put(ctx, privilege))
	}
	require.Error(t, store.Put(ctx, GraphPrivilege{Role: "agent", Graph: "lib"}), "a privilege needs a segment")
	_, err := system.CreateNode(&storage.Node{ID: "forged", Labels: []string{graphPrivilegeLabel},
		Properties: map[string]any{"role": "agent", "graph": "lib", "segment": "NODES *"}})
	require.NoError(t, err)

	all, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []GraphPrivilege{
		{Role: "agent", Graph: "*", Segment: "NODES Definition"},
		{Role: "agent", Graph: "lib", Segment: "NODES Document"},
		{Role: "agent", Graph: "lib", Denied: true, Segment: "NODES Secret"},
		{Role: "agent", Graph: "other", Segment: "NODES Document"},
		{Role: "editor", Graph: "lib", Segment: "NODES *"},
	}, all)

	held, err := store.ForRoles(ctx, []string{"AGENT"}, "Lib")
	require.NoError(t, err)
	require.Equal(t, []GraphPrivilege{
		{Role: "agent", Graph: "*", Segment: "NODES Definition"},
		{Role: "agent", Graph: "lib", Segment: "NODES Document"},
		{Role: "agent", Graph: "lib", Denied: true, Segment: "NODES Secret"},
	}, held)

	removed, err := store.Remove(ctx, GraphPrivilege{Role: "Agent", Graph: "LIB", Denied: true, Segment: "NODES Secret"})
	require.NoError(t, err)
	require.True(t, removed)
	removed, err = store.Remove(ctx, GraphPrivilege{Role: "agent", Graph: "lib", Denied: true, Segment: "NODES Secret"})
	require.NoError(t, err)
	require.False(t, removed, "removing what isn't recorded")

	var none *GraphPrivilegeStore
	listed, err := none.List(ctx)
	require.NoError(t, err)
	require.Empty(t, listed)
}

// A read that fails is an error, never "no privileges" or "absent".
func TestGraphPrivilegeStoreReadFailures(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)
	privilege := GraphPrivilege{Role: "agent", Graph: "lib", Segment: "NODES *"}

	unreadable := NewGraphPrivilegeStore(&nodeReadFailingEngine{system})
	require.Error(t, unreadable.Put(ctx, privilege))
	_, err := unreadable.Remove(ctx, privilege)
	require.Error(t, err)

	unlisted := NewGraphPrivilegeStore(&labelReadFailingEngine{system})
	_, err = unlisted.List(ctx)
	require.Error(t, err)
	_, err = unlisted.ForRoles(ctx, []string{"agent"}, "lib")
	require.Error(t, err)
}

// A graph has at most one access scope: Put creates or replaces it, Get and
// List read it back (List by graph, ignoring nodes that aren't this store's
// records), Delete removes it.
func TestAccessScopeStore(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)
	store := NewAccessScopeStore(system)

	_, found, err := store.Get(ctx, "lib")
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, store.Put(ctx, AccessScope{Graph: "LIB", Name: "collections", Definition: "MEMBERSHIP v1"}))
	require.NoError(t, store.Put(ctx, AccessScope{Graph: "lib", Name: "collections", Definition: "MEMBERSHIP v2"}))
	require.NoError(t, store.Put(ctx, AccessScope{Graph: "archive", Name: "folders", Definition: "MEMBERSHIP f"}))
	_, err = system.CreateNode(&storage.Node{ID: "forged", Labels: []string{accessScopeLabel},
		Properties: map[string]any{"graph": "lib", "name": "x", "definition": "y"}})
	require.NoError(t, err)

	scope, found, err := store.Get(ctx, " Lib ")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, AccessScope{Graph: "lib", Name: "collections", Definition: "MEMBERSHIP v2"}, scope)

	scopes, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []AccessScope{
		{Graph: "archive", Name: "folders", Definition: "MEMBERSHIP f"},
		{Graph: "lib", Name: "collections", Definition: "MEMBERSHIP v2"},
	}, scopes)

	deleted, err := store.Delete(ctx, "lib")
	require.NoError(t, err)
	require.True(t, deleted)
	deleted, err = store.Delete(ctx, "lib")
	require.NoError(t, err)
	require.False(t, deleted)

	var none *AccessScopeStore
	_, found, err = none.Get(ctx, "lib")
	require.NoError(t, err)
	require.False(t, found)
}

func TestAccessScopeStoreReadFailures(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)

	unreadable := NewAccessScopeStore(&nodeReadFailingEngine{system})
	_, _, err := unreadable.Get(ctx, "lib")
	require.Error(t, err)
	require.Error(t, unreadable.Put(ctx, AccessScope{Graph: "lib", Name: "n", Definition: "d"}))
	_, err = unreadable.Delete(ctx, "lib")
	require.Error(t, err)

	_, err = NewAccessScopeStore(&labelReadFailingEngine{system}).List(ctx)
	require.Error(t, err)
}

// Renaming a role moves its graph privileges; deleting it removes them (the
// one role-owned record enumerator). Another role's privileges stay.
func TestGraphPrivilegesFollowRoleRenameAndDelete(t *testing.T) {
	ctx := context.Background()
	system := newImpersonationSystem(t)
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	require.NoError(t, roles.CreateRole(ctx, "other"))
	store := NewGraphPrivilegeStore(system)
	require.NoError(t, store.Put(ctx, GraphPrivilege{Role: "ops", Graph: "lib", Segment: "NODES Document"}))
	require.NoError(t, store.Put(ctx, GraphPrivilege{Role: "ops", Graph: "lib", Denied: true, Segment: "NODES Secret"}))
	require.NoError(t, store.Put(ctx, GraphPrivilege{Role: "other", Graph: "lib", Segment: "NODES *"}))

	require.NoError(t, roles.RenameRole(ctx, "ops", "renamed"))
	list, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []GraphPrivilege{
		{Role: "other", Graph: "lib", Segment: "NODES *"},
		{Role: "renamed", Graph: "lib", Segment: "NODES Document"},
		{Role: "renamed", Graph: "lib", Denied: true, Segment: "NODES Secret"},
	}, list)

	require.NoError(t, roles.DeleteRole(ctx, "renamed"))
	list, err = store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []GraphPrivilege{{Role: "other", Graph: "lib", Segment: "NODES *"}}, list)
}

// The unrestricted-view effect is a valid contract class alone and combined.
func TestOperationUnrestrictedViewIsAValidClass(t *testing.T) {
	require.True(t, OperationUnrestrictedView.Valid())
	require.True(t, (OperationRead | OperationWrite | OperationUnrestrictedView).Valid())
	require.False(t, (OperationNone | OperationUnrestrictedView).Valid())
	require.True(t, (OperationRead | OperationUnrestrictedView).Has(OperationUnrestrictedView))
}
