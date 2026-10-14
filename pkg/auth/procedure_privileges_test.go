package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A procedure name matches a grant's name or glob as in Neo4j: * any
// characters, dots included, ? one character (not one byte);
// case-insensitively.
func TestProcedureGlobMatches(t *testing.T) {
	for _, c := range []struct {
		glob, name string
		want       bool
	}{
		{"db.cdc.query", "db.cdc.query", true},
		{"db.cdc.*", "db.cdc.query", true},
		{"db.cdc.*", "db.cdc.current", true},
		{"db.cdc.*", "db.txlog.entries", false},
		{"*", "db.txlog.entries", true},
		{"db.*.query", "db.cdc.query", true},
		{"db.cdc.quer?", "db.cdc.query", true},
		{"db.cdc.quer?", "db.cdc.queries", false},
		{"DB.CDC.*", "db.cdc.query", true},
		{"db.cdc", "db.cdc.query", false},
		{"custom.?", "custom.é", true},
		{"custom.??", "custom.é", false},
		{"custom.?x", "custom.日x", true},
	} {
		require.Equal(t, c.want, ProcedureGlobMatches(c.glob, c.name), "%s ~ %s", c.glob, c.name)
	}
}

// Grants are stored in the system database: a boosted grant allows its
// procedures to the role (role names compared as privileges compare them), a
// plain one doesn't; revoking removes it; another store over the same
// database sees each change.
func TestProcedurePrivilegeStore(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	store := NewProcedurePrivilegeStore(system)
	other := NewProcedurePrivilegeStore(system)
	allows := func(roles []string, procedure string) bool {
		allowed, err := other.AllowsBoosted(roles, procedure)
		require.NoError(t, err)
		return allowed
	}
	require.False(t, allows([]string{"cdc_reader"}, "db.cdc.query"))

	require.NoError(t, store.Grant(ctx, "CDC_Reader", "db.cdc.*", true, false))
	require.NoError(t, store.Grant(ctx, "cdc_reader", "db.cdc.*", true, false)) // again: no change
	require.NoError(t, store.Grant(ctx, "viewer", "db.txlog.entries", false, false))
	grants, err := store.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ProcedurePrivilege{
		{Role: "cdc_reader", Procedure: "db.cdc.*", Boosted: true},
		{Role: "viewer", Procedure: "db.txlog.entries", Boosted: false},
	}, grants)
	require.True(t, allows([]string{"role_cdc_reader"}, "db.cdc.query"))
	require.False(t, allows([]string{"cdc_reader"}, "db.txlog.entries"))
	require.False(t, allows([]string{"viewer"}, "db.txlog.entries")) // not boosted
	require.False(t, allows(nil, "db.cdc.query"))

	revoked, err := store.Revoke(ctx, "cdc_reader", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.True(t, revoked)
	revoked, err = store.Revoke(ctx, "cdc_reader", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.False(t, revoked)
	require.False(t, allows([]string{"cdc_reader"}, "db.cdc.query"))
}

// A role holds one grant per procedure and kind, immutable or not, as in
// Neo4j 5.26.30 Enterprise (auth disabled, as immutable privileges need):
//   - GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* then GRANT IMMUTABLE …: SHOW
//     ROLE r PRIVILEGES gives one row, immutable TRUE;
//   - GRANT IMMUTABLE … then GRANT …: "has no effect. The role already has
//     the privilege", still one row, immutable TRUE;
//   - REVOKE IMMUTABLE … of a regular grant: "has no effect. The role does
//     not have the privilege"; of an immutable one: removed;
//   - a plain REVOKE removes the grant, immutable or not.
func TestProcedurePrivilegeStoreImmutableGrants(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	store := NewProcedurePrivilegeStore(system)
	list := func() []ProcedurePrivilege {
		grants, err := store.List(ctx)
		require.NoError(t, err)
		return grants
	}
	immutableGrant := []ProcedurePrivilege{{Role: "ops", Procedure: "db.cdc.*", Boosted: true, Immutable: true}}

	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, false))
	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, true))
	require.Equal(t, immutableGrant, list())
	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, false))
	require.Equal(t, immutableGrant, list())
	grant, found, err := store.Lookup(ctx, "OPS", "db.cdc.*", true)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, immutableGrant[0], grant)
	_, found, err = store.Lookup(ctx, "ops", "db.cdc.*", false)
	require.NoError(t, err)
	require.False(t, found)

	revoked, err := store.Revoke(ctx, "ops", "db.cdc.*", true, true)
	require.NoError(t, err)
	require.True(t, revoked)
	require.Empty(t, list())

	// REVOKE IMMUTABLE of a regular grant changes nothing.
	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, false))
	revoked, err = store.Revoke(ctx, "ops", "db.cdc.*", true, true)
	require.NoError(t, err)
	require.False(t, revoked)
	allowed, err := store.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed)

	// A plain REVOKE removes an immutable grant.
	require.NoError(t, store.Grant(ctx, "ops", "db.cdc.*", true, true))
	revoked, err = store.Revoke(ctx, "ops", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.True(t, revoked)
	require.Empty(t, list())
}

// Another process or server instance over the same storage sees each
// change at once: here two stores over two namespaced views of one engine,
// as two servers each open their own.
func TestProcedurePrivilegeStoreSharedStorage(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	first := NewProcedurePrivilegeStore(storage.NewNamespacedEngine(engine, "system"))
	second := NewProcedurePrivilegeStore(storage.NewNamespacedEngine(engine, "system"))

	require.NoError(t, first.Grant(ctx, "ops", "db.cdc.*", true, false))
	allowed, err := second.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.True(t, allowed)
	revoked, err := first.Revoke(ctx, "ops", "db.cdc.*", true, false)
	require.NoError(t, err)
	require.True(t, revoked)
	allowed, err = second.AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.False(t, allowed)
}

// A principal's privileges are the union of its roles': each procedure may
// be granted through a different role.
func TestProcedurePrivilegeStoreRoleUnion(t *testing.T) {
	ctx := context.Background()
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
	store := NewProcedurePrivilegeStore(system)
	require.NoError(t, store.Grant(ctx, "a", "db.cdc.earliest", true, false))
	require.NoError(t, store.Grant(ctx, "b", "db.cdc.query", true, false))
	for procedure, want := range map[string]bool{"db.cdc.earliest": true, "db.cdc.query": true, "db.cdc.current": false} {
		allowed, err := store.AllowsBoosted([]string{"a", "b"}, procedure)
		require.NoError(t, err)
		require.Equal(t, want, allowed, procedure)
	}
}

// Procedure grants are kept apart from graph data: a node with the grant
// label that GRANT didn't write (another ID, or properties changed after
// GRANT wrote it) is not a grant.
func TestProcedurePrivilegeStoreReadsOnlyItsOwnNodes(t *testing.T) {
	ctx := context.Background()
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	system := storage.NewNamespacedEngine(engine, "system")
	_, err := system.CreateNode(&storage.Node{
		ID:         "4c1e0c2a-generated",
		Labels:     []string{procedurePrivilegeLabel},
		Properties: map[string]any{"role": "editor", "procedure": "*", "boosted": true},
	})
	require.NoError(t, err)
	store := NewProcedurePrivilegeStore(system)
	require.NoError(t, store.Grant(ctx, "editor", "db.ping", false, false))
	granted, err := system.GetNode(procedurePrivilegeNodeID(ProcedurePrivilege{Role: "editor", Procedure: "db.ping"}))
	require.NoError(t, err)
	granted.Properties["procedure"], granted.Properties["boosted"] = "*", true
	require.NoError(t, system.UpdateNode(granted))

	grants, err := store.List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)
	allowed, err := store.AllowsBoosted([]string{"editor"}, "db.cdc.query")
	require.NoError(t, err)
	require.False(t, allowed)
}

// Deleting a role removes its procedure grants, so a role created later
// under the same name starts without them; other roles keep theirs.
func TestDeleteRoleRemovesProcedureGrants(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	system := storage.NewNamespacedEngine(inner, "system")
	t.Cleanup(func() { _ = system.Close() })
	roles := NewRoleStore(system)
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	grants := NewProcedurePrivilegeStore(system)
	require.NoError(t, grants.Grant(ctx, "ops", "db.cdc.*", true, false))
	require.NoError(t, grants.Grant(ctx, "ops", "db.ping", false, true))
	require.NoError(t, grants.Grant(ctx, "viewer", "db.cdc.*", true, false))

	require.NoError(t, roles.DeleteRole(ctx, "OPS"))
	require.NoError(t, roles.CreateRole(ctx, "ops"))
	allowed, err := NewProcedurePrivilegeStore(system).AllowsBoosted([]string{"ops"}, "db.cdc.query")
	require.NoError(t, err)
	require.False(t, allowed)
	remaining, err := grants.List(ctx)
	require.NoError(t, err)
	require.Equal(t, []ProcedurePrivilege{{Role: "viewer", Procedure: "db.cdc.*", Boosted: true}}, remaining)
}
