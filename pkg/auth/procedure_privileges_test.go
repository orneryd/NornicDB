package auth

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A procedure name matches a grant's name or glob as in Neo4j: * any
// characters, dots included, ? one; case-insensitively.
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
	require.NoError(t, other.Load(ctx))
	require.False(t, other.AllowsBoosted([]string{"cdc_reader"}, "db.cdc.query"))

	require.NoError(t, store.Grant(ctx, "CDC_Reader", "db.cdc.*", true))
	require.NoError(t, store.Grant(ctx, "cdc_reader", "db.cdc.*", true)) // again: no change
	require.NoError(t, store.Grant(ctx, "viewer", "db.txlog.entries", false))
	require.Equal(t, []ProcedurePrivilege{
		{Role: "cdc_reader", Procedure: "db.cdc.*", Boosted: true},
		{Role: "viewer", Procedure: "db.txlog.entries", Boosted: false},
	}, store.List())
	require.True(t, other.AllowsBoosted([]string{"role_cdc_reader"}, "db.cdc.query"))
	require.False(t, other.AllowsBoosted([]string{"cdc_reader"}, "db.txlog.entries"))
	require.False(t, other.AllowsBoosted([]string{"viewer"}, "db.txlog.entries")) // not boosted
	require.False(t, other.AllowsBoosted(nil, "db.cdc.query"))

	revoked, err := store.Revoke(ctx, "cdc_reader", "db.cdc.*", true)
	require.NoError(t, err)
	require.True(t, revoked)
	revoked, err = store.Revoke(ctx, "cdc_reader", "db.cdc.*", true)
	require.NoError(t, err)
	require.False(t, revoked)
	require.False(t, other.AllowsBoosted([]string{"cdc_reader"}, "db.cdc.query"))
}
