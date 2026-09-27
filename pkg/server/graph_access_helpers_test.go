package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestServerGraphAccessAndCanAccessGraph: with a database manager a
// request's database needs every database cypher.AccessDatabases lists for
// it (a composite constituent needs its composite and its target database,
// an alias needs itself and its database), and the principal may use it
// only if it may use all of them; without a manager only the name itself is
// needed and checked.
func TestServerGraphAccessAndCanAccessGraph(t *testing.T) {
	server, authenticator := setupTestServer(t)
	ctx := context.Background()
	for _, name := range []string{"da", "db"} {
		require.NoError(t, server.dbManager.CreateDatabase(name))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "da", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "db", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, server.dbManager.CreateAlias("alias_b", "db"))

	for graph, want := range map[string]string{
		"da":        "da",
		"cmp":       "cmp",
		"cmp.a":     "da",
		"cmp.b":     "db",
		"cmp.none":  "cmp.none",
		"alias_b":   "db",
		"missing":   "missing",
		"missing.x": "missing.x",
	} {
		dataDatabase, _ := server.graphAccess(nil, graph)
		require.Equal(t, want, dataDatabase, graph)
	}

	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"da", "cmp"}))
	editor := &auth.JWTClaims{Roles: []string{string(auth.RoleEditor)}}
	require.True(t, server.canAccessGraph(editor, "da"))
	require.True(t, server.canAccessGraph(editor, "cmp"))
	require.True(t, server.canAccessGraph(editor, "cmp.a"))
	require.False(t, server.canAccessGraph(editor, "cmp.b"), "db is not allowed")
	require.False(t, server.canAccessGraph(editor, "db"))
	require.False(t, server.canAccessGraph(editor, "alias_b"))
	require.False(t, server.canAccessGraph(nil, "da"), "an unauthenticated request may use no database")

	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"da", "db"}))
	require.False(t, server.canAccessGraph(editor, "cmp.a"), "cmp is not allowed")
	require.True(t, server.canAccessGraph(editor, "db"))

	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"alias_b"}))
	require.False(t, server.canAccessGraph(editor, "alias_b"), "the alias's database db is not allowed")

	// Without a database manager the name alone is needed and checked.
	noManager := &Server{auth: authenticator, allowlistStore: server.allowlistStore}
	dataDatabase, allowed := noManager.graphAccess(nil, "cmp.a")
	require.Equal(t, "cmp.a", dataDatabase)
	require.True(t, allowed)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"da", "cmp"}))
	require.False(t, noManager.canAccessGraph(editor, "cmp.a"), "without a manager cmp.a is checked by its own name")
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"cmp.a"}))
	require.True(t, noManager.canAccessGraph(editor, "cmp.a"))
}
