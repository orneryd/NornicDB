package bolt

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestSessionGraphAccessDataDatabase: with a multidb manager, a RUN's or BEGIN's
// database needs every database cypher.AccessDatabases lists for it (a
// composite constituent needs its composite and its target database, an
// alias needs itself and its database); without one (no server, or a
// manager that is not a *multidb.DatabaseManager) it needs only its name.
func TestSessionGraphAccessDataDatabase(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, name := range []string{"da", "db"} {
		require.NoError(t, mgr.CreateDatabase(name))
	}
	require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "da", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "db", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, mgr.CreateAlias("alias_b", "db"))

	session := &Session{server: &Server{dbManager: mgr}}
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
		dataDatabase, _ := session.graphAccess(nil, graph)
		require.Equal(t, want, dataDatabase, graph)
	}

	noServer := &Session{}
	dataDatabase, allowed := noServer.graphAccess(nil, "cmp.a")
	require.Equal(t, "cmp.a", dataDatabase)
	require.True(t, allowed)
	otherManager := &Session{server: &Server{dbManager: &mockDBManager{stores: map[string]storage.Engine{}, defaultDB: "graph"}}}
	dataDatabase, allowed = otherManager.graphAccess(nil, "cmp.a")
	require.Equal(t, "cmp.a", dataDatabase)
	require.True(t, allowed)
}

// TestSessionCanAccessGraph: a principal may use a graph only if it may use
// every database the graph needs, so a constituent is allowed only when both
// its composite and its target database are, and without a multidb manager
// only the name itself is checked.
func TestSessionCanAccessGraph(t *testing.T) {
	mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	for _, name := range []string{"da", "db"} {
		require.NoError(t, mgr.CreateDatabase(name))
	}
	require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "da", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "db", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, mgr.CreateAlias("alias_b", "db"))

	editor := []string{string(auth.RoleEditor)}
	withComposite := auth.NewAllowlistDatabaseAccessMode(map[string][]string{string(auth.RoleEditor): {"da", "cmp"}}, editor)
	withoutComposite := auth.NewAllowlistDatabaseAccessMode(map[string][]string{string(auth.RoleEditor): {"da", "db"}}, editor)
	aliasOnly := auth.NewAllowlistDatabaseAccessMode(map[string][]string{string(auth.RoleEditor): {"alias_b"}}, editor)

	session := &Session{server: &Server{dbManager: mgr}}
	require.True(t, sessionAllows(session, withComposite, "da"))
	require.True(t, sessionAllows(session, withComposite, "cmp"))
	require.True(t, sessionAllows(session, withComposite, "cmp.a"))
	require.False(t, sessionAllows(session, withComposite, "cmp.b"), "db is not allowed")
	require.False(t, sessionAllows(session, withComposite, "db"))
	require.False(t, sessionAllows(session, withoutComposite, "cmp.a"), "cmp is not allowed")
	require.True(t, sessionAllows(session, withoutComposite, "db"))
	require.False(t, sessionAllows(session, aliasOnly, "alias_b"), "the alias's database db is not allowed")
	require.False(t, sessionAllows(session, withoutComposite, "alias_b"), "the alias itself is not allowed")
	require.True(t, sessionAllows(session, auth.FullDatabaseAccessMode, "cmp.b"))
	require.False(t, sessionAllows(session, auth.DenyAllDatabaseAccessMode, "da"))

	noManager := &Session{}
	require.False(t, sessionAllows(noManager, withComposite, "cmp.a"), "without a manager cmp.a is checked by its own name")
	require.True(t, sessionAllows(noManager, auth.NewAllowlistDatabaseAccessMode(map[string][]string{string(auth.RoleEditor): {"cmp.a"}}, editor), "cmp.a"))
}

// sessionAllows is graphAccess's allowed result.
func sessionAllows(session *Session, mode auth.DatabaseAccessMode, graph string) bool {
	_, allowed := session.graphAccess(mode, graph)
	return allowed
}
