package cypher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newImpersonationExecutor(t *testing.T) (*StorageExecutor, storage.Engine) {
	t.Helper()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	require.NoError(t, auth.NewRoleStore(system).CreateRole(context.Background(), "svc"))
	return NewStorageExecutor(system), system
}

// GRANT, DENY and REVOKE IMPERSONATE, with a user list, (*) or no list (all
// users), as Neo4j 5.26 writes them, and SHOW PRIVILEGES lists them in
// Neo4j's columns (action impersonate, segment USER(name)).
func TestImpersonatePrivilegeCommands(t *testing.T) {
	ctx := context.Background()
	exec, system := newImpersonationExecutor(t)
	store := auth.NewImpersonatePrivilegeStore(system)
	run := func(query string) {
		t.Helper()
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err, query)
		require.Empty(t, result.Rows)
	}
	list := func() []auth.ImpersonatePrivilege {
		t.Helper()
		privileges, err := store.List(ctx)
		require.NoError(t, err)
		return privileges
	}

	run("GRANT IMPERSONATE (alice, `bob smith`) ON DBMS TO svc")
	run("DENY IMPERSONATE (alice) ON DBMS TO svc")
	run("grant impersonate (*) on dbms to `svc`")
	require.Equal(t, []auth.ImpersonatePrivilege{
		{Role: "svc", AllUsers: true},
		{Role: "svc", User: "alice", Denied: true},
		{Role: "svc", User: "alice"},
		{Role: "svc", User: "bob smith"},
	}, list())

	shown, err := exec.Execute(ctx, "SHOW PRIVILEGES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"access", "action", "resource", "graph", "segment", "role", "immutable"}, shown.Columns)
	require.Equal(t, [][]interface{}{
		{"GRANTED", "impersonate", "database", "*", "USER(*)", "svc", false},
		{"DENIED", "impersonate", "database", "*", "USER(alice)", "svc", false},
		{"GRANTED", "impersonate", "database", "*", "USER(alice)", "svc", false},
		{"GRANTED", "impersonate", "database", "*", "USER(bob smith)", "svc", false},
	}, shown.Rows)
	shown, err = exec.Execute(ctx, "SHOW ROLE other PRIVILEGES", nil)
	require.NoError(t, err)
	require.Empty(t, shown.Rows)

	run("REVOKE GRANT IMPERSONATE (alice) ON DBMS FROM svc")
	require.Equal(t, []auth.ImpersonatePrivilege{
		{Role: "svc", AllUsers: true},
		{Role: "svc", User: "alice", Denied: true},
		{Role: "svc", User: "bob smith"},
	}, list())
	run("REVOKE IMPERSONATE ON DBMS FROM svc")
	run("REVOKE IMPERSONATE (alice, `bob smith`) ON DBMS FROM svc")
	require.Empty(t, list())

	run("GRANT IMPERSONATE ON DBMS TO svc")
	run("DENY IMPERSONATE (alice) ON DBMS TO svc")
	run("REVOKE DENY IMPERSONATE (alice) ON DBMS FROM svc")
	require.Equal(t, []auth.ImpersonatePrivilege{{Role: "svc", AllUsers: true}}, list())
	run("REVOKE IMPERSONATE (*) ON DBMS FROM nobody") // a missing role changes nothing
	require.Len(t, list(), 1)
}

// GRANT and DENY need the role to exist (ArgumentError, as Neo4j); while
// authentication is enabled IMMUTABLE privileges can't be set or revoked
// (Forbidden); malformed statements are syntax errors; other DENY forms
// stay unsupported.
func TestImpersonatePrivilegeCommandErrors(t *testing.T) {
	ctx := context.Background()
	exec, system := newImpersonationExecutor(t)
	_, err := exec.Execute(ctx, "GRANT IMPERSONATE (alice) ON DBMS TO nobody", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	require.Contains(t, err.Error(), "Failed to grant impersonate privilege to role 'nobody'")
	_, err = exec.Execute(ctx, "DENY IMPERSONATE (alice) ON DBMS TO nobody", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	require.Contains(t, err.Error(), "Failed to deny impersonate privilege to role 'nobody'")

	for _, query := range []string{"GRANT IMMUTABLE IMPERSONATE (alice) ON DBMS TO svc", "DENY IMMUTABLE IMPERSONATE (alice) ON DBMS TO svc"} {
		_, err = exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	}
	open := WithAuthenticationDisabled(ctx)
	_, err = exec.Execute(open, "GRANT IMMUTABLE IMPERSONATE (alice) ON DBMS TO svc", nil)
	require.NoError(t, err)
	for _, query := range []string{"REVOKE IMPERSONATE (alice) ON DBMS FROM svc", "REVOKE IMMUTABLE GRANT IMPERSONATE (alice) ON DBMS FROM svc"} {
		_, err = exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	}
	_, err = exec.Execute(open, "REVOKE GRANT IMMUTABLE IMPERSONATE (alice) ON DBMS FROM svc", nil)
	require.NoError(t, err)
	privileges, err := auth.NewImpersonatePrivilegeStore(system).List(ctx)
	require.NoError(t, err)
	require.Empty(t, privileges)

	for _, query := range []string{
		"GRANT IMPERSONATE (alice ON DBMS TO svc",
		"GRANT IMPERSONATE () ON DBMS TO svc",
		"GRANT IMPERSONATE (alice) ON DATABASE neo4j TO svc",
		"GRANT IMPERSONATE (alice) ON DBMS FROM svc",
		"REVOKE IMPERSONATE (alice) ON DBMS TO svc",
		"GRANT IMPERSONATE (alice) ON DBMS TO",
		"GRANT IMPERSONATE (*, alice) ON DBMS TO svc",
		"GRANT IMPERSONATE (alice, *) ON DBMS TO svc",
		"GRANT IMPERSONATE (alice,) ON DBMS TO svc",
		"GRANT IMPERSONATE (alice) ON DBMS TO svc, ",
	} {
		_, err = exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	_, err = exec.Execute(ctx, "DENY EXECUTE PROCEDURE db.a ON DBMS TO svc", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.UnsupportedAdministrationCommand")
}

// impersonatingContext is a statement's context while root impersonates
// target, as Bolt builds it, with an administrator's permissions.
func impersonatingContext(target string) context.Context {
	ctx := WithRequestIdentity(context.Background(), &RequestIdentity{
		User:           &AuthenticatedUser{Name: target, Roles: []string{"admin"}},
		ImpersonatedBy: &AuthenticatedUser{Name: "root", Roles: []string{"admin"}},
	})
	return WithPermissionChecker(ctx, func(string) bool { return true })
}

// While impersonating, an updating administration command is refused
// whatever the impersonated user's privileges, before it runs; SHOW and
// TERMINATE commands (TERMINATE decides each transaction: mayTerminate)
// and graph statements run.
func TestImpersonationRefusesUpdatingAdministration(t *testing.T) {
	ctx := impersonatingContext("alice")
	executing, authenticated := StatementUsers(ctx)
	require.Equal(t, "alice", executing)
	require.Equal(t, "root", authenticated)
	for _, query := range []string{
		"GRANT IMPERSONATE (bob) ON DBMS TO svc",
		"REVOKE EXECUTE PROCEDURE db.a ON DBMS FROM svc",
		"DENY IMPERSONATE (bob) ON DBMS TO svc",
		"CREATE DATABASE other",
		"DROP DATABASE other",
		"CREATE ALIAS a FOR DATABASE b",
		"  /* c */ grant impersonate on dbms to svc",
	} {
		err := AuthorizeQuery(ctx, query)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
		require.Contains(t, err.Error(), "impersonating a user", query)
	}
	for _, query := range []string{"SHOW PRIVILEGES", "SHOW TRANSACTIONS", "SHOW USERS", "TERMINATE TRANSACTIONS 'nornic-transaction-1'", "MATCH (n) RETURN n", "CREATE (:N)"} {
		require.NoError(t, AuthorizeQuery(ctx, query), query)
	}

	exec, system := newImpersonationExecutor(t)
	_, err := exec.Execute(ctx, "GRANT IMPERSONATE (bob) ON DBMS TO svc", nil)
	requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	privileges, err := auth.NewImpersonatePrivilegeStore(system).List(context.Background())
	require.NoError(t, err)
	require.Empty(t, privileges)

	// Not impersonating (no ImpersonatedBy, or the same user), the same
	// command is the principal's to run.
	self := WithRequestIdentity(context.Background(), &RequestIdentity{User: &AuthenticatedUser{Name: "root"}, ImpersonatedBy: &AuthenticatedUser{Name: "root"}})
	require.NoError(t, AuthorizeQuery(WithPermissionChecker(self, func(string) bool { return true }), "GRANT IMPERSONATE (bob) ON DBMS TO svc"))
	executing, authenticated = StatementUsers(context.Background())
	require.Empty(t, executing)
	require.Empty(t, authenticated)
}

// SHOW TRANSACTIONS lists a transaction under its executing user, Neo4j's
// username, and SHOW CURRENT USER is the executing user too.
func TestImpersonatedTransactionUsername(t *testing.T) {
	exec, _ := newImpersonationExecutor(t)
	ctx := impersonatingContext("imp-target-936")
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
	listed, err := exec.Execute(context.Background(), "SHOW TRANSACTIONS YIELD username RETURN username", nil)
	require.NoError(t, err)
	require.Contains(t, listed.Rows, []interface{}{"imp-target-936"})
	current, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(ctx, "SHOW CURRENT USER YIELD user RETURN user", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"imp-target-936"}}, current.Rows)
}

// NewPrincipalAccess, the one resolver constructor of HTTP and Bolt, applies
// the principal's roles to the database privileges and its entitlements to
// the rest, behind the graph access; without a principal only the graph
// access decides.
func TestNewPrincipalAccess(t *testing.T) {
	sources := PrincipalAccessSources{
		GraphAccess: func(graph string) (string, bool) { return graph, graph != "walled" },
		DatabaseAccess: func(roles []string, database string) auth.ResolvedAccess {
			if len(roles) == 1 && roles[0] == "reader" {
				return auth.ResolvedAccess{Read: true}
			}
			return auth.ResolvedAccess{}
		},
	}
	open := NewPrincipalAccess(sources, nil)
	require.True(t, open.Allows("nornic", "write"))
	require.True(t, open.Allows("nornic", "admin"))
	require.False(t, open.Allows("walled", "read"))

	reader := NewPrincipalAccess(sources, &AccessPrincipal{Roles: []string{"reader"}, Entitlement: func(permission string) bool { return permission == "schema" }})
	require.True(t, reader.Allows("nornic", "read"))
	require.False(t, reader.Allows("nornic", "write"))
	require.True(t, reader.Allows("nornic", "schema"))
	require.False(t, reader.Allows("nornic", "admin"))
	require.False(t, reader.Allows("walled", "read"))

	nobody := NewPrincipalAccess(PrincipalAccessSources{}, &AccessPrincipal{})
	require.False(t, nobody.Allows("nornic", "read"), "a principal without entitlements holds none")
	require.True(t, nobody.Allows("nornic", "access"))
}

// Only a bare * is every user: a backticked `*` is the user named *, a list
// with a bare * is a syntax error, and a backticked name may hold ) and ,.
func TestImpersonatePrivilegeQuotedStarIsAUser(t *testing.T) {
	ctx := context.Background()
	exec, system := newImpersonationExecutor(t)
	store := auth.NewImpersonatePrivilegeStore(system)
	list := func() []auth.ImpersonatePrivilege {
		t.Helper()
		privileges, err := store.List(ctx)
		require.NoError(t, err)
		return privileges
	}
	_, err := exec.Execute(ctx, "GRANT IMPERSONATE (`*`) ON DBMS TO svc", nil)
	require.NoError(t, err)
	require.Equal(t, []auth.ImpersonatePrivilege{{Role: "svc", User: "*"}}, list())
	allowed, err := store.AllowsImpersonation([]string{"svc"}, false, "admin")
	require.NoError(t, err)
	require.False(t, allowed, "the user named * is not every user")

	_, err = exec.Execute(ctx, "GRANT IMPERSONATE (`a), b`) ON DBMS TO `svc`", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "GRANT IMPERSONATE (*) ON DBMS TO svc", nil)
	require.NoError(t, err)
	require.Equal(t, []auth.ImpersonatePrivilege{
		{Role: "svc", AllUsers: true},
		{Role: "svc", User: "*"},
		{Role: "svc", User: "a), b"},
	}, list())
	shown, err := exec.Execute(ctx, "SHOW PRIVILEGES", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{
		{"GRANTED", "impersonate", "database", "*", "USER(*)", "svc", false},
		{"GRANTED", "impersonate", "database", "*", "USER(`*`)", "svc", false},
		{"GRANTED", "impersonate", "database", "*", "USER(a), b)", "svc", false},
	}, shown.Rows)

	_, err = exec.Execute(ctx, "REVOKE IMPERSONATE (`*`) ON DBMS FROM svc", nil)
	require.NoError(t, err)
	require.Equal(t, []auth.ImpersonatePrivilege{
		{Role: "svc", AllUsers: true},
		{Role: "svc", User: "a), b"},
	}, list(), "revoking the user named * keeps IMPERSONATE (*)")

	for _, query := range []string{"GRANT IMPERSONATE (*, alice) ON DBMS TO svc", "DENY IMPERSONATE (`*`, *) ON DBMS TO svc"} {
		_, err = exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	require.Len(t, list(), 2, "a refused statement changes nothing")
}

// While impersonating, even an administrator can't change the DBMS through a
// procedure whose contract says it does (ProcedureSpec.UpdatesDBMS): custom
// procedure install, declare and drop are refused, called directly or from
// dynamic Cypher, and nothing changes; graph procedures still run.
func TestImpersonationRefusesDBMSUpdatingProcedures(t *testing.T) {
	t.Cleanup(ClearUserProcedures)
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	ctx := impersonatingContext("alice")
	install := "CALL apoc.custom.installProcedure('imp936_marker() :: (v :: INTEGER)', 'RETURN 1 AS v', 'neo4j', 'read')"
	listed := func() [][]interface{} {
		t.Helper()
		result, err := exec.Execute(context.Background(), "CALL apoc.custom.list() YIELD name RETURN name", nil)
		require.NoError(t, err)
		return result.Rows
	}
	for _, query := range []string{
		install,
		"CALL apoc.custom.declareProcedure('imp936_marker() :: (v :: INTEGER)', 'RETURN 1 AS v')",
		"CALL apoc.custom.dropProcedure('imp936_marker')",
		"CALL apoc.cypher.run(\"" + install + "\", {}) YIELD value RETURN value",
		"CALL apoc.cypher.doIt(\"CALL apoc.custom.dropProcedure('imp936_marker')\", {}) YIELD value RETURN value",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
		require.Contains(t, err.Error(), "impersonating a user", query)
	}
	require.Empty(t, listed(), "a refused install registers nothing")

	// The same administrator, not impersonating, installs it; impersonating,
	// it can't drop it.
	admin := WithPermissionChecker(context.Background(), func(string) bool { return true })
	_, err := exec.Execute(admin, install, nil)
	require.NoError(t, err)
	require.Len(t, listed(), 1)
	_, err = exec.Execute(ctx, "CALL apoc.custom.dropProcedure('imp936_marker')", nil)
	requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	require.Len(t, listed(), 1)
	rows, err := exec.Execute(ctx, "CALL custom.imp936_marker() YIELD v RETURN v", nil)
	require.NoError(t, err, "calling a custom procedure changes nothing of the DBMS")
	require.Equal(t, [][]interface{}{{int64(1)}}, rows.Rows)
	_, err = exec.Execute(admin, "CALL apoc.custom.dropProcedure('imp936_marker')", nil)
	require.NoError(t, err)

	require.True(t, QueryPermissionRequirements(install).UpdatesDBMS)
	require.False(t, QueryPermissionRequirements("CALL db.labels()").UpdatesDBMS)
	require.False(t, QueryPermissionRequirements("CALL apoc.custom.list()").UpdatesDBMS)
}

// newRemoteCompositeManager is a database manager with composite "mixed",
// whose constituent "loc" is local and "rem" remote (served by a test server
// counting the requests it gets), and composite "plain", all local.
func newRemoteCompositeManager(t *testing.T) (*multidb.DatabaseManager, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"results":[{"columns":["n"],"data":[]}],"errors":[]}`))
	}))
	t.Cleanup(remote.Close)
	manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = manager.Close() })
	require.NoError(t, manager.CreateDatabase("tenant_local"))
	require.NoError(t, manager.CreateCompositeDatabase("mixed", []multidb.ConstituentRef{
		{Alias: "loc", DatabaseName: "tenant_local", Type: "local", AccessMode: "read_write"},
		{Alias: "rem", DatabaseName: "tenant_remote", Type: "remote", AccessMode: "read_write", URI: remote.URL, AuthMode: "oidc_forwarding"},
	}))
	require.NoError(t, manager.CreateCompositeDatabase("plain", []multidb.ConstituentRef{
		{Alias: "loc", DatabaseName: "tenant_local", Type: "local", AccessMode: "read_write"},
	}))
	return manager, &requests
}

// GraphIsRemote is true for a remote constituent and for a composite with
// one, false for local databases, constituents and composites.
func TestGraphIsRemote(t *testing.T) {
	manager, _ := newRemoteCompositeManager(t)
	adapter := &testDatabaseManagerAdapter{manager: manager}
	for graph, remote := range map[string]bool{
		"mixed.rem": true, "mixed": true,
		"mixed.loc": false, "plain": false, "plain.loc": false, "tenant_local": false, manager.DefaultDatabaseName(): false,
	} {
		require.Equal(t, remote, GraphIsRemote(adapter, graph), graph)
	}
	require.False(t, GraphIsRemote(nil, "mixed.rem"))
}

// While impersonating, a statement selecting a remote constituent, directly
// or as a composite's, is refused before anything is sent to the remote
// server, which would run it as the login's credential or the
// constituent's stored one, never as the impersonated user; local
// constituents still run. Not impersonating, the same statement reaches the
// remote server.
func TestImpersonationRefusesRemoteGraphs(t *testing.T) {
	manager, requests := newRemoteCompositeManager(t)
	defaultStore, err := manager.GetStorage(manager.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	ctx := impersonatingContext("alice")

	for _, query := range []string{
		"USE mixed.rem MATCH (n) RETURN n",
		"USE mixed CALL { USE mixed.rem MATCH (n) RETURN n } RETURN n",
		"CALL { USE mixed.rem MATCH (n) RETURN n } RETURN n",
	} {
		_, err := exec.Execute(ctx, query, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
		require.Contains(t, err.Error(), "remote database", query)
	}
	require.Zero(t, requests.Load(), "nothing reached the remote server")

	_, err = exec.Execute(ctx, "USE mixed.loc MATCH (n) RETURN count(n)", nil)
	require.NoError(t, err, "a local constituent runs")
	_, err = exec.Execute(ctx, "USE plain.loc MATCH (n) RETURN count(n)", nil)
	require.NoError(t, err)

	_, _ = exec.Execute(context.Background(), "USE mixed.rem MATCH (n) RETURN n", nil)
	require.NotZero(t, requests.Load(), "not impersonating, the statement reaches the remote server")
}

// While impersonating, even an administrator can't change state every
// session shares: emptying the query caches or starting, stopping or
// clearing the query statistics collector is refused, directly or from
// dynamic Cypher, and the collector keeps collecting; reading its status or
// statistics still runs.
func TestImpersonationRefusesSharedStateProcedures(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	ctx := impersonatingContext("alice")
	for _, call := range []string{
		"CALL db.clearQueryCaches()",
		"CALL db.stats.collect('QUERIES')",
		"CALL db.stats.stop('QUERIES')",
		"CALL db.stats.clear('QUERIES')",
	} {
		for _, query := range []string{
			call,
			"CALL apoc.cypher.run(\"" + call + "\", {}) YIELD value RETURN value",
			"CALL apoc.cypher.doIt(\"" + call + "\", {}) YIELD value RETURN value",
		} {
			_, err := exec.Execute(ctx, query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
			require.Contains(t, err.Error(), "impersonating a user", query)
		}
	}
	status, err := exec.Execute(ctx, "CALL db.stats.status() YIELD status RETURN status", nil)
	require.NoError(t, err, "reading the collector's status runs")
	require.Equal(t, [][]interface{}{{"collecting"}}, status.Rows, "a refused stop left collection on, as it starts")
	_, err = exec.Execute(ctx, "CALL db.stats.retrieve('GRAPH COUNTS')", nil)
	require.NoError(t, err)

	// Not impersonating, the administrator may.
	admin := WithPermissionChecker(context.Background(), func(string) bool { return true })
	_, err = exec.Execute(admin, "CALL db.clearQueryCaches()", nil)
	require.NoError(t, err)
}

// recordingEmbeddingController records the pauses and resumes that reach it.
type recordingEmbeddingController struct {
	status  EmbeddingStatus
	changes []bool
}

func (c *recordingEmbeddingController) SetEmbeddingPaused(_ context.Context, paused bool) (EmbeddingStatus, error) {
	c.changes = append(c.changes, paused)
	c.status.Paused = paused
	return c.status, nil
}

func (c *recordingEmbeddingController) EmbeddingStatus() EmbeddingStatus { return c.status }

// Automatic embedding's pause is DBMS-wide (#1021): while impersonating,
// even an administrator's pause and resume are refused before they reach
// the embedder, directly or from dynamic Cypher; reading the status needs
// the impersonated user's admin, as it does signed in.
func TestImpersonationAndEmbeddingControl(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	controller := &recordingEmbeddingController{status: EmbeddingStatus{Pending: 3}}
	exec.SetEmbeddingController(controller)
	ctx := impersonatingContext("alice")
	for _, call := range []string{"CALL db.embedding.pause()", "CALL db.embedding.resume()"} {
		for _, query := range []string{call, "CALL apoc.cypher.run(\"" + call + "\", {}) YIELD value RETURN value"} {
			_, err := exec.Execute(ctx, query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
			require.Contains(t, err.Error(), "impersonating a user", query)
		}
	}
	require.Empty(t, controller.changes, "a refused pause or resume never reaches the embedder")
	status, err := exec.Execute(ctx, "CALL db.embedding.status()", nil)
	require.NoError(t, err, "an impersonated administrator reads the status")
	require.Equal(t, [][]interface{}{{false, int64(3)}}, status.Rows)

	// An impersonated user without admin may not read it either.
	nonAdmin := WithPermissionChecker(WithRequestIdentity(context.Background(), &RequestIdentity{
		User:           &AuthenticatedUser{Name: "bob", Roles: []string{"reader"}},
		ImpersonatedBy: &AuthenticatedUser{Name: "root", Roles: []string{"admin"}},
	}), func(permission string) bool { return permission != "admin" })
	_, err = exec.Execute(nonAdmin, "CALL db.embedding.status()", nil)
	requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	require.Contains(t, err.Error(), "Executing admin procedure 'db.embedding.status' permission has not been granted for user 'bob' with roles [reader].")

	// Signed in, the administrator pauses and resumes.
	admin := WithPermissionChecker(context.Background(), func(string) bool { return true })
	for _, call := range []string{"CALL db.embedding.pause()", "CALL db.embedding.resume()"} {
		_, err := exec.Execute(admin, call, nil)
		require.NoError(t, err, call)
	}
	require.Equal(t, []bool{true, false}, controller.changes)
}

// Every built-in procedure that changes state other sessions share is
// marked UpdatesDBMS, and only those: a new built-in must be classified
// here. The others change one database's data or schema, the calling
// transaction, or nothing.
func TestBuiltInProceduresUpdatingTheDBMS(t *testing.T) {
	ensureBuiltInProceduresRegistered()
	var marked []string
	for _, spec := range globalProcedureRegistry.ListBuiltIns() {
		if spec.UpdatesDBMS {
			marked = append(marked, spec.Name)
		}
	}
	require.Equal(t, []string{
		"apoc.custom.declareProcedure",
		"apoc.custom.dropProcedure",
		"apoc.custom.installProcedure",
		"db.clearQueryCaches",
		"db.embedding.pause",
		"db.embedding.resume",
		"db.stats.clear",
		"db.stats.collect",
		"db.stats.stop",
	}, marked)
}

// TERMINATE TRANSACTIONS decides each transaction as Neo4j does, the same
// for an impersonated user as for that user signed in: every user may end
// its own transactions (while impersonating, the impersonated user's), and
// another user's only with transaction-management rights (admin); one it
// may not end is reported not found and keeps running.
func TestImpersonatedTerminateTransactions(t *testing.T) {
	const alice, bob = "imp936-term-alice", "imp936-term-bob"
	as := func(user string, admin, impersonated bool) context.Context {
		identity := &RequestIdentity{User: &AuthenticatedUser{Name: user}}
		if impersonated {
			identity.ImpersonatedBy = &AuthenticatedUser{Name: "imp936-term-svc"}
		}
		return WithPermissionChecker(WithRequestIdentity(context.Background(), identity),
			func(permission string) bool { return permission != "admin" || admin })
	}
	var begun []string
	begin := func(user string) string {
		t.Helper()
		owner := NewStorageExecutor(newTestMemoryEngine(t))
		ctx := as(user, false, false)
		_, err := owner.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		t.Cleanup(func() { _, _ = owner.Execute(ctx, "ROLLBACK", nil) })
		listed, err := owner.Execute(context.Background(), "SHOW TRANSACTIONS YIELD transactionId, username WHERE username = $user RETURN transactionId", map[string]interface{}{"user": user})
		require.NoError(t, err)
		for _, row := range listed.Rows {
			known := false
			for _, id := range begun {
				known = known || id == row[0]
			}
			if !known {
				begun = append(begun, row[0].(string))
				return row[0].(string)
			}
		}
		t.Fatalf("no new transaction listed for %s", user)
		return ""
	}
	running := func(id string) bool {
		t.Helper()
		listed, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(context.Background(), "SHOW TRANSACTIONS YIELD transactionId, status WHERE transactionId = $id RETURN status", map[string]interface{}{"id": id})
		require.NoError(t, err)
		return len(listed.Rows) == 1 && listed.Rows[0][0] == "Running"
	}
	terminate := func(ctx context.Context, id string) []interface{} {
		t.Helper()
		result, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(ctx, "TERMINATE TRANSACTION $id", map[string]interface{}{"id": id})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1)
		return result.Rows[0]
	}
	aliceImpersonated, aliceSignedIn, bobTx := begin(alice), begin(alice), begin(bob)

	require.Equal(t, []interface{}{aliceImpersonated, alice, "Transaction terminated."}, terminate(as(alice, false, true), aliceImpersonated),
		"an impersonated user ends its own transaction")
	require.Equal(t, []interface{}{aliceSignedIn, alice, "Transaction terminated."}, terminate(as(alice, false, false), aliceSignedIn),
		"as the same user signed in may")
	require.Equal(t, []interface{}{bobTx, nil, "Transaction not found."}, terminate(as(alice, false, true), bobTx),
		"another user's, without admin, is not found")
	require.True(t, running(bobTx), "and keeps running")
	require.Equal(t, []interface{}{bobTx, nil, "Transaction not found."}, terminate(as(alice, false, false), bobTx))
	require.Equal(t, []interface{}{bobTx, bob, "Transaction terminated."}, terminate(as(alice, true, true), bobTx),
		"an impersonated administrator ends another user's")

	// A write-only user (write, no read: a combination NornicDB supports)
	// ends its own transaction too, signed in or impersonated, as Neo4j lets
	// every user; another user's stays not found and running.
	writeOnly := func(user string, impersonated bool) context.Context {
		identity := &RequestIdentity{User: &AuthenticatedUser{Name: user}}
		if impersonated {
			identity.ImpersonatedBy = &AuthenticatedUser{Name: "imp936-term-svc"}
		}
		return WithPermissionChecker(WithRequestIdentity(context.Background(), identity),
			func(permission string) bool { return permission == "write" })
	}
	ownImpersonated, ownSignedIn, otherTx := begin(alice), begin(alice), begin(bob)
	require.Equal(t, []interface{}{ownImpersonated, alice, "Transaction terminated."}, terminate(writeOnly(alice, true), ownImpersonated))
	require.Equal(t, []interface{}{ownSignedIn, alice, "Transaction terminated."}, terminate(writeOnly(alice, false), ownSignedIn))
	require.Equal(t, []interface{}{otherTx, nil, "Transaction not found."}, terminate(writeOnly(alice, false), otherTx))
	require.True(t, running(otherTx))
}
