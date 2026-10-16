package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// GRANT / REVOKE EXECUTE [BOOSTED] PROCEDURE … ON DBMS parse as Neo4j writes
// them; other privilege statements are not procedure privileges.
func TestParseProcedurePrivilegeCommand(t *testing.T) {
	command, ok, err := parseProcedurePrivilegeCommand("GRANT EXECUTE BOOSTED PROCEDURE db.cdc.*, db.txlog.entries ON DBMS TO cdc_reader, `other role`")
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, procedurePrivilegeCommand{grant: true, boosted: true, procedures: []string{"db.cdc.*", "db.txlog.entries"}, roles: []string{"cdc_reader", "other role"}}, command)

	command, ok, err = parseProcedurePrivilegeCommand("REVOKE GRANT IMMUTABLE EXECUTE PROCEDURES db.cdc.query ON DBMS FROM cdc_reader")
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, procedurePrivilegeCommand{immutable: true, procedures: []string{"db.cdc.query"}, roles: []string{"cdc_reader"}}, command)

	for _, statement := range []string{"GRANT ROLE admin TO bob", "GRANT TRAVERSE ON GRAPH * TO r", "DENY EXECUTE PROCEDURE x ON DBMS TO r", "MATCH (n) RETURN n"} {
		_, ok, _ = parseProcedurePrivilegeCommand(statement)
		require.False(t, ok, statement)
	}
	for _, statement := range []string{"GRANT EXECUTE PROCEDURE db.x ON GRAPH * TO r", "GRANT EXECUTE PROCEDURE db.x ON DBMS FROM r", "GRANT EXECUTE PROCEDURE ON DBMS TO r"} {
		_, ok, err = parseProcedurePrivilegeCommand(statement)
		require.True(t, ok, statement)
		require.Error(t, err, statement)
	}
}

// On the system database, GRANT records a procedure privilege for an existing
// role, SHOW [ROLE r] PRIVILEGES lists it in Neo4j's columns, REVOKE removes
// it; a role that doesn't exist is refused, and other GRANTs are unsupported.
func TestProcedurePrivilegeStatements(t *testing.T) {
	ctx := context.Background()
	systemBase := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(systemBase, "system")
	require.NoError(t, auth.NewRoleStore(system).CreateRole(ctx, "cdc_reader"))
	exec := NewStorageExecutor(system)

	_, err := exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO cdc_reader", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "SHOW ROLE cdc_reader PRIVILEGES", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"access", "action", "resource", "graph", "segment", "role", "immutable"}, result.Columns)
	require.Equal(t, [][]interface{}{{"GRANTED", "execute_boosted", "database", "*", "PROCEDURE(db.cdc.*)", "cdc_reader", false}}, result.Rows)
	result, err = exec.Execute(ctx, "SHOW PRIVILEGES YIELD role, segment WHERE role = 'cdc_reader' RETURN segment", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"PROCEDURE(db.cdc.*)"}}, result.Rows)
	require.True(t, allowsBoosted(t, system, "cdc_reader", "db.cdc.query"))

	_, err = exec.Execute(ctx, "REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM cdc_reader", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "SHOW PRIVILEGES", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO nobody", nil)
	require.ErrorContains(t, err, "Role does not exist")
	_, err = exec.Execute(ctx, "GRANT ROLE cdc_reader TO bob", nil)
	require.Error(t, err)
}

// A query that needs administrator rights only for the procedure it calls is
// authorized by a boosted grant for that procedure; an administration command
// or another admin procedure still needs administrator rights.
func TestAuthorizeQueryWithBoostedProcedureGrant(t *testing.T) {
	require.Equal(t, []string{"db.cdc.query"}, QueryPermissionRequirements("CALL db.cdc.query()").BoostedProcedures)
	require.Empty(t, QueryPermissionRequirements("SHOW USERS").BoostedProcedures)
	require.True(t, QueryPermissionRequirements("GRANT EXECUTE BOOSTED PROCEDURE x ON DBMS TO r").Admin)
	require.True(t, QueryPermissionRequirements("SHOW PRIVILEGES").Admin)

	nonAdmin := WithPermissionChecker(context.Background(), func(permission string) bool { return permission == "read" })
	granted := WithProcedureGrantChecker(nonAdmin, func(procedure string) bool { return auth.ProcedureGlobMatches("db.cdc.*", procedure) })
	require.Error(t, AuthorizeQuery(nonAdmin, "CALL db.cdc.query()"))
	require.NoError(t, AuthorizeQuery(granted, "CALL db.cdc.query()"))
	require.Error(t, AuthorizeQuery(granted, "CALL db.txlog.entries()"))
	// Every admin procedure a query calls must be granted, a chained one too.
	require.NoError(t, AuthorizeQuery(granted, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 5) YIELD id AS change RETURN count(*)"))
	require.Error(t, AuthorizeQuery(granted, "CALL db.cdc.query() YIELD id CALL db.txlog.entries() YIELD txId RETURN count(*)"))
	require.Error(t, AuthorizeQuery(nonAdmin, "CALL db.cdc.earliest() YIELD id CALL db.cdc.query(id, [], 5) YIELD id AS change RETURN count(*)"))
	require.Error(t, AuthorizeQuery(granted, "SHOW USERS"))
	require.Error(t, AuthorizeQuery(granted, "GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO r"))
}

// allowsBoosted reports whether system's committed grants give role
// procedure boosted.
func allowsBoosted(t *testing.T, system storage.Engine, role, procedure string) bool {
	t.Helper()
	allowed, err := auth.NewProcedurePrivilegeStore(system).AllowsBoosted([]string{role}, procedure)
	require.NoError(t, err)
	return allowed
}

// A backticked procedure name or glob, whole or in parts, is stored and
// matched unquoted, as Neo4j 5.26 accepts it: Neo4j 5.26.30 Enterprise lists
// GRANT EXECUTE BOOSTED PROCEDURE `db.cdc.*` as PROCEDURE(db.cdc.*) (one row
// with the unquoted grant, "has no effect" when that is already granted),
// and GRANT EXECUTE BOOSTED PROCEDURE db.`cdc`.query, `*` as PROCEDURE(db.cdc.query)
// and PROCEDURE(*).
func TestProcedurePrivilegeQuotedGlobs(t *testing.T) {
	ctx := context.Background()
	for _, written := range []string{"`db.cdc.*`", "db.`cdc`.*", "`*`", "`db`.`cdc`.`query`"} {
		t.Run(written, func(t *testing.T) {
			systemBase := newTestMemoryEngine(t)
			system := storage.NewNamespacedEngine(systemBase, "system")
			exec := NewStorageExecutor(system)
			_, err := exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE "+written+" ON DBMS TO viewer", nil)
			require.NoError(t, err)
			require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
			_, err = exec.Execute(ctx, "REVOKE EXECUTE BOOSTED PROCEDURE "+written+" ON DBMS FROM viewer", nil)
			require.NoError(t, err)
			require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
		})
	}
	require.Equal(t, "db.a`b.*", unquoteProcedureGlob("db.`a``b`.*"))
	require.Equal(t, "db.`cdc", unquoteProcedureGlob("db.`cdc"))
}

// With authentication disabled, GRANT IMMUTABLE is stored and listed as
// immutable (a regular grant becomes immutable), REVOKE IMMUTABLE removes
// only an immutable grant and a plain REVOKE removes either. Neo4j 5.26.30
// Enterprise with authentication disabled gives, for SHOW ROLE r
// PRIVILEGES after GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON
// DBMS TO r: "GRANTED", "execute_boosted", "database", "*",
// "PROCEDURE(db.cdc.*)", "r", TRUE.
func TestProcedurePrivilegeImmutable(t *testing.T) {
	ctx := WithAuthenticationDisabled(context.Background())
	systemBase := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(systemBase, "system")
	exec := NewStorageExecutor(system)
	run := func(statement string) *ExecuteResult {
		t.Helper()
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		return result
	}

	run("GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer")
	require.Equal(t, [][]interface{}{{"GRANTED", "execute_boosted", "database", "*", "PROCEDURE(db.cdc.*)", "viewer", true}},
		run("SHOW ROLE viewer PRIVILEGES").Rows)
	run("GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer") // already granted: still immutable
	require.Equal(t, [][]interface{}{{true}}, run("SHOW ROLE viewer PRIVILEGES YIELD immutable RETURN immutable").Rows)

	run("GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO editor")
	run("REVOKE IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM editor")
	require.True(t, allowsBoosted(t, system, "editor", "db.cdc.query"))
	run("REVOKE IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM viewer")
	require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))

	run("GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO editor")
	require.Equal(t, [][]interface{}{{true}}, run("SHOW ROLE editor PRIVILEGES YIELD immutable RETURN immutable").Rows)
	run("REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM editor")
	require.Empty(t, run("SHOW PRIVILEGES").Rows)
}

// With authentication enabled (no WithAuthenticationDisabled mark), as
// Neo4j 5.26.30 Enterprise does: GRANT IMMUTABLE fails with
// Neo.ClientError.Security.Forbidden "Permission cannot be granted for
// ASSIGN IMMUTABLE PRIVILEGE." (even to a role that doesn't exist); REVOKE
// of an immutable grant, with or without IMMUTABLE, fails with
// Neo.ClientError.Security.Forbidden "Immutable privileges cannot be
// revoked." and changes nothing, also for the statement's other grants;
// REVOKE IMMUTABLE of a regular grant has no effect; regular grants are
// granted and revoked as usual.
func TestProcedurePrivilegeImmutableWithAuthentication(t *testing.T) {
	systemBase := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(systemBase, "system")
	open := WithAuthenticationDisabled(context.Background())
	exec := NewStorageExecutor(system)
	_, err := exec.Execute(open, "GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.ping ON DBMS TO viewer", nil)
	require.NoError(t, err)

	ctx := context.Background()
	for _, statement := range []string{
		"GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer",
		"GRANT IMMUTABLE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO nobody",
	} {
		_, err = exec.Execute(ctx, statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
		require.ErrorContains(t, err, "Permission cannot be granted for ASSIGN IMMUTABLE PRIVILEGE.")
	}
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.other ON DBMS TO viewer", nil)
	require.NoError(t, err)
	for _, statement := range []string{
		"REVOKE EXECUTE BOOSTED PROCEDURE db.ping ON DBMS FROM viewer",
		"REVOKE IMMUTABLE EXECUTE BOOSTED PROCEDURE db.ping ON DBMS FROM viewer",
		"REVOKE EXECUTE BOOSTED PROCEDURE db.other, db.ping ON DBMS FROM viewer",
	} {
		_, err = exec.Execute(ctx, statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
		require.ErrorContains(t, err, "Immutable privileges cannot be revoked.")
	}
	_, err = exec.Execute(ctx, "REVOKE IMMUTABLE EXECUTE BOOSTED PROCEDURE db.other ON DBMS FROM viewer", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "SHOW ROLE viewer PRIVILEGES YIELD segment, immutable RETURN segment, immutable", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"PROCEDURE(db.other)", false}, {"PROCEDURE(db.ping)", true}}, result.Rows)
	_, err = exec.Execute(ctx, "REVOKE EXECUTE BOOSTED PROCEDURE db.other ON DBMS FROM viewer", nil)
	require.NoError(t, err)
	result, err = exec.Execute(ctx, "SHOW ROLE viewer PRIVILEGES YIELD segment RETURN segment", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"PROCEDURE(db.ping)"}}, result.Rows)
}

// GRANT to a role that doesn't exist fails, as Neo4j 5.26.30 Enterprise's
// Neo.ClientError.Statement.ArgumentError "Failed to grant execute_boosted
// privilege to role 'nobody': Role does not exist."; REVOKE from one
// succeeds and changes nothing (Neo4j: no error, an ImpossibleRevokeCommand
// notification "Role does not exist").
func TestProcedurePrivilegeMissingRole(t *testing.T) {
	ctx := context.Background()
	systemBase := newTestMemoryEngine(t)
	system := storage.NewNamespacedEngine(systemBase, "system")
	exec := NewStorageExecutor(system)
	_, err := exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer, nobody", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArgumentError")
	require.ErrorContains(t, err, "Failed to grant execute_boosted privilege to role 'nobody': Role does not exist.")
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.ping ON DBMS TO nobody", nil)
	require.ErrorContains(t, err, "Failed to grant execute_boosted privilege to role 'nobody'")
	require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
	_, err = exec.Execute(ctx, "REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM nobody", nil)
	require.NoError(t, err)
}

// A GRANT or REVOKE in an explicit transaction takes effect on commit and
// is undone by rollback; SHOW PRIVILEGES in the transaction sees its own
// changes. The transaction's database is the system database however the
// executor reaches it: the database manager's system storage, a namespaced
// store in the system namespace, or the executor's own storage.
func TestProcedurePrivilegeInExplicitTransaction(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	for name, setup := range map[string]func() (*StorageExecutor, storage.Engine){
		"database manager's system storage": func() (*StorageExecutor, storage.Engine) {
			systemBase := newTestMemoryEngine(t)
			system := storage.NewNamespacedEngine(systemBase, "system")
			exec := NewStorageExecutor(system)
			exec.SetDatabaseManager(systemOnlyManager{engine: storage.Engine(system)})
			return exec, system
		},
		"system namespace": func() (*StorageExecutor, storage.Engine) {
			system := storage.NewNamespacedEngine(inner, "system")
			exec := NewStorageExecutor(system)
			exec.SetDatabaseManager(systemOnlyManager{engine: storage.Engine(storage.NewNamespacedEngine(inner, "system"))})
			return exec, system
		},
		"no database manager": func() (*StorageExecutor, storage.Engine) {
			systemBase := newTestMemoryEngine(t)
			system := storage.NewNamespacedEngine(systemBase, "system")
			return NewStorageExecutor(system), system
		},
	} {
		t.Run(name, func(t *testing.T) {
			exec, system := setup()
			run := func(statement string) *ExecuteResult {
				t.Helper()
				result, err := exec.Execute(ctx, statement, nil)
				require.NoError(t, err, statement)
				return result
			}
			run("BEGIN")
			run("GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer")
			require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
			require.Len(t, run("SHOW PRIVILEGES").Rows, 1)
			run("ROLLBACK")
			require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))

			run("BEGIN")
			run("GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer")
			run("COMMIT")
			require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))

			run("BEGIN")
			run("REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM viewer")
			require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
			run("ROLLBACK")
			require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
			run("BEGIN")
			run("REVOKE EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS FROM viewer")
			run("COMMIT")
			require.False(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
		})
	}
}

// Administration commands choose system with their transaction's first
// statement, even when the client opened it on a user database.
func TestProcedurePrivilegeInOtherDatabaseTransaction(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	require.NoError(t, auth.NewProcedurePrivilegeStore(system).Grant(ctx, "viewer", "db.ping", false, false))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	exec.SetDatabaseManager(systemOnlyManager{engine: storage.Engine(system)})

	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, "SHOW PRIVILEGES", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
	require.True(t, allowsBoosted(t, system, "viewer", "db.cdc.query"))
}

// Procedure grants are kept apart from graph data: the system database
// runs no statement that writes graph data, through Execute, inside an
// explicit transaction, or as a statement a procedure runs, so a node with
// the grant label can't be made there with Cypher. Reads and the
// administration commands still run there, and other databases' writes are
// unaffected.
func TestSystemDatabaseRefusesGraphWrites(t *testing.T) {
	ctx := context.Background()
	inner := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })
	system := storage.NewNamespacedEngine(inner, "system")
	exec := NewStorageExecutor(system)

	for _, refused := range []struct{ statement, message string }{
		{"CREATE (:_ProcedurePrivilege {role: 'editor', procedure: '*', boosted: true})", "not supported on the system database"},
		{"MERGE (g:_ProcedurePrivilege {role: 'editor'}) SET g.procedure = '*', g.boosted = true", "not supported on the system database"},
		{`CALL apoc.cypher.run("CREATE (n:_ProcedurePrivilege {role: 'editor'}) RETURN n", {})`, "can only be executed in a user database"},
	} {
		_, err := exec.Execute(ctx, refused.statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
		require.ErrorContains(t, err, refused.message, refused.statement)
	}
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:_ProcedurePrivilege {role: 'editor', procedure: '*', boosted: true})", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	nodes, err := system.GetNodesByLabel("_ProcedurePrivilege")
	require.NoError(t, err)
	require.Empty(t, nodes)
	_, err = exec.Execute(ctx, "GRANT EXECUTE BOOSTED PROCEDURE db.ping ON DBMS TO viewer", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:_ProcedurePrivilege) RETURN n.procedure AS procedure", nil)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")

	user := NewStorageExecutor(storage.NewNamespacedEngine(inner, "nornic"))
	_, err = user.Execute(ctx, "CREATE (:Person {name: 'a'})", nil)
	require.NoError(t, err)

	// Selected with USE, the system database refuses MATCH and a bare
	// RETURN as well, as Neo4j 5.26.30 does, and still runs a call of a
	// system procedure.
	for _, statement := range []string{
		"USE system CREATE (:_ProcedurePrivilege {role: 'editor', procedure: '*', boosted: true})",
		"USE system MATCH (n:_ProcedurePrivilege) RETURN n",
		"USE system RETURN 1 AS one",
	} {
		_, err = user.Execute(ctx, statement, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SemanticError")
	}
	result, err := user.Execute(ctx, "USE system CALL db.ping() YIELD success RETURN success", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{true}}, result.Rows)
}
