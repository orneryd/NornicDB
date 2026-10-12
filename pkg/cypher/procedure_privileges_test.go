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
	require.Equal(t, procedurePrivilegeCommand{procedures: []string{"db.cdc.query"}, roles: []string{"cdc_reader"}}, command)

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
	system := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = system.Close() })
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
	require.True(t, auth.NewProcedurePrivilegeStore(system).AllowsBoosted([]string{"cdc_reader"}, "db.cdc.query"))

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
