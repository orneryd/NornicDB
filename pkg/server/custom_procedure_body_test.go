package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

// A custom procedure's body is a statement the procedure runs, like one
// apoc.cypher.run runs: an administration command in it is refused with
// Neo.ClientError.Transaction.ForbiddenDueToTransactionType whoever calls
// the procedure, so a procedure an editor installed grants nothing when an
// administrator calls it.
func TestCustomProcedureBodyRunsNoAdministrationCommand(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	_, err := users.CreateUser("ed", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	editor := getAuthToken(t, users, "ed")
	admin := getAuthToken(t, users, "admin")

	require.Empty(t, txErrorCode(t, server, "nornic", editor,
		"CALL apoc.custom.installProcedure('r3grant() :: VOID', 'GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO editor', 'nornic', 'read')"))
	require.Empty(t, txErrorCode(t, server, "nornic", admin,
		"CREATE PROCEDURE custom.r3revoke() MODE READ AS REVOKE EXECUTE BOOSTED PROCEDURE * ON DBMS FROM editor"))
	for _, call := range []string{"CALL custom.r3grant()", "CALL custom.r3revoke()"} {
		require.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", txErrorCode(t, server, "nornic", admin, call), call)
	}
	grants, err := server.procedurePrivileges.List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)

	// A data body still runs.
	require.Empty(t, txErrorCode(t, server, "nornic", editor,
		"CALL apoc.custom.installProcedure('r3count() :: (c :: INTEGER)', 'MATCH (n) RETURN count(n) AS c', 'nornic', 'read')"))
	require.Empty(t, txErrorCode(t, server, "nornic", admin, "CALL custom.r3count() YIELD c RETURN c"))
}
