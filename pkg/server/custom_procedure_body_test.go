package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// A custom procedure's body is a statement the procedure runs, like one
// apoc.cypher.run runs: an administration command in it is refused with
// Neo.ClientError.Transaction.ForbiddenDueToTransactionType whoever calls
// the procedure, so calling a procedure grants nothing.
func TestCustomProcedureBodyRunsNoAdministrationCommand(t *testing.T) {
	t.Cleanup(cypher.ClearUserProcedures)
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")

	// A READ procedure cannot hold an administration command at all.
	require.NotEmpty(t, txErrorCode(t, server, "nornic", admin,
		"CALL apoc.custom.installProcedure('r3readgrant() :: VOID', 'GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO editor', 'nornic', 'read')"))
	require.NotEmpty(t, txErrorCode(t, server, "nornic", admin,
		"CREATE PROCEDURE custom.r3readrevoke() MODE READ AS REVOKE EXECUTE BOOSTED PROCEDURE * ON DBMS FROM editor"))
	require.Empty(t, txErrorCode(t, server, "nornic", admin,
		"CALL apoc.custom.installProcedure('r3grant() :: VOID', 'GRANT EXECUTE BOOSTED PROCEDURE * ON DBMS TO editor', 'nornic', 'write')"))
	require.Empty(t, txErrorCode(t, server, "nornic", admin,
		"CREATE PROCEDURE custom.r3revoke() MODE WRITE AS REVOKE EXECUTE BOOSTED PROCEDURE * ON DBMS FROM editor"))
	for _, call := range []string{"CALL custom.r3grant()", "CALL custom.r3revoke()"} {
		require.Equal(t, "Neo.ClientError.Transaction.ForbiddenDueToTransactionType", txErrorCode(t, server, "nornic", admin, call), call)
		// A reader may not call a WRITE procedure at all.
		require.NotEmpty(t, txErrorCode(t, server, "nornic", reader, call), call)
	}
	grants, err := server.procedurePrivileges.List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants)

	// A data body still runs.
	require.Empty(t, txErrorCode(t, server, "nornic", admin,
		"CALL apoc.custom.installProcedure('r3count() :: (c :: INTEGER)', 'MATCH (n) RETURN count(n) AS c', 'nornic', 'read')"))
	require.Empty(t, txErrorCode(t, server, "nornic", reader, "CALL custom.r3count() YIELD c RETURN c"))
}
