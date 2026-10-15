package server

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// statementRows runs statement on database's /tx/commit endpoint and returns
// its rows and its first error code ("" when it succeeded).
func statementRows(t *testing.T, server *Server, database, token, statement string) ([][]any, string) {
	t.Helper()
	body, _ := systemProcedureResponse(t, server, token, database, statement, false)
	if len(body.Errors) > 0 {
		t.Logf("%s on %s: %s %s", statement, database, body.Errors[0].Code, body.Errors[0].Message)
		return nil, body.Errors[0].Code
	}
	var rows [][]any
	for _, result := range body.Results {
		for _, row := range result.Data {
			rows = append(rows, row.Row)
		}
	}
	return rows, ""
}

// A custom procedure can only be installed, replaced or dropped by a
// principal holding the admin entitlement: an editor who may use one
// database can't change what an administrator's call of the procedure runs
// on another database.
func TestCustomProcedureIsReplacedOnlyByAdministrators(t *testing.T) {
	t.Cleanup(cypher.ClearUserProcedures)
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	require.Empty(t, txErrorCode(t, server, "system", admin, "CREATE DATABASE vault"))
	require.Empty(t, txErrorCode(t, server, "vault", admin, "CREATE (:Secret {v: 'top'}), (:Secret {v: 'secret'})"))
	require.Empty(t, txErrorCode(t, server, "vault", admin,
		"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'MATCH (s:Secret) RETURN count(s) AS c', 'vault', 'read')"))
	report := func() [][]any {
		t.Helper()
		rows, code := statementRows(t, server, "vault", admin, "CALL custom.report() YIELD c RETURN c")
		require.Empty(t, code)
		return rows
	}
	require.Equal(t, [][]any{{float64(2)}}, report())

	// The editor may use nornic only.
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"nornic"}))
	_, err := users.CreateUser("ed", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	editor := getAuthToken(t, users, "ed")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "vault", editor, "MATCH (s:Secret) RETURN s.v AS v"))

	for _, statement := range []string{
		"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'MATCH (s:Secret) WITH collect(s.v) AS vs CALL apoc.cypher.run(\"USE nornic CREATE (:Leak {vs: $vs})\", {vs: vs}) YIELD value RETURN size(vs) AS c', 'nornic', 'write')",
		"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'MATCH (s:Secret) DETACH DELETE s RETURN 0 AS c', 'nornic', 'write')",
		"CALL apoc.custom.declareProcedure('report() :: (c :: INTEGER)', 'MATCH (s:Secret) DETACH DELETE s RETURN 0 AS c', 'write')",
		"CALL apoc.custom.dropProcedure('report')",
		"CREATE OR REPLACE PROCEDURE custom.report() MODE WRITE AS MATCH (s:Secret) DETACH DELETE s RETURN 0 AS c",
		"CALL apoc.cypher.run(\"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'RETURN 0 AS c', 'nornic', 'read')\", {}) YIELD value RETURN value",
	} {
		require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "nornic", editor, statement), statement)
	}

	// The administrator's call still runs the administrator's procedure, and
	// the data is as it was.
	require.Equal(t, [][]any{{float64(2)}}, report())
	rows, code := statementRows(t, server, "vault", admin, "MATCH (s:Secret) RETURN count(s) AS remaining")
	require.Empty(t, code)
	require.Equal(t, [][]any{{float64(2)}}, rows)
	rows, code = statementRows(t, server, "nornic", admin, "MATCH (l:Leak) RETURN count(l) AS leaked")
	require.Empty(t, code)
	require.Equal(t, [][]any{{float64(0)}}, rows)
	rows, code = statementRows(t, server, "vault", admin, "SHOW PROCEDURES YIELD name, mode, admin WHERE name STARTS WITH 'apoc.custom.' OR name = 'custom.report' RETURN name, mode, admin ORDER BY name")
	require.Empty(t, code)
	require.Equal(t, [][]any{
		{"apoc.custom.declareProcedure", "WRITE", true},
		{"apoc.custom.dropProcedure", "WRITE", true},
		{"apoc.custom.installProcedure", "WRITE", true},
		{"apoc.custom.list", "READ", false},
		{"apoc.custom.show", "READ", false},
		{"custom.report", "READ", false},
	}, rows)

	// An administrator replaces it, and the next call runs the new body.
	require.Empty(t, txErrorCode(t, server, "nornic", admin,
		"CALL apoc.custom.installProcedure('report() :: (c :: INTEGER)', 'RETURN 7 AS c', 'nornic', 'read')"))
	require.Equal(t, [][]any{{float64(7)}}, report())
}
