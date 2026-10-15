package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The system database runs what Neo4j 5.26.30's runs: administration and
// SHOW commands and calls of its procedures. Any other statement (RETURN 1,
// RETURN COUNT { … }, EXISTS { … }, UNWIND, CALL { }), and a procedure call
// whose RETURN reads the graph, fails with Neo4j's
// Neo.ClientError.Statement.SemanticError, on every route that reaches the
// system database.
func TestSystemDatabaseRunsOnlyCommandsAndProcedureCalls(t *testing.T) {
	server, users := setupTestServer(t)
	for _, token := range []string{getAuthToken(t, users, "admin"), getAuthToken(t, users, "reader")} {
		for _, statement := range []string{
			"RETURN 1 AS one",
			"RETURN COUNT { (u:_User) } AS c",
			"RETURN EXISTS { (u:_User) WHERE u.username = 'admin' } AS e",
			"RETURN COLLECT { MATCH (u:_User) RETURN u.username } AS names",
			"UNWIND [1] AS x RETURN x",
			"WITH 1 AS x RETURN x",
			"CALL { RETURN 1 AS x } RETURN x",
			"RETURN 1 AS one UNION RETURN 2 AS one",
			"CALL db.ping() YIELD success RETURN COUNT { (n) } AS c",
		} {
			for _, route := range []struct{ database, statement string }{
				{"system", statement},
				{"system", "EXPLAIN " + statement},
				{"nornic", "USE system " + statement},
				{"nornic", "CALL { USE system " + statement + " } RETURN 1 AS done"},
			} {
				code := txErrorCode(t, server, route.database, token, route.statement)
				if code == "Neo.ClientError.Security.Forbidden" {
					continue // a reader refused before the policy: nothing ran either
				}
				require.Equal(t, "Neo.ClientError.Statement.SemanticError", code, "%s on %s", route.statement, route.database)
			}
		}
	}

	admin := getAuthToken(t, users, "admin")
	response := makeRequest(t, server, "POST", "/db/system/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "RETURN 1 AS one"}}}, "Bearer "+admin)
	require.Contains(t, response.Body.String(), "Not a recognised system command or procedure")
	for _, statement := range []string{
		"SHOW USERS",
		"SHOW DATABASES YIELD name RETURN name",
		"CALL db.ping() YIELD success RETURN success, 1 AS x",
		"CALL dbms.components()",
		"EXPLAIN SHOW USERS",
	} {
		require.Empty(t, txErrorCode(t, server, "system", admin, statement), statement)
	}
}

// EXPLAIN and PROFILE of an administration command behave the same on both
// parsers: EXPLAIN runs nothing, PROFILE runs the command, and a reader's
// PROFILE GRANT is refused.
func TestExplainAndProfileOfAdministrationCommands(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	grant := "GRANT EXECUTE BOOSTED PROCEDURE db.cdc.* ON DBMS TO viewer"

	require.Empty(t, txErrorCode(t, server, "system", admin, "EXPLAIN "+grant))
	grants, err := server.procedurePrivileges.List(ctx)
	require.NoError(t, err)
	require.Empty(t, grants, "EXPLAIN runs nothing")
	require.Equal(t, "Neo.ClientError.Security.Forbidden", txErrorCode(t, server, "system", reader, "PROFILE "+grant))
	require.Empty(t, txErrorCode(t, server, "system", admin, "PROFILE "+grant))
	grants, err = server.procedurePrivileges.List(ctx)
	require.NoError(t, err)
	require.Len(t, grants, 1, "PROFILE runs the command")

	response := makeRequest(t, server, "POST", "/db/system/tx/commit", map[string]any{"statements": []map[string]any{{"statement": "EXPLAIN " + grant}}}, "Bearer "+reader)
	var body TransactionResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	for _, failure := range body.Errors {
		require.False(t, strings.Contains(failure.Code, "SyntaxError"), "EXPLAIN GRANT parses on both parsers: %v", failure)
	}
}
