package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// firstTxError runs statement on database's /tx/commit endpoint and returns its
// first error's code and message ("" when it succeeded).
func firstTxError(t *testing.T, server *Server, database, token, statement string) (code, message string) {
	t.Helper()
	body, _ := systemProcedureResponse(t, server, token, database, statement, false)
	if len(body.Errors) == 0 {
		return "", ""
	}
	return body.Errors[0].Code, body.Errors[0].Message
}

// The system database runs what Neo4j 5.26.30's runs: administration and
// SHOW commands and calls of its procedures. Any other statement (RETURN 1,
// RETURN COUNT { … }, EXISTS { … }, UNWIND, CALL { }), and a procedure call
// whose RETURN reads the graph, fails with Neo.ClientError.Statement.SemanticError;
// a SHOW command's tail that runs another clause or reads the graph fails
// with Neo4j's SyntaxError for it. The same error comes for an
// administrator and a reader, on every route that reaches the system
// database: directly, under EXPLAIN, after USE system, in a statement
// apoc.cypher.run runs, in CALL { USE system … }, and through the embedded
// executor.
func TestSystemDatabaseRunsOnlyCommandsAndProcedureCalls(t *testing.T) {
	ctx := context.Background()
	server, users := setupTestServer(t)
	const (
		semantic = "Neo.ClientError.Statement.SemanticError"
		syntax   = "Neo.ClientError.Statement.SyntaxError"
	)
	notRecognised := "Not a recognised system command or procedure"
	graphAccess := "Graph access is not supported on the system database"
	for _, c := range []struct{ statement, code, message string }{
		{"RETURN 1 AS one", semantic, notRecognised},
		{"RETURN COUNT { (u:_User) } AS c", semantic, notRecognised},
		{"RETURN EXISTS { (u:_User) WHERE u.username = 'admin' } AS e", semantic, notRecognised},
		{"RETURN COLLECT { MATCH (u:_User) RETURN u.username } AS names", semantic, graphAccess},
		{"UNWIND [1] AS x RETURN x", semantic, notRecognised},
		{"WITH 1 AS x RETURN x", semantic, notRecognised},
		{"CALL { RETURN 1 AS x } RETURN x", semantic, notRecognised},
		{"RETURN 1 AS one UNION RETURN 2 AS one", semantic, notRecognised},
		{"CALL db.ping() YIELD success RETURN COUNT { (n) } AS c", semantic, graphAccess},
		{"CALL dbms.components() YIELD name RETURN name, toString(COUNT { (n) }) AS c", semantic, graphAccess},
		{"CALL dbms.components() YIELD name WHERE (n)-->() RETURN name", semantic, graphAccess},
		{"SHOW DATABASES YIELD name RETURN name, COUNT { (u:_User) } AS c", syntax, "The COUNT expression is not valid on SHOW commands."},
		{"SHOW PROCEDURES YIELD name RETURN name, toString(COUNT { (u:_User) }) AS c", syntax, "The COUNT expression is not valid on SHOW commands."},
		{"SHOW DATABASES YIELD name WHERE EXISTS { (u:_User) WHERE u.username = 'admin' } RETURN name", syntax, "The EXISTS expression is not valid on SHOW commands."},
		{"SHOW DATABASES YIELD name RETURN COLLECT { MATCH (u:_User) RETURN u.password_hash } AS h", syntax, "The COLLECT expression is not valid on SHOW commands."},
		{"SHOW SETTINGS YIELD name RETURN name, [(u:_User)-->(r) | u.password_hash] AS h", syntax, "Pattern comprehensions are not valid on SHOW commands."},
		{"SHOW FUNCTIONS YIELD name WHERE exists((u:_User)-->()) RETURN name", syntax, "Pattern expressions are not valid on SHOW commands."},
		{"SHOW CURRENT USER YIELD user RETURN user, COUNT { (n) } AS c", syntax, "The COUNT expression is not valid on SHOW commands."},
		{"SHOW FUNCTIONS YIELD name RETURN name, EXISTS { (n:_ProcedurePrivilege) } AS e", syntax, "The EXISTS expression is not valid on SHOW commands."},
		{"SHOW PROCEDURES YIELD name WITH name LIMIT 1 RETURN COUNT { (u:_User) } AS c", syntax, "Invalid input 'WITH'"},
		{"SHOW DATABASES YIELD name UNWIND [1] AS x RETURN name, x", syntax, "Invalid input 'UNWIND'"},
		{"SHOW DATABASES YIELD name CALL { RETURN 1 AS one } RETURN name, one", syntax, "Invalid input 'CALL'"},
		{"SHOW DATABASES YIELD name MATCH (u:_User) RETURN u.password_hash AS h", syntax, "Invalid input 'MATCH'"},
	} {
		isShow := strings.HasPrefix(c.statement, "SHOW")
		// SHOW DATABASES and SHOW CURRENT USER are administration commands,
		// which a USE refuses before anything runs, as in Neo4j.
		useMessage := c.message
		if strings.HasPrefix(c.statement, "SHOW DATABASES") || strings.HasPrefix(c.statement, "SHOW CURRENT USER") {
			useMessage = "The `USE` clause is not required for Administration Commands"
		}
		routes := []struct{ database, statement, message string }{
			{"system", c.statement, c.message},
			{"system", "EXPLAIN " + c.statement, c.message},
			{"nornic", "USE system " + c.statement, useMessage},
			{"nornic", "CALL apoc.cypher.run('USE system " + strings.ReplaceAll(c.statement, "'", "\\'") + "', {}) YIELD value RETURN value", useMessage},
		}
		for _, user := range []string{"admin", "reader"} {
			token := getAuthToken(t, users, user)
			for _, route := range routes {
				code, message := firstTxError(t, server, route.database, token, route.statement)
				require.Equal(t, c.code, code, "%s: %s on %s", user, route.statement, route.database)
				require.Contains(t, message, route.message, "%s: %s on %s", user, route.statement, route.database)
			}
			// CALL { USE system SHOW … } is a SyntaxError in Neo4j whatever
			// the tail; the other statements meet the policy.
			wrapped := "CALL { USE system " + c.statement + " } RETURN 1 AS done"
			code, message := firstTxError(t, server, "nornic", token, wrapped)
			if isShow {
				require.Equal(t, syntax, code, "%s: %s", user, wrapped)
			} else {
				require.Equal(t, c.code, code, "%s: %s", user, wrapped)
				require.Contains(t, message, c.message, "%s: %s", user, wrapped)
			}
		}
		// The embedded executor: the same policy.
		for name, run := range map[string]func() error{
			"GetCypherExecutor": func() error {
				_, err := server.db.GetCypherExecutor().Execute(ctx, "USE system "+c.statement, nil)
				return err
			},
			"ExecuteCypher": func() error {
				_, err := server.db.ExecuteCypher(ctx, "USE system "+c.statement, nil)
				return err
			},
			"Cypher": func() error {
				_, err := server.db.Cypher(ctx, "USE system "+c.statement, nil)
				return err
			},
		} {
			err := run()
			require.Error(t, err, "%s: %s", name, c.statement)
			require.Contains(t, err.Error(), useMessage, "%s: %s", name, c.statement)
		}
	}

	admin := getAuthToken(t, users, "admin")
	reader := getAuthToken(t, users, "reader")
	for _, statement := range []string{
		"SHOW USERS",
		"SHOW DATABASES YIELD name RETURN name",
		"SHOW DATABASES YIELD name ORDER BY name SKIP 0 LIMIT 5 RETURN name",
		"SHOW DATABASES YIELD name WHERE name IN [x IN ['nornic'] WHERE x <> ''] RETURN name, [x IN [1, 2] | x] AS xs",
		"CALL db.ping() YIELD success RETURN success, 1 AS x",
		"CALL dbms.components()",
		"EXPLAIN SHOW USERS",
	} {
		require.Empty(t, txErrorCode(t, server, "system", admin, statement), statement)
	}
	for _, statement := range []string{"SHOW DATABASES YIELD name RETURN name", "SHOW CURRENT USER YIELD user RETURN user", "CALL db.ping()"} {
		require.Empty(t, txErrorCode(t, server, "system", reader, statement), statement)
	}
	// On a user database a SHOW tail may read the graph, as in Neo4j.
	require.Empty(t, txErrorCode(t, server, "nornic", admin, "SHOW PROCEDURES YIELD name RETURN name, COUNT { (n) } AS c LIMIT 1"))
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
