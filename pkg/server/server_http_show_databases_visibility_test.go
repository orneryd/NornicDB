package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestHTTPShowDatabasesHidesDatabasesInEveryForm pins that no comment, spacing,
// final ";", tail or name shows a database the principal may not access.
func TestHTTPShowDatabasesHidesDatabasesInEveryForm(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	dbName := server.dbManager.DefaultDatabaseName()
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(context.Background(), "viewer", []string{dbName}))
	require.NoError(t, server.privilegesStore.SavePrivilege(context.Background(), "viewer", dbName, true, false))
	token := getAuthToken(t, authenticator, "reader")
	admin := getAuthToken(t, authenticator, "admin")

	names := func(bearer, statement string) []any {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+dbName+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+bearer)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		require.Empty(t, response.Errors, "%q", statement)
		require.Len(t, response.Results, 1, "%q", statement)
		var rows []any
		for _, row := range response.Results[0].Data {
			rows = append(rows, row.Row[0])
		}
		return rows
	}
	require.Contains(t, names(admin, "SHOW DATABASES"), "private")
	require.Contains(t, names(admin, "SHOW DATABASES YIELD name"), "private")
	require.Equal(t, []any{"private"}, names(admin, "SHOW DATABASE private"))
	require.Contains(t, names(admin, "SHOW DATABASE"), "private")

	for _, statement := range []string{
		"SHOW DATABASES",
		"show databases",
		"SHOW DATABASES;",
		"SHOW DATABASES /* review */",
		"SHOW DATABASES // review",
		"SHOW DATABASES // review\n",
		"SHOW DATABASES /* a */ ; /* b */",
		"/* review */ SHOW DATABASES",
		"// review\nSHOW DATABASES",
		"SHOW /* review */ DATABASES",
		"  SHOW\t\nDATABASES  ",
		"SHOW\u00a0DATABASES\u2003",
		"\ufeffSHOW DATABASES",
		"SHOW DATABASES YIELD name",
		"SHOW DATABASES YIELD name RETURN name",
		"SHOW DATABASES YIELD *",
		"SHOW DATABASES /* review */ YIELD name",
		"SHOW DATABASE",
		"SHOW DATABASE /* review */",
		"SHOW DATABASE YIELD name",
	} {
		// Neo4j: the system database "is always visible".
		require.Equal(t, []any{dbName, "system"}, names(token, statement), "%q", statement)
	}
	for _, statement := range []string{
		"SHOW DATABASE " + dbName,
		"SHOW DATABASE " + dbName + " YIELD name",
		"SHOW DATABASES " + dbName,
	} {
		require.Equal(t, []any{dbName}, names(token, statement), "%q", statement)
	}
	require.Equal(t, []any{"system"}, names(token, "SHOW DATABASE system"))
	for _, statement := range []string{
		"SHOW DATABASES WHERE name = 'private'",
		"SHOW DATABASES YIELD name WHERE name = 'private'",
		"SHOW DATABASE private",
		"SHOW DATABASE private YIELD name",
		"SHOW DATABASE private WHERE name = 'private'",
		"SHOW DATABASE /* review */ private",
		"SHOW DATABASE absent",
		"SHOW DATABASES private",
		"SHOW DATABASES absent",
	} {
		require.Empty(t, names(token, statement), "%q", statement)
	}

	failure := func(statement string) QueryError {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+dbName+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
		require.Len(t, response.Errors, 1, "%q", statement)
		return response.Errors[0]
	}
	require.Equal(t, failure("SHOW CONSTITUENTS FOR COMPOSITE DATABASE absent"), failure("SHOW CONSTITUENTS FOR COMPOSITE DATABASE private"))

	// A hidden database answers as an absent one on every route that names it.
	// State that one executor holds must not answer for a name the principal
	// may not access.
	names(token, ":param marker => 7")
	answer := func(method, path, statement string) (int, string) {
		var body any
		if method == http.MethodPost {
			body = map[string]any{"statements": []map[string]any{{"statement": statement}}}
		}
		recorder := makeRequest(t, server, method, path, body, "Bearer "+token)
		// The whole observable answer: status, headers and body. Only the
		// bookmark differs between any two answers, and the name each one asked.
		shape := map[string]any{}
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&shape))
		delete(shape, "lastBookmarks")
		headers := recorder.Header().Clone()
		headers.Del("Date")
		headers.Del("Content-Length")
		encoded, err := json.Marshal(map[string]any{"body": shape, "headers": headers})
		require.NoError(t, err)
		text := strings.ReplaceAll(string(encoded), "private", "NAME")
		return recorder.Code, strings.ReplaceAll(text, "absent", "NAME")
	}
	for _, statement := range []string{"SHOW LIMITS FOR DATABASE %s", "SHOW DATABASE %s", "SHOW CONSTITUENTS FOR COMPOSITE DATABASE %s", "SHOW ALIASES FOR DATABASE %s", ":use %s\nRETURN 1"} {
		hiddenCode, hiddenAnswer := answer(http.MethodPost, "/db/"+dbName+"/tx/commit", fmt.Sprintf(statement, "private"))
		absentCode, absentAnswer := answer(http.MethodPost, "/db/"+dbName+"/tx/commit", fmt.Sprintf(statement, "absent"))
		require.Equal(t, absentCode, hiddenCode, statement)
		require.Equal(t, absentAnswer, hiddenAnswer, statement)
	}
	for _, statement := range []string{
		"RETURN 1", "CREATE (n)", "CREATE INDEX i FOR (n:L) ON (n.p)", "SHOW DATABASES", "CREATE DATABASE d", "", "// only a comment",
		// Syntactically invalid, before any permission is read.
		"RETRUN 1", "MATCH (n RETURN n", "RETURN 1 /* unclosed", "EXPLAIN PROFILE RETURN 1", "CYPHER 99 RETURN 1", "RETURN 1; RETURN 2",
		// Fail while running, or for a value only execution reads.
		"RETURN 1/0", "UNWIND [1] AS x RETURN x/0", "RETURN $missing", "CALL no.such.proc()", "RETURN no.such(1)", "CREATE (:A {p: 1/0})", "MATCH (n) RETURN n.p/0",
		// Shell commands answer before a statement is authorized, from state of
		// the executor they reach.
		":params", ":param marker => 7", ":params clear", ":use " + dbName + "\nRETURN 1",
	} {
		for _, route := range []string{"/tx/commit", "/tx"} {
			hiddenCode, hiddenErrors := answer(http.MethodPost, "/db/private"+route, statement)
			absentCode, absentErrors := answer(http.MethodPost, "/db/absent"+route, statement)
			require.Equal(t, absentCode, hiddenCode, "%s %q", route, statement)
			require.Equal(t, absentErrors, hiddenErrors, "%s %q", route, statement)
		}
		hiddenCode, hiddenErrors := answer(http.MethodPost, "/db/"+dbName+"/tx/commit", "USE private "+statement)
		absentCode, absentErrors := answer(http.MethodPost, "/db/"+dbName+"/tx/commit", "USE absent "+statement)
		require.Equal(t, absentCode, hiddenCode, "USE %q", statement)
		require.Equal(t, absentErrors, hiddenErrors, "USE %q", statement)
	}
	// MCP, through the real route and its selection scope: pinned by URL, and named
	// in the tool's arguments.
	require.NotNil(t, server.mcpServer)
	mcpAnswer := func(path string, body map[string]any) (int, string) {
		recorder := makeRequest(t, server, http.MethodPost, path, body, "Bearer "+token)
		headers := recorder.Header().Clone()
		headers.Del("Date")
		headers.Del("Content-Length")
		encoded, err := json.Marshal(map[string]any{"body": recorder.Body.String(), "headers": headers})
		require.NoError(t, err)
		text := strings.ReplaceAll(string(encoded), "private", "NAME")
		return recorder.Code, strings.ReplaceAll(text, "absent", "NAME")
	}
	for _, tool := range []string{"discover", "recall", "store", "tasks"} {
		arguments := map[string]any{"query": "x", "id": "x", "content": "x"}
		hiddenCode, hiddenAnswer := mcpAnswer("/mcp/private/tools/call", map[string]any{"name": tool, "arguments": arguments})
		absentCode, absentAnswer := mcpAnswer("/mcp/absent/tools/call", map[string]any{"name": tool, "arguments": arguments})
		require.Equal(t, absentCode, hiddenCode, tool)
		require.Equal(t, absentAnswer, hiddenAnswer, tool)
		named := func(database string) map[string]any {
			return map[string]any{"name": tool, "arguments": map[string]any{"query": "x", "id": "x", "content": "x", "database": database}}
		}
		hiddenCode, hiddenAnswer = mcpAnswer("/mcp/tools/call", named("private"))
		absentCode, absentAnswer = mcpAnswer("/mcp/tools/call", named("absent"))
		require.Equal(t, absentCode, hiddenCode, tool)
		require.Equal(t, absentAnswer, hiddenAnswer, tool)
	}
	for _, route := range []string{"", "/cluster"} {
		hiddenCode, hiddenErrors := answer(http.MethodGet, "/db/private"+route, "")
		absentCode, absentErrors := answer(http.MethodGet, "/db/absent"+route, "")
		require.Equal(t, absentCode, hiddenCode, route)
		require.Equal(t, absentErrors, hiddenErrors, route)
	}
}
