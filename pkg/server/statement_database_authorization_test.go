package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// TestHTTPEveryDatabaseAStatementSelectsIsAuthorized: a statement may reach
// only the databases its principal may use, whichever way it selects one
// (the request's database, USE, :USE, a USE in a CALL subquery, a composite
// constituent, whose access is its composite's and its target database's).
func TestHTTPEveryDatabaseAStatementSelectsIsAuthorized(t *testing.T) {
	server, authenticator := setupTestServer(t)
	ctx := context.Background()
	for _, name := range []string{"da", "db"} {
		require.NoError(t, server.dbManager.CreateDatabase(name))
	}
	require.NoError(t, server.dbManager.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "da", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "db", Type: "local", AccessMode: "read_write"},
	}))
	_, err := authenticator.CreateUser("limited", "password123", []auth.Role{auth.RoleEditor})
	require.NoError(t, err)
	require.NoError(t, server.allowlistStore.SaveRoleDatabases(ctx, "editor", []string{"da", "cmp"}))
	for _, db := range []string{"da", "cmp"} {
		require.NoError(t, server.privilegesStore.SavePrivilege(ctx, "editor", db, true, true))
	}
	adminToken := getAuthToken(t, authenticator, "admin")
	token := getAuthToken(t, authenticator, "limited")

	run := func(bearer, db, statement string) TransactionResponse {
		t.Helper()
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+db+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+bearer)
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response), recorder.Body.String())
		return response
	}
	require.Empty(t, run(adminToken, "db", "CREATE (:S {v: 'in-db'})").Errors)
	require.Empty(t, run(adminToken, "da", "CREATE (:S {v: 'in-da'})").Errors)

	for _, request := range []struct{ db, statement string }{
		{"db", "MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE db MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE db CREATE (:S {v: 'denied'})"},
		{"da", ":USE db\nMATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "CALL { USE db MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"cmp", "USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "CALL { USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"da", "USE cmp.b MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp.b", "MATCH (n:S) RETURN collect(n.v) AS vs"},
	} {
		response := run(token, request.db, request.statement)
		require.NotEmpty(t, response.Errors, "%s on %s", request.statement, request.db)
		require.Equal(t, "Neo.ClientError.Security.Forbidden", response.Errors[0].Code, "%s on %s", request.statement, request.db)
	}

	for _, request := range []struct{ db, statement string }{
		{"da", "MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"da", "USE da MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp", "CALL { USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs } RETURN vs"},
		{"da", "USE cmp.a MATCH (n:S) RETURN collect(n.v) AS vs"},
		{"cmp.a", "MATCH (n:S) RETURN collect(n.v) AS vs"},
	} {
		response := run(token, request.db, request.statement)
		require.Empty(t, response.Errors, "%s on %s", request.statement, request.db)
		require.Equal(t, []any{"in-da"}, response.Results[0].Data[0].Row[0], "%s on %s", request.statement, request.db)
	}

	// Nothing was written to db.
	response := run(adminToken, "db", "MATCH (n:S) RETURN collect(n.v) AS vs")
	require.Empty(t, response.Errors)
	require.Equal(t, []any{"in-db"}, response.Results[0].Data[0].Row[0])
}
