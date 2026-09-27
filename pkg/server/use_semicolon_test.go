package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHTTPUseSemicolon: over HTTP, USE <db>; followed by another statement
// is Neo4j's SyntaxError "Expected exactly one statement per query but got:
// 2", and USE <db>; alone is a USE with no clause.
func TestHTTPUseSemicolon(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	db := server.dbManager.DefaultDatabaseName()
	for statement, message := range map[string]string{
		"USE " + db + "; MATCH (n) RETURN count(n) AS c": "Expected exactly one statement per query but got: 2",
		"USE " + db + ";": "Query cannot conclude with USE GRAPH (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
	} {
		recorder := makeRequest(t, server, http.MethodPost, "/db/"+db+"/tx/commit", map[string]any{
			"statements": []map[string]any{{"statement": statement}},
		}, "Bearer "+token)
		var response TransactionResponse
		require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response), recorder.Body.String())
		require.Len(t, response.Errors, 1, statement)
		require.Equal(t, "Neo.ClientError.Statement.SyntaxError", response.Errors[0].Code, statement)
		require.Equal(t, message, response.Errors[0].Message, statement)
	}
}
