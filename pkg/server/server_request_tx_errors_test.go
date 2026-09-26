package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRequestTransactionErrors: a request of several statements to a
// database that doesn't exist fails with DatabaseNotFound before any
// statement runs, and a statement's own Neo4j error reads as code: message.
func TestRequestTransactionErrors(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	body := map[string]interface{}{"statements": []map[string]interface{}{
		{"statement": "CREATE (:NoSuchDb)"},
		{"statement": "RETURN 1 AS x"},
	}}
	rec := makeRequest(t, server, http.MethodPost, "/db/nosuchdb683/tx/commit", body, token)
	var response TransactionResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response), rec.Body.String())
	require.Len(t, response.Errors, 1)
	require.Equal(t, "Neo.ClientError.Database.DatabaseNotFound", response.Errors[0].Code)
	require.Empty(t, response.Results)

	own := &requestStatementError{QueryError{Code: "Neo.ClientError.Statement.AccessMode", Message: "no"}}
	require.Equal(t, "Neo.ClientError.Statement.AccessMode: no", own.Error())
}
