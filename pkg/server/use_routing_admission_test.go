package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// A statement routed by USE is admitted by the database it runs on, not by
// the one it was sent to (review 4300778902): the source database's query
// time limit does not stop a statement that runs entirely on another one.
func TestUseRoutedStatementIsAdmittedByItsTargetDatabase(t *testing.T) {
	server, authenticator := setupTestServer(t)
	require.NoError(t, server.dbManager.CreateDatabase("routed_target"))
	limits := multidb.DefaultLimits()
	limits.Query.MaxQueryTime = time.Nanosecond
	require.NoError(t, server.dbManager.SetDatabaseLimits("nornic", limits))
	host := httptest.NewServer(server.buildRouter())
	defer host.Close()
	token := "Bearer " + getAuthToken(t, authenticator, "admin")
	request := func(statement string) TransactionResponse {
		t.Helper()
		payload, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: statement}}})
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, host.URL+"/db/nornic/tx/commit", bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Authorization", token)
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		var body TransactionResponse
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		return body
	}
	routed := request("USE routed_target RETURN 1 AS x")
	require.Empty(t, routed.Errors, "the routed statement runs under routed_target's limits only")
	require.Len(t, routed.Results, 1)
	// The source database's own statements keep its limit.
	local := request("UNWIND range(1, 10000000) AS x RETURN count(x) AS c")
	require.NotEmpty(t, local.Errors, "nornic's own statement is admitted under nornic's 1ns limit")
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", local.Errors[0].Code)
}
