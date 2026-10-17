package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

// Every HTTP transaction endpoint reports a statement error through
// statementError: a statement the executor marked as stopped by its own
// deadline (the database's query time limit) is Neo4j's transaction timeout,
// not SyntaxError (#1339).
func TestHTTPDeadlineStatus(t *testing.T) {
	for _, err := range []error{
		nornicerrors.MarkStatementTimeout(context.DeadlineExceeded, true),
		fmt.Errorf("route: %w", nornicerrors.MarkStatementTimeout(fmt.Errorf("pipeline stage: %w", context.DeadlineExceeded), false)),
	} {
		failure := statementError(err)
		require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", failure.Code)
		require.Equal(t, nornicerrors.TransactionTimedOutMessage, failure.Message)
	}
	// A dependency's own deadline the executor did not mark is not the
	// statement's timeout (review 4300775974).
	failure := statementError(fmt.Errorf("embedder: %w", context.DeadlineExceeded))
	require.NotEqual(t, "Neo.ClientError.Transaction.TransactionTimedOut", failure.Code)
}

// End to end: a statement the database's query time limit stops is reported
// over HTTP as Neo4j's transaction timeout, through the executor's marking.
func TestHTTPDatabaseLimitStatus(t *testing.T) {
	server, authenticator := setupTestServer(t)
	limits := multidb.DefaultLimits()
	limits.Query.MaxQueryTime = 100 * time.Millisecond
	require.NoError(t, server.dbManager.SetDatabaseLimits("nornic", limits))
	host := httptest.NewServer(server.buildRouter())
	defer host.Close()
	payload, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: "UNWIND range(1, 100000000) AS x WITH x WHERE x < 0 RETURN count(x) AS c"}}})
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, host.URL+"/db/nornic/tx/commit", bytes.NewReader(payload))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+getAuthToken(t, authenticator, "admin"))
	req.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	require.NoError(t, err)
	defer response.Body.Close()
	var body TransactionResponse
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	require.NotEmpty(t, body.Errors)
	require.Equal(t, "Neo.ClientError.Transaction.TransactionTimedOut", body.Errors[0].Code)
	require.Equal(t, nornicerrors.TransactionTimedOutMessage, body.Errors[0].Message)
}
