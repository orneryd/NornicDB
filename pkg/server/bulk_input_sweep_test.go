package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher/testutil"
	"github.com/stretchr/testify/require"
)

// The generated bulk-input sweep over HTTP: a canceled request ends its
// statement promptly for every registered function and procedure that takes
// a list, a map or any value.
func TestHTTPBulkInputCancellationSweep(t *testing.T) {
	server, authenticator := setupTestServer(t)
	authorization := "Bearer " + getAuthToken(t, authenticator, "admin")
	router := server.buildRouter()
	executor := server.db.GetCypherExecutor()
	size := testutil.DefaultBulkInputSize
	run := func(ctx context.Context, query string, params map[string]interface{}, started func()) error {
		body, err := json.Marshal(map[string]interface{}{
			"statements": []map[string]interface{}{{"statement": query, "parameters": params}},
		})
		if err != nil {
			return err
		}
		request := httptest.NewRequest(http.MethodPost, "/db/nornic/tx/commit", bytes.NewReader(body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", authorization)
		recorder := httptest.NewRecorder()
		started()
		router.ServeHTTP(recorder, request)
		// A canceled request's response returns at once (the transaction
		// routes' timeout handler answers it), while its statement may still
		// run: the call ends when the server's statement has ended.
		waitForTransactionRequests(server, 30*time.Second)
		if err := ctx.Err(); err != nil && recorder.Code == http.StatusServiceUnavailable {
			// The client went away: its cancellation is the outcome it sees.
			return err
		}
		return httpStatementError(recorder)
	}
	rebuild := func(t *testing.T) {
		for _, statement := range testutil.BulkInputFixture(size) {
			require.NoError(t, run(context.Background(), statement, nil, func() {}))
		}
	}
	rebuild(t)
	calls := testutil.BulkInputCalls(t, executor, size)
	require.NotEmpty(t, calls)
	testutil.RunBulkInputSweep(t, calls, run, testutil.DecodedBulkInputBudget, rebuild)
}

// httpStatementError is the statement's error in a transaction response: its
// first error, as the cancellation or deadline error when it reports one.
func httpStatementError(recorder *httptest.ResponseRecorder) error {
	var response TransactionResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		return fmt.Errorf("status %d: %w", recorder.Code, err)
	}
	if len(response.Errors) == 0 {
		if recorder.Code != http.StatusOK {
			return fmt.Errorf("status %d", recorder.Code)
		}
		return nil
	}
	failure := response.Errors[0]
	for _, ended := range []error{context.Canceled, context.DeadlineExceeded} {
		if strings.Contains(failure.Message, ended.Error()) {
			return fmt.Errorf("%s: %w", failure.Code, ended)
		}
	}
	return fmt.Errorf("%s: %s", failure.Code, failure.Message)
}

// waitForTransactionRequests returns once the server runs no transaction
// request (its statements have ended), or after limit.
func waitForTransactionRequests(server *Server, limit time.Duration) {
	for deadline := time.Now().Add(limit); server.activeTxReqs.Load() > 0 && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
}

// The HTTP sweep's clock runs until the server's statement has ended, not
// until the response: a statement that ran on after its request was
// canceled (the timeout handler answers at once) is late in the sweep.
func TestHTTPBulkInputSweepWaitsForTheStatement(t *testing.T) {
	server, _ := setupTestServer(t)
	server.activeTxReqs.Add(1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		server.activeTxReqs.Add(-1)
	}()
	started := time.Now()
	waitForTransactionRequests(server, 30*time.Second)
	require.GreaterOrEqual(t, time.Since(started), 190*time.Millisecond)
	require.Zero(t, server.activeTxReqs.Load())
}
