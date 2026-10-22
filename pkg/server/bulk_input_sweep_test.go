package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

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
		if recorder.Code != http.StatusOK {
			return fmt.Errorf("status %d", recorder.Code)
		}
		return nil
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
