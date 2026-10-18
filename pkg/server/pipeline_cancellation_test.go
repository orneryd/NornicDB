package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestHTTPPipelineStageDeadlines(t *testing.T) {
	server, authenticator := setupTestServer(t)
	token := getAuthToken(t, authenticator, "admin")
	for _, explicit := range []bool{false, true} {
		for _, tail := range []string{
			"WITH DISTINCT x RETURN count(*)",
			"WITH x ORDER BY x DESC RETURN count(*)",
			"RETURN collect(x + 1)",
			"WITH x + 1 AS y RETURN sum(y)",
		} {
			t.Run(strconv.FormatBool(explicit)+"/"+tail, func(t *testing.T) {
				path := "/db/nornic/tx/commit"
				if explicit {
					opened := makeRequest(t, server, "POST", "/db/nornic/tx", map[string]interface{}{"statements": []interface{}{}}, "Bearer "+token)
					require.Equal(t, http.StatusCreated, opened.Code)
					var response TransactionResponse
					require.NoError(t, json.Unmarshal(opened.Body.Bytes(), &response))
					commit, err := url.Parse(response.Commit)
					require.NoError(t, err)
					path = strings.TrimSuffix(commit.Path, "/commit")
				}
				data, err := json.Marshal(TransactionRequest{Statements: []StatementRequest{{Statement: "UNWIND range(1, 10000000) AS x " + tail}}})
				require.NoError(t, err)
				// The database query time limit owns the deadline, as MaxQueryTime installs it.
				ctx, cancel := context.WithTimeoutCause(context.Background(), 200*time.Millisecond, nornicerrors.ErrDatabaseQueryTimeLimit)
				defer cancel()
				deadline, _ := ctx.Deadline()
				request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data)).WithContext(ctx)
				request.Header.Set("Content-Type", "application/json")
				request.Header.Set("Authorization", "Bearer "+token)
				recorder := httptest.NewRecorder()
				// Exercise the authenticated transaction handler synchronously.
				// The outer request-timeout middleware can return its own response
				// before the query has exited, which cannot prove stage cancellation.
				handler := server.withAuth(func(w http.ResponseWriter, r *http.Request) {
					if explicit {
						server.handleExecuteInTransaction(w, r, "nornic", path[strings.LastIndex(path, "/")+1:])
					} else {
						server.handleImplicitTransaction(w, r, "nornic")
					}
				}, auth.PermRead)
				handler(recorder, request)
				returned := time.Now()
				var response TransactionResponse
				require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
				require.NotEmpty(t, response.Errors, recorder.Body.String())
				standard := "Neo.ClientError.Transaction.TransactionTimedOut"
				require.Equal(t, standard, response.Errors[0].Code)
				require.Equal(t, nornicerrors.TransactionTimedOutMessage, response.Errors[0].Message)
				require.Less(t, returned.Sub(deadline), 250*time.Millisecond)
				t.Logf("context_deadline=%s response=%s overdue=%s explicit=%t", deadline.Format(time.RFC3339Nano), returned.Format(time.RFC3339Nano), returned.Sub(deadline), explicit)
				healthy := makeRequest(t, server, "POST", "/db/nornic/tx/commit", map[string]interface{}{"statements": []map[string]interface{}{{"statement": "RETURN 1"}}}, "Bearer "+token)
				require.NoError(t, json.Unmarshal(healthy.Body.Bytes(), &response))
				require.Empty(t, response.Errors)
			})
		}
	}
}
