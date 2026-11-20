package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestNOR78CompositeRemoteHTTPErrorCleanup uses the preserved call_then_error
// atomicity query with an actual local leaf and actual HTTP remote participant.
// TransactionNotFound on a later request to an already aborted HTTP transaction
// is characterized separately from an unexpected extra error during cleanup.
func TestNOR78CompositeRemoteHTTPErrorCleanup(t *testing.T) {
	previous := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previous) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			remote, remoteAuth := setupTestServer(t)
			remoteToken := "Bearer " + getAuthToken(t, remoteAuth, "admin")
			remoteRouter := remote.buildRouter()
			remoteHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set("Authorization", remoteToken)
				remoteRouter.ServeHTTP(w, r)
			}))
			t.Cleanup(remoteHTTP.Close)
			coordinator, auth := setupTestServer(t)
			require.NoError(t, coordinator.dbManager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "nor78_local"}))
			require.NoError(t, coordinator.dbManager.CreateCompositeDatabase(t.Context(), "comp", []multidb.ConstituentRef{
				{Alias: "one", DatabaseName: "nor78_local", Type: "local", AccessMode: "read_write"},
				{Alias: "two", DatabaseName: remote.dbManager.DefaultDatabaseName(), Type: "remote", AccessMode: "read_write", URI: remoteHTTP.URL, AuthMode: "oidc_forwarding"},
			}))
			localHTTP := httptest.NewServer(coordinator.buildRouter())
			t.Cleanup(localHTTP.Close)
			token := "Bearer " + getAuthToken(t, auth, "admin")
			client := &http.Client{Timeout: 20 * time.Second}
			send := func(t *testing.T, method, endpoint, query string, params map[string]any) (int, TransactionResponse, string) {
				statements := []map[string]any{}
				if query != "" {
					statements = append(statements, map[string]any{"statement": query, "parameters": params})
				}
				body, err := json.Marshal(map[string]any{"statements": statements})
				require.NoError(t, err)
				req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
				require.NoError(t, err)
				req.Header.Set("Authorization", token)
				req.Header.Set("Content-Type", "application/json")
				res, err := client.Do(req)
				require.NoError(t, err)
				defer res.Body.Close()
				var decoded TransactionResponse
				require.NoError(t, json.NewDecoder(res.Body).Decode(&decoded))
				return res.StatusCode, decoded, res.Header.Get("Location")
			}
			count := func(t *testing.T, run string) float64 {
				status, r, _ := send(t, http.MethodPost, localHTTP.URL+"/db/nor78_local/tx/commit", "MATCH (n:AcAtom {run:$run}) RETURN count(n) AS c", map[string]any{"run": run})
				require.Equal(t, http.StatusOK, status)
				require.Empty(t, r.Errors)
				require.Len(t, r.Results, 1)
				require.Len(t, r.Results[0].Data, 1)
				return r.Results[0].Data[0].Row[0].(float64)
			}
			const query = "USE comp CALL { USE comp.one CREATE (:AcAtom {run: $run}) RETURN 1 AS w } CALL { USE comp.two RETURN 1 / $d AS z } RETURN w, z"
			for _, d := range []int64{1, 0} {
				t.Run(fmt.Sprintf("d=%d", d), func(t *testing.T) {
					run := t.Name()
					status, opened, location := send(t, http.MethodPost, localHTTP.URL+"/db/comp/tx", "", nil)
					require.Equal(t, http.StatusCreated, status)
					require.Empty(t, opened.Errors)
					require.NotEmpty(t, location)
					// Location may be relative depending on the public host handling.
					if location[0] == '/' {
						location = localHTTP.URL + location
					}
					status, failed, _ := send(t, http.MethodPost, location, query, map[string]any{"run": run, "d": d})
					t.Logf("d=%d statement HTTP=%d errors=%+v", d, status, failed.Errors)
					assert.Equal(t, http.StatusOK, status)
					if d == 0 {
						assert.Len(t, failed.Errors, 1, "statement failure must not add TransactionNotFound as a cleanup failure")
						if len(failed.Errors) > 0 {
							assert.Equal(t, "Neo.ClientError.Statement.ArithmeticError", failed.Errors[0].Code)
						}
					} else {
						require.Empty(t, failed.Errors)
						require.Len(t, failed.Results, 1)
						require.Len(t, failed.Results[0].Data, 1)
						assert.Equal(t, []any{float64(1), float64(1)}, failed.Results[0].Data[0].Row)
					}
					endedStatus, ended, _ := send(t, http.MethodDelete, location, "", nil)
					t.Logf("d=%d subsequent DELETE HTTP=%d errors=%+v", d, endedStatus, ended.Errors)
					if d == 0 {
						assert.Equal(t, http.StatusNotFound, endedStatus, "HTTP statement error already ended transaction")
						assert.Len(t, ended.Errors, 1)
						if len(ended.Errors) > 0 {
							assert.Equal(t, "Neo.ClientError.Transaction.TransactionNotFound", ended.Errors[0].Code)
						}
					} else {
						assert.Equal(t, http.StatusOK, endedStatus)
						assert.Empty(t, ended.Errors)
					}
					assert.Zero(t, count(t, run), "local write must not survive either explicit rollback or statement failure")
				})
			}
		})
	}
}
