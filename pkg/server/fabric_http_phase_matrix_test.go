package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestFabricHTTPPreAdmissionEntryMatrix(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "500ms")
	for _, composite := range []bool{false, true} {
		for _, entry := range []string{"begin", "execute", "explicit_commit", "implicit_single", "implicit_multi"} {
			t.Run(fmt.Sprintf("composite_%v/%s", composite, entry), func(t *testing.T) {
				s, a := setupTestServer(t)
				require.NoError(t, s.dbManager.CreateCompositeDatabase("phasecomp", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"}}))
				token := "Bearer " + getAuthToken(t, a, "admin")
				entered := make(chan time.Time, 1)
				require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: "review.phaseWait", Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
					deadline, ok := ctx.Deadline()
					if !ok {
						return nil, fmt.Errorf("statement has no deadline")
					}
					entered <- deadline
					<-ctx.Done()
					return nil, ctx.Err()
				}))
				defer cypher.ClearUserProcedures()
				h := httptest.NewServer(s.buildRouter())
				defer h.Close()
				post := func(path, body string) TransactionResponse {
					if !strings.HasPrefix(path, "http") {
						path = h.URL + path
					}
					req, err := http.NewRequest(http.MethodPost, path, strings.NewReader(body))
					require.NoError(t, err)
					req.Header.Set("Authorization", token)
					req.Header.Set("Content-Type", "application/json")
					response, err := h.Client().Do(req)
					require.NoError(t, err)
					defer response.Body.Close()
					var result TransactionResponse
					require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
					return result
				}
				db, prefix := "nornic", ""
				if composite {
					db = "phasecomp"
					prefix = "USE phasecomp.l "
				}
				base := "/db/" + db + "/tx"
				// Initialize the public parser/cache before timing the blocked statement.
				post(base+"/commit", `{"statements":[{"statement":"`+prefix+`RETURN 1 AS k"}]}`)
				path := base
				if entry == "execute" || entry == "explicit_commit" {
					opened := post(base, `{"statements":[]}`)
					require.Empty(t, opened.Errors)
					path = opened.Commit
					if entry == "execute" {
						path = strings.TrimSuffix(path, "/commit")
					}
				} else if strings.HasPrefix(entry, "implicit_") {
					path = base + "/commit"
				}
				statements := []StatementRequest{{Statement: prefix + "CALL review.phaseWait()"}}
				if entry == "implicit_multi" {
					statements = append(statements, StatementRequest{Statement: "RETURN 7 AS k"})
				}
				body, err := json.Marshal(map[string]interface{}{"statements": statements})
				require.NoError(t, err)
				start := time.Now()
				result := post(path, string(body))
				elapsed := time.Since(start)
				require.NotEmpty(t, result.Errors, "expired statement must never reach COMMIT")
				select {
				case deadline := <-entered:
					require.WithinDuration(t, start.Add(500*time.Millisecond), deadline, 250*time.Millisecond)
				case <-time.After(time.Second):
					t.Fatal("context-aware statement not reached")
				}
				require.Less(t, elapsed, 2*time.Second, "pre-admission statement execution must remain bounded")
				require.GreaterOrEqual(t, elapsed, 400*time.Millisecond)
			})
		}
	}
}
