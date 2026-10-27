package server

import (
	"context"
	"encoding/json"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestReviewR8CommitPreAdmissionBound(t *testing.T) {
	t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "500ms")
	for _, kind := range []string{"execute", "commit"} {
		t.Run(kind, func(t *testing.T) {
			s, a := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, a, "admin")
			entered := make(chan bool, 1)
			release := make(chan struct{})
			proc := "review.r8wait" + kind
			require.NoError(t, cypher.RegisterUserProcedure(cypher.ProcedureSpec{Name: proc, Mode: cypher.ProcedureModeRead, MinArgs: 0, MaxArgs: 0}, func(ctx context.Context, _ *cypher.StorageExecutor, _ string, _ []interface{}) (*cypher.ExecuteResult, error) {
				_, bounded := ctx.Deadline()
				entered <- bounded
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
					return &cypher.ExecuteResult{}, nil
				}
			}))
			defer cypher.ClearUserProcedures()
			h := httptest.NewUnstartedServer(s.buildRouter())
			h.Config.WriteTimeout = 200 * time.Millisecond
			h.Start()
			defer h.Close()
			defer close(release)
			post := func(path, body string) (*http.Response, error) {
				if !strings.HasPrefix(path, "http") {
					path = h.URL + path
				}
				r, e := http.NewRequest(http.MethodPost, path, strings.NewReader(body))
				if e != nil {
					return nil, e
				}
				r.Header.Set("Authorization", token)
				r.Header.Set("Content-Type", "application/json")
				return h.Client().Do(r)
			}
			begin, err := post("/db/nornic/tx", `{"statements":[]}`)
			require.NoError(t, err)
			var opened TransactionResponse
			require.NoError(t, json.NewDecoder(begin.Body).Decode(&opened))
			begin.Body.Close()
			require.Empty(t, opened.Errors)
			require.NotEmpty(t, opened.Commit)
			path := opened.Commit
			if kind == "execute" {
				path = strings.TrimSuffix(path, "/commit")
			}
			done := make(chan error, 1)
			go func() {
				resp, err := post(path, `{"statements":[{"statement":"CALL `+proc+`()"}]}`)
				if resp != nil {
					resp.Body.Close()
				}
				done <- err
			}()
			select {
			case bounded := <-entered:
				if !bounded {
					t.Errorf("%s optional statement has no execution deadline before COMMIT admission", kind)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("procedure not reached")
			}
			select {
			case <-done:
				if kind == "commit" {
					t.Log("request returned before release")
				}
			case <-time.After(1200 * time.Millisecond):
				if kind == "commit" {
					t.Errorf("pre-admission handler still waits beyond HTTP deadline and socket WriteTimeout")
				} else {
					t.Error("execute route failed to retain timeout")
				}
			}
			// Release through the deferred close before waiting for httptest shutdown.
			go func() { time.Sleep(10 * time.Millisecond) /* release belongs to this test */ }()
		})
	}
}
