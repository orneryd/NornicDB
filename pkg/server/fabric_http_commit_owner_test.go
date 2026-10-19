package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txsession"
	"github.com/stretchr/testify/require"
)

func TestReviewR7HTTPCommitOwner(t *testing.T) {
	for _, mode := range []string{"timeout", "disconnect"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "timeout" {
				t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "500ms")
			} else {
				t.Setenv("NORNICDB_HTTP_TX_TIMEOUT", "5s")
			}
			server, auth := setupTestServer(t)
			token := "Bearer " + getAuthToken(t, auth, "admin")
			require.NoError(t, server.dbManager.CreateDatabase("idleblocked"))
			leaf, err := server.dbManager.GetStorage("idleblocked")
			require.NoError(t, err)
			_, err = leaf.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Seed"}})
			require.NoError(t, err)
			require.NoError(t, server.dbManager.CreateCompositeDatabase("commitcomp", []multidb.ConstituentRef{{Alias: "l", DatabaseName: "idleblocked", Type: "local", AccessMode: "read_write"}}))
			setter, ok := unwrapInnerStorage(leaf).(interface {
				SetLifecycleController(storage.MVCCLifecycleController)
			})
			require.True(t, ok)
			controller := &idleSnapshotLock{entered: make(chan struct{}), finished: make(chan struct{})}
			setter.SetLifecycleController(controller)
			defer setter.SetLifecycleController(nil)
			controller.mu.Lock()
			var release sync.Once
			defer release.Do(controller.mu.Unlock)
			server.txSessions = txsession.NewManager(time.Minute, server.newExecutorForDatabase)
			server.txSessions.SetLifetimeContext(server.requestsCtx)
			// buildRouter includes the production middleware.
			httpServer := httptest.NewServer(server.buildRouter())
			defer func() { release.Do(controller.mu.Unlock); httpServer.Close() }()
			post := func(ctx context.Context, path, body string) (*http.Response, error) {
				if !strings.HasPrefix(path, "http") {
					path = httpServer.URL + path
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, path, strings.NewReader(body))
				if err != nil {
					return nil, err
				}
				req.Header.Set("Authorization", token)
				req.Header.Set("Content-Type", "application/json")
				return httpServer.Client().Do(req)
			}
			begin, err := post(context.Background(), "/db/commitcomp/tx", `{"statements":[{"statement":"USE commitcomp.l CREATE (:ReviewR7Outcome {id:'late'}) RETURN 7 AS k"}]}`)
			require.NoError(t, err)
			var opened TransactionResponse
			require.NoError(t, json.NewDecoder(begin.Body).Decode(&opened))
			begin.Body.Close()
			require.Empty(t, opened.Errors)
			reqCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type outcome struct {
				response *http.Response
				err      error
			}
			done := make(chan outcome, 1)
			go func() { r, e := post(reqCtx, opened.Commit, `{"statements":[]}`); done <- outcome{r, e} }()
			select {
			case <-controller.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("commit not admitted")
			}
			if mode == "disconnect" {
				cancel()
			}
			if mode == "timeout" {
				select {
				case out := <-done:
					if out.response != nil {
						body, _ := io.ReadAll(out.response.Body)
						out.response.Body.Close()
						t.Fatalf("COMMIT returned before outcome: HTTP %d %s", out.response.StatusCode, body)
					}
					t.Fatalf("COMMIT returned before outcome: %v", out.err)
				case <-time.After(1200 * time.Millisecond):
					t.Log("connected COMMIT still owns outcome beyond configured 500ms request timeout")
				}
			} else {
				select {
				case out := <-done:
					require.Error(t, out.err)
				case <-time.After(3 * time.Second):
					t.Fatal("disconnected client did not return")
				}
			}
			require.Equal(t, int64(1), server.activeTxReqs.Load(), "terminal handler should still own commit")
			if mode == "disconnect" {
				time.Sleep(32 * time.Second)
				require.Equal(t, int64(1), server.activeTxReqs.Load())
				t.Log("disconnected commit still has one owner beyond 30 seconds")
			}
			release.Do(controller.mu.Unlock)
			if mode == "timeout" {
				select {
				case out := <-done:
					require.NoError(t, out.err)
					defer out.response.Body.Close()
					require.Equal(t, http.StatusOK, out.response.StatusCode)
					var result TransactionResponse
					require.NoError(t, json.NewDecoder(out.response.Body).Decode(&result))
					require.Empty(t, result.Errors)
				case <-time.After(5 * time.Second):
					t.Fatal("COMMIT outcome not delivered after release")
				}
			}
			require.Eventually(t, func() bool { return server.activeTxReqs.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
			nodes, err := leaf.GetNodesByLabel("ReviewR7Outcome")
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(opened.Commit, "/commit"), strings.NewReader(`{"statements":[]}`))
			require.NoError(t, err)
			req.Header.Set("Authorization", token)
			retired, err := httpServer.Client().Do(req)
			require.NoError(t, err)
			retired.Body.Close()
			require.Equal(t, http.StatusNotFound, retired.StatusCode)
			t.Logf("%s: committed exactly one node; active handler drained and session retired", mode)
		})
	}
}
