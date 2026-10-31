package server

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Real HTTP admission and routing select this read before DROP. Hold the
// existing flags resolver before the handler's first EnsureWarm; neither the
// product timeout worker nor the test waits for pending refreshes.
func TestSearchOriginHTTPDispatchBeforeDrop(t *testing.T) {
	for _, recreate := range []bool{false, true} {
		t.Run(fmt.Sprintf("recreate=%t", recreate), func(t *testing.T) {
			server, authn := setupTestServer(t)
			token := getAuthToken(t, authn, "admin")
			const name = "http_origin"
			require.NoError(t, server.dbManager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: name}))
			flags := flagsResolver(name, true, true, "lazy", "lazy")
			server.db.SetDbSearchFlagsResolver(flags)
			store, err := server.dbManager.GetStorage(name)
			require.NoError(t, err)
			svc, err := server.db.EnsureSearchIndexes(context.Background(), name, store, nornicdb.SearchIndexBuildWait)
			require.NoError(t, err)
			require.False(t, svc.IsReady())
			exec := cypher.NewStorageExecutor(store)
			exec.SetSearchService(svc)
			create := "CREATE VECTOR INDEX http_vec FOR (n:Doc) ON (n.embedding)"
			_, err = exec.Execute(context.Background(), create, nil)
			require.NoError(t, err)
			vector := make([]float64, svc.VectorIndexDimensions())
			vector[0] = 1
			_, err = store.CreateNode(&storage.Node{ID: "source", Labels: []string{"Doc"}, Properties: map[string]any{"content": "httpword", "embedding": vector, "other": vector}})
			require.NoError(t, err)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			var selected atomic.Bool
			server.db.SetDbSearchFlagsResolver(func(database string) (bool, bool, string, string) {
				if database == name && selected.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return flags(database)
			})
			httpServer := httptest.NewServer(server.buildRouter())
			defer httpServer.Close()
			request, err := http.NewRequest(http.MethodPost, httpServer.URL+"/nornicdb/search", strings.NewReader(`{"database":"http_origin","query":"httpword","limit":5}`))
			require.NoError(t, err)
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("Content-Type", "application/json")
			type result struct {
				status int
				body   string
				err    error
			}
			done := make(chan result, 1)
			go func() {
				response, err := httpServer.Client().Do(request)
				if err != nil {
					done <- result{err: err}
					return
				}
				body, err := io.ReadAll(response.Body)
				_ = response.Body.Close()
				done <- result{response.StatusCode, string(body), err}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("HTTP request did not reach search flags resolution")
			}
			_, err = exec.Execute(context.Background(), "DROP INDEX http_vec", nil)
			require.NoError(t, err)
			if recreate {
				_, err = exec.Execute(context.Background(), create, nil)
				require.NoError(t, err)
			}
			unblock()
			select {
			case response := <-done:
				require.NoError(t, response.err)
				require.Equal(t, http.StatusOK, response.status, response.body)
			case <-time.After(10 * time.Second):
				t.Fatal("HTTP read did not finish")
			}
			require.True(t, svc.IsReady())
			require.Zero(t, svc.CountPropertyVectorEntries("embedding"), "HTTP request rebound the first read after DROP")
			require.Equal(t, 1, svc.CountPropertyVectorEntries("other"))
		})
	}
}
