package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	gql "github.com/99designs/gqlgen/graphql"
	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/testutil"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The supported standalone handler is also a public ingress. A websocket's
// handshake is not admission of all the later operations on that connection.
func TestSearchOriginGraphQLOperationDrop(t *testing.T) {
	for _, ws := range []bool{false, true} {
		for _, beforeDrop := range []bool{false, true} {
			t.Run(fmt.Sprintf("websocket=%t/beforeDrop=%t", ws, beforeDrop), func(t *testing.T) {
				cfg := nornicdb.DefaultConfig()
				db, err := nornicdb.Open(t.TempDir(), cfg)
				require.NoError(t, err)
				defer db.Close()
				manager := testutil.DatabaseManager(t, db)
				name := manager.DefaultDatabaseName()
				store := db.GetStorage()
				svc, err := db.EnsureSearchIndexes(context.Background(), name, store, nornicdb.SearchIndexBuildWait)
				require.NoError(t, err)
				exec := cypher.NewStorageExecutor(store)
				exec.SetSearchService(svc)
				_, err = exec.Execute(context.Background(), "CREATE VECTOR INDEX gql_vec FOR (n:Doc) ON (n.embedding)", nil)
				require.NoError(t, err)
				vector := make([]float64, svc.VectorIndexDimensions())
				vector[0] = 1
				_, err = store.CreateNode(&storage.Node{ID: "gql_origin", Labels: []string{"Doc"}, Properties: map[string]any{"embedding": vector, "other": vector}})
				require.NoError(t, err)
				require.NoError(t, svc.RefreshNode("gql_origin", storage.CaptureVectorWorkOrigin()))
				require.Equal(t, 1, svc.CountPropertyVectorEntries("embedding"))
				h := NewHandler(db, manager)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				if beforeDrop {
					h.server.AroundOperations(func(ctx context.Context, next gql.OperationHandler) gql.ResponseHandler {
						close(entered)
						<-release
						return next(ctx)
					})
				}
				server := httptest.NewServer(h)
				defer func() { unblock(); server.Close() }()
				var conn *websocket.Conn
				if ws {
					dialer := websocket.Dialer{Subprotocols: []string{"graphql-transport-ws"}}
					conn, _, err = dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
					require.NoError(t, err)
					defer conn.Close()
					require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
					require.NoError(t, conn.WriteJSON(map[string]any{"type": "connection_init"}))
					var ack map[string]any
					require.NoError(t, conn.ReadJSON(&ack))
					require.Equal(t, "connection_ack", ack["type"])
				}
				type outcome struct {
					body string
					err  error
				}
				done := make(chan outcome, 1)
				send := func() {
					go func() {
						if ws {
							if err := conn.WriteJSON(map[string]any{"id": "origin", "type": "subscribe", "payload": map[string]any{"query": "mutation { rebuildSearchIndex }"}}); err != nil {
								done <- outcome{err: err}
								return
							}
							var response map[string]any
							for {
								if err := conn.ReadJSON(&response); err != nil {
									done <- outcome{err: err}
									return
								}
								if response["type"] == "next" || response["type"] == "error" {
									body, _ := json.Marshal(response)
									done <- outcome{body: string(body)}
									return
								}
							}
						}
						response, err := http.Post(server.URL, "application/json", strings.NewReader(`{"query":"mutation { rebuildSearchIndex }"}`))
						if err != nil {
							done <- outcome{err: err}
							return
						}
						body, err := io.ReadAll(response.Body)
						_ = response.Body.Close()
						done <- outcome{string(body), err}
					}()
				}
				if beforeDrop {
					send()
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("operation did not dispatch")
					}
				}
				_, err = exec.Execute(context.Background(), "DROP INDEX gql_vec", nil)
				require.NoError(t, err)
				if beforeDrop {
					unblock()
				} else {
					send()
				}
				select {
				case response := <-done:
					require.NoError(t, response.err)
					require.Contains(t, response.body, `"rebuildSearchIndex":true`)
				case <-time.After(10 * time.Second):
					t.Fatal("GraphQL operation did not finish")
				}
				rebuilt, err := db.GetOrCreateSearchService(name, store)
				require.NoError(t, err)
				expected := 1
				if beforeDrop {
					expected = 0
				}
				require.Equal(t, expected, rebuilt.CountPropertyVectorEntries("embedding"), "operation admission was rebound or inherited the websocket handshake")
				require.Equal(t, 1, rebuilt.CountPropertyVectorEntries("other"))
			})
		}
	}
}
