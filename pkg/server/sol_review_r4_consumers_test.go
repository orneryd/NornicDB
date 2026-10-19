package server

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type solR4ServerEmbedder struct {
	countingEmbedder
	entered        chan struct{}
	once           sync.Once
	active, unsafe atomic.Bool
}

func (e *solR4ServerEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if text != "needle" {
		return e.countingEmbedder.Embed(ctx, text)
	}
	e.active.Store(true)
	defer e.active.Store(false)
	e.once.Do(func() { close(e.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func (e *solR4ServerEmbedder) Close() error { e.unsafe.Store(e.active.Load()); return nil }

func TestSolR4ConsumerShutdown(t *testing.T) {
	for _, mode := range []string{"http_search", "heimdall_query"} {
		t.Run(mode, func(t *testing.T) {
			server, _ := setupTestServer(t)
			server.auth = nil
			server.db.SetEmbedConfigForDB(nil)
			dbName := server.dbManager.DefaultDatabaseName()
			engine, err := server.dbManager.GetStorage(dbName)
			require.NoError(t, err)
			vector := make([]float32, 1024)
			vector[0] = 1
			_, err = engine.CreateNode(&storage.Node{ID: "seed", Labels: []string{"Doc"}, Properties: map[string]interface{}{"content": "needle"}, ChunkEmbeddings: [][]float32{vector}})
			require.NoError(t, err)
			svc, err := server.db.GetOrCreateSearchService(dbName, engine)
			require.NoError(t, err)
			require.Eventually(t, svc.IsReady, 10*time.Second, 10*time.Millisecond)
			require.NoError(t, server.db.BuildSearchIndexes(context.Background()))
			require.Positive(t, svc.EmbeddingCount(), "HTTP fixture must have an indexed vector before deciding to embed")
			e := &solR4ServerEmbedder{countingEmbedder: countingEmbedder{dims: 1024}, entered: make(chan struct{})}
			require.True(t, server.db.SetEmbedder(e))
			queried := make(chan error, 1)
			go func() {
				if mode == "http_search" {
					response := makeRequest(t, server, http.MethodPost, "/nornicdb/search", map[string]interface{}{"database": dbName, "query": "needle", "limit": 1}, "")
					t.Logf("HTTP shutdown response=%d %s", response.Code, response.Body.String())
					if response.Code < 400 {
						queried <- errors.New("HTTP search reported success after Close cancelled its provider")
						return
					}
					queried <- nil
				} else {
					router := newHeimdallDBRouter(server.db, server.dbManager, nil)
					_, err := router.Query(context.Background(), dbName, "CALL db.index.vector.embed('needle') YIELD embedding RETURN embedding", nil)
					if err == nil {
						queried <- errors.New("Heimdall query reported success after Close cancelled its provider")
						return
					}
					if !errors.Is(err, context.Canceled) && !errors.Is(err, nornicdb.ErrClosed) {
						queried <- err
						return
					}
					queried <- nil
				}
			}()
			select {
			case <-e.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("consumer did not enter provider")
			}
			closed := make(chan error, 1)
			go func() { closed <- server.db.Close() }()
			select {
			case err := <-queried:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("consumer hung on shutdown")
			}
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Close did not join consumer")
			}
			require.False(t, e.unsafe.Load(), "provider was freed while consumer active")
		})
	}
}
