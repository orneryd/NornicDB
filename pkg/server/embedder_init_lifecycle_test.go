package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// shutdownHealthEmbedder's health check blocks until shutdown and then
// succeeds, so a healthy embedder arrives while the server or database closes.
type shutdownHealthEmbedder struct {
	countingEmbedder
	enterOnce sync.Once
	entered   chan struct{}
}

func (e *shutdownHealthEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	e.enterOnce.Do(func() { close(e.entered) })
	<-ctx.Done()
	return []float32{0.1, 0.2, 0.3, 0.4}, nil
}

// Stopping the server or closing the database while embedder initialisation
// is still running ends it without installing an embedder: Stop returns only
// after the goroutine has exited, and closing the database ends it too.
func TestEmbedderInitEndsWithoutInstallingWhenClosed(t *testing.T) {
	for _, phase := range []string{"health check", "retry wait"} {
		for _, closer := range []string{"server stop", "database close"} {
			t.Run(phase+"/"+closer, func(t *testing.T) {
				s, _ := setupTestServer(t)
				// Long enough that only shutdown ends the wait.
				s.embedderRetryInitialBackoff = time.Hour
				s.embedderRetryMaxBackoff = time.Hour
				entered := make(chan struct{})
				var once sync.Once
				s.newEmbedder = func(*embed.Config) (embed.Embedder, error) {
					if phase == "health check" {
						return &shutdownHealthEmbedder{countingEmbedder: countingEmbedder{dims: 4}, entered: entered}, nil
					}
					once.Do(func() { close(entered) })
					return nil, errors.New("forced constructor failure")
				}

				s.startEmbedderInit(&embed.Config{Provider: "openai", APIURL: "http://127.0.0.1:1", Model: "m", Dimensions: 4}, 0, nil, s.db)
				select {
				case <-entered:
				case <-time.After(10 * time.Second):
					t.Fatal("embedder initialisation didn't start")
				}

				if closer == "server stop" {
					require.NoError(t, s.Stop(context.Background()))
					select {
					case <-s.embedderInitDone:
					default:
						t.Fatal("embedder initialisation still running after Stop returned")
					}
				} else {
					require.NoError(t, s.db.Close())
					select {
					case <-s.embedderInitDone:
					case <-time.After(10 * time.Second):
						t.Fatal("embedder initialisation still running after the database closed")
					}
					require.NoError(t, s.Stop(context.Background()))
				}

				// SetEmbedder installs the query embedder first.
				require.Nil(t, s.db.GetCypherExecutor().GetEmbedder())
			})
		}
	}
}
