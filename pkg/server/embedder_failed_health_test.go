package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/stretchr/testify/require"
)

// A constructed local embedder owns a model/warmup worker even if its health
// check fails. Only Close releases that worker.
type failedHealthOwnedEmbedder struct {
	countingEmbedder
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closes    atomic.Int32
}

func (e *failedHealthOwnedEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, errors.New("health check failed after successful construction")
}

func (e *failedHealthOwnedEmbedder) Close() error {
	e.closes.Add(1)
	e.closeOnce.Do(func() { close(e.stop) })
	<-e.done
	return nil
}

type failedHealthWarningHandler struct {
	warned chan struct{}
	once   sync.Once
}

func (h *failedHealthWarningHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *failedHealthWarningHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		h.once.Do(func() { close(h.warned) })
	}
	return nil
}
func (h *failedHealthWarningHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *failedHealthWarningHandler) WithGroup(string) slog.Handler      { return h }

func TestEmbedderFailedHealthClosesBeforeBackoff(t *testing.T) {
	for _, provider := range []string{"local", "openai"} {
		for _, cacheSize := range []int{0, 16} {
			t.Run(fmt.Sprintf("%s/cache%d", provider, cacheSize), func(t *testing.T) {
				s, _ := setupTestServer(t)
				s.embedderRetryInitialBackoff = time.Hour
				s.embedderRetryMaxBackoff = time.Hour
				warned := make(chan struct{})
				s.log = slog.New(&failedHealthWarningHandler{warned: warned})
				e := &failedHealthOwnedEmbedder{
					countingEmbedder: countingEmbedder{dims: 4},
					stop:             make(chan struct{}),
					done:             make(chan struct{}),
				}
				go func() { defer close(e.done); <-e.stop }()
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = s.Stop(ctx)
					_ = e.Close() // Release the probe's worker on the RED path, too.
				})
				s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) { return e, nil }
				s.startEmbedderInit(&embed.Config{Provider: provider, Model: "test", Dimensions: 4}, cacheSize, nil, s.db)

				// This warning occurs after the attempt's cancellation check. The
				// failed embedder must already be released before entering backoff.
				select {
				case <-warned:
				case <-time.After(5 * time.Second):
					t.Fatal("failed health check was not reported")
				}
				select {
				case <-e.done:
				default:
					t.Error("failed-health embedder's worker is still alive during retry backoff")
				}
				require.Nil(t, s.db.GetCypherExecutor().GetEmbedder())
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, s.Stop(ctx))
				require.NoError(t, s.db.Close())
				require.Equal(t, int32(1), e.closes.Load(), "release each unused embedder exactly once")
			})
		}
	}
}
