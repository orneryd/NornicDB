package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/mcp"
	"github.com/stretchr/testify/require"
)

// failingHealthEmbedder is created successfully but fails its health check.
type failingHealthEmbedder struct{ countingEmbedder }

func (e *failingHealthEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, errors.New("forced health check failure")
}

func runEmbedderRetry(t *testing.T, ctx context.Context, s *Server, cfg *embed.Config, cacheSize int, mcpServer *mcp.Server) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		s.initEmbedderWithRetry(ctx, cfg, cacheSize, mcpServer, s.db)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("embedder retry loop didn't return")
	}
}

// The embedder retry loop stops before its first attempt once its context has
// ended, without creating or installing an embedder.
func TestInitEmbedderWithRetryStopsWhenContextDone(t *testing.T) {
	s, _ := setupTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var attempts atomic.Int32
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		attempts.Add(1)
		return &countingEmbedder{dims: 4}, nil
	}
	runEmbedderRetry(t, ctx, s, &embed.Config{Provider: "openai", Model: "m", Dimensions: 4}, 0, nil)
	require.Zero(t, attempts.Load())
	require.Nil(t, s.db.GetCypherExecutor().GetEmbedder())
}

// Failed attempts back off, doubling up to the cap and then retrying at the
// capped interval; the first healthy embedder is installed.
func TestInitEmbedderWithRetryBacksOffUntilHealthy(t *testing.T) {
	s, _ := setupTestServer(t)
	s.embedderRetryInitialBackoff = time.Millisecond
	s.embedderRetryMaxBackoff = 3 * time.Millisecond
	var attempts atomic.Int32
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		switch attempts.Add(1) {
		case 1, 2:
			return nil, errors.New("forced constructor failure")
		case 3, 4:
			return &failingHealthEmbedder{countingEmbedder{dims: 4}}, nil
		default:
			return &countingEmbedder{dims: 4}, nil
		}
	}
	runEmbedderRetry(t, context.Background(), s, &embed.Config{Provider: "local", Model: "m", Dimensions: 4}, 16, mcp.NewServer(s.db, mcp.DefaultServerConfig()))
	require.Equal(t, int32(5), attempts.Load())
	require.NotNil(t, s.db.GetCypherExecutor().GetEmbedder())
}

// A failing remote provider retries until its context ends during a wait at
// the capped interval.
func TestInitEmbedderWithRetryStopsDuringCappedWait(t *testing.T) {
	s, _ := setupTestServer(t)
	s.embedderRetryInitialBackoff = 10 * time.Second
	s.embedderRetryMaxBackoff = 10 * time.Second
	var attempts atomic.Int32
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		attempts.Add(1)
		return nil, errors.New("forced constructor failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(50*time.Millisecond, cancel)
	runEmbedderRetry(t, ctx, s, &embed.Config{Provider: "openai", APIURL: "http://127.0.0.1:1", Model: "m", Dimensions: 4}, 0, nil)
	require.Equal(t, int32(1), attempts.Load())
}

func TestInitEmbedderWithRetryRedactsReadyEndpoint(t *testing.T) {
	s, _ := setupTestServer(t)
	var output bytes.Buffer
	s.log = slog.New(slog.NewJSONHandler(&output, nil))
	s.newEmbedder = func(context.Context, *embed.Config) (embed.Embedder, error) {
		return &countingEmbedder{dims: 4}, nil
	}
	cfg := &embed.Config{
		Provider:   "openai",
		APIURL:     "https://synthetic-user:synthetic-pass@embeddings.invalid/v1?access_token=synthetic-query-token",
		APIKey:     "synthetic-api-key",
		Model:      "test-model",
		Dimensions: 4,
	}
	runEmbedderRetry(t, context.Background(), s, cfg, 0, nil)
	if !strings.Contains(output.String(), "server.embedding.ready") {
		t.Fatal("expected remote embedding ready event was not recorded; output withheld")
	}
	testfixture.RequireMarkersAbsent(t, output.String(), "synthetic-user", "synthetic-pass", "synthetic-query-token", "synthetic-api-key")
}

func TestInitEmbedderWithRetryRedactsFailureEndpointAndError(t *testing.T) {
	s, _ := setupTestServer(t)
	var output bytes.Buffer
	s.log = slog.New(slog.NewJSONHandler(&output, nil))
	s.embedderRetryInitialBackoff = time.Second
	s.newEmbedder = func(_ context.Context, cfg *embed.Config) (embed.Embedder, error) {
		return nil, fmt.Errorf("request to %s failed with %s", cfg.APIURL, cfg.APIKey)
	}
	cfg := &embed.Config{
		Provider:   "openai",
		APIURL:     "https://synthetic-user:synthetic-pass@embeddings.invalid/v1?access_token=synthetic-query-token",
		APIKey:     "synthetic-api-key",
		Model:      "test-model",
		Dimensions: 4,
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	runEmbedderRetry(t, ctx, s, cfg, 0, nil)
	if !strings.Contains(output.String(), "server.embedding.initialization_attempt_failed") {
		t.Fatal("expected remote embedding failure event was not recorded; output withheld")
	}
	testfixture.RequireMarkersAbsent(t, output.String(), "synthetic-user", "synthetic-pass", "synthetic-query-token", "synthetic-api-key")
}

func TestSearchRerankerReadyRedactsConfiguredEndpoint(t *testing.T) {
	var output bytes.Buffer
	_, _ = setupTestServerWithConfig(t, func(cfg *Config) {
		cfg.Logger = slog.New(slog.NewJSONHandler(&output, nil))
		cfg.Features = &nornicConfig.FeatureFlagsConfig{
			SearchRerankEnabled:  true,
			SearchRerankProvider: "https://synthetic-user:synthetic-pass@reranker-provider.invalid/v1?access_token=synthetic-query-token",
			SearchRerankAPIURL:   "https://synthetic-user:synthetic-pass@reranker.invalid/v1?access_token=synthetic-query-token",
			SearchRerankAPIKey:   "synthetic-api-key",
		}
	})
	if !strings.Contains(output.String(), "server.search_rerank.ready") {
		t.Fatal("expected external reranker ready event was not recorded; output withheld")
	}
	testfixture.RequireMarkersAbsent(t, output.String(), "synthetic-user", "synthetic-pass", "synthetic-query-token", "synthetic-api-key")
}
