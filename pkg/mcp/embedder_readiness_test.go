package mcp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

type readinessEmbedder struct {
	calls        atomic.Int64
	chunkStarted chan struct{}
	chunkRelease chan struct{}
}

func (e *readinessEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls.Add(1)
	return []float32{1, 0}, nil
}
func (e *readinessEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	result := make([][]float32, len(texts))
	for i, text := range texts {
		result[i], _ = e.Embed(ctx, text)
	}
	return result, nil
}
func (e *readinessEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	if e.chunkStarted != nil {
		close(e.chunkStarted)
		<-e.chunkRelease
	}
	return []string{text}, nil
}
func (*readinessEmbedder) Dimensions() int { return 2 }
func (*readinessEmbedder) Model() string   { return "readiness-test" }
func (*readinessEmbedder) Backend() string { return "cpu" }

func newReadinessServer(t *testing.T) (*nornicdb.DB, *Server) {
	t.Helper()
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = true
	cfg.Memory.EmbeddingDimensions = 2
	db, err := nornicdb.Open(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.BuildSearchIndexes(context.Background()))
	return db, NewServer(db, DefaultServerConfig())
}

func TestDiscoverUsesSharedEmbedderReadiness(t *testing.T) {
	db, server := newReadinessServer(t)
	embedder := &readinessEmbedder{}
	db.SetEmbedder(embedder)
	_, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "alpha"})
	require.NoError(t, err)
	require.Positive(t, embedder.calls.Load(), "MCP discover must read the DB's ready provider")
}

func TestDiscoverConcurrentEmbedderPublication(t *testing.T) {
	db, server := newReadinessServer(t)
	embedder := &readinessEmbedder{}
	server.SetEmbedder(embedder)
	start := make(chan struct{})
	failures := make(chan error, 100)
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for range 100 {
			server.SetEmbedder(embedder)
		}
	})
	workers.Go(func() {
		<-start
		for range 100 {
			_, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "alpha"})
			if err != nil {
				failures <- err
			}
		}
	})
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.Same(t, embedder, db.GetCypherExecutor().GetEmbedder(), "MCP publication must reach the DB binding")
}

func TestDiscoverKeepsOneProviderSnapshot(t *testing.T) {
	_, server := newReadinessServer(t)
	old := &readinessEmbedder{chunkStarted: make(chan struct{}), chunkRelease: make(chan struct{})}
	server.SetEmbedder(old)
	finished := make(chan error, 1)
	go func() {
		_, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "alpha"})
		finished <- err
	}()
	<-old.chunkStarted
	replacement := &readinessEmbedder{}
	server.SetEmbedder(replacement)
	close(old.chunkRelease)
	require.NoError(t, <-finished)
	require.Positive(t, old.calls.Load(), "chunking and embedding must use the same captured provider")
	require.Zero(t, replacement.calls.Load())
	_, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "alpha"})
	require.NoError(t, err)
	require.Positive(t, replacement.calls.Load())
}
