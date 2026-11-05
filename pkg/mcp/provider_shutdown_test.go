package mcp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type discoverShutdownEmbedder struct {
	readinessEmbedder
	entered chan struct{}
	active  atomic.Bool
	unsafe  atomic.Bool
}

func (e *discoverShutdownEmbedder) Embed(ctx context.Context, _ string) ([]float32, error) {
	e.active.Store(true)
	defer e.active.Store(false)
	close(e.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (e *discoverShutdownEmbedder) Close() error {
	e.unsafe.Store(e.active.Load())
	return nil
}

func TestDiscoverJoinsProviderBeforeDatabaseClose(t *testing.T) {
	db, server := newReadinessServer(t)
	e := &discoverShutdownEmbedder{entered: make(chan struct{})}
	require.True(t, db.SetEmbedder(e))
	queried := make(chan error, 1)
	go func() {
		_, err := server.handleDiscover(context.Background(), map[string]interface{}{"query": "held query"})
		queried <- err
	}()
	select {
	case <-e.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("discover did not enter the provider")
	}
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case <-queried:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel discover's provider call")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join discover")
	}
	require.False(t, e.unsafe.Load(), "MCP still used a released provider")
}
