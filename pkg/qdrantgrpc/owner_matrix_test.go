package qdrantgrpc

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Shutdown matrix S2, qdrant gRPC Server: concurrent and repeated Stop callers
// all join the one stop; after any of them returns the serving goroutine has
// actually returned, and WaitStopped reports the joined outcome.
func TestOwnerMatrixQdrantConcurrentStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = "127.0.0.1:0"
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	defer base.Close()
	s, err := NewServer(cfg, newTestCollectionStore(t), base, nil, nil)
	require.NoError(t, err)
	require.NoError(t, s.Start())

	const callers = 4
	stopped := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func() { s.Stop(); stopped <- struct{}{} }()
	}
	for i := 0; i < callers; i++ {
		select {
		case <-stopped:
			select {
			case <-s.work.Stopped():
			default:
				t.Fatal("a Stop caller returned before the server's retirement completed")
			}
		case <-time.After(30 * time.Second):
			t.Fatal("concurrent Stop did not complete")
		}
	}
	s.Stop() // repeat joins the completed stop
	require.False(t, s.IsRunning())
	require.NoError(t, s.WaitStopped(context.Background()))
}
