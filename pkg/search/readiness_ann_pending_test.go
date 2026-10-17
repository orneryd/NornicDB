package search

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// annPendingService stores four nodes and holds the native graph build before
// it installs, so the ANN job stays pending until release.
func annPendingService(t *testing.T) (*Service, func()) {
	t.Helper()
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	for i := range 4 {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, float32(i), 0}}})
		require.NoError(t, err)
	}
	svc := NewServiceWithDimensions(engine, 3)
	t.Cleanup(func() { _ = svc.Close() })
	release := make(chan struct{})
	hnswBeforeInstallHook = func(ctx context.Context) {
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	t.Cleanup(func() { hnswBeforeInstallHook = nil })
	var once sync.Once
	releaseANN := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseANN)
	return svc, releaseANN
}

func searchMethodAndTop(t *testing.T, svc *Service) (string, string) {
	t.Helper()
	opts := DefaultSearchOptions()
	opts.Limit = 4
	resp, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, opts)
	require.NoError(t, err)
	require.NotEmpty(t, resp.Results)
	return resp.SearchMethod, resp.Results[0].ID
}

// The readiness boundary after NODE admission in an explicit build. The base
// source is complete; the warmup waits for the service-owned ANN job. A build
// whose request is cancelled before it publishes readiness leaves the service
// not ready (search reports "building", the server's retryable 503), while the
// ANN job is not the request's to cancel: it still installs, and the next
// build restores readiness over the complete source.
func TestBuildCancellationAfterNodeAdmissionWithANNPending(t *testing.T) {
	requireCancelledBuildState := func(t *testing.T, svc *Service) {
		t.Helper()
		require.False(t, svc.IsReady())
		require.False(t, svc.BuildInProgress())
		_, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, DefaultSearchOptions())
		require.ErrorIs(t, err, ErrSearchIndexBuilding)
	}

	t.Run("cancelled while the warmup waits for the graph", func(t *testing.T) {
		svc, releaseANN := annPendingService(t)
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		var once sync.Once
		hold := hnswBeforeInstallHook
		hnswBeforeInstallHook = func(jobCtx context.Context) {
			once.Do(func() { close(entered) })
			hold(jobCtx)
		}
		built := make(chan error, 1)
		go func() { built <- svc.BuildIndexes(ctx) }()
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("the warmup did not start the ANN job")
		}
		cancel()
		select {
		case err := <-built:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(10 * time.Second):
			t.Fatal("the cancelled build kept waiting for the graph")
		}
		requireCancelledBuildState(t, svc)
		releaseANN()
		requireHNSWReady(t, svc)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.True(t, svc.IsReady())
		_, top := searchMethodAndTop(t, svc)
		require.Equal(t, "nornic:n0", top)
	})

	t.Run("cancelled after the warmup, before readiness", func(t *testing.T) {
		svc, releaseANN := annPendingService(t)
		releaseANN() // the graph installs during the warmup
		ctx, cancel := context.WithCancel(context.Background())
		cancelAfterWarmup := func(s *Service) {
			if s == svc {
				cancel()
			}
		}
		annWarmupStartedHook.Store(&cancelAfterWarmup)
		t.Cleanup(func() { annWarmupStartedHook.Store(nil) })
		require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
		annWarmupStartedHook.Store(nil)
		requireCancelledBuildState(t, svc)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.True(t, svc.IsReady())
		method, top := searchMethodAndTop(t, svc)
		require.Equal(t, "vector_hnsw", method)
		require.Equal(t, "nornic:n0", top)
	})
}
