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

// The readiness boundary after NODE admission while the ANN job is pending
// (the base source is complete; the graph is only an accelerator).
func TestBuildCancellationAfterNodeAdmissionWithANNPending(t *testing.T) {
	t.Run("cancelled after the build returned", func(t *testing.T) {
		svc, releaseANN := annPendingService(t)
		ctx, cancel := context.WithCancel(context.Background())
		require.NoError(t, svc.BuildIndexes(ctx))
		cancel()
		// The request's cancellation neither withdraws readiness nor cancels
		// the service-owned ANN job: search answers exactly meanwhile.
		require.True(t, svc.IsReady())
		method, top := searchMethodAndTop(t, svc)
		require.Equal(t, "vector_brute", method)
		require.Equal(t, "nornic:n0", top)
		releaseANN()
		requireHNSWReady(t, svc)
		method, top = searchMethodAndTop(t, svc)
		require.Equal(t, "vector_hnsw", method)
		require.Equal(t, "nornic:n0", top)
	})

	t.Run("cancelled before readiness was published", func(t *testing.T) {
		svc, releaseANN := annPendingService(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancelAtWarmup := func(s *Service) {
			if s == svc {
				cancel()
			}
		}
		annWarmupStartedHook.Store(&cancelAtWarmup)
		t.Cleanup(func() { annWarmupStartedHook.Store(nil) })
		require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
		annWarmupStartedHook.Store(nil)
		// A build cancelled after it made the service unavailable leaves it
		// not ready: search reports "building", which the server maps to the
		// retryable 503. Nothing is left half-published.
		require.False(t, svc.IsReady())
		require.False(t, svc.BuildInProgress())
		_, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, DefaultSearchOptions())
		require.ErrorIs(t, err, ErrSearchIndexBuilding)
		// The ANN job is service-owned: the request's cancellation did not stop it.
		releaseANN()
		requireHNSWReady(t, svc)
		// The next build restores readiness over the complete source.
		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.True(t, svc.IsReady())
		_, top := searchMethodAndTop(t, svc)
		require.Equal(t, "nornic:n0", top)
		require.Eventually(t, func() bool { return svc.lifecycleState() == lifecycleReady }, time.Second, time.Millisecond)
	})
}
