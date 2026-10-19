package search

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Each construction entry shares one service job. Cancelling an internal
// consumer's wait cannot turn a second caller into a second constructor.
func TestHNSWConstructionEntriesReuseServiceJob(t *testing.T) {
	for _, entry := range []string{"pipeline", "standard-strategy", "internal-wait"} {
		t.Run(entry, func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			svc := NewServiceWithDimensions(newNamespacedEngine(t), 3)
			t.Cleanup(func() { _ = svc.Close() })
			require.NoError(t, svc.IndexNode(&storage.Node{ID: "item", ChunkEmbeddings: [][]float32{{1, 0, 0}}}))
			started, release := make(chan struct{}), make(chan struct{})
			hnswBeforeInstallHook = func(ctx context.Context) {
				close(started)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			t.Cleanup(func() { hnswBeforeInstallHook = nil })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waited := make(chan error, 1)
			switch entry {
			case "pipeline":
				pipeline, err := svc.getOrCreateVectorPipeline(ctx)
				require.NoError(t, err)
				require.IsType(t, &BruteForceCandidateGen{}, pipeline.candidateGen)
			case "standard-strategy":
				strategy, err := svc.resolveStandardVectorStrategyWithGPU(ctx, vectorBuildSeeds{}, 1, 3, nil, false)
				require.NoError(t, err)
				require.True(t, strategy.pendingHNSW)
			case "internal-wait":
				go func() { _, err := svc.getOrCreateHNSWIndex(ctx, 3); waited <- err }()
			}
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("construction did not start")
			}
			svc.hnswMu.RLock()
			first := svc.hnswBuild
			svc.hnswMu.RUnlock()
			cancel()
			if entry == "internal-wait" {
				require.ErrorIs(t, <-waited, context.Canceled)
			}
			_, second, err := svc.startHNSWBuild(3)
			require.NoError(t, err)
			require.Same(t, first, second)
			require.NoError(t, first.ctx.Err(), "the request cancelled service construction")
			close(release)
			graph, err := svc.getOrCreateHNSWIndex(context.Background(), 3)
			require.NoError(t, err)
			require.Equal(t, 1, graph.Size())
		})
	}
}
