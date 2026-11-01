package linkpredict

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type cancelingGraphRead struct {
	storage.Engine
	cancel  context.CancelFunc
	readErr error
}

func (e *cancelingGraphRead) AllNodes(context.Context) ([]*storage.Node, error) {
	e.cancel()
	return nil, e.readErr
}

func TestGraphBuilderCancellationPreservesFailureIdentity(t *testing.T) {
	ordinary := errors.New("controlled ordinary node-read failure")
	for _, tc := range []struct {
		name     string
		readErr  error
		canceled bool
	}{
		{"canceled read", context.Canceled, true},
		{"ordinary failure after cancellation", ordinary, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			builder := NewGraphBuilder(&cancelingGraphRead{Engine: base, cancel: cancel, readErr: tc.readErr}, nil)
			graph, err := builder.Build(ctx)
			require.Nil(t, graph)
			if tc.canceled {
				require.Same(t, context.Canceled, err)
			} else {
				require.ErrorIs(t, err, ordinary)
				require.ErrorContains(t, err, "failed to get nodes")
			}
		})
	}
}

func TestGraphBuilderCanceledCallerDoesNotPublishCacheHit(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	cfg := DefaultBuildConfig()
	cfg.CachePath = t.TempDir()
	builder := NewGraphBuilder(base, cfg)
	want := Graph{storage.NodeID("test:a"): NodeSet{}}
	require.NoError(t, builder.saveToCache(want))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	graph, err := builder.Build(ctx)
	require.Nil(t, graph)
	require.Same(t, context.Canceled, err)
	require.Zero(t, builder.Stats().CacheHits)
	healthy, err := builder.Build(context.Background())
	require.NoError(t, err)
	require.Equal(t, want, healthy)
}

func TestGraphBuilderFinalProgressCancellationPublishesNothing(t *testing.T) {
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	for i := range 10 {
		_, err := base.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("test:n%d", i))})
		require.NoError(t, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := DefaultBuildConfig()
	cfg.ChunkSize = 10
	cfg.GCAfterChunk = false
	cfg.ProgressCallback = func(processed, total int, _ time.Duration) {
		if processed == total {
			cancel()
		}
	}
	builder := NewGraphBuilder(base, cfg)
	graph, err := builder.Build(ctx)
	require.ErrorIs(t, ctx.Err(), context.Canceled, "the callback must actually cancel")
	require.Nil(t, graph)
	require.Same(t, context.Canceled, err)
	require.Zero(t, builder.Stats().BuildsCompleted)
}
