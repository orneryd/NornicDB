package search

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// onMessage runs do once, inline, when a log record contains text.
type onMessage struct {
	text string
	do   func()
	once sync.Once
}

func (h *onMessage) Enabled(context.Context, slog.Level) bool { return true }
func (h *onMessage) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *onMessage) WithGroup(string) slog.Handler            { return h }
func (h *onMessage) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.text) {
		h.once.Do(h.do)
	}
	return nil
}

// TestEveryGraphPublicationRetiresCachedAnswers: a cached search answer never
// outlives the publication of a new graph or strategy, whichever path
// publishes it (decision B, 17 Oct): the rebuild swap, the restore from disk
// and the runtime transition swap. The initial build is covered by
// TestApproximateReadDuringBackgroundBuildAnswersExactly.
func TestEveryGraphPublicationRetiresCachedAnswers(t *testing.T) {
	t.Setenv("NORNICDB_GPU_BACKEND", "cpu")
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	ctx := context.Background()
	const sentinel = "sentinel-answer"
	cache := func(t *testing.T, svc *Service) {
		t.Helper()
		require.NotNil(t, svc.resultCache)
		svc.resultCache.Put(sentinel, &SearchResponse{Status: "success"})
		require.NotNil(t, svc.resultCache.Get(sentinel))
	}
	retired := func(t *testing.T, svc *Service, path string) {
		t.Helper()
		require.Nil(t, svc.resultCache.Get(sentinel), "a cached answer outlived the %s", path)
	}
	nodes := func(t *testing.T, engine storage.Engine, svc *Service) {
		t.Helper()
		for i, v := range [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}, {1, 1, 0}} {
			node := admissionNode("nornic:n"+string(rune('a'+i)), "text", v, "named")
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node))
		}
	}

	t.Run("rebuild swap", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { require.NoError(t, engine.Close()) })
		svc := configureNodeAdmissionService(t, engine, t.TempDir(), "v2", nil)
		require.NoError(t, svc.BuildIndexes(ctx))
		nodes(t, engine, svc)
		_, err := svc.getOrCreateHNSWIndex(ctx, 3)
		require.NoError(t, err)
		cache(t, svc)
		require.NoError(t, svc.maybeRebuildHNSW(ctx, -1, -1, time.Nanosecond))
		retired(t, svc, "rebuild swap")
	})

	t.Run("restore from disk", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { require.NoError(t, engine.Close()) })
		dir := t.TempDir()
		seed := configureNodeAdmissionService(t, engine, dir, "v2", nil)
		require.NoError(t, seed.BuildIndexes(ctx))
		nodes(t, engine, seed)
		_, err := seed.getOrCreateHNSWIndex(ctx, 3)
		require.NoError(t, err)
		seed.PersistIndexesToDisk()
		require.NoError(t, seed.Close())

		restored := configureNodeAdmissionService(t, engine, dir, "v2", nil)
		// Cache an answer inside the build, after its sources are restored and
		// before the saved graph is published.
		inserted := false
		restored.SetLogger(slog.New(&onMessage{text: "Search indexes loaded from disk", do: func() {
			cache(t, restored)
			inserted = true
		}}))
		require.NoError(t, restored.BuildIndexes(ctx))
		require.True(t, inserted, "the build did not restore its sources from disk")
		restored.hnswMu.RLock()
		graph := restored.hnswIndex
		restored.hnswMu.RUnlock()
		require.NotNil(t, graph, "the saved graph was not restored")
		retired(t, restored, "restore from disk")
	})

	t.Run("runtime transition swap", func(t *testing.T) {
		engine := storage.NewMemoryEngine()
		t.Cleanup(func() { require.NoError(t, engine.Close()) })
		svc := NewServiceWithDimensions(engine, 3)
		t.Cleanup(func() { _ = svc.Close() })
		require.NoError(t, svc.BuildIndexes(ctx))
		nodes(t, engine, svc)
		graph, err := svc.getOrCreateHNSWIndex(ctx, 3)
		require.NoError(t, err)
		cache(t, svc)
		svc.indexMu.Lock()
		svc.applyTransitionSwapLocked(strategyModeHNSW, graph, svc.vectorIndex, nil)
		svc.indexMu.Unlock()
		retired(t, svc, "runtime transition swap")
	})
}
