package search

import (
	"context"
	"errors"
	"fmt"

	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type failedPopulationEngine struct {
	*storage.MemoryEngine
	mode             string
	active           atomic.Bool
	entered, release chan struct{}
	once             sync.Once
}

type failedPopulationSnapshot struct {
	storage.ReadSnapshotView
	e   *failedPopulationEngine
	ctx context.Context
}

var errPopulationRead = errors.New("injected population read failure")

func (e *failedPopulationEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	if e.active.Load() && e.mode == "open" && buildWriteCallerOnStack("openBuildSnapshot") {
		e.once.Do(func() { close(e.entered); <-e.release })
		return nil, errPopulationRead
	}
	inner, err := e.MemoryEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &failedPopulationSnapshot{ReadSnapshotView: inner, e: e, ctx: ctx}, nil
}

func (s *failedPopulationSnapshot) IterateNodes(fn func(*storage.Node) bool) error {
	if !s.e.active.Load() || s.e.mode == "edge" {
		return s.ReadSnapshotView.IterateNodes(fn)
	}
	if err := s.ReadSnapshotView.IterateNodes(func(n *storage.Node) bool { fn(n); return false }); err != nil {
		return err
	}
	s.e.once.Do(func() { close(s.e.entered); <-s.e.release })
	if s.e.mode == "cancel" {
		return s.ctx.Err()
	}
	return errPopulationRead
}

func (s *failedPopulationSnapshot) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	if s.e.active.Load() && s.e.mode == "edge" {
		s.e.once.Do(func() { close(s.e.entered); <-s.e.release })
		return errPopulationRead
	}
	return s.ReadSnapshotView.StreamEdges(ctx, fn)
}

func populationPersistService(t *testing.T, engine storage.Engine, dir string) *Service {
	t.Helper()
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	svc.FixVectorDimensions()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
	svc.SetPersistenceEnabled(true)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func populationArtifactBytes(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := map[string][]byte{}
	for _, name := range []string{"bm25", "vectors.meta", "build_settings", "hnsw"} {
		body, err := os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) {
			continue
		}
		require.NoError(t, err)
		files[name] = body
	}
	return files
}

func TestFailedPopulationCannotPersistOrReplaceArtifacts(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	t.Setenv("NORNICDB_SEARCH_INDEX_PERSIST_DELAY_SEC", "1")
	for _, mode := range []string{"cancel", "node", "edge", "open"} {
		for _, entry := range []string{"timer", "shutdown", "direct"} {
			t.Run(mode+"/"+entry, func(t *testing.T) {
				e := &failedPopulationEngine{MemoryEngine: newTestMemoryEngine(t), mode: mode, entered: make(chan struct{}), release: make(chan struct{})}
				t.Cleanup(func() { _ = e.Close() })
				for i := 0; i < 3; i++ {
					_, err := e.CreateNode(triageNode(fmt.Sprintf("nornic:original%d", i)))
					require.NoError(t, err)
				}
				dir := t.TempDir()
				seed := populationPersistService(t, e, dir)
				require.NoError(t, seed.BuildIndexes(context.Background()))
				require.NoError(t, seed.Close())
				before := populationArtifactBytes(t, dir)
				committedVectors, vectorErr := os.ReadFile(filepath.Join(dir, "vectors.vec"))
				require.NoError(t, vectorErr)
				require.Len(t, before, 4)
				svc := populationPersistService(t, e, dir)
				// Force a real scan while valid prior artifacts/settings exist.
				svc.SetFulltextProperties([]string{"text"})
				e.active.Store(true)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				open := sync.OnceFunc(func() { close(e.release) })
				t.Cleanup(open)
				done := make(chan error, 1)
				go func() { done <- svc.BuildIndexes(ctx) }()
				select {
				case <-e.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("population did not reach failure boundary")
				}
				node := triageNode("nornic:acknowledged")
				_, err := e.CreateNode(node)
				require.NoError(t, err)
				requireReturnsDuringBuild(t, "acknowledged write", func() error { return svc.IndexNode(node) })
				// An already-fired timer or shutdown flush can enter while the
				// iterator is paused, using a context that is still alive.
				svc.runPersistWithContext(context.Background())
				require.Equal(t, before, populationArtifactBytes(t, dir))
				if mode == "cancel" {
					cancel()
				}
				open()
				select {
				case err := <-done:
					if mode == "cancel" {
						require.ErrorIs(t, err, context.Canceled)
					} else {
						require.ErrorIs(t, err, errPopulationRead)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("failed population did not return")
				}
				require.False(t, svc.IsReady())
				// Timer and shutdown paths use service/background lifetime, not ctx.
				switch entry {
				case "timer":
					svc.schedulePersist()
					time.Sleep(1200 * time.Millisecond)
				case "shutdown":
					svc.PersistIndexesToDisk()
				case "direct":
					require.NoError(t, svc.persistBaseIndexes())
					svc.runPersistWithContext(context.Background())
				}
				after := populationArtifactBytes(t, dir)
				require.Equal(t, before, after, "incomplete sources overwrote valid artifacts")
				vectors, err := os.ReadFile(filepath.Join(dir, "vectors.vec"))
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(vectors), len(committedVectors))
				require.Equal(t, committedVectors, vectors[:len(committedVectors)], "failed build replaced committed vector payloads")
				require.NoError(t, svc.Close())
				e.active.Store(false)
				restarted := populationPersistService(t, e, dir)
				require.NoError(t, restarted.BuildIndexes(context.Background()))
				require.Equal(t, 4, restarted.FulltextDocCount(), "nonempty partial artifacts skipped authoritative rebuild")
				for _, id := range []string{"nornic:original0", "nornic:original1", "nornic:original2", "nornic:acknowledged"} {
					_, ok := restarted.getVectorLookup()(nodeNamedVectorKey(id, "v"))
					require.True(t, ok, id)
				}
			})
		}
	}
}

func TestCancelledWarmupPreservesCommittedPopulationAndRetries(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	e := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = e.Close() })
	_, err := e.CreateNode(triageNode("nornic:original"))
	require.NoError(t, err)
	dir := t.TempDir()
	seed := populationPersistService(t, e, dir)
	require.NoError(t, seed.BuildIndexes(context.Background()))
	require.NoError(t, seed.Close())
	before := populationArtifactBytes(t, dir)
	svc := populationPersistService(t, e, dir)
	svc.SetFulltextProperties([]string{"text"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.SetLogger(slog.New(&triageReplayLog{match: "starting vector pipeline warmup", during: func() {
		n := triageNode("nornic:acknowledged")
		_, err := e.CreateNode(n)
		require.NoError(t, err)
		svc.SetPersistenceEnabled(true) // keep the acknowledged write in the final drain
		require.NoError(t, svc.IndexNode(n))
		cancel()
	}}))
	require.ErrorIs(t, svc.BuildIndexes(ctx), context.Canceled)
	require.False(t, svc.IsReady())
	svc.PersistIndexesToDisk()
	require.Equal(t, before, populationArtifactBytes(t, dir))
	// A same-service retry must clear the incomplete in-memory sources too.
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.True(t, svc.IsReady())
	require.Equal(t, 2, svc.FulltextDocCount())
	_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:acknowledged", "v"))
	require.True(t, ok)
}

func TestInterruptedPopulationRejectsPrebuildPersistenceAndSavedAdmission(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	e := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = e.Close() })
	_, err := e.CreateNode(triageNode("nornic:original"))
	require.NoError(t, err)
	dir := t.TempDir()
	seed := populationPersistService(t, e, dir)
	require.NoError(t, seed.BuildIndexes(context.Background()))
	require.NoError(t, seed.Close())
	before := populationArtifactBytes(t, dir)
	// A process can stop after opening the population, before it returns.
	require.NoError(t, writeMsgpackSnapshot(filepath.Join(dir, "build_settings.population"), &populationRecord{}))
	n := triageNode("nornic:acknowledged")
	_, err = e.CreateNode(n)
	require.NoError(t, err)
	svc := populationPersistService(t, e, dir)
	require.NoError(t, svc.IndexNode(n))
	svc.PersistIndexesToDisk()
	require.Equal(t, before, populationArtifactBytes(t, dir))
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, 2, svc.FulltextDocCount())
	for _, id := range []string{"nornic:original", "nornic:acknowledged"} {
		_, ok := svc.getVectorLookup()(nodeNamedVectorKey(id, "v"))
		require.True(t, ok, id)
	}
}

func TestFailedFinalReplayPreservesCommittedPopulationArtifacts(t *testing.T) {
	t.Setenv("NORNICDB_HNSW_BUILD_GPU_ENABLED", "false")
	e := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = e.Close() })
	_, err := e.CreateNode(triageNode("nornic:original"))
	require.NoError(t, err)
	dir := t.TempDir()
	seed := populationPersistService(t, e, dir)
	require.NoError(t, seed.BuildIndexes(context.Background()))
	require.NoError(t, seed.Close())
	before := populationArtifactBytes(t, dir)
	svc := populationPersistService(t, e, dir)
	svc.SetFulltextProperties([]string{"text"})
	injected := errors.New("final replay rejected")
	svc.SetLogger(slog.New(&triageReplayLog{match: "starting vector pipeline warmup", during: func() {
		svc.vectorFileStore.mu.Lock()
		svc.vectorFileStore.writeRecord = func(*os.File, string, []float32) error { return injected }
		svc.vectorFileStore.mu.Unlock()
		svc.SetPersistenceEnabled(true)
		n := triageNode("nornic:acknowledged")
		_, err := e.CreateNode(n)
		require.NoError(t, err)
		require.NoError(t, svc.IndexNode(n))
	}}))
	require.ErrorIs(t, svc.BuildIndexes(context.Background()), injected)
	svc.PersistIndexesToDisk()
	require.Equal(t, before, populationArtifactBytes(t, dir), "final replay failure published a replacement checkpoint")
	require.NoError(t, svc.Close())
	restarted := populationPersistService(t, e, dir)
	require.NoError(t, restarted.BuildIndexes(context.Background()))
	require.Equal(t, 2, restarted.FulltextDocCount())
	_, ok := restarted.getVectorLookup()(nodeNamedVectorKey("nornic:acknowledged", "v"))
	require.True(t, ok)
}
