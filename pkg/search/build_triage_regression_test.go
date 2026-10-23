package search

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Track actual snapshot ownership, including capability probes. The build's
// relationship scan can be parked after the node baseline has reached disk.
type triageSnapshotEngine struct {
	*storage.MemoryEngine
	active           atomic.Int32
	entered, release chan struct{}
	once             sync.Once
}

type triageSnapshot struct {
	storage.ReadSnapshotView
	owner *triageSnapshotEngine
	once  sync.Once
}

func (e *triageSnapshotEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inner, err := e.MemoryEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	e.active.Add(1)
	return &triageSnapshot{ReadSnapshotView: inner, owner: e}, nil
}

func (v *triageSnapshot) Close() {
	v.once.Do(func() { v.ReadSnapshotView.Close(); v.owner.active.Add(-1) })
}

func (v *triageSnapshot) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	if v.owner.entered != nil {
		v.owner.once.Do(func() { close(v.owner.entered); <-v.owner.release })
	}
	return v.ReadSnapshotView.StreamEdges(ctx, fn)
}

type triageSnapshotLog struct {
	engine         *triageSnapshotEngine
	checks, pinned atomic.Int32
}

func (h *triageSnapshotLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *triageSnapshotLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *triageSnapshotLog) WithGroup(string) slog.Handler            { return h }
func (h *triageSnapshotLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "Persist: saving BM25") || strings.Contains(r.Message, "starting vector pipeline warmup") {
		h.checks.Add(1)
		if h.engine.active.Load() != 0 {
			h.pinned.Add(1)
		}
	}
	return nil
}

func newTriageService(t *testing.T, engine storage.Engine) *Service {
	t.Helper()
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	svc.FixVectorDimensions()
	dir := t.TempDir()
	svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
	svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
	svc.SetPersistenceEnabled(true)
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

func triageNode(id string) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Properties: map[string]any{"text": id}, NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}}
}

func TestBuildTriageNilContext(t *testing.T) {
	engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newTriageService(t, engine)
	require.NotPanics(t, func() { require.NoError(t, svc.BuildIndexes(nil)) })
	require.Zero(t, engine.active.Load())
}

func TestBuildTriageSnapshotClosedBeforePersistAndWarmup(t *testing.T) {
	engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newTriageService(t, engine)
	h := &triageSnapshotLog{engine: engine}
	svc.SetLogger(slog.New(h))
	// Exercise the storage scan and then the saved-index restore route.
	for attempt := 0; attempt < 2; attempt++ {
		h.checks.Store(0)
		h.pinned.Store(0)
		require.NoError(t, svc.BuildIndexes(context.Background()))
		require.Positive(t, h.checks.Load(), "persistence/warmup was not reached")
		require.Zero(t, h.pinned.Load(), "baseline snapshot survived into persistence/warmup")
		require.Zero(t, engine.active.Load())
	}
}

func TestBuildTriageClosesReplacedVectorFileStore(t *testing.T) {
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newTriageService(t, engine)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	previous := svc.vectorFileStore
	require.NotNil(t, previous)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.NotSame(t, previous, svc.vectorFileStore)
	require.ErrorIs(t, previous.Add("retired", []float32{1, 0, 0}), errVecFileClosed,
		"restoring a saved index must release the preceding file and directory capability")
	require.Equal(t, 1, svc.EmbeddingCount())
}

func TestBuildTriageDeferredWriteFailure(t *testing.T) {
	for _, kind := range []string{"named", "main", "chunk", "property", "edge"} {
		t.Run(kind, func(t *testing.T) {
			engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t), entered: make(chan struct{}), release: make(chan struct{})}
			t.Cleanup(func() { _ = engine.Close() })
			_, err := engine.CreateNode(triageNode("nornic:baseline"))
			require.NoError(t, err)
			svc := newTriageService(t, engine)
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(engine.release) }) }
			t.Cleanup(unblock)
			built := make(chan error, 1)
			go func() { built <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-engine.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("build never reached relationship scan")
			}
			injected := errors.New("injected vector file I/O failure")
			svc.indexMu.Lock()
			vfs := svc.vectorFileStore
			require.NotNil(t, vfs)
			vfs.mu.Lock()
			vfs.writeRecord = func(file *os.File, id string, vec []float32) error {
				owner, ok := vectorEntryOwner(id)
				if ok && owner == "nornic:bad" && (kind != "chunk" || id == nodeChunkVectorKey("nornic:bad", 1)) {
					return injected
				}
				return writeVectorRecord(file, id, vec)
			}
			vfs.mu.Unlock()
			svc.indexMu.Unlock()
			var write func() error
			if kind != "edge" {
				bad := triageNode("nornic:bad")
				switch kind {
				case "main":
					bad.NamedEmbeddings = nil
					bad.ChunkEmbeddings = [][]float32{{1, 0, 0}}
				case "chunk":
					bad.NamedEmbeddings = nil
					bad.ChunkEmbeddings = [][]float32{{0, 1, 0}, {1, 0, 0}}
				case "property":
					bad.NamedEmbeddings = nil
					bad.Properties["v"] = []float32{1, 0, 0}
				}
				_, err = engine.CreateNode(bad)
				require.NoError(t, err)
				write = func() error { return svc.IndexNode(bad) }
			} else {
				bad := &storage.Edge{ID: "nornic:bad", StartNode: "nornic:baseline", EndNode: "nornic:baseline", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
				require.NoError(t, engine.CreateEdge(bad))
				write = func() error { return svc.IndexEdge(bad) }
			}
			// The API has acknowledged these writes while the scan is parked.
			ack := make(chan error, 1)
			go func() { ack <- write() }()
			select {
			case err := <-ack:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("write waited for the scan")
			}
			if kind == "edge" {
				// Relationship vectors live in memory. Inject their install-time
				// capacity failure after admission, then retain a later removal.
				require.NoError(t, svc.RemoveNode("nornic:baseline"))
				svc.vectorMemoryMaxBytes = 1
				injected = ErrIndexMemoryBudgetExceeded
			} else {
				good := triageNode("nornic:good")
				_, err = engine.CreateNode(good)
				require.NoError(t, err)
				require.NoError(t, svc.IndexNode(good))
			}
			unblock()
			select {
			case err := <-built:
				require.ErrorIs(t, err, injected, "acknowledged replay failure must reach BuildIndexes")
				require.Contains(t, err.Error(), "nornic:bad")
			case <-time.After(15 * time.Second):
				t.Fatal("build did not finish")
			}
			require.False(t, svc.ready.Load(), "failed replay must not publish readiness")
			if kind == "edge" {
				_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:baseline", "v"))
				require.False(t, found, "one failed replay must not discard a later removal")
			} else {
				_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:good", "v"))
				require.True(t, found, "one failed replay must not discard later acknowledged writes")
			}
			require.Zero(t, engine.active.Load())
		})
	}
}

type triageReplayLog struct {
	match  string
	during func()
	once   sync.Once
}

func (h *triageReplayLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *triageReplayLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *triageReplayLog) WithGroup(string) slog.Handler            { return h }
func (h *triageReplayLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, h.match) {
		h.once.Do(h.during)
	}
	return nil
}

func testTriageLateReplayFailure(t *testing.T, message string, before func(*Service)) {
	t.Helper()
	engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(triageNode("nornic:baseline"))
	require.NoError(t, err)
	svc := newTriageService(t, engine)
	injected := errors.New("injected late vector file I/O failure")
	called := false
	svc.SetLogger(slog.New(&triageReplayLog{match: message, during: func() {
		called = true
		svc.vectorFileStore.mu.Lock()
		svc.vectorFileStore.writeRecord = func(file *os.File, id string, vec []float32) error {
			if owner, ok := vectorEntryOwner(id); ok && owner == "nornic:bad" {
				return injected
			}
			return writeVectorRecord(file, id, vec)
		}
		svc.vectorFileStore.mu.Unlock()
		if before != nil {
			before(svc)
		}
		for _, id := range []string{"nornic:bad", "nornic:good"} {
			node := triageNode(id)
			_, err := engine.CreateNode(node)
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(node), "write should be acknowledged before replay")
		}
	}}))
	err = svc.BuildIndexes(context.Background())
	require.True(t, called, "build did not reach the replay boundary")
	require.ErrorIs(t, err, injected)
	require.Contains(t, err.Error(), "nornic:bad")
	require.False(t, svc.ready.Load())
	_, found := svc.getVectorLookup()(nodeNamedVectorKey("nornic:good", "v"))
	require.True(t, found, "failure discarded a later acknowledged write")
	require.Zero(t, engine.active.Load())
}

func TestBuildTriageFailureDuringBasePersist(t *testing.T) {
	testTriageLateReplayFailure(t, "Persist: saving BM25", nil)
}

func TestBuildTriageLiveWriteIOError(t *testing.T) {
	engine := &triageSnapshotEngine{MemoryEngine: newTestMemoryEngine(t)}
	t.Cleanup(func() { _ = engine.Close() })
	svc := newTriageService(t, engine)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.NoError(t, svc.IndexNode(triageNode("nornic:baseline")))
	svc.indexMu.Lock()
	vfs := svc.vectorFileStore
	vfs.mu.Lock()
	injected := errors.New("injected live vector file I/O failure")
	vfs.writeRecord = func(*os.File, string, []float32) error { return injected }
	vfs.mu.Unlock()
	svc.indexMu.Unlock()
	require.ErrorIs(t, svc.IndexNode(triageNode("nornic:bad")), injected)
}
