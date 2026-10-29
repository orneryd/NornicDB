package search

// Live writes during an explicit build: they never wait for it, and the
// indexes after the build hold exactly what they would hold had the writes
// waited. The build reads a storage snapshot the writes overtake, drops BM25
// during the warmup and reloads it, and saves its indexes; none of that loses
// a write.

import (
	"context"
	"fmt"

	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nodeSnapshotGateEngine reads the stored nodes for the build, then parks
// before applying them: a snapshot live writes overtake.
type nodeSnapshotGateEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once             sync.Once
	releaseOnce      sync.Once
}

func newNodeSnapshotGateEngine(t *testing.T) *nodeSnapshotGateEngine {
	t.Helper()
	e := &nodeSnapshotGateEngine{MemoryEngine: newTestMemoryEngine(t), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { e.open(); _ = e.MemoryEngine.Close() })
	return e
}

func (e *nodeSnapshotGateEngine) open() { e.releaseOnce.Do(func() { close(e.release) }) }

// OpenReadSnapshot gates the build's node read through its snapshot, as
// IterateNodes gates a read of the engine itself.
func (e *nodeSnapshotGateEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.MemoryEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &hookedReadSnapshot{ReadSnapshotView: inner, beforeNodes: func() error {
		if buildWriteCallerOnStack("BuildIndexes") {
			e.once.Do(func() { close(e.entered) })
			<-e.release
		}
		return nil
	}}, nil
}

// hookedReadSnapshot runs a test engine's hook before the build reads through
// its snapshot, as the engine's own scans run it. The snapshot's content is
// fixed when it opens, so a hook that parks lets the test change storage
// after the build's read point.
type hookedReadSnapshot struct {
	storage.ReadSnapshotView
	beforeNodes, beforeEdges func() error
}

func (s *hookedReadSnapshot) IterateNodes(fn func(*storage.Node) bool) error {
	if s.beforeNodes != nil {
		if err := s.beforeNodes(); err != nil {
			return err
		}
	}
	return s.ReadSnapshotView.IterateNodes(fn)
}

func (s *hookedReadSnapshot) StreamEdges(ctx context.Context, fn func(*storage.Edge) error) error {
	if s.beforeEdges != nil {
		if err := s.beforeEdges(); err != nil {
			return err
		}
	}
	return s.ReadSnapshotView.StreamEdges(ctx, fn)
}

func (e *nodeSnapshotGateEngine) IterateNodes(fn func(*storage.Node) bool) error {
	var snapshot []*storage.Node
	if err := e.MemoryEngine.IterateNodes(func(node *storage.Node) bool {
		snapshot = append(snapshot, storage.CopyNode(node))
		return true
	}); err != nil {
		return err
	}
	if buildWriteCallerOnStack("BuildIndexes") {
		e.once.Do(func() { close(e.entered) })
		<-e.release
	}
	for _, node := range snapshot {
		if !fn(node) {
			break
		}
	}
	return nil
}

func buildWriteCallerOnStack(name string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, name) {
			return true
		}
		if !more {
			return false
		}
	}
}

// warmupGateLog parks the build in its warmup, after it dropped BM25.
type warmupGateLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *warmupGateLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *warmupGateLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *warmupGateLog) WithGroup(string) slog.Handler            { return h }
func (h *warmupGateLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "cleared BM25 in-memory state") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

func buildWriteNode(id, text string, vec []float32) *storage.Node {
	return &storage.Node{ID: storage.NodeID(id), Labels: []string{"Doc"}, Properties: map[string]any{"text": text}, NamedEmbeddings: map[string][]float32{"v": vec}}
}

func buildWriteService(t *testing.T, engine storage.Engine, dir, backend string, options *ServiceOptions) *Service {
	t.Helper()
	svc := NewService(engine, 3, options)
	svc.FixVectorDimensions()
	if dir != "" {
		svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
		svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
		svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
		svc.SetPersistenceEnabled(true)
	}
	return svc
}

func requireReturnsDuringBuild(t *testing.T, what string, write func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(5 * time.Second):
		t.Fatalf("%s waited for the build (memory headroom=%d)\n%s", what, memoryHeadroom(), serviceLockStacks())
	}
}

func buildWriteSearchIDs(t *testing.T, svc *Service, query string) []string {
	t.Helper()
	resp, err := svc.Search(context.Background(), query, nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		ids = append(ids, r.ID)
	}
	return ids
}

func requireBuildWriteVector(t *testing.T, svc *Service, id string, want []float32) {
	t.Helper()
	got, ok := svc.getVectorLookup()(id)
	require.True(t, ok, "vector %s missing", id)
	require.InDeltaSlice(t, want, got, 1e-6, "vector %s", id)
}

// TestBuildLiveWritesOvertakeTheSnapshot: writes made while the build holds
// an older storage snapshot return at once and win over that snapshot: an
// update, a delete, a create and an edge write.
func TestBuildLiveWritesOvertakeTheSnapshot(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, backend := range []string{"v1", "v2"} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/persisted=%t", backend, persisted), func(t *testing.T) {
				engine := newNodeSnapshotGateEngine(t)
				dir := ""
				if persisted {
					dir = t.TempDir()
				}
				svc := buildWriteService(t, engine, dir, backend, nil)
				t.Cleanup(func() { _ = svc.Close() })
				// Open the gate before the service closes, so Close can join the build.
				t.Cleanup(engine.open)
				for _, n := range []*storage.Node{
					buildWriteNode("nornic:upd", "oldterm", []float32{1, 0, 0}),
					buildWriteNode("nornic:del", "goneterm", []float32{0, 1, 0}),
					buildWriteNode("nornic:end", "endterm", []float32{1, 1, 0}),
				} {
					_, err := engine.CreateNode(n)
					require.NoError(t, err)
				}
				edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:upd", EndNode: "nornic:end", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
				require.NoError(t, engine.CreateEdge(edge))

				built := make(chan error, 1)
				go func() { built <- svc.BuildIndexes(context.Background()) }()
				select {
				case <-engine.entered:
				case <-time.After(10 * time.Second):
					t.Fatal("the build did not read its node snapshot")
				}

				updated := buildWriteNode("nornic:upd", "newterm", []float32{0, 0, 1})
				require.NoError(t, engine.UpdateNode(updated))
				requireReturnsDuringBuild(t, "an update", func() error { return svc.IndexNode(updated) })
				require.NoError(t, engine.DeleteNode("nornic:del"))
				requireReturnsDuringBuild(t, "a delete", func() error { return svc.RemoveNode("nornic:del") })
				created := buildWriteNode("nornic:new", "freshterm", []float32{0, 1, 0})
				_, err := engine.CreateNode(created)
				require.NoError(t, err)
				requireReturnsDuringBuild(t, "a create", func() error { return svc.IndexNode(created) })
				edgeUpdate := &storage.Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode, Type: edge.Type, Properties: map[string]any{"v": []float32{0, 1, 0}}}
				require.NoError(t, engine.UpdateEdge(edgeUpdate))
				requireReturnsDuringBuild(t, "an edge write", func() error { return svc.IndexEdge(edgeUpdate) })

				engine.open()
				require.NoError(t, <-built)
				require.Equal(t, []string{"nornic:upd"}, buildWriteSearchIDs(t, svc, "newterm"))
				require.Empty(t, buildWriteSearchIDs(t, svc, "oldterm"), "the snapshot's older text replaced the live update")
				require.Empty(t, buildWriteSearchIDs(t, svc, "goneterm"), "the snapshot resurrected a deleted node")
				require.Equal(t, []string{"nornic:new"}, buildWriteSearchIDs(t, svc, "freshterm"))
				requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:upd", "v"), []float32{0, 0, 1})
				_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:del", "v"))
				require.False(t, ok, "the deleted node's vector came back")
				requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:new", "v"), []float32{0, 1, 0})
				svc.mu.RLock()
				got := svc.edgePropVector[string(edge.ID)]["v"]
				svc.mu.RUnlock()
				require.Equal(t, []float32{0, 1, 0}, got, "the stream's older edge replaced the live edge write")
			})
		}
	}
}

// TestBuildLiveWritesDuringWarmupSurviveTheBM25Reload: the build drops BM25
// during its warmup and reloads the saved copy. Writes made meanwhile return
// at once, are not lost to the reload, and a restart serves them.
func TestBuildLiveWritesDuringWarmupSurviveTheBM25Reload(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			dir := t.TempDir()
			engine := newTestMemoryEngine(t)
			t.Cleanup(func() { _ = engine.Close() })
			_, err := engine.CreateNode(buildWriteNode("nornic:a", "oldterm", []float32{1, 0, 0}))
			require.NoError(t, err)
			svc := buildWriteService(t, engine, dir, backend, nil)
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = svc.Close()
				}
			})
			gate := &warmupGateLog{entered: make(chan struct{}), release: make(chan struct{})}
			release := sync.OnceFunc(func() { close(gate.release) })
			t.Cleanup(release)
			svc.SetLogger(slog.New(gate))

			built := make(chan error, 1)
			go func() { built <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-gate.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the build did not reach its warmup")
			}
			updated := buildWriteNode("nornic:a", "newterm", []float32{0, 1, 0})
			require.NoError(t, engine.UpdateNode(updated))
			requireReturnsDuringBuild(t, "an update during the warmup", func() error { return svc.IndexNode(updated) })
			created := buildWriteNode("nornic:b", "bterm", []float32{0, 0, 1})
			_, err = engine.CreateNode(created)
			require.NoError(t, err)
			requireReturnsDuringBuild(t, "a create during the warmup", func() error { return svc.IndexNode(created) })
			release()
			require.NoError(t, <-built)

			require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "newterm"))
			require.Empty(t, buildWriteSearchIDs(t, svc, "oldterm"), "the BM25 reload replaced the live update")
			require.Equal(t, []string{"nornic:b"}, buildWriteSearchIDs(t, svc, "bterm"))
			requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:a", "v"), []float32{0, 1, 0})
			svc.PersistIndexesToDisk()
			closed = true
			require.NoError(t, svc.Close())

			restarted := buildWriteService(t, engine, dir, backend, nil)
			t.Cleanup(func() { _ = restarted.Close() })
			require.NoError(t, restarted.BuildIndexes(context.Background()))
			require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, restarted, "newterm"))
			require.Empty(t, buildWriteSearchIDs(t, restarted, "oldterm"))
			require.Equal(t, []string{"nornic:b"}, buildWriteSearchIDs(t, restarted, "bterm"))
		})
	}
}

// TestBuildBudgetedWritesWaitForTheRealAnswer: with an index memory budget a
// write still waits for the build, so it returns the budget's answer.
func TestBuildBudgetedWritesWaitForTheRealAnswer(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	_, err := engine.CreateNode(buildWriteNode("nornic:a", "aterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	svc := buildWriteService(t, engine, "", "v2", &ServiceOptions{VectorMemoryMaxBytes: 1 << 30})
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	b := buildWriteNode("nornic:b", "bterm", []float32{0, 1, 0})
	_, err = engine.CreateNode(b)
	require.NoError(t, err)
	written := make(chan error, 1)
	go func() { written <- svc.IndexNode(b) }()
	select {
	case err := <-written:
		t.Fatalf("a budgeted write returned during the build: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	engine.open()
	require.NoError(t, <-built)
	require.NoError(t, <-written)
	require.Equal(t, []string{"nornic:b"}, buildWriteSearchIDs(t, svc, "bterm"))
}

// TestBuildQueuedWriteIsRejectedLikeALiveWrite: a write the live path would
// reject (an embedding of the wrong dimensions on a fixed-dimension index) is
// rejected during a build too, with the same error, and the build keeps the
// stored node instead of dropping it.
func TestBuildQueuedWriteIsRejectedLikeALiveWrite(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			engine := newNodeSnapshotGateEngine(t)
			svc := buildWriteService(t, engine, "", backend, nil)
			t.Cleanup(func() { _ = svc.Close() })
			t.Cleanup(engine.open)
			_, err := engine.CreateNode(buildWriteNode("nornic:keep", "keepterm", []float32{1, 0, 0}))
			require.NoError(t, err)
			built := make(chan error, 1)
			go func() { built <- svc.BuildIndexes(context.Background()) }()
			select {
			case <-engine.entered:
			case <-time.After(10 * time.Second):
				t.Fatal("the build did not read its node snapshot")
			}
			malformed := buildWriteNode("nornic:keep", "badterm", []float32{1, 0})
			done := make(chan error, 1)
			go func() { done <- svc.IndexNode(malformed) }()
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrDimensionMismatch, "a write the live path rejects was acknowledged during the build")
			case <-time.After(5 * time.Second):
				t.Fatal("the write waited for the build")
			}
			engine.open()
			require.NoError(t, <-built)
			require.Equal(t, []string{"nornic:keep"}, buildWriteSearchIDs(t, svc, "keepterm"), "the build dropped the stored node")
			require.Empty(t, buildWriteSearchIDs(t, svc, "badterm"))
			requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:keep", "v"), []float32{1, 0, 0})
		})
	}
}

// TestBuildQueuedWriteOwnsItsValues: a caller may reuse its buffers once
// IndexNode or IndexEdge returns. The build indexes the values the call
// passed, not what the caller wrote into the buffers afterwards.
func TestBuildQueuedWriteOwnsItsValues(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	for _, n := range []*storage.Node{buildWriteNode("nornic:s", "sterm", []float32{1, 0, 0}), buildWriteNode("nornic:e", "eterm", []float32{0, 1, 0})} {
		_, err := engine.CreateNode(n)
		require.NoError(t, err)
	}
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	named, chunk, prop, edgeVec := []float32{0, 0, 1}, []float32{0, 1, 0}, []float32{1, 0, 0}, []float32{0, 0, 1}
	node := &storage.Node{ID: "nornic:buf", Labels: []string{"Doc"}, Properties: map[string]any{"text": "bufterm", "p": prop},
		NamedEmbeddings: map[string][]float32{"v": named}, ChunkEmbeddings: [][]float32{chunk}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": edgeVec}}
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(edge)))
	requireReturnsDuringBuild(t, "a node write", func() error { return svc.IndexNode(node) })
	requireReturnsDuringBuild(t, "an edge write", func() error { return svc.IndexEdge(edge) })
	// The caller reuses its buffers.
	for _, buf := range [][]float32{named, chunk, prop, edgeVec} {
		buf[0], buf[1], buf[2] = 0.5, 0.5, 0.5
	}
	node.Properties["text"] = "reusedterm"
	engine.open()
	require.NoError(t, <-built)
	requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:buf", "v"), []float32{0, 0, 1})
	requireBuildWriteVector(t, svc, "nornic:buf", []float32{0, 1, 0})
	requireBuildWriteVector(t, svc, nodePropertyVectorKey("nornic:buf", "p"), []float32{1, 0, 0})
	require.Equal(t, []string{"nornic:buf"}, buildWriteSearchIDs(t, svc, "bufterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "reusedterm"))
	svc.mu.RLock()
	got := svc.edgePropVector[string(edge.ID)]["v"]
	svc.mu.RUnlock()
	require.Equal(t, []float32{0, 0, 1}, got, "the build indexed the caller's reused edge buffer")
}

// BenchmarkQueuedNodeCopy measures the copy a write handed to a build makes:
// a node with a 1024-dimension chunk embedding, a 1024-dimension named
// embedding, a 1024-dimension property vector and text properties. Writes
// outside a build make no copy.
func BenchmarkQueuedNodeCopy(b *testing.B) {
	vec := func() []float32 {
		v := make([]float32, 1024)
		for i := range v {
			v[i] = float32(i) / 1024
		}
		return v
	}
	node := &storage.Node{ID: "nornic:bench", Labels: []string{"Doc", "Chunk"},
		Properties:      map[string]any{"title": "a title", "content": strings.Repeat("text ", 200), "p": vec()},
		NamedEmbeddings: map[string][]float32{"default": vec()}, ChunkEmbeddings: [][]float32{vec()},
		EmbedMeta: map[string]any{"embedding_space": "model-1024"}}
	b.ReportAllocs()
	for range b.N {
		if copied, _ := copyQueuedNode(node); copied == nil {
			b.Fatal("nil copy")
		}
	}
}

// TestBuildFirstVectorChoosesTheDimensionInOrder: on an empty index whose
// dimensions are not fixed, the first vector chooses them. A write that
// waited for the build would come after the build's first stored vector, so a
// write with a different dimension fails with ErrDimensionMismatch, and the
// build's vectors keep their dimension.
func TestBuildFirstVectorChoosesTheDimensionInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(buildWriteNode("nornic:stored", "storedterm", []float32{1, 0, 0, 0}))
	require.NoError(t, err)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	written := make(chan error, 1)
	go func() { written <- svc.IndexNode(buildWriteNode("nornic:live", "liveterm", []float32{0, 1, 0})) }()
	engine.open()
	require.NoError(t, <-built)
	select {
	case err := <-written:
		require.ErrorIs(t, err, ErrDimensionMismatch, "a write overtook the build's choice of the dimensions")
	case <-time.After(10 * time.Second):
		t.Fatal("the write did not finish after the build")
	}
	require.Equal(t, 4, svc.VectorIndexDimensions())
	requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:stored", "v"), []float32{1, 0, 0, 0})
	_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:live", "v"))
	require.False(t, ok)
}

// TestBuildPropertyIndexRemovalIsOrderedWithQueuedWrites: a property-vector
// index removal made after a write was acknowledged removes that write's
// vector too, as the same calls one after another would.
func TestBuildPropertyIndexRemovalIsOrderedWithQueuedWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	node := &storage.Node{ID: "nornic:p", Labels: []string{"Doc"}, Properties: map[string]any{"text": "pterm", "p": []float32{1, 0, 0}}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write with a property vector", func() error { return svc.IndexNode(node) })
	svc.RemovePropertyVectorIndex("p")
	engine.open()
	require.NoError(t, <-built)
	require.Zero(t, svc.CountPropertyVectorEntries("p"), "an acknowledged write overtook the property-vector index removal")
	_, ok := svc.getVectorLookup()(nodePropertyVectorKey("nornic:p", "p"))
	require.False(t, ok)
	require.Equal(t, []string{"nornic:p"}, buildWriteSearchIDs(t, svc, "pterm"))
}

// TestBuildIndexingSettingIsOrderedWithQueuedWrites: a write acknowledged
// before an indexing setting changes is indexed under the earlier setting, as
// the same calls one after another would.
func TestBuildIndexingSettingIsOrderedWithQueuedWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	node := &storage.Node{ID: "nornic:t", Labels: []string{"Doc"}, Properties: map[string]any{"title": "titleterm", "other": "otherterm"}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write", func() error { return svc.IndexNode(node) })
	svc.SetFulltextProperties([]string{"other"})
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:t"}, buildWriteSearchIDs(t, svc, "titleterm"), "the write was indexed under a setting made after it")
}

// TestBuildRelationshipRemovalIsOrderedWithQueuedEdgeWrites: a storage
// event's RemoveRelationship made after an IndexEdge was acknowledged leaves
// the relationship out of the relationship index, as the same calls one after
// another would.
func TestBuildRelationshipRemovalIsOrderedWithQueuedEdgeWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	svc.EnableRelationshipSearch(nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	for _, n := range []*storage.Node{buildWriteNode("nornic:s", "sterm", []float32{1, 0, 0}), buildWriteNode("nornic:e", "eterm", []float32{0, 1, 0})} {
		_, err := engine.CreateNode(n)
		require.NoError(t, err)
	}
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "relterm", "v": []float32{1, 0, 0}}}
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(edge)))
	requireReturnsDuringBuild(t, "an edge write", func() error { return svc.IndexEdge(edge) })
	require.NoError(t, engine.DeleteEdge(edge.ID))
	svc.RemoveRelationship(edge.ID)
	engine.open()
	require.NoError(t, <-built)
	rs := svc.relationshipSearch()
	require.NotNil(t, rs)
	rs.mu.Lock()
	_, present := rs.entries[string(edge.ID)]
	hits := rs.fulltext.Search("relterm", 10)
	rs.mu.Unlock()
	require.False(t, present, "an acknowledged edge write restored a relationship removed after it")
	require.Empty(t, hits, "an acknowledged edge write restored the removed relationship's keyword document")
}

type queuedTypedStruct struct {
	Values []uint16
}

// queuedTypedProperties returns property values of typed containers the
// deferred apply can read, and a function that rewrites every buffer in them
// the way a caller reusing its buffers would.
func queuedTypedProperties() (map[string]any, func(byte)) {
	u64, i32, nested, arr := []uint64{424242}, []int32{737373}, []uint64{515151}, [2][]int64{{262626}, {0}}
	ptr, st := &[]int{1}, queuedTypedStruct{Values: []uint16{7}}
	inner := []uint8{9}
	props := map[string]any{
		"text": "typedterm", "u64": u64, "i32": i32,
		"nested": map[string]any{"deep": map[string][]uint64{"k": nested}},
		"array":  arr, "ptr": ptr, "struct": st, "list": []map[string]any{{"b": inner}},
	}
	rewrite := func(b byte) {
		u64[0], i32[0], nested[0], arr[0][0] = uint64(b), int32(b), uint64(b), int64(b)
		(*ptr)[0], st.Values[0], inner[0] = int(b), uint16(b), b
	}
	return props, rewrite
}

// TestQueuedValuesAreOwnedWhateverTheirType: the queued copy of a property
// value shares no buffer with the caller, for any container type.
func TestQueuedValuesAreOwnedWhateverTheirType(t *testing.T) {
	props, rewrite := queuedTypedProperties()
	copied := cloneQueuedMap(props)
	want, _ := queuedTypedProperties()
	rewrite(1)
	require.Equal(t, want, copied, "the queued copy changed with the caller's buffers")
}

// TestBuildQueuedWriteOwnsTypedContainers: a caller rewriting typed property
// buffers (read by the chunk-span fallback's JSON encoding, among others)
// after IndexNode returns does not race the build applying the write.
func TestBuildQueuedWriteOwnsTypedContainers(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	props, rewrite := queuedTypedProperties()
	node := &storage.Node{ID: "nornic:typed", Labels: []string{"Doc"}, Properties: props, ChunkEmbeddings: [][]float32{{0, 1, 0}}}
	// The stored copy only has to exist (the build's warmup removes orphans).
	_, err := engine.CreateNode(&storage.Node{ID: node.ID, Labels: []string{"Doc"}, Properties: map[string]any{"text": "typedterm"}})
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write with typed containers", func() error { return svc.IndexNode(node) })
	stop := make(chan struct{})
	rewriting := make(chan struct{})
	go func() {
		defer close(rewriting)
		for b := byte(0); ; b++ {
			select {
			case <-stop:
				return
			default:
				rewrite(b)
			}
		}
	}()
	engine.open()
	require.NoError(t, <-built)
	close(stop)
	<-rewriting
	require.Equal(t, []string{"nornic:typed"}, buildWriteSearchIDs(t, svc, "typedterm"))
	requireBuildWriteVector(t, svc, "nornic:typed", []float32{0, 1, 0})
}
