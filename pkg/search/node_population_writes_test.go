package search

// Live writes during a population: they never wait for it, and the published
// sources hold exactly what a service without a build would hold. The
// population reads a storage snapshot that the writes overtake, saves BM25
// while they arrive, and parks BM25 only when none changed it.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// snapshotGateEngine reads the stored nodes, then parks the population before
// applying them: a snapshot that live writes overtake.
type snapshotGateEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once             sync.Once
}

func newSnapshotGateEngine(t *testing.T) *snapshotGateEngine {
	t.Helper()
	e := &snapshotGateEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { e.open(); _ = e.MemoryEngine.Close() })
	return e
}

func (e *snapshotGateEngine) open() {
	select {
	case <-e.release:
	default:
		close(e.release)
	}
}

func (e *snapshotGateEngine) IterateNodes(fn func(*storage.Node) bool) error {
	var snapshot []*storage.Node
	if err := e.MemoryEngine.IterateNodes(func(node *storage.Node) bool {
		snapshot = append(snapshot, storage.CopyNode(node))
		return true
	}); err != nil {
		return err
	}
	if callerOnStack("runBasePopulationAttempt") {
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

// persistGateLog parks the population inside its base BM25 save.
type persistGateLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *persistGateLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *persistGateLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *persistGateLog) WithGroup(string) slog.Handler            { return h }
func (h *persistGateLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "Persist: saving BM25") && callerOnStack("persistBaseIndexes") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

// requireReturnsWhileBlocked runs a write while the population is parked and
// requires it to return without waiting for the population.
func requireReturnsWhileBlocked(t *testing.T, what string, write func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- write() }()
	select {
	case err := <-done:
		require.NoError(t, err, what)
	case <-time.After(5 * time.Second):
		t.Fatalf("%s waited for the population", what)
	}
}

func searchIDs(t *testing.T, svc *Service, query string) []string {
	t.Helper()
	resp, err := svc.Search(context.Background(), query, nil, &SearchOptions{Limit: 10})
	require.NoError(t, err)
	ids := make([]string, 0, len(resp.Results))
	for _, r := range resp.Results {
		ids = append(ids, r.ID)
	}
	return ids
}

func requireVector(t *testing.T, svc *Service, id string, want []float32) {
	t.Helper()
	got, ok := svc.getVectorLookup()(id)
	require.True(t, ok, "vector %s missing", id)
	require.InDeltaSlice(t, want, got, 1e-6, "vector %s", id)
}

// TestPopulationLiveWritesOvertakeTheSnapshot: writes made while the
// population holds an older storage snapshot return at once, and win over that
// snapshot: an update, a delete, a create, and an edge write.
func TestPopulationLiveWritesOvertakeTheSnapshot(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		for _, persisted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/persisted=%t", backend, persisted), func(t *testing.T) {
				servingTestEnv(t)
				engine := newSnapshotGateEngine(t)
				var svc *Service
				if persisted {
					svc = configureNodeAdmissionService(t, engine, t.TempDir(), backend, nil)
				} else {
					svc = NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, backend, nil)
					t.Cleanup(func() { require.NoError(t, svc.Close()) })
					svc.FixVectorDimensions()
				}
				// Open the gate before the service closes, so Close can join the build.
				t.Cleanup(engine.open)
				for _, n := range []*storage.Node{
					admissionNode("nornic:upd", "oldterm", []float32{1, 0, 0}, "named"),
					admissionNode("nornic:del", "goneterm", []float32{0, 1, 0}, "named"),
					admissionNode("nornic:end", "endterm", []float32{1, 1, 0}, "named"),
				} {
					_, err := engine.CreateNode(n)
					require.NoError(t, err)
				}
				edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:upd", EndNode: "nornic:end", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
				require.NoError(t, engine.CreateEdge(edge))

				buildDone := make(chan error, 1)
				go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
				waitClosed(t, engine.entered, "the population's node snapshot")

				updated := admissionNode("nornic:upd", "newterm", []float32{0, 0, 1}, "named")
				require.NoError(t, engine.UpdateNode(updated))
				requireReturnsWhileBlocked(t, "an update", func() error { return svc.IndexNode(updated) })
				require.NoError(t, engine.DeleteNode("nornic:del"))
				requireReturnsWhileBlocked(t, "a delete", func() error { return svc.RemoveNode("nornic:del") })
				created := admissionNode("nornic:new", "freshterm", []float32{0, 1, 0}, "named")
				_, err := engine.CreateNode(created)
				require.NoError(t, err)
				requireReturnsWhileBlocked(t, "a create", func() error { return svc.IndexNode(created) })
				edgeUpdate := &storage.Edge{ID: edge.ID, StartNode: edge.StartNode, EndNode: edge.EndNode, Type: edge.Type, Properties: map[string]any{"v": []float32{0, 1, 0}}}
				require.NoError(t, engine.UpdateEdge(edgeUpdate))
				requireReturnsWhileBlocked(t, "an edge write", func() error { return svc.IndexEdge(edgeUpdate) })

				engine.open()
				require.NoError(t, <-buildDone)
				require.Equal(t, []string{"nornic:upd"}, searchIDs(t, svc, "newterm"))
				require.Empty(t, searchIDs(t, svc, "oldterm"), "the snapshot's older text replaced the live update")
				require.Empty(t, searchIDs(t, svc, "goneterm"), "the snapshot resurrected a deleted node")
				require.Equal(t, []string{"nornic:new"}, searchIDs(t, svc, "freshterm"))
				requireVector(t, svc, "nornic:upd-named-v", []float32{0, 0, 1})
				_, ok := svc.getVectorLookup()("nornic:del-named-v")
				require.False(t, ok, "the deleted node's vector came back")
				requireVector(t, svc, "nornic:new-named-v", []float32{0, 1, 0})
				svc.mu.RLock()
				got := svc.edgePropVector[string(edge.ID)]["v"]
				svc.mu.RUnlock()
				require.Equal(t, []float32{0, 1, 0}, got, "the stream's older edge replaced the live edge write")
			})
		}
	}
}

// TestPopulationBasePersistDoesNotParkOverLiveWrites: writes made while the
// explicit build saves its base indexes return at once; the saved BM25 is not
// parked over them, they are searchable after the build, and a restart serves
// them too.
func TestPopulationBasePersistDoesNotParkOverLiveWrites(t *testing.T) {
	for _, backend := range []string{"v1", "v2"} {
		t.Run(backend, func(t *testing.T) {
			servingTestEnv(t)
			dir := t.TempDir()
			engine := storage.NewMemoryEngine()
			t.Cleanup(func() { _ = engine.Close() })
			_, err := engine.CreateNode(admissionNode("nornic:a", "oldterm", []float32{1, 0, 0}, "named"))
			require.NoError(t, err)
			svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, backend, nil)
			closed := false
			t.Cleanup(func() {
				if !closed {
					_ = svc.Close()
				}
			})
			svc.FixVectorDimensions()
			svc.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
			svc.SetVectorIndexPath(filepath.Join(dir, "vectors"))
			svc.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
			svc.SetPersistenceEnabled(true)
			gate := &persistGateLog{entered: make(chan struct{}), release: make(chan struct{})}
			var once sync.Once
			release := func() { once.Do(func() { close(gate.release) }) }
			t.Cleanup(release)
			svc.SetLogger(slog.New(gate))

			buildDone := make(chan error, 1)
			go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
			waitClosed(t, gate.entered, "the base BM25 save")
			updated := admissionNode("nornic:a", "newterm", []float32{0, 1, 0}, "named")
			require.NoError(t, engine.UpdateNode(updated))
			requireReturnsWhileBlocked(t, "an update during the base save", func() error { return svc.IndexNode(updated) })
			created := admissionNode("nornic:b", "bterm", []float32{0, 0, 1}, "named")
			_, err = engine.CreateNode(created)
			require.NoError(t, err)
			requireReturnsWhileBlocked(t, "a create during the base save", func() error { return svc.IndexNode(created) })
			release()
			require.NoError(t, <-buildDone)

			svc.indexMu.RLock()
			parked := svc.nodeSources.fulltextCheckpoint
			svc.indexMu.RUnlock()
			require.Empty(t, parked, "BM25 stayed parked at a checkpoint older than the live writes")
			require.Equal(t, []string{"nornic:a"}, searchIDs(t, svc, "newterm"))
			require.Empty(t, searchIDs(t, svc, "oldterm"))
			require.Equal(t, []string{"nornic:b"}, searchIDs(t, svc, "bterm"))
			requireVector(t, svc, "nornic:a-named-v", []float32{0, 1, 0})
			closed = true
			require.NoError(t, svc.Close())

			restarted := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, backend, nil)
			t.Cleanup(func() { require.NoError(t, restarted.Close()) })
			restarted.FixVectorDimensions()
			restarted.SetFulltextIndexPath(filepath.Join(dir, "bm25"))
			restarted.SetVectorIndexPath(filepath.Join(dir, "vectors"))
			restarted.SetHNSWIndexPath(filepath.Join(dir, "hnsw"))
			restarted.SetPersistenceEnabled(true)
			require.NoError(t, restarted.BuildIndexes(context.Background()))
			require.Equal(t, []string{"nornic:a"}, searchIDs(t, restarted, "newterm"))
			require.Empty(t, searchIDs(t, restarted, "oldterm"))
			require.Equal(t, []string{"nornic:b"}, searchIDs(t, restarted, "bterm"))
			requireVector(t, restarted, "nornic:a-named-v", []float32{0, 1, 0})
		})
	}
}

// TestPopulationBudgetedWritesWaitForTheRealAnswer: with an index memory
// budget a write still waits for the population, so it returns the budget's
// answer.
func TestPopulationBudgetedWritesWaitForTheRealAnswer(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	_, err := engine.CreateNode(admissionNode("nornic:a", "aterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", &ServiceOptions{VectorMemoryMaxBytes: 1 << 30})
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(engine.open)
	svc.FixVectorDimensions()
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")
	b := admissionNode("nornic:b", "bterm", []float32{0, 1, 0}, "named")
	_, err = engine.CreateNode(b)
	require.NoError(t, err)
	writeDone, writerID := make(chan error, 1), make(chan string, 1)
	go func() { writerID <- serviceTestGoroutineID(); writeDone <- svc.IndexNode(b) }()
	id := <-writerID
	require.Eventually(t, func() bool { return serviceStackContains(id, "awaitNodeAdmission") }, 5*time.Second, time.Millisecond)
	engine.open()
	require.NoError(t, <-buildDone)
	require.NoError(t, <-writeDone)
	require.Equal(t, []string{"nornic:b"}, searchIDs(t, svc, "bterm"))
}

// temporalRejectEngine fails the temporal-currency read for a node whose text
// is "badterm": a write the live path would reject with that error.
type temporalRejectEngine struct{ *snapshotGateEngine }

var errTemporalRead = errors.New("temporal read failed")

func (e *temporalRejectEngine) IsCurrentTemporalNode(node *storage.Node, _ time.Time) (bool, error) {
	if node.Properties["text"] == "badterm" {
		return false, errTemporalRead
	}
	return true, nil
}

// TestPopulationQueuedWriteIsRejectedLikeALiveWrite: a write the live path
// rejects is rejected during a population too, with the same error, and the
// population keeps the stored node instead of dropping it.
func TestPopulationQueuedWriteIsRejectedLikeALiveWrite(t *testing.T) {
	servingTestEnv(t)
	gate := newSnapshotGateEngine(t)
	engine := &temporalRejectEngine{gate}
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(gate.open)
	svc.FixVectorDimensions()
	_, err := gate.CreateNode(admissionNode("nornic:keep", "keepterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, gate.entered, "the population's node snapshot")
	done := make(chan error, 1)
	go func() { done <- svc.IndexNode(admissionNode("nornic:keep", "badterm", []float32{0, 1, 0}, "named")) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, errTemporalRead, "a write the live path rejects was acknowledged during the population")
	case <-time.After(5 * time.Second):
		t.Fatal("the write waited for the population")
	}
	gate.open()
	require.NoError(t, <-buildDone)
	require.Equal(t, []string{"nornic:keep"}, searchIDs(t, svc, "keepterm"), "the population dropped the stored node")
	requireVector(t, svc, "nornic:keep-named-v", []float32{1, 0, 0})
}

// TestPopulationQueuedWriteOwnsItsValues: a caller may reuse its buffers once
// IndexNode or IndexEdge returns. The population indexes the values the call
// passed, not what the caller wrote into the buffers afterwards.
func TestPopulationQueuedWriteOwnsItsValues(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(engine.open)
	svc.FixVectorDimensions()
	for _, id := range []string{"nornic:s", "nornic:e"} {
		_, err := engine.CreateNode(admissionNode(id, "node", []float32{1, 0, 0}, "named"))
		require.NoError(t, err)
	}
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")
	named, chunk, prop, edgeVec := []float32{0, 0, 1}, []float32{0, 1, 0}, []float32{1, 0, 0}, []float32{0, 0, 1}
	node := &storage.Node{ID: "nornic:buf", Labels: []string{"Doc"}, Properties: map[string]any{"text": "bufterm", "p": prop},
		NamedEmbeddings: map[string][]float32{"v": named}, ChunkEmbeddings: [][]float32{chunk}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": edgeVec}}
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(edge)))
	requireReturnsWhileBlocked(t, "a node write", func() error { return svc.IndexNode(node) })
	requireReturnsWhileBlocked(t, "an edge write", func() error { return svc.IndexEdge(edge) })
	// The caller reuses its buffers.
	for _, buf := range [][]float32{named, chunk, prop, edgeVec} {
		buf[0], buf[1], buf[2] = 0.5, 0.5, 0.5
	}
	node.Properties["text"] = "reusedterm"
	engine.open()
	require.NoError(t, <-buildDone)
	requireVector(t, svc, "nornic:buf-named-v", []float32{0, 0, 1})
	requireVector(t, svc, "nornic:buf", []float32{0, 1, 0})
	requireVector(t, svc, "nornic:buf-prop-p", []float32{1, 0, 0})
	require.Equal(t, []string{"nornic:buf"}, searchIDs(t, svc, "bufterm"))
	require.Empty(t, searchIDs(t, svc, "reusedterm"))
	svc.mu.RLock()
	got := svc.edgePropVector[string(edge.ID)]["v"]
	svc.mu.RUnlock()
	require.Equal(t, []float32{0, 0, 1}, got, "the population indexed the caller's reused edge buffer")
}

// TestPopulationPropertyIndexRemovalIsOrderedWithQueuedWrites: a
// property-vector index removal made after a write was acknowledged removes
// that write's vector too, as the same calls one after another would.
func TestPopulationPropertyIndexRemovalIsOrderedWithQueuedWrites(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(engine.open)
	svc.FixVectorDimensions()
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")
	node := &storage.Node{ID: "nornic:p", Labels: []string{"Doc"}, Properties: map[string]any{"text": "pterm", "p": []float32{1, 0, 0}}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsWhileBlocked(t, "a write with a property vector", func() error { return svc.IndexNode(node) })
	svc.RemovePropertyVectorIndex("p")
	engine.open()
	require.NoError(t, <-buildDone)
	require.Zero(t, svc.CountPropertyVectorEntries("p"), "an acknowledged write overtook the property-vector index removal")
	_, ok := svc.getVectorLookup()("nornic:p-prop-p")
	require.False(t, ok)
	require.Equal(t, []string{"nornic:p"}, searchIDs(t, svc, "pterm"))
}

// TestPopulationIndexingSettingIsOrderedWithQueuedWrites: a write acknowledged
// before an indexing setting changes is indexed under the earlier setting, as
// the same calls one after another would.
func TestPopulationIndexingSettingIsOrderedWithQueuedWrites(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(engine.open)
	svc.FixVectorDimensions()
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")
	node := &storage.Node{ID: "nornic:t", Labels: []string{"Doc"}, Properties: map[string]any{"title": "titleterm", "other": "otherterm"}}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsWhileBlocked(t, "a write", func() error { return svc.IndexNode(node) })
	svc.SetFulltextProperties([]string{"other"})
	engine.open()
	require.NoError(t, <-buildDone)
	require.Equal(t, []string{"nornic:t"}, searchIDs(t, svc, "titleterm"), "the write was indexed under a setting made after it")
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

// TestPopulationQueuedWriteOwnsTypedContainers: a caller rewriting typed
// property buffers (read by the chunk-span fallback's JSON encoding, among
// others) after IndexNode returns does not race the population applying it.
func TestPopulationQueuedWriteOwnsTypedContainers(t *testing.T) {
	servingTestEnv(t)
	engine := newSnapshotGateEngine(t)
	svc := NewServiceWithDimensionsAndBM25EngineAndOptions(engine, 3, "v2", nil)
	t.Cleanup(func() { require.NoError(t, svc.Close()) })
	t.Cleanup(engine.open)
	svc.FixVectorDimensions()
	buildDone := make(chan error, 1)
	go func() { buildDone <- svc.BuildIndexes(context.Background()) }()
	waitClosed(t, engine.entered, "the population's node snapshot")
	props, rewrite := queuedTypedProperties()
	node := &storage.Node{ID: "nornic:typed", Labels: []string{"Doc"}, Properties: props, ChunkEmbeddings: [][]float32{{0, 1, 0}}}
	// The stored copy only has to exist (the population removes orphans).
	_, err := engine.CreateNode(&storage.Node{ID: node.ID, Labels: []string{"Doc"}, Properties: map[string]any{"text": "typedterm"}})
	require.NoError(t, err)
	requireReturnsWhileBlocked(t, "a write with typed containers", func() error { return svc.IndexNode(node) })
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
	require.NoError(t, <-buildDone)
	close(stop)
	<-rewriting
	require.Equal(t, []string{"nornic:typed"}, searchIDs(t, svc, "typedterm"))
	requireVector(t, svc, "nornic:typed", []float32{0, 1, 0})
}
