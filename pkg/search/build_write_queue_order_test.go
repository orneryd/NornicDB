package search

// The build sequence's contract, member by member and as a whole: calls made
// while a build holds its snapshot leave the indexes, and report to each
// caller, what the same calls made one after another once the build has
// finished would (the serial order). Reads during the build are not compared:
// a write becomes visible when the build installs it.

import (
	"context"
	"fmt"

	"log/slog"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func startGatedBuild(t *testing.T, engine *nodeSnapshotGateEngine, svc *Service) chan error {
	t.Helper()
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its node snapshot")
	}
	return built
}

// temporalWindowEngine reports nodes whose "window" property is "closing" as
// no longer current once expired is set.
type temporalWindowEngine struct {
	*nodeSnapshotGateEngine
	expired atomic.Bool
}

func (e *temporalWindowEngine) IsCurrentTemporalNode(node *storage.Node, _ time.Time) (bool, error) {
	if node.Properties["window"] == "closing" && e.expired.Load() {
		return false, nil
	}
	return true, nil
}

// TestBuildTemporalCurrencyIsDecidedAtTheCall: a node current at the call is
// indexed even if it stops being current before the build installs it.
func TestBuildTemporalCurrencyIsDecidedAtTheCall(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	gate := newNodeSnapshotGateEngine(t)
	engine := &temporalWindowEngine{nodeSnapshotGateEngine: gate}
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(gate.open)
	built := startGatedBuild(t, gate, svc)
	node := &storage.Node{ID: "nornic:w", Labels: []string{"Doc"}, Properties: map[string]any{"text": "windowterm", "window": "closing"}}
	_, err := gate.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write while current", func() error { return svc.IndexNode(node) })
	engine.expired.Store(true)
	gate.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:w"}, buildWriteSearchIDs(t, svc, "windowterm"), "currency was decided at apply, not at the call")
}

func buildWriteEdgeEndpoints(t *testing.T, engine storage.Engine) {
	t.Helper()
	for _, id := range []storage.NodeID{"nornic:s", "nornic:e"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "node"}})
		require.NoError(t, err)
	}
}

// TestBuildStoredVectorChoosesTheDimensionBeforeQueuedVectors: on an index
// whose dimensions are not fixed, the build's first stored vector (here a
// stored edge's) chooses them before any vector written during the build: an
// edge vector or a node's property vector.
func TestBuildStoredVectorChoosesTheDimensionBeforeQueuedVectors(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, kind := range []string{"edge", "property"} {
		t.Run(kind, func(t *testing.T) {
			run := func(gated bool) string {
				engine := newNodeSnapshotGateEngine(t)
				svc := NewService(engine, 4, nil)
				t.Cleanup(func() { _ = svc.Close() })
				t.Cleanup(engine.open)
				buildWriteEdgeEndpoints(t, engine)
				stored := &storage.Edge{ID: "nornic:stored", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
				require.NoError(t, engine.CreateEdge(stored))
				var built chan error
				if gated {
					built = startGatedBuild(t, engine, svc)
				} else {
					engine.open()
					require.NoError(t, svc.BuildIndexes(context.Background()))
				}
				done := make(chan error, 1)
				if kind == "edge" {
					live := &storage.Edge{ID: "nornic:live", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{0, 1}}}
					require.NoError(t, engine.CreateEdge(storage.CopyEdge(live)))
					go func() { done <- svc.IndexEdge(live) }()
				} else {
					node := &storage.Node{ID: "nornic:pv", Labels: []string{"Doc"}, Properties: map[string]any{"text": "pvterm", "p": []float32{0, 1}}}
					_, err := engine.CreateNode(storage.CopyNode(node))
					require.NoError(t, err)
					go func() { done <- svc.IndexNode(node) }()
				}
				if gated {
					engine.open()
					require.NoError(t, <-built)
				}
				require.NoError(t, <-done)
				return buildReplayState(svc)
			}
			require.Equal(t, run(false), run(true), "a vector written during the build was judged by another dimension choice than after it")
		})
	}
}

// TestBuildFirstWriteVectorThenMismatchReportsLikeTheSerialOrder: on an index
// whose dimensions are not fixed, a write with a vector and then an update
// with another dimension, made by one caller during a build, report what the
// same calls report after the build (the update fails with
// ErrDimensionMismatch). Each call waits for the build while the dimensions
// are open, so the caller issues them from its own goroutine.
func TestBuildFirstWriteVectorThenMismatchReportsLikeTheSerialOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	run := func(gated bool) ([]error, string) {
		engine := newNodeSnapshotGateEngine(t)
		_, err := engine.CreateNode(buildWriteNode("nornic:keep", "keepterm", []float32{1, 0, 0}))
		require.NoError(t, err)
		svc := NewService(engine, 3, nil)
		t.Cleanup(func() { _ = svc.Close() })
		t.Cleanup(engine.open)
		var built chan error
		if gated {
			built = startGatedBuild(t, engine, svc)
		} else {
			engine.open()
			require.NoError(t, svc.BuildIndexes(context.Background()))
		}
		done := make(chan []error, 1)
		go func() {
			first := svc.IndexNode(buildWriteNode("nornic:first", "firstterm", []float32{0, 1, 0}))
			done <- []error{first, svc.IndexNode(buildWriteNode("nornic:keep", "badterm", []float32{1, 0}))}
		}()
		if gated {
			engine.open()
			require.NoError(t, <-built)
		}
		errs := <-done
		return errs, buildReplayState(svc) + "\n" + strings.Join(buildWriteSearchIDs(t, svc, "keepterm"), ",")
	}
	serialErrs, serialState := run(false)
	gatedErrs, gatedState := run(true)
	require.NoError(t, serialErrs[0])
	require.ErrorIs(t, serialErrs[1], ErrDimensionMismatch)
	require.NoError(t, gatedErrs[0])
	require.ErrorIs(t, gatedErrs[1], ErrDimensionMismatch, "the call during the build reported another outcome than after it")
	require.Equal(t, serialState, gatedState)
}

// TestBuildRemovalThenRecreationDuringABuild: an orphan cleanup's removal
// followed by the node's re-creation leaves the re-created node indexed.
func TestBuildRemovalThenRecreationDuringABuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(buildWriteNode("nornic:o", "oterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	built := startGatedBuild(t, engine, svc)
	requireReturnsDuringBuild(t, "a removal", func() error { return svc.RemoveNode("nornic:o") })
	again := buildWriteNode("nornic:o", "againterm", []float32{0, 1, 0})
	require.NoError(t, engine.UpdateNode(again))
	requireReturnsDuringBuild(t, "a re-creation", func() error { return svc.IndexNode(again) })
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:o"}, buildWriteSearchIDs(t, svc, "againterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "oterm"))
}

// finalPersistGateLog parks the first build in its final save.
type finalPersistGateLog struct {
	entered, release chan struct{}
	once             sync.Once
}

func (h *finalPersistGateLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *finalPersistGateLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *finalPersistGateLog) WithGroup(string) slog.Handler            { return h }
func (h *finalPersistGateLog) Handle(_ context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "Persist: syncing") && buildWriteCallerOnStack("finishBuildWithFinalPersist") {
		h.once.Do(func() { close(h.entered); <-h.release })
	}
	return nil
}

// TestBuildOverlappingPersistedBuildsKeepTheTail: a write made after the
// first build's last installation, while a second build waits for buildMu,
// survives the second build's restore of the saved indexes.
func TestBuildOverlappingPersistedBuildsKeepTheTail(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newTestMemoryEngine(t)
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(buildWriteNode("nornic:a", "oldterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	svc := buildWriteService(t, engine, t.TempDir(), "v2", nil)
	gate := &finalPersistGateLog{entered: make(chan struct{}), release: make(chan struct{})}
	release := sync.OnceFunc(func() { close(gate.release) })
	t.Cleanup(func() { release(); _ = svc.Close() })
	svc.SetLogger(slog.New(gate))
	built := make(chan error, 2)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the first build did not reach its final save")
	}
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	require.Eventually(t, func() bool {
		svc.buildWrites.mu.Lock()
		defer svc.buildWrites.mu.Unlock()
		return svc.buildWrites.owners == 2
	}, 5*time.Second, time.Millisecond)
	updated := buildWriteNode("nornic:a", "newterm", []float32{0, 1, 0})
	require.NoError(t, engine.UpdateNode(updated))
	requireReturnsDuringBuild(t, "a tail write", func() error { return svc.IndexNode(updated) })
	release()
	require.NoError(t, <-built)
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "newterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "oldterm"))
	requireBuildWriteVector(t, svc, nodeNamedVectorKey("nornic:a", "v"), []float32{0, 1, 0})
}

// TestBuildPropertyIndexRemovalCoversTheBuildsStoredNodes: a property-vector
// index removal made during the build also removes the vectors the build adds
// for stored nodes afterwards, as a removal after the build would.
func TestBuildPropertyIndexRemovalCoversTheBuildsStoredNodes(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:stored", Labels: []string{"Doc"}, Properties: map[string]any{"text": "storedterm", "p": []float32{1, 0, 0}}})
	require.NoError(t, err)
	built := startGatedBuild(t, engine, svc)
	svc.RemovePropertyVectorIndex("p")
	engine.open()
	require.NoError(t, <-built)
	require.Zero(t, svc.CountPropertyVectorEntries("p"), "the build re-added a property-vector index removed during it")
	require.Equal(t, []string{"nornic:stored"}, buildWriteSearchIDs(t, svc, "storedterm"))
}

// TestBuildIndexingSettingAppliesAfterTheBuild: an indexing setting changed
// during the build applies after it: the build's stored nodes are indexed
// under the setting in force before, as a change after the build would leave
// them.
func TestBuildIndexingSettingAppliesAfterTheBuild(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:stored", Labels: []string{"Doc"}, Properties: map[string]any{"title": "titleterm", "other": "otherterm"}})
	require.NoError(t, err)
	built := startGatedBuild(t, engine, svc)
	svc.SetFulltextProperties([]string{"other"})
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:stored"}, buildWriteSearchIDs(t, svc, "titleterm"), "the build indexed its stored nodes under a setting changed during it")
	require.Equal(t, []string{"other"}, svc.FulltextProperties())
}

// startGatedBuildWithWrite establishes the same acknowledged-write point for
// each ordering check below. The caller controls when the build may finish.
func startGatedBuildWithWrite(t *testing.T, dir string) (*nodeSnapshotGateEngine, *Service, chan error) {
	t.Helper()
	engine := newNodeSnapshotGateEngine(t)
	t.Cleanup(engine.open)
	svc := buildWriteService(t, engine, dir, "v2", nil)
	built := startGatedBuild(t, engine, svc)
	node := buildWriteNode("nornic:a", "aterm", []float32{1, 0, 0})
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write", func() error { return svc.IndexNode(node) })
	return engine, svc, built
}

// TestBuildCloseClearAndPersistenceToggleKeepTheOrder: Close, ClearVectorIndex
// and a persistence toggle made after an acknowledged write.
func TestBuildCloseClearAndPersistenceToggleKeepTheOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	t.Run("clear", func(t *testing.T) {
		engine, svc, built := startGatedBuildWithWrite(t, "")
		t.Cleanup(func() { _ = svc.Close() })
		cleared := make(chan struct{})
		go func() { svc.ClearVectorIndex(); close(cleared) }()
		engine.open()
		<-built
		<-cleared
		_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:a", "v"))
		require.False(t, ok, "an acknowledged write survived a later ClearVectorIndex")
	})
	t.Run("persistence", func(t *testing.T) {
		engine, svc, built := startGatedBuildWithWrite(t, t.TempDir())
		t.Cleanup(func() { _ = svc.Close() })
		toggled := make(chan struct{})
		go func() { svc.SetPersistenceEnabled(false); close(toggled) }()
		engine.open()
		require.NoError(t, <-built)
		<-toggled
		require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, svc, "aterm"))
	})
	t.Run("close", func(t *testing.T) {
		dir := t.TempDir()
		engine, svc, built := startGatedBuildWithWrite(t, dir)
		closed := make(chan error, 1)
		go func() { closed <- svc.Close() }()
		engine.open()
		<-built
		require.NoError(t, <-closed)
		restarted := buildWriteService(t, engine.MemoryEngine, dir, "v2", nil)
		t.Cleanup(func() { _ = restarted.Close() })
		require.NoError(t, restarted.BuildIndexes(context.Background()))
		require.Equal(t, []string{"nornic:a"}, buildWriteSearchIDs(t, restarted, "aterm"))
	})
}

// TestBuildWriteIsVisibleWhenTheBuildInstallsIt pins the visibility contract:
// a write made during a build is not visible to reads during the build, and is
// once the build has finished.
func TestBuildWriteIsVisibleWhenTheBuildInstallsIt(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	a := buildWriteNode("nornic:a", "aterm", []float32{1, 0, 0})
	_, err := engine.CreateNode(storage.CopyNode(a))
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write", func() error { return svc.IndexNode(a) })
	_, visibleDuring := svc.getVectorLookup()(nodeNamedVectorKey("nornic:a", "v"))
	engine.open()
	require.NoError(t, <-built)
	_, visibleAfter := svc.getVectorLookup()(nodeNamedVectorKey("nornic:a", "v"))
	require.False(t, visibleDuring)
	require.True(t, visibleAfter)
}

// --- differential replay -------------------------------------------------

// buildReplayOp is one public call with the storage change it stands for.
type buildReplayOp struct {
	kind  string
	id    string
	text  string
	vec   []float32
	prop  []float32
	keys  []string
	flags [2]bool
}

func buildReplayOps(r *rand.Rand, n int) []buildReplayOp {
	ids := buildReplayIDs
	edges := buildReplayEdgeIDs
	unit := func() []float32 { v := []float32{0, 0, 0}; v[r.Intn(3)] = 1; return v }
	ops := make([]buildReplayOp, 0, n)
	for i := range n {
		switch k := r.Intn(11); {
		case k < 4:
			op := buildReplayOp{kind: "node", id: ids[r.Intn(len(ids))], text: fmt.Sprintf("w%dterm", i), vec: unit()}
			if r.Intn(2) == 0 {
				op.prop = unit()
			}
			ops = append(ops, op)
		case k < 5:
			// nornic:n0 and nornic:n1 are the edges' endpoints: deleting one also
			// deletes the edges in storage, and no delete path clears edge
			// vectors yet (lane B adds it, RefreshEdge), so they are not removed.
			ops = append(ops, buildReplayOp{kind: "remove", id: ids[2+r.Intn(len(ids)-2)]})
		case k < 7:
			ops = append(ops, buildReplayOp{kind: "edge", id: edges[r.Intn(len(edges))], vec: unit(), text: fmt.Sprintf("r%dterm", i)})
		case k < 8:
			ops = append(ops, buildReplayOp{kind: "unrelate", id: edges[r.Intn(len(edges))]})
		case k < 9:
			ops = append(ops, buildReplayOp{kind: "drop"})
		case k < 10:
			keys := [][]string{nil, {"text"}, {"text", "title"}, {"title"}}[r.Intn(4)]
			ops = append(ops, buildReplayOp{kind: "setting", keys: keys})
		default:
			ops = append(ops, buildReplayOp{kind: "flags", flags: [2]bool{r.Intn(3) > 0, r.Intn(3) > 0}})
		}
	}
	return ops
}

func buildReplaySeed(t *testing.T, engine storage.Engine) {
	t.Helper()
	for i := range 5 {
		_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:n%d", i)), Labels: []string{"Doc"},
			Properties:      map[string]any{"text": fmt.Sprintf("seed%dterm", i), "title": fmt.Sprintf("title%dterm", i), "p": []float32{0, 0, 1}},
			NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}})
		require.NoError(t, err)
	}
	for i := range 3 {
		require.NoError(t, engine.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nornic:e%d", i)), StartNode: "nornic:n0", EndNode: "nornic:n1", Type: "REL",
			Properties: map[string]any{"v": []float32{1, 0, 0}, "note": fmt.Sprintf("seedrel%dterm", i)}}))
	}
}

// applyBuildReplayOp makes the storage change, then the public call, then
// reuses the caller's buffers, and returns the call's outcome.
func applyBuildReplayOp(engine storage.Engine, svc *Service, op buildReplayOp) string {
	switch op.kind {
	case "node":
		vec := slicesClone32(op.vec)
		props := map[string]any{"text": op.text, "title": "title" + op.text}
		if op.prop != nil {
			props["p"] = slicesClone32(op.prop)
		}
		node := &storage.Node{ID: storage.NodeID(op.id), Labels: []string{"Doc"}, Properties: props, NamedEmbeddings: map[string][]float32{"v": vec}}
		if _, err := engine.GetNode(node.ID); err != nil {
			_, _ = engine.CreateNode(storage.CopyNode(node))
		} else {
			_ = engine.UpdateNode(storage.CopyNode(node))
		}
		err := svc.IndexNode(node)
		vec[0], vec[1], vec[2] = 0.5, 0.5, 0.5
		props["text"] = "reused"
		return fmt.Sprint(err)
	case "remove":
		_ = engine.DeleteNode(storage.NodeID(op.id))
		return fmt.Sprint(svc.RemoveNode(storage.NodeID(op.id)))
	case "edge":
		vec := slicesClone32(op.vec)
		edge := &storage.Edge{ID: storage.EdgeID(op.id), StartNode: "nornic:n0", EndNode: "nornic:n1", Type: "REL", Properties: map[string]any{"v": vec, "note": op.text}}
		if _, err := engine.GetEdge(edge.ID); err != nil {
			_ = engine.CreateEdge(storage.CopyEdge(edge))
		} else {
			_ = engine.UpdateEdge(storage.CopyEdge(edge))
		}
		err := svc.IndexEdge(edge)
		vec[0], vec[1], vec[2] = 0.5, 0.5, 0.5
		return fmt.Sprint(err)
	case "unrelate":
		svc.RemoveRelationship(storage.EdgeID(op.id))
		return "<nil>"
	case "drop":
		svc.RemovePropertyVectorIndex("p")
		return "<nil>"
	case "setting":
		svc.SetFulltextProperties(op.keys)
		return "<nil>"
	case "flags":
		return fmt.Sprint(svc.SetIndexFlags(op.flags[0], op.flags[1]))
	}
	return "?"
}

func slicesClone32(v []float32) []float32 { return append([]float32(nil), v...) }

var (
	buildReplayIDs     = []string{"nornic:n0", "nornic:n1", "nornic:n2", "nornic:n3", "nornic:n4", "nornic:new0", "nornic:new1"}
	buildReplayEdgeIDs = []string{"nornic:e0", "nornic:e1", "nornic:e2"}
)

// buildReplayState is everything the indexes hold, in a comparable form.
func buildReplayState(svc *Service) string {
	var lines []string
	for _, id := range buildReplayIDs {
		if text, ok := svc.fulltext().GetDocument(id); ok {
			lines = append(lines, "bm25 "+id+" "+text)
		}
	}
	lookup := svc.getVectorLookup()
	dims := svc.VectorIndexDimensions()
	svc.indexMu.RLock()
	svc.mu.RLock()
	for id, labels := range svc.nodeLabels {
		lines = append(lines, fmt.Sprintf("labels %s %v", id, labels))
	}
	for id, named := range svc.nodeNamedVector {
		for name, vid := range named {
			v, _ := lookup(vid)
			lines = append(lines, fmt.Sprintf("named %s %s %.3f", id, name, v))
		}
	}
	for id, props := range svc.nodePropVector {
		for key, vid := range props {
			v, _ := lookup(vid)
			lines = append(lines, fmt.Sprintf("prop %s %s %.3f", id, key, v))
		}
	}
	for id, props := range svc.edgePropVector {
		for key, v := range props {
			lines = append(lines, fmt.Sprintf("edge %s %s %.3f", id, key, v))
		}
	}
	lines = append(lines, fmt.Sprintf("dims %d props %v flags %v %v", dims, svc.fulltextProperties, svc.bm25Enabled.Load(), svc.vectorEnabled.Load()))
	svc.mu.RUnlock()
	svc.indexMu.RUnlock()
	if rs := svc.relationshipSearch(); rs != nil {
		rs.mu.Lock()
		for id := range rs.entries {
			lines = append(lines, "rel "+id)
		}
		for _, id := range buildReplayEdgeIDs {
			if text, ok := rs.fulltext.GetDocument(id); ok {
				lines = append(lines, "reltext "+id+" "+text)
			}
		}
		rs.mu.Unlock()
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// TestBuildSequenceReplaysAsTheSerialOrder replays random sequences of public
// calls made during a build and the same calls made after it, and requires
// equal outcomes and equal indexes.
func TestBuildSequenceReplaysAsTheSerialOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for seed := int64(1); seed <= 80; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ops := buildReplayOps(rand.New(rand.NewSource(seed)), 25)
			run := func(gated bool) ([]string, string) {
				engine := newNodeSnapshotGateEngine(t)
				buildReplaySeed(t, engine)
				// Even seeds use automatic dimensions: every vector write then
				// waits for the build (vectorNeedsOrderedDecision).
				var svc *Service
				if seed%2 == 0 {
					svc = NewService(engine, 3, nil)
				} else {
					svc = buildWriteService(t, engine, filepath.Join(t.TempDir(), "idx"), "v2", nil)
				}
				svc.EnableRelationshipSearch(nil)
				t.Cleanup(func() { _ = svc.Close() })
				t.Cleanup(engine.open)
				var built chan error
				if gated {
					built = startGatedBuild(t, engine, svc)
				} else {
					engine.open()
					require.NoError(t, svc.BuildIndexes(context.Background()))
				}
				// The calls are made one after another by one caller. A call
				// that waits for the build (a vector whose dimension decision
				// is still open) holds the calls after it, so the build is
				// released once the caller has finished or stalls.
				outcomes := make([]string, 0, len(ops))
				progress, finished := make(chan struct{}, len(ops)), make(chan struct{})
				go func() {
					defer close(finished)
					for _, op := range ops {
						outcomes = append(outcomes, applyBuildReplayOp(engine, svc, op))
						progress <- struct{}{}
					}
				}()
				if gated {
				waiting:
					for {
						select {
						case <-finished:
							break waiting
						case <-progress:
						case <-time.After(200 * time.Millisecond):
							break waiting
						}
					}
					engine.open()
					require.NoError(t, <-built)
				}
				<-finished
				return outcomes, buildReplayState(svc)
			}
			serialOut, serialState := run(false)
			gatedOut, gatedState := run(true)
			require.Equal(t, serialOut, gatedOut, "outcomes differ from the serial order; calls: %+v", ops)
			require.Equal(t, serialState, gatedState, "indexes differ from the serial order; calls: %+v", ops)
		})
	}
}

// relationshipLoaderGateEngine reads the stored edges for the relationship
// index's loader, then parks before applying them: a snapshot live
// relationship updates overtake.
type relationshipLoaderGateEngine struct {
	*storage.MemoryEngine
	entered, release  chan struct{}
	once, releaseOnce sync.Once
}

func (e *relationshipLoaderGateEngine) open() { e.releaseOnce.Do(func() { close(e.release) }) }

func (e *relationshipLoaderGateEngine) StreamEdges(ctx context.Context, fn func(edge *storage.Edge) error) error {
	var snapshot []*storage.Edge
	if err := e.MemoryEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		snapshot = append(snapshot, storage.CopyEdge(edge))
		return nil
	}); err != nil {
		return err
	}
	if buildWriteCallerOnStack("runRelationshipIndex") {
		e.once.Do(func() { close(e.entered) })
		<-e.release
	}
	for _, edge := range snapshot {
		if err := fn(edge); err != nil {
			return err
		}
	}
	return nil
}

// TestRelationshipLoaderLeavesLiveUpdatesInPlace: the relationship index's
// loader applies the stored relationships from a snapshot; a live update or
// removal made meanwhile wins over the loader's older copy.
func TestRelationshipLoaderLeavesLiveUpdatesInPlace(t *testing.T) {
	engine := &relationshipLoaderGateEngine{MemoryEngine: newTestMemoryEngine(t), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { engine.open(); _ = engine.MemoryEngine.Close() })
	for _, id := range []storage.NodeID{"nornic:s", "nornic:e"} {
		_, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "node"}})
		require.NoError(t, err)
	}
	updated := &storage.Edge{ID: "nornic:upd", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "oldrelterm"}}
	removed := &storage.Edge{ID: "nornic:del", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "gonerelterm"}}
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(updated)))
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(removed)))
	svc := NewService(engine, 3, nil)
	t.Cleanup(func() { engine.open(); _ = svc.Close() })
	svc.EnableRelationshipSearch(nil)
	svc.StartRelationshipIndex()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the relationship loader did not read its snapshot")
	}
	newer := &storage.Edge{ID: updated.ID, StartNode: updated.StartNode, EndNode: updated.EndNode, Type: "REL", Properties: map[string]any{"note": "newrelterm"}}
	require.NoError(t, engine.UpdateEdge(storage.CopyEdge(newer)))
	svc.IndexRelationship(newer)
	require.NoError(t, engine.DeleteEdge(removed.ID))
	svc.RemoveRelationship(removed.ID)
	engine.open()
	rs := svc.relationshipSearch()
	select {
	case <-rs.ready:
	case <-time.After(10 * time.Second):
		t.Fatal("the relationship loader did not finish")
	}
	rs.mu.Lock()
	text, _ := rs.fulltext.GetDocument(string(updated.ID))
	_, removedPresent := rs.entries[string(removed.ID)]
	rs.mu.Unlock()
	require.Contains(t, text, "newrelterm", "the loader's older copy replaced a live update")
	require.False(t, removedPresent, "the loader resurrected a relationship removed while it ran")
}

// TestBuildRelationshipUpdateFollowsAQueuedSetting: a relationship-search
// update made after an indexing setting changed during a build is indexed
// under that setting, as the same calls one after another would.
func TestBuildRelationshipUpdateFollowsAQueuedSetting(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	svc.EnableRelationshipSearch(nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	buildWriteEdgeEndpoints(t, engine)
	built := startGatedBuild(t, engine, svc)
	svc.SetFulltextProperties([]string{"title"})
	edge := &storage.Edge{ID: "nornic:r", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"title": "reltitleterm", "note": "relnoteterm"}}
	require.NoError(t, engine.CreateEdge(storage.CopyEdge(edge)))
	svc.IndexRelationship(edge)
	engine.open()
	require.NoError(t, <-built)
	rs := svc.relationshipSearch()
	rs.mu.Lock()
	text, _ := rs.fulltext.GetDocument("nornic:r")
	rs.mu.Unlock()
	require.Contains(t, text, "reltitleterm")
	require.NotContains(t, text, "relnoteterm", "the relationship was indexed under the setting in force before the queued one")
}
