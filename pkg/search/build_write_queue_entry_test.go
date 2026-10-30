package search

// An entry decision reads only inputs no queued operation ahead can change;
// everything else is decided in queue order (build_write_queue.go). Each test
// compares a sequence of calls made during a gated build with the same calls
// made after it.

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestBuildRemoveLastVectorThenNewDimensions: with automatic dimensions, a
// removal of the index's last vector followed by a write with other
// dimensions succeeds and chooses them, as the same calls do after the build.
func TestBuildRemoveLastVectorThenNewDimensions(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, gated := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "gated"}[gated], func(t *testing.T) {
			engine := newNodeSnapshotGateEngine(t)
			seed := buildWriteNode("nornic:seed", "seedterm", []float32{1, 0, 0})
			_, err := engine.CreateNode(storage.CopyNode(seed))
			require.NoError(t, err)
			svc := NewService(engine, 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			t.Cleanup(engine.open)
			require.NoError(t, svc.IndexNode(seed))
			var built chan error
			if gated {
				built = startGatedBuild(t, engine, svc)
			} else {
				engine.open()
				require.NoError(t, svc.BuildIndexes(context.Background()))
			}
			next := buildWriteNode("nornic:next", "nextterm", []float32{0, 1})
			done := make(chan error, 1)
			go func() {
				if err := engine.DeleteNode(seed.ID); err != nil {
					done <- err
					return
				}
				if err := svc.RemoveNode(seed.ID); err != nil {
					done <- err
					return
				}
				if _, err := engine.CreateNode(storage.CopyNode(next)); err != nil {
					done <- err
					return
				}
				done <- svc.IndexNode(next)
			}()
			if gated {
				select {
				case err := <-done:
					t.Fatalf("the write with new dimensions was decided before the removal ahead of it was installed: %v", err)
				case <-time.After(200 * time.Millisecond):
				}
				engine.open()
				require.NoError(t, <-built)
			}
			require.NoError(t, <-done)
			require.Equal(t, 2, svc.VectorIndexDimensions())
			_, ok := svc.getVectorLookup()(nodeNamedVectorKey("nornic:next", "v"))
			require.True(t, ok)
		})
	}
}

// TestBuildQueuedIndexFlagsDecideInOrder: SetIndexFlags reports a change
// against the flags the calls ahead of it leave, and a write queued behind a
// flag change is indexed under the flags of its turn.
func TestBuildQueuedIndexFlagsDecideInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	run := func(gated bool) (changed [2]bool, indexed bool) {
		engine := newNodeSnapshotGateEngine(t)
		_, err := engine.CreateNode(&storage.Node{ID: "nornic:stored", Labels: []string{"Doc"}, Properties: map[string]any{"text": "storedterm"}})
		require.NoError(t, err)
		svc := buildWriteService(t, engine, "", "v2", nil)
		t.Cleanup(func() { _ = svc.Close() })
		t.Cleanup(engine.open)
		var built chan error
		if gated {
			built = startGatedBuild(t, engine, svc)
		} else {
			engine.open()
			require.NoError(t, svc.BuildIndexes(context.Background()))
		}
		node := &storage.Node{ID: "nornic:f", Labels: []string{"Doc"}, Properties: map[string]any{"text": "flagterm"}}
		_, err = engine.CreateNode(storage.CopyNode(node))
		require.NoError(t, err)
		changed[0] = svc.SetIndexFlags(false, true)
		changed[1] = svc.SetIndexFlags(false, true)
		require.NoError(t, svc.IndexNode(node))
		if gated {
			engine.open()
			require.NoError(t, <-built)
		}
		_, indexed = svc.fulltext().GetDocument("nornic:f")
		return changed, indexed
	}
	serialChanged, serialIndexed := run(false)
	gatedChanged, gatedIndexed := run(true)
	require.Equal(t, [2]bool{true, false}, serialChanged)
	require.Equal(t, serialChanged, gatedChanged, "SetIndexFlags reported against the flags at the call")
	require.False(t, serialIndexed)
	require.Equal(t, serialIndexed, gatedIndexed, "the write was indexed under the flags before the queued change")
}

func liveHeapBytes() uint64 {
	sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}

// TestBuildWriteBeyondMemoryLimitWaitsInOrder: the queue reserves what the
// runtime's soft memory limit (memory.runtime_limit) leaves when it opens. A
// copy past that reservation, a node write or a relationship-search update,
// is not queued: it waits for the build and is applied after the writes
// queued before it. The queue retains nothing once the build has installed
// it.
func TestBuildWriteBeyondMemoryLimitWaitsInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	buildWriteEdgeEndpoints(t, engine)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:big-rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "relterm"}}))
	svc := buildWriteService(t, engine, "", "v2", nil)
	svc.EnableRelationshipSearch(nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	// The reservation: 12 MiB above the live heap when the queue opens.
	runtime.GC()
	previous := debug.SetMemoryLimit(int64(liveHeapBytes()) + 12<<20)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })
	built := startGatedBuild(t, engine, svc)
	debug.SetMemoryLimit(previous) // the reservation is taken; nothing else is held back
	blob := strings.Repeat("x", 8<<20)
	large := func(text string) *storage.Node {
		node := buildWriteNode("nornic:large", text, []float32{1, 0, 0})
		node.Properties["blob"] = blob
		return node
	}
	first := large("firstterm")
	// Storage holds a small copy: the stored node only has to exist.
	_, err := engine.CreateNode(&storage.Node{ID: first.ID, Labels: first.Labels, Properties: map[string]any{"text": "firstterm"}})
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "a write within the reservation", func() error { return svc.IndexNode(first) })
	second := large("secondterm")
	require.NoError(t, engine.UpdateNode(storage.NodeUpdateRequest{After: &storage.Node{ID: second.ID, Labels: second.Labels, Properties: map[string]any{"text": "secondterm"}}}))
	relationship := &storage.Edge{ID: "nornic:big-rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "updatedrelterm", "blob": blob}}
	nodeDone, relDone := make(chan error, 1), make(chan struct{})
	go func() { nodeDone <- svc.IndexNode(second) }()
	go func() { svc.IndexRelationship(relationship); close(relDone) }()
	select {
	case err := <-nodeDone:
		t.Fatalf("a node write past the reservation was queued: %v", err)
	case <-relDone:
		t.Fatal("a relationship-search update past the reservation was queued")
	case <-time.After(200 * time.Millisecond):
	}
	engine.open()
	require.NoError(t, <-built)
	require.NoError(t, <-nodeDone)
	<-relDone
	require.Equal(t, []string{"nornic:large"}, buildWriteSearchIDs(t, svc, "secondterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "firstterm"), "the waiting write was overtaken by the one queued before it")
	rs := svc.relationshipSearch()
	rs.mu.Lock()
	text, _ := rs.fulltext.GetDocument("nornic:big-rel")
	rs.mu.Unlock()
	require.Contains(t, text, "updatedrelterm")
	svc.buildWrites.mu.Lock()
	defer svc.buildWrites.mu.Unlock()
	require.Zero(t, svc.buildWrites.bytes)
	require.Empty(t, svc.buildWrites.writes)
}

// TestBuildSupersededWriteIsSkippedAtInstall: a queued write whose valid hook
// reports it superseded (a refresh with a stale reservation) is not installed
// and does not suppress the build's own storage copy.
func TestBuildSupersededWriteIsSkippedAtInstall(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	_, err := engine.CreateNode(buildWriteNode("nornic:r", "storedterm", []float32{1, 0, 0}))
	require.NoError(t, err)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	stale := buildWriteNode("nornic:r", "staleterm", []float32{0, 1, 0})
	queued, err := svc.readLockOrQueue(deferredIndexWrite{node: stale, valid: func() bool { return false }})
	require.NoError(t, err)
	require.True(t, queued)
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:r"}, buildWriteSearchIDs(t, svc, "storedterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "staleterm"))
}

// TestBuildRelationshipPhaseReadsWithTheNodeSnapshot: the build reads its
// stored relationships where it reads its nodes. A relationship stored during
// the build, whose IndexEdge follows a queued change that turns vectors off,
// keeps the vector it had when the build began, as the same calls after the
// build would leave it.
func TestBuildRelationshipPhaseReadsWithTheNodeSnapshot(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	run := func(gated bool) []float32 {
		engine := newNodeSnapshotGateEngine(t)
		buildWriteEdgeEndpoints(t, engine)
		stored := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
		require.NoError(t, engine.CreateEdge(storage.CopyEdge(stored)))
		svc := buildWriteService(t, engine, "", "v2", nil)
		t.Cleanup(func() { _ = svc.Close() })
		t.Cleanup(engine.open)
		var built chan error
		if gated {
			built = startGatedBuild(t, engine, svc)
		} else {
			engine.open()
			require.NoError(t, svc.BuildIndexes(context.Background()))
		}
		svc.SetIndexFlags(true, false)
		updated := &storage.Edge{ID: stored.ID, StartNode: stored.StartNode, EndNode: stored.EndNode, Type: stored.Type, Properties: map[string]any{"v": []float32{0, 1, 0}}}
		require.NoError(t, engine.UpdateEdge(storage.EdgeUpdateRequest{After: storage.CopyEdge(updated)}))
		require.NoError(t, svc.IndexEdge(updated))
		if gated {
			engine.open()
			require.NoError(t, <-built)
		}
		svc.mu.RLock()
		defer svc.mu.RUnlock()
		return svc.edgePropVector[string(stored.ID)]["v"]
	}
	serial := run(false)
	require.Equal(t, []float32{1, 0, 0}, serial)
	require.Equal(t, serial, run(true), "the relationship phase read storage written after the build's node snapshot")
}

// nodeReadParkEngine parks the build before it reads its nodes, through the
// engine or through its snapshot: storage changed while it is parked is
// after the build's read point only if the snapshot already holds it.
type nodeReadParkEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once, opened     sync.Once
}

func newNodeReadParkEngine(t *testing.T) *nodeReadParkEngine {
	t.Helper()
	e := &nodeReadParkEngine{MemoryEngine: newTestMemoryEngine(t), entered: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { e.open(); _ = e.MemoryEngine.Close() })
	return e
}

func (e *nodeReadParkEngine) open() { e.opened.Do(func() { close(e.release) }) }
func (e *nodeReadParkEngine) park() { e.once.Do(func() { close(e.entered); <-e.release }) }

func (e *nodeReadParkEngine) IterateNodes(fn func(*storage.Node) bool) error {
	e.park()
	return e.MemoryEngine.IterateNodes(fn)
}

func (e *nodeReadParkEngine) OpenReadSnapshot(ctx context.Context) (storage.ReadSnapshotView, error) {
	inner, err := e.MemoryEngine.OpenReadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return &hookedReadSnapshot{ReadSnapshotView: inner, beforeNodes: func() error { e.park(); return nil }}, nil
}

// TestBuildBaselineIsOneStoragePoint: the build reads its nodes and its
// relationships through one storage snapshot. A node and its relationship
// changed together in one transaction after the snapshot opened (the build is
// parked before its node read), whose index
// calls follow a queued change that turns both indexes off, keep the pair
// they had at the snapshot: never the new node text with the old vector.
func TestBuildBaselineIsOneStoragePoint(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeReadParkEngine(t)
	buildWriteEdgeEndpoints(t, engine)
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "oldterm"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}))
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-engine.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its nodes")
	}
	svc.SetIndexFlags(false, false)
	node := &storage.Node{ID: "nornic:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "newterm"}}
	edge := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{0, 1, 0}}}
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("nornic"))
	require.NoError(t, tx.UpdateGraphNode(storage.NodeUpdateRequest{After: storage.CopyNode(node), Policy: storage.ClientNodeEdits}))
	require.NoError(t, tx.UpdateEdge(storage.EdgeUpdateRequest{After: storage.CopyEdge(edge)}))
	require.NoError(t, tx.Commit())
	require.NoError(t, svc.IndexNode(node))
	require.NoError(t, svc.IndexEdge(edge))
	engine.open()
	require.NoError(t, <-built)
	text, _ := svc.fulltext().GetDocument("nornic:doc")
	svc.mu.RLock()
	vec := svc.edgePropVector["nornic:rel"]["v"]
	svc.mu.RUnlock()
	require.Contains(t, text, "oldterm", "the node baseline is not the snapshot's")
	require.Equal(t, []float32{1, 0, 0}, vec, "the relationship baseline is not the snapshot's")
}

// TestBuildIndexesAcknowledgedWriteBehindNodes: a node acknowledged into the
// storage write-behind buffer and not yet flushed is part of the build's
// baseline, as every storage read sees it.
func TestBuildIndexesAcknowledgedWriteBehindNodes(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("nornic"))
	_, err = tx.CreateNode(&storage.Node{ID: "nornic:buffered", Labels: []string{"Doc"}, Properties: map[string]any{"text": "bufferedterm"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	got, err := engine.GetNode("nornic:buffered")
	require.NoError(t, err, "the acknowledged node is visible to storage reads")
	require.NotNil(t, got)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, []string{"nornic:buffered"}, buildWriteSearchIDs(t, svc, "bufferedterm"))
}

// snapshotlessGateEngine is a gated engine that cannot open read snapshots
// (like CompositeEngine or RemoteEngine).
type snapshotlessGateEngine struct {
	storage.Engine
	gate *nodeSnapshotGateEngine
}

func (e *snapshotlessGateEngine) IterateNodes(fn func(*storage.Node) bool) error {
	return e.gate.IterateNodes(fn)
}

// TestBuildWithoutReadSnapshotsMakesWritesWait: on an engine without read
// snapshots the build has no baseline to order writes against, so a write
// made during it waits for it, and is applied after it.
func TestBuildWithoutReadSnapshotsMakesWritesWait(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	gate := newNodeSnapshotGateEngine(t)
	engine := &snapshotlessGateEngine{Engine: gate.MemoryEngine, gate: gate}
	require.False(t, storage.SupportsReadSnapshot(context.Background(), engine))
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(gate.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its nodes")
	}
	node := buildWriteNode("nornic:w", "waitterm", []float32{1, 0, 0})
	_, err := gate.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- svc.IndexNode(node) }()
	select {
	case err := <-done:
		t.Fatalf("a write was queued on a build without a read snapshot: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	gate.open()
	require.NoError(t, <-built)
	require.NoError(t, <-done)
	require.Equal(t, []string{"nornic:w"}, buildWriteSearchIDs(t, svc, "waitterm"))
}

// TestBuildConcurrentIndexFlagsReportTheirOwnChange: SetIndexFlags calls made
// concurrently during a build each report the change their own call makes,
// against the flags the calls queued before it leave. The calls set one of
// two states, so in any order the reported changes are as many as the
// transitions: odd when the final state differs from the initial one, even
// when it does not.
func TestBuildConcurrentIndexFlagsReportTheirOwnChange(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	initial := [2]bool{svc.bm25Enabled.Load(), svc.vectorEnabled.Load()}
	const rounds, callers = 20, 8
	var changes atomic.Int32
	for range rounds {
		var wg sync.WaitGroup
		for i := range callers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if svc.SetIndexFlags(true, i%2 == 0) {
					changes.Add(1)
				}
			}()
		}
		wg.Wait()
	}
	svc.buildWrites.mu.Lock()
	queued := 0
	for _, write := range svc.buildWrites.writes {
		if write.apply != nil && write.dims {
			queued++
		}
	}
	final := svc.buildWrites.flags
	svc.buildWrites.mu.Unlock()
	require.Equal(t, rounds*callers, queued, "every call is queued on the build")
	require.Equal(t, initial != final, changes.Load()%2 == 1, "the reported changes are not the transitions of the queued order")
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, final, [2]bool{svc.bm25Enabled.Load(), svc.vectorEnabled.Load()})
}

// settledLiveHeap is the live heap once memory an earlier test released
// meanwhile has been collected: two collections in a row that agree.
func settledLiveHeap() uint64 {
	runtime.GC()
	last := liveHeapBytes()
	for range 20 {
		time.Sleep(10 * time.Millisecond)
		runtime.GC()
		next := liveHeapBytes()
		if max(next, last)-min(next, last) < 32<<10 {
			return next
		}
		last = next
	}
	return last
}

// TestBuildQueuedCopiesCountTheirHeap: the bytes the queue counts for a
// queued copy cover what the copy takes on the heap, so the reservation
// bounds what the queue retains. Measured over 4,000 queued copies of a
// relationship-search update (a 1024-dimension vector property) and of a node
// write (1 KiB of text, a vector and embedding metadata).
func TestBuildQueuedCopiesCountTheirHeap(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	node := buildWriteNode("nornic:counted", strings.Repeat("x", 1<<10), []float32{1, 0, 0})
	node.EmbedMeta = map[string]any{"chunk_count": int64(1), "model": "m"}
	_, err := engine.CreateNode(storage.CopyNode(node))
	require.NoError(t, err)
	svc := buildWriteService(t, engine, "", "v2", nil)
	svc.EnableRelationshipSearch(nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	edge := &storage.Edge{ID: "nornic:counted", Type: "REL", Properties: map[string]any{"v": make([]float32, 1024)}}
	const copies = 4000
	for _, c := range []struct {
		name  string
		write func()
	}{
		{"relationship update", func() { svc.IndexRelationship(edge) }},
		{"node write", func() {
			// A fresh node, which the caller drops: what the queue holds is then
			// all the copy retains, its text included.
			fresh := storage.CopyNode(node)
			fresh.Properties["text"] = strings.Repeat("x", 1<<10)
			require.NoError(t, svc.IndexNode(fresh))
		}},
	} {
		before := settledLiveHeap()
		svc.buildWrites.mu.Lock()
		counted := svc.buildWrites.bytes
		svc.buildWrites.mu.Unlock()
		for range copies {
			c.write()
		}
		held := float64(int64(settledLiveHeap())-int64(before)) / copies
		svc.buildWrites.mu.Lock()
		perCopy := float64(svc.buildWrites.bytes-counted) / copies
		svc.buildWrites.mu.Unlock()
		t.Logf("%s: %.0f B on the heap, %.0f B counted", c.name, held, perCopy)
		require.GreaterOrEqual(t, perCopy, held, "%s: the queue counts less than the copy holds", c.name)
		require.LessOrEqual(t, perCopy, 1.25*held, "%s: the queue counts far more than the copy holds", c.name)
	}
	engine.open()
	require.NoError(t, <-built)
}
