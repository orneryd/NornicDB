package search

// An entry decision reads only inputs no queued operation ahead can change;
// everything else is decided in queue order (node_population_writes.go). Each test
// compares a sequence of calls made during a gated build with the same calls
// made after it.

import (
	"context"
	"fmt"
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

// TestPopulationOrderRemoveLastVectorThenNewDimensions: with automatic dimensions, a
// removal of the index's last vector followed by a write with other
// dimensions succeeds and chooses them, as the same calls do after the build.
func TestPopulationOrderRemoveLastVectorThenNewDimensions(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, gated := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "gated"}[gated], func(t *testing.T) {
			engine := newSnapshotGateEngine(t)
			seed := admissionNode("nornic:seed", "seedterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(storage.CopyNode(seed))
			require.NoError(t, err)
			svc := NewServiceWithDimensions(engine, 3)
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
			next := admissionNode("nornic:next", "nextterm", []float32{0, 1}, "named")
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
			_, ok := svc.getVectorLookup()("nornic:next-named-v")
			require.True(t, ok)
		})
	}
}

// TestPopulationOrderQueuedIndexFlagsDecideInOrder: SetIndexFlags reports a change
// against the flags the calls ahead of it leave, and a write queued behind a
// flag change is indexed under the flags of its turn.
func TestPopulationOrderQueuedIndexFlagsDecideInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	run := func(gated bool) (changed [2]bool, indexed bool) {
		engine := newSnapshotGateEngine(t)
		_, err := engine.CreateNode(&storage.Node{ID: "nornic:stored", Labels: []string{"Doc"}, Properties: map[string]any{"text": "storedterm"}})
		require.NoError(t, err)
		svc := populationOrderService(t, engine, "", "v2", nil)
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

// TestPopulationOrderWriteBeyondMemoryLimitWaitsInOrder: the queue reserves what the
// runtime's soft memory limit (memory.runtime_limit) leaves when it opens. A
// copy past that reservation, a node write or a relationship-search update,
// is not queued: it waits for the build and is applied after the writes
// queued before it. The queue retains nothing once the build has installed
// it.
func TestPopulationOrderWriteBeyondMemoryLimitWaitsInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newSnapshotGateEngine(t)
	buildWriteEdgeEndpoints(t, engine)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:big-rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"note": "relterm"}}))
	svc := populationOrderService(t, engine, "", "v2", nil)
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
		node := admissionNode("nornic:large", text, []float32{1, 0, 0}, "named")
		node.Properties["blob"] = blob
		return node
	}
	first := large("firstterm")
	// Storage holds a small copy: the stored node only has to exist.
	_, err := engine.CreateNode(&storage.Node{ID: first.ID, Labels: first.Labels, Properties: map[string]any{"text": "firstterm"}})
	require.NoError(t, err)
	requireReturnsWhileBlocked(t, "a write within the reservation", func() error { return svc.IndexNode(first) })
	second := large("secondterm")
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: second.ID, Labels: second.Labels, Properties: map[string]any{"text": "secondterm"}}))
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
	require.Equal(t, []string{"nornic:large"}, searchIDs(t, svc, "secondterm"))
	require.Empty(t, searchIDs(t, svc, "firstterm"), "the waiting write was overtaken by the one queued before it")
	rs := svc.relationshipSearch()
	rs.mu.Lock()
	text, _ := rs.fulltext.GetDocument("nornic:big-rel")
	rs.mu.Unlock()
	require.Contains(t, text, "updatedrelterm")
	svc.baseBuildControlMu.Lock()
	defer svc.baseBuildControlMu.Unlock()
	if job := svc.nodeAdmission; job != nil {
		require.Zero(t, job.bytes)
		require.Empty(t, job.deferred)
	}
}

// TestPopulationOrderSupersededWriteIsSkippedAtInstall: a queued write whose valid hook
// reports it superseded (a refresh with a stale reservation) is not installed
// and does not suppress the build's own storage copy.
func TestPopulationOrderSupersededWriteIsSkippedAtInstall(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newSnapshotGateEngine(t)
	_, err := engine.CreateNode(admissionNode("nornic:r", "storedterm", []float32{1, 0, 0}, "named"))
	require.NoError(t, err)
	svc := populationOrderService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	stale := admissionNode("nornic:r", "staleterm", []float32{0, 1, 0}, "named")
	_, queued, err := svc.lockNodeMutation(0, true, deferredIndexWrite{node: stale, valid: func() bool { return false }})
	require.NoError(t, err)
	require.True(t, queued)
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, []string{"nornic:r"}, searchIDs(t, svc, "storedterm"))
	require.Empty(t, searchIDs(t, svc, "staleterm"))
}

// TestPopulationOrderDimensionMismatchIsReturned: on fixed dimensions an
// update whose embedding has other dimensions returns ErrDimensionMismatch,
// live and during a population alike, and the node keeps its indexed vector
// and text instead of losing them to a skipped vector.
func TestPopulationOrderDimensionMismatchIsReturned(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	for _, gated := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "during a population"}[gated], func(t *testing.T) {
			engine := newSnapshotGateEngine(t)
			svc := populationOrderService(t, engine, "", "v2", nil)
			t.Cleanup(func() { _ = svc.Close() })
			t.Cleanup(engine.open)
			good := admissionNode("nornic:m", "goodterm", []float32{1, 0, 0}, "named")
			_, err := engine.CreateNode(storage.CopyNode(good))
			require.NoError(t, err)
			require.NoError(t, svc.IndexNode(good))
			var built chan error
			if gated {
				built = startGatedBuild(t, engine, svc)
			} else {
				engine.open()
				require.NoError(t, svc.BuildIndexes(context.Background()))
			}
			bad := admissionNode("nornic:m", "badterm", []float32{1, 0}, "named")
			require.ErrorIs(t, svc.IndexNode(bad), ErrDimensionMismatch)
			if gated {
				engine.open()
				require.NoError(t, <-built)
			}
			requireVector(t, svc, "nornic:m-named-v", []float32{1, 0, 0})
			text, _ := svc.fulltext().GetDocument("nornic:m")
			require.Contains(t, text, "goodterm")
		})
	}
}

// TestPopulationOrderRelationshipPhaseReadsWithTheNodeSnapshot: the build reads its
// stored relationships where it reads its nodes. A relationship stored during
// the build, whose IndexEdge follows a queued change that turns vectors off,
// keeps the vector it had when the build began, as the same calls after the
// build would leave it.
func TestPopulationOrderRelationshipPhaseReadsWithTheNodeSnapshot(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	run := func(gated bool) []float32 {
		engine := newSnapshotGateEngine(t)
		buildWriteEdgeEndpoints(t, engine)
		stored := &storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}
		require.NoError(t, engine.CreateEdge(storage.CopyEdge(stored)))
		svc := populationOrderService(t, engine, "", "v2", nil)
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
		require.NoError(t, engine.UpdateEdge(storage.CopyEdge(updated)))
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

// nodeReadParkEngine parks the population before it reads its nodes, through the
// engine or through its snapshot: storage changed while it is parked is
// after the population's read point only if the snapshot already holds it.
type nodeReadParkEngine struct {
	*storage.MemoryEngine
	entered, release chan struct{}
	once, opened     sync.Once
}

func newNodeReadParkEngine(t *testing.T) *nodeReadParkEngine {
	t.Helper()
	e := &nodeReadParkEngine{MemoryEngine: storage.NewMemoryEngine(), entered: make(chan struct{}), release: make(chan struct{})}
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

// TestPopulationOrderBaselineIsOneStoragePoint: the build reads its nodes and its
// relationships through one storage snapshot. A node and its relationship
// changed together in one transaction after the snapshot opened (the build is
// parked before its node read), whose index
// calls follow a queued change that turns both indexes off, keep the pair
// they had at the snapshot: never the new node text with the old vector.
func TestPopulationOrderBaselineIsOneStoragePoint(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeReadParkEngine(t)
	buildWriteEdgeEndpoints(t, engine)
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:doc", Labels: []string{"Doc"}, Properties: map[string]any{"text": "oldterm"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&storage.Edge{ID: "nornic:rel", StartNode: "nornic:s", EndNode: "nornic:e", Type: "REL", Properties: map[string]any{"v": []float32{1, 0, 0}}}))
	svc := populationOrderService(t, engine, "", "v2", nil)
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
	require.NoError(t, tx.UpdateNode(storage.CopyNode(node)))
	require.NoError(t, tx.UpdateEdge(storage.CopyEdge(edge)))
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

// TestPopulationOrderIndexesAcknowledgedWriteBehindNodes: a node acknowledged into the
// storage write-behind buffer and not yet flushed is part of the build's
// baseline, as every storage read sees it.
func TestPopulationOrderIndexesAcknowledgedWriteBehindNodes(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
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
	svc := populationOrderService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	require.NoError(t, svc.BuildIndexes(context.Background()))
	require.Equal(t, []string{"nornic:buffered"}, searchIDs(t, svc, "bufferedterm"))
}

// snapshotlessGateEngine is a gated engine that cannot open read snapshots
// (like CompositeEngine or RemoteEngine).
type snapshotlessGateEngine struct {
	storage.Engine
	gate *snapshotGateEngine
}

func (e *snapshotlessGateEngine) IterateNodes(fn func(*storage.Node) bool) error {
	return e.gate.IterateNodes(fn)
}

// TestPopulationOrderWithoutReadSnapshotsMakesWritesWait: on an engine without read
// snapshots the build has no baseline to order writes against, so a write
// made during it waits for it, and is applied after it.
func TestPopulationOrderWithoutReadSnapshotsMakesWritesWait(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	gate := newSnapshotGateEngine(t)
	engine := &snapshotlessGateEngine{Engine: gate.MemoryEngine, gate: gate}
	require.False(t, storage.SupportsReadSnapshot(context.Background(), engine))
	svc := populationOrderService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(gate.open)
	built := make(chan error, 1)
	go func() { built <- svc.BuildIndexes(context.Background()) }()
	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the build did not read its nodes")
	}
	node := admissionNode("nornic:w", "waitterm", []float32{1, 0, 0}, "named")
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
	require.Equal(t, []string{"nornic:w"}, searchIDs(t, svc, "waitterm"))
}

// TestPopulationOrderConcurrentIndexFlagsReportTheirOwnChange: SetIndexFlags calls made
// concurrently during a build each report the change their own call makes,
// against the flags the calls queued before it leave. The calls set one of
// two states, so in any order the reported changes are as many as the
// transitions: odd when the final state differs from the initial one, even
// when it does not.
func TestPopulationOrderConcurrentIndexFlagsReportTheirOwnChange(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newSnapshotGateEngine(t)
	svc := populationOrderService(t, engine, "", "v2", nil)
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
	svc.baseBuildControlMu.Lock()
	job := svc.nodeAdmission
	if job == nil {
		svc.baseBuildControlMu.Unlock()
		t.Fatal("no population is running")
	}
	queued := 0
	for _, write := range job.deferred {
		if write.apply != nil && write.dims {
			queued++
		}
	}
	final := job.flags
	svc.baseBuildControlMu.Unlock()
	require.Equal(t, rounds*callers, queued, "every call is queued on the population")
	require.Equal(t, initial != final, changes.Load()%2 == 1, "the reported changes are not the transitions of the queued order")
	engine.open()
	require.NoError(t, <-built)
	require.Equal(t, final, [2]bool{svc.bm25Enabled.Load(), svc.vectorEnabled.Load()})
}

// TestPopulationOrderRestoredHNSWKeepsQueuedWrites: a population that
// restores its saved HNSW applies the writes queued on it after the restore.
// A vector written while the population opens its snapshot (before the
// restore) or streams its relationships (after it) is in the restored graph
// and found by ANN search, as when it is written after the population.
func TestPopulationOrderRestoredHNSWKeepsQueuedWrites(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
	for _, gate := range []struct{ name, caller string }{{"after", ""}, {"snapshot", "openPopulationSnapshot"}, {"edges", "buildRelationshipVectorIndexes"}} {
		t.Run(gate.name, func(t *testing.T) {
			engine := newLifecycleGateEngine(t)
			for i := range 12 {
				_, err := engine.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:seed%d", i)), Labels: []string{"Doc"}, Properties: map[string]any{"text": "oldterm"}, NamedEmbeddings: map[string][]float32{"v": {-1, 0, 0}}})
				require.NoError(t, err)
			}
			dir := t.TempDir()
			initial := populationOrderService(t, engine, dir, "v2", nil)
			initial.SetRuntimeStrategyTransitionsEnabled(false)
			require.NoError(t, initial.BuildIndexes(context.Background()))
			require.NotNil(t, initial.servingHNSW())
			require.Equal(t, 12, initial.servingHNSW().Size())
			require.NoError(t, initial.Close())

			svc := populationOrderService(t, engine, dir, "v2", nil)
			svc.SetRuntimeStrategyTransitionsEnabled(false)
			t.Cleanup(func() { _ = svc.Close() })
			var built chan error
			var open func()
			if gate.caller != "" {
				g := engine.arm(gate.name, gate.caller)
				open = g.open
				built = make(chan error, 1)
				go func() { built <- svc.BuildIndexes(context.Background()) }()
				waitClosed(t, g.entered, "the population's "+gate.name+" gate")
			} else {
				require.NoError(t, svc.BuildIndexes(context.Background()))
			}
			node := &storage.Node{ID: "nornic:new", Labels: []string{"Doc"}, Properties: map[string]any{"text": "newterm"}, NamedEmbeddings: map[string][]float32{"v": {1, 0, 0}}}
			_, err := engine.CreateNode(storage.CopyNode(node))
			require.NoError(t, err)
			if gate.caller != "" {
				requireReturnsWhileBlocked(t, "a live vector write", func() error { return svc.IndexNode(node) })
				open()
				require.NoError(t, <-built)
			} else {
				require.NoError(t, svc.IndexNode(node))
			}
			idx := svc.servingHNSW()
			require.NotNil(t, idx)
			require.Equal(t, 13, idx.Size(), "the restored graph lacks the queued vector")
			minimum := 0.9
			result, err := svc.Search(context.Background(), "", []float32{1, 0, 0}, &SearchOptions{Limit: 5, MinSimilarity: &minimum})
			require.NoError(t, err)
			ids := make([]string, 0, len(result.Results))
			for _, hit := range result.Results {
				ids = append(ids, hit.ID)
			}
			require.Contains(t, ids, "nornic:new", "the acknowledged vector is missing from ANN search after the saved-graph restore")
		})
	}
}
