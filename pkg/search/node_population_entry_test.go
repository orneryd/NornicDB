package search

// An entry decision reads only inputs no queued operation ahead can change;
// everything else is decided in queue order (node_population_writes.go). Each test
// compares a sequence of calls made during a gated build with the same calls
// made after it.

import (
	"context"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
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

// TestPopulationOrderWriteBeyondMemoryLimitWaitsInOrder: a write that would take the
// live heap past the runtime's soft memory limit (memory.runtime_limit) is
// not queued; it waits for the build and is applied after the writes queued
// before it. The queue retains nothing once the build has installed it.
func TestPopulationOrderWriteBeyondMemoryLimitWaitsInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newSnapshotGateEngine(t)
	svc := populationOrderService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	large := func(text string) *storage.Node {
		node := admissionNode("nornic:large", text, []float32{1, 0, 0}, "named")
		node.Properties["blob"] = strings.Repeat("x", 8<<20)
		return node
	}
	first := large("firstterm")
	// Storage holds a small copy: the stored node only has to exist.
	_, err := engine.CreateNode(&storage.Node{ID: first.ID, Labels: first.Labels, Properties: map[string]any{"text": "firstterm"}})
	require.NoError(t, err)
	requireReturnsWhileBlocked(t, "the first write", func() error { return svc.IndexNode(first) })
	runtime.GC()
	previous := debug.SetMemoryLimit(int64(liveHeapBytes()) + 4<<20)
	t.Cleanup(func() { debug.SetMemoryLimit(previous) })
	second := large("secondterm")
	require.NoError(t, engine.UpdateNode(&storage.Node{ID: second.ID, Labels: second.Labels, Properties: map[string]any{"text": "secondterm"}}))
	done := make(chan error, 1)
	go func() { done <- svc.IndexNode(second) }()
	select {
	case err := <-done:
		t.Fatalf("a write past the memory limit was queued: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	debug.SetMemoryLimit(previous)
	engine.open()
	require.NoError(t, <-built)
	require.NoError(t, <-done)
	require.Equal(t, []string{"nornic:large"}, searchIDs(t, svc, "secondterm"))
	require.Empty(t, searchIDs(t, svc, "firstterm"), "the waiting write was overtaken by the one queued before it")
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
