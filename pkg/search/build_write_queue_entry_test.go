package search

// An entry decision reads only inputs no queued operation ahead can change;
// everything else is decided in queue order (build_write_queue.go). Each test
// compares a sequence of calls made during a gated build with the same calls
// made after it.

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"strings"
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
			_, ok := svc.getVectorLookup()("nornic:next-named-v")
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

// TestBuildWriteBeyondMemoryLimitWaitsInOrder: a write that would take the
// live heap past the runtime's soft memory limit (memory.runtime_limit) is
// not queued; it waits for the build and is applied after the writes queued
// before it. The queue retains nothing once the build has installed it.
func TestBuildWriteBeyondMemoryLimitWaitsInOrder(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "1000")
	engine := newNodeSnapshotGateEngine(t)
	svc := buildWriteService(t, engine, "", "v2", nil)
	t.Cleanup(func() { _ = svc.Close() })
	t.Cleanup(engine.open)
	built := startGatedBuild(t, engine, svc)
	large := func(text string) *storage.Node {
		node := buildWriteNode("nornic:large", text, []float32{1, 0, 0})
		node.Properties["blob"] = strings.Repeat("x", 8<<20)
		return node
	}
	first := large("firstterm")
	// Storage holds a small copy: the stored node only has to exist.
	_, err := engine.CreateNode(&storage.Node{ID: first.ID, Labels: first.Labels, Properties: map[string]any{"text": "firstterm"}})
	require.NoError(t, err)
	requireReturnsDuringBuild(t, "the first write", func() error { return svc.IndexNode(first) })
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
	require.Equal(t, []string{"nornic:large"}, buildWriteSearchIDs(t, svc, "secondterm"))
	require.Empty(t, buildWriteSearchIDs(t, svc, "firstterm"), "the waiting write was overtaken by the one queued before it")
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

// BenchmarkStoredRelationshipRead measures what the build retains between its
// node snapshot and its relationship phase: per relationship, its ID, type,
// endpoints and vector-shaped properties (here one 1024-dimension vector and
// one text property, which is not retained).
func BenchmarkStoredRelationshipRead(b *testing.B) {
	engine := storage.NewMemoryEngine()
	defer engine.Close()
	vec := make([]float32, 1024)
	for i := range vec {
		vec[i] = float32(i) / 1024
	}
	const relationships = 1000
	for i := range relationships {
		start, end := storage.NodeID(fmt.Sprintf("nornic:s%d", i)), storage.NodeID(fmt.Sprintf("nornic:e%d", i))
		for _, id := range []storage.NodeID{start, end} {
			if _, err := engine.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}}); err != nil {
				b.Fatal(err)
			}
		}
		edge := &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("nornic:r%d", i)), StartNode: start, EndNode: end, Type: "REL",
			Properties: map[string]any{"v": vec, "note": strings.Repeat("text ", 40)}}
		if err := engine.CreateEdge(edge); err != nil {
			b.Fatal(err)
		}
	}
	svc := NewServiceWithDimensions(engine, 1024)
	defer svc.Close()
	// Retained: what the read keeps per relationship, counted as the build
	// queue counts a copied edge (an engine that decodes values, like Badger,
	// allocates all of it; the memory engine shares its stored vectors).
	held, err := svc.readStoredRelationships(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	retained := 0
	for _, edge := range held {
		_, size := copyQueuedEdge(edge)
		retained += size + len(edge.ID) + len(edge.Type) + len(edge.StartNode) + len(edge.EndNode)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		edges, err := svc.readStoredRelationships(context.Background())
		if err != nil || len(edges) != relationships {
			b.Fatalf("read %d relationships: %v", len(edges), err)
		}
	}
	b.ReportMetric(float64(retained)/relationships, "retained-B/rel")
}
