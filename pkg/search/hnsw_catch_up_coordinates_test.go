package search

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This constructed restored topology exposes one ownership boundary. It does
// not stand in for the CPU-parallel 6K public first-build workload: a live
// canonical replacement must not remove an insertion's navigation coordinates.
func TestHNSWCatchUpRetainsNavigationCoordinatesDuringCanonicalReplacement(t *testing.T) {
	servingTestEnv(t)
	store := newNamespacedEngine(t)
	svc := NewService(store, 3, nil)
	c := &catchUpCase{ctx: t.Context(), engine: store, svc: svc}
	var lookupRelease sync.Once
	resume := make(chan struct{})
	release := func() { lookupRelease.Do(func() { close(resume) }) }
	var heldMu bool
	var coordinates *hnswConstructionCoordinates
	t.Cleanup(func() {
		release()
		if heldMu {
			svc.hnswCatchUp.mu.Unlock()
		}
		c.ops.Wait()
		if coordinates != nil {
			require.NoError(t, coordinates.Close())
		}
		svc.hnswRebuildInFlight.Store(false)
		svc.indexMu.Lock()
		svc.hnswCatchUp.end()
		svc.indexMu.Unlock()
		require.NoError(t, svc.Close())
	})
	g, e, oldX, nextX := []float32{-1, 0, 0}, []float32{0.5, 0.866, 0}, []float32{0, 0, 1}, []float32{1, 0, 0}
	for id, vec := range map[string][]float32{"G": g, "E": e, "X": oldX} {
		require.NoError(t, c.store(id, vec))
	}
	nodes := []hnswFixtureNode{
		{id: "G", vec: g, level: 2, links: [][]string{{"E", "X", "distractor-000"}, {"E"}, {}}},
		{id: "E", vec: e, level: 1, links: [][]string{{"G", "X"}, {"G"}}},
		{id: "X", vec: oldX, level: 0, links: [][]string{{"E", "G"}}},
	}
	// More entries than the ordinary query beam prevent an exhaustive-query
	// scan from concealing the unreachable vector. All remain reachable in
	// this controlled restored topology before the replacement schedule.
	for i := 0; i < 512; i++ {
		id := fmt.Sprintf("distractor-%03d", i)
		require.NoError(t, c.store(id, g))
		links := []string{"G"}
		if i > 0 {
			links = append(links, fmt.Sprintf("distractor-%03d", i-1))
		}
		if i+1 < 512 {
			links = append(links, fmt.Sprintf("distractor-%03d", i+1))
		}
		nodes = append(nodes, hnswFixtureNode{id: id, vec: g, level: 0, links: [][]string{links}})
	}
	restored := newHNSWFixture(t, 3, 8, "G", nodes)
	// Use the real construction source owner. Only the topology is restored to
	// a small, deterministic valid graph so the navigation seed is known.
	graph, _, err := svc.buildHNSWSnapshot(t.Context(), 3, restored.config, svc.vectorIndex, nil, nil)
	require.NoError(t, err)
	coordinates = graph.construction
	graph.mu.Lock()
	graph.nodeLevel, graph.vecOff = restored.nodeLevel, restored.vecOff
	graph.neighborsArena, graph.neighborsOff = restored.neighborsArena, restored.neighborsOff
	graph.neighborCountsArena, graph.neighborCountsOff = restored.neighborCountsArena, restored.neighborCountsOff
	graph.internalToID, graph.idToInternal = restored.internalToID, restored.idToInternal
	graph.deleted, graph.liveOrder = restored.deleted, restored.liveOrder
	graph.liveCount, graph.liveOrderDead = restored.liveCount, 0
	graph.entryPoint, graph.hasEntryPoint, graph.maxLevel = restored.entryPoint, true, restored.maxLevel
	graph.quantized = nil
	for i := range graph.vecOff {
		graph.vecOff[i] = -1
	}
	graph.vectors = nil
	constructionLookup := graph.vectorLookup
	graph.mu.Unlock()
	require.NoError(t, graph.SetQuantization(t.Context(), HNSWQuantizationConfig{Mode: HNSWQuantizationScalar}))
	lookup := svc.getVectorLookup()
	var eReads atomic.Int32
	held := make(chan struct{})
	graph.SetVectorLookup(func(id string) ([]float32, bool) {
		if id == "E" && eReads.Add(1) == 2 {
			close(held)
			<-resume
		}
		return constructionLookup(id)
	})
	require.NoError(t, c.store("X", nextX))
	svc.hnswRebuildInFlight.Store(true)
	svc.indexMu.Lock()
	currentX, exists := lookup("X")
	require.True(t, exists)
	svc.hnswCatchUp.begin(graph, map[string][]float32{"X": currentX}, map[string]struct{}{})
	svc.indexMu.Unlock()
	inserted := c.run(func() error { _, err := svc.hnswCatchUp.applyNext(); return err })
	awaitHeld(t, "layer-zero seed lookup", held, inserted)
	// Hold the existing catch-up queue mutex: RefreshNode removes E from the
	// canonical store, then waits here to record its graph removal.
	svc.hnswCatchUp.mu.Lock()
	heldMu = true
	nextE := []float32{0.6, 0.8, 0}
	node, err := store.GetNode(t.Context(), "E")
	require.NoError(t, err)
	node.ChunkEmbeddings = [][]float32{nextE}
	require.NoError(t, store.UpdateNode(t.Context(), storage.NodeUpdateRequest{After: node}))
	refreshed := c.run(func() error { return svc.RefreshNode(t.Context(), "E") })
	deadline := time.Now().Add(catchUpPrompt)
	for {
		if _, exists := lookup("E"); !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not enter its canonical replacement interval")
		}
		runtime.Gosched()
	}
	release()
	// Joining graph work is safe here; completing the catch-up step itself
	// still waits for the queue mutex. No assertion relies on a sleep.
	graph.mu.Lock()
	newX := graph.idToInternal["X"]
	levelX := graph.nodeLevel[newX]
	graph.mu.Unlock()
	require.Zero(t, levelX, "the fixture must insert below the global entry level")
	svc.hnswCatchUp.mu.Unlock()
	heldMu = false
	require.NoError(t, awaitEvent(t, "victim insertion", inserted))
	require.NoError(t, awaitEvent(t, "normal refresh", refreshed))
	for {
		more, err := svc.hnswCatchUp.applyNext()
		require.NoError(t, err)
		if !more {
			break
		}
	}
	own, err := hnswOwnCoordinates(graph, "X")
	require.NoError(t, err)
	require.True(t, hnswCodesAre(own.codes, hnswScaledCoordinates(nextX)), "membership and scalar coordinates can be correct even when navigation is broken")
	canonical, ok := lookup("X")
	require.True(t, ok)
	require.InDeltaSlice(t, hnswScaledCoordinates(nextX), hnswScaledCoordinates(canonical), 0.001)
	// Public ANN search must find X from graph navigation, without a beam as
	// wide as the population. Membership/current codes alone are insufficient.
	rows, err := graph.Search(t.Context(), nextX, 1, -1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "X", rows[0].ID)
	require.GreaterOrEqual(t, rows[0].Score, float32(0.999))
	graph.mu.RLock()
	defer graph.mu.RUnlock()
	require.Equal(t, "G", graph.internalToID[graph.entryPoint])
	require.Less(t, int(graph.nodeLevel[graph.idToInternal["E"]]), graph.maxLevel)
	seen := map[uint32]bool{graph.entryPoint: true}
	queue := []uint32{graph.entryPoint}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		neighbors, _ := graph.neighborsAtLevelLocked(id, 0)
		for _, next := range neighbors {
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	require.True(t, seen[graph.idToInternal["X"]], "the acknowledged current vector must remain reachable, without relying on an exhaustive-query fallback")
}
