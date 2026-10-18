package search

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/math/vector"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// requireEveryLiveVectorFound searches with a beam as wide as the live
// population and requires every live vector, reported as exhausted.
func requireEveryLiveVectorFound(t *testing.T, index *HNSWIndex) {
	t.Helper()
	index.mu.RLock()
	live := make([]string, 0, index.liveCount)
	for node, deleted := range index.deleted {
		if !deleted {
			live = append(live, index.internalToID[node])
		}
	}
	index.mu.RUnlock()
	query := make([]float32, index.GetDimensions())
	for i := range query {
		query[i] = 1
	}
	results, exhausted, err := index.searchWithEfExhaustion(context.Background(), query, len(live), -1, len(live))
	require.NoError(t, err)
	got := make([]string, 0, len(results))
	for _, result := range results {
		got = append(got, result.ID)
	}
	require.ElementsMatch(t, live, got)
	require.True(t, exhausted)
}

type hnswFixtureNode struct {
	id    string
	vec   []float32
	level int
	links [][]string // per level
}

// newHNSWFixture builds an index with exactly the given levels, links and
// entry point, as a saved graph would hold them.
func newHNSWFixture(t *testing.T, dims, m int, entry string, nodes []hnswFixtureNode) *HNSWIndex {
	t.Helper()
	config := DefaultHNSWConfig()
	config.M = m
	index := NewHNSWIndex(dims, config)
	index.mu.Lock()
	defer index.mu.Unlock()
	for i, node := range nodes {
		require.Len(t, node.vec, dims, node.id)
		off := len(index.vectors)
		index.vectors = append(index.vectors, node.vec...)
		vector.NormalizeInPlace(index.vectors[off : off+dims])
		index.vecOff = append(index.vecOff, int32(off))
		index.nodeLevel = append(index.nodeLevel, uint16(node.level))
		slots, ok := index.neighborSlotsForNode(node.level)
		require.True(t, ok)
		index.neighborsOff = append(index.neighborsOff, int32(len(index.neighborsArena)))
		index.neighborsArena = append(index.neighborsArena, make([]uint32, slots)...)
		index.neighborCountsOff = append(index.neighborCountsOff, int32(len(index.neighborCountsArena)))
		index.neighborCountsArena = append(index.neighborCountsArena, make([]uint16, node.level+1)...)
		index.internalToID = append(index.internalToID, node.id)
		index.idToInternal[node.id] = uint32(i)
		index.deleted = append(index.deleted, false)
		index.liveCount++
	}
	for i, node := range nodes {
		for level, links := range node.links {
			ids := make([]uint32, 0, len(links))
			for _, link := range links {
				target, ok := index.idToInternal[link]
				require.True(t, ok, link)
				ids = append(ids, target)
			}
			require.LessOrEqual(t, len(ids), index.neighborCapacity(level), node.id)
			index.setNeighborsAtLevelLocked(uint32(i), level, ids)
		}
	}
	index.entryPoint, index.hasEntryPoint = index.idToInternal[entry], true
	index.maxLevel = int(index.nodeLevel[index.entryPoint])
	return index
}

// tombstoneAsSaved flags a node removed the way a saved graph holds it: the
// flag only, its links kept.
func tombstoneAsSaved(index *HNSWIndex, id string) {
	index.mu.Lock()
	defer index.mu.Unlock()
	internal := index.idToInternal[id]
	index.deleted[internal] = true
	index.liveCount--
	delete(index.idToInternal, id)
}

// saveAndLoad round-trips index through a file, resolving vectors from it.
func saveAndLoad(t *testing.T, index *HNSWIndex, vectors map[string][]float32) *HNSWIndex {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, index.Save(path))
	loaded, err := LoadHNSWIndex(path, func(id string) ([]float32, bool) { v, ok := vectors[id]; return v, ok })
	require.NoError(t, err)
	require.NotNil(t, loaded)
	return loaded
}

func fixtureVectors(nodes []hnswFixtureNode) map[string][]float32 {
	vectors := map[string][]float32{}
	for _, node := range nodes {
		vectors[node.id] = node.vec
	}
	return vectors
}

// Review r2 F1 (M=2, four layer-0 slots): T is the entry side's only bridge
// into B and C, and C still links to B. Removing T must leave B and C
// reachable, through Remove and through Load of a graph saved with T's
// links.
func TestHNSWRemovalKeepsDetachedComponentReachable(t *testing.T) {
	x, y := []float32{1, 0}, []float32{0, 1}
	nodes := []hnswFixtureNode{
		{id: "A", vec: x, level: 1, links: [][]string{{"T", "L1", "L2", "L3"}, nil}},
		{id: "T", vec: []float32{1, 1}, links: [][]string{{"L4", "B"}}},
		{id: "L1", vec: x, links: [][]string{{"A"}}},
		{id: "L2", vec: x, links: [][]string{{"A"}}},
		{id: "L3", vec: x, links: [][]string{{"A"}}},
		{id: "L4", vec: x, links: [][]string{{"A"}}},
		{id: "B", vec: y, links: [][]string{{"C"}}},
		{id: "C", vec: y, links: [][]string{{"B"}}},
	}
	t.Run("remove", func(t *testing.T) {
		index := newHNSWFixture(t, 2, 2, "A", nodes)
		index.Remove("T")
		requireEveryLiveVectorFound(t, index)
	})
	t.Run("load", func(t *testing.T) {
		index := newHNSWFixture(t, 2, 2, "A", nodes)
		tombstoneAsSaved(index, "T")
		vectors := fixtureVectors(nodes)
		delete(vectors, "T")
		requireEveryLiveVectorFound(t, saveAndLoad(t, index, vectors))
	})
}

// Review r2 F2: a chain of 26 hosts. X0 is linked only from T; every later
// Xi only from host H(i-1); the Qi keep the hosts reachable. Removing T must
// leave the whole chain reachable, however long it is.
func TestHNSWRemovalKeepsLongOrphanChainReachable(t *testing.T) {
	const hosts = 26
	e1, e2, e3 := []float32{1, 0, 0}, []float32{0, 1, 0}, []float32{0, 0, 1}
	axis := func(i int) []float32 {
		if i%2 == 0 {
			return e1
		}
		return e2
	}
	nodes := []hnswFixtureNode{
		{id: "A", vec: e3, level: 1, links: [][]string{{"T", "L1", "L2", "H0"}, nil}},
		{id: "T", vec: e3, links: [][]string{{"L4", "X0"}}},
		{id: "L1", vec: e3, links: [][]string{{"A"}}},
		{id: "L2", vec: e3, links: [][]string{{"A"}}},
		{id: "L4", vec: e3, links: [][]string{{"A"}}},
	}
	for i := 0; i < hosts; i++ {
		next := "A"
		if i+1 < hosts {
			next = fmt.Sprintf("H%d", i+1)
		}
		hostLinks := []string{fmt.Sprintf("Q%d-1", i), fmt.Sprintf("Q%d-2", i), fmt.Sprintf("Q%d-3", i)}
		if i+1 < hosts {
			hostLinks = append([]string{fmt.Sprintf("X%d", i+1)}, hostLinks...)
		}
		nodes = append(nodes,
			hnswFixtureNode{id: fmt.Sprintf("H%d", i), vec: axis(i), links: [][]string{hostLinks}},
			hnswFixtureNode{id: fmt.Sprintf("X%d", i), vec: axis(i), links: [][]string{{fmt.Sprintf("H%d", i)}}},
		)
		for j := 1; j <= 3; j++ {
			nodes = append(nodes, hnswFixtureNode{id: fmt.Sprintf("Q%d-%d", i, j), vec: axis(i), links: [][]string{{next}}})
		}
	}
	t.Run("remove", func(t *testing.T) {
		index := newHNSWFixture(t, 3, 2, "A", nodes)
		index.Remove("T")
		requireEveryLiveVectorFound(t, index)
	})
	t.Run("load", func(t *testing.T) {
		index := newHNSWFixture(t, 3, 2, "A", nodes)
		tombstoneAsSaved(index, "T")
		vectors := fixtureVectors(nodes)
		delete(vectors, "T")
		requireEveryLiveVectorFound(t, saveAndLoad(t, index, vectors))
	})
}

// Review r2 F3's selection trace, on the insert path: host H is full with B,
// C, D, E at angles 0.1 to 0.5 and is the only node linking to C, D and E.
// X at -0.05 links only to H (every other candidate is closer to H than to
// X), so inserting it re-selects H's list: the diversity rule keeps X and B
// and drops C, D and E. They must stay reachable.
func TestHNSWInsertPruningKeepsDroppedNodesReachable(t *testing.T) {
	angle := func(a float64) []float32 { return []float32{float32(math.Cos(a)), float32(math.Sin(a))} }
	nodes := []hnswFixtureNode{
		{id: "A", vec: angle(math.Pi), level: 1, links: [][]string{{"H"}, nil}},
		{id: "H", vec: angle(0), links: [][]string{{"B", "C", "D", "E"}}},
		{id: "B", vec: angle(0.1), links: [][]string{{"H"}}},
		{id: "C", vec: angle(0.3), links: [][]string{{"H"}}},
		{id: "D", vec: angle(0.4), links: [][]string{{"H"}}},
		{id: "E", vec: angle(0.5), links: [][]string{{"H"}}},
	}
	index := newHNSWFixture(t, 2, 2, "A", nodes)
	require.NoError(t, index.Add("X", angle(-0.05)))
	requireEveryLiveVectorFound(t, index)
}

// requireNarrowSearchAnswers searches with a beam narrower than the live
// population, so through the graph, and requires a result: a descent that
// ends on a removed vector must still seed the layer-0 search.
func requireNarrowSearchAnswers(t *testing.T, index *HNSWIndex, query []float32) {
	t.Helper()
	if index.Size() < 3 {
		return
	}
	results, err := index.SearchWithEf(context.Background(), query, 1, -1, 2)
	require.NoError(t, err)
	require.Len(t, results, 1)
}

// Removing the entry point again and again keeps every remaining vector
// reachable, and narrow searches keep answering.
func TestHNSWEntryPointRemovalKeepsEveryVectorReachable(t *testing.T) {
	index := NewHNSWIndex(2, DefaultHNSWConfig())
	for i := 0; i < 60; i++ {
		require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), []float32{1, float32(i) / 60}))
	}
	for removed := 0; removed < 40; removed++ {
		index.mu.RLock()
		entry := index.internalToID[index.entryPoint]
		index.mu.RUnlock()
		index.Remove(entry)
		requireEveryLiveVectorFound(t, index)
		requireNarrowSearchAnswers(t, index, []float32{1, 0.5})
	}
}

// Runs of neighbouring removed vectors in a chain and removed regions of a
// random graph keep every live vector reachable.
func TestHNSWRemovalOfRunsAndRegionsKeepsEveryVectorReachable(t *testing.T) {
	t.Run("collinear chain", func(t *testing.T) {
		index := NewHNSWIndex(2, DefaultHNSWConfig())
		for i := 0; i < 60; i++ {
			require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), []float32{1, float32(i) / 60}))
		}
		for _, run := range [][2]int{{0, 3}, {10, 20}, {30, 35}, {40, 42}} {
			for i := run[0]; i < run[1]; i++ {
				index.Remove(fmt.Sprintf("v-%d", i))
			}
		}
		requireEveryLiveVectorFound(t, index)
	})
	t.Run("random, removed regions", func(t *testing.T) {
		rng := rand.New(rand.NewSource(7))
		vectors := make([][]float32, 1500)
		index := NewHNSWIndex(16, DefaultHNSWConfig())
		for i := range vectors {
			vectors[i] = make([]float32, 16)
			for j := range vectors[i] {
				vectors[i][j] = float32(rng.NormFloat64())
			}
			require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), vectors[i]))
		}
		for _, center := range vectors[:3] {
			order := make([]int, len(vectors))
			score := make([]float32, len(vectors))
			for i, v := range vectors {
				order[i] = i
				for j := range v {
					score[i] += v[j] * center[j]
				}
			}
			slices.SortFunc(order, func(a, b int) int {
				if score[a] != score[b] {
					if score[a] > score[b] {
						return -1
					}
					return 1
				}
				return a - b
			})
			for _, i := range order[:300] {
				index.Remove(fmt.Sprintf("v-%d", i))
			}
		}
		requireEveryLiveVectorFound(t, index)
	})
}

// A random mix of Add, Remove, Update, removal of the entry point, bursts of
// removals and a drain to an empty index, in both vector storage modes and
// for several seeds, with Save/Load round trips: after every step every live
// vector is found by a population-wide search and narrow searches answer.
func TestHNSWReachabilityHoldsUnderRandomMutationsAndReloads(t *testing.T) {
	for _, seed := range []int64{3, 11, 29, 47, 101} {
		for _, lookupMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("seed=%d/lookup=%t", seed, lookupMode), func(t *testing.T) {
				store := map[string][]float32{}
				lookup := func(id string) ([]float32, bool) { v, ok := store[id]; return v, ok }
				var index *HNSWIndex
				if lookupMode {
					index = newHNSWIndex(8, DefaultHNSWConfig(), lookup)
				} else {
					index = NewHNSWIndex(8, DefaultHNSWConfig())
				}
				rng := rand.New(rand.NewSource(seed))
				randomVector := func() []float32 {
					v := make([]float32, 8)
					for j := range v {
						v[j] = float32(rng.NormFloat64())
					}
					return v
				}
				var live []string
				next := 0
				add := func() {
					id := fmt.Sprintf("v-%d", next)
					next++
					store[id] = randomVector()
					require.NoError(t, index.Add(id, store[id]))
					live = append(live, id)
				}
				remove := func(at int) {
					index.Remove(live[at])
					delete(store, live[at])
					live = slices.Delete(live, at, at+1)
				}
				for i := 0; i < 200; i++ {
					add()
				}
				for op := 0; op < 1500; op++ {
					switch pick := rng.Intn(6); {
					case pick == 0 && len(live) > 20:
						remove(rng.Intn(len(live)))
					case pick == 1 && len(live) > 0:
						id := live[rng.Intn(len(live))]
						store[id] = randomVector()
						require.NoError(t, index.Update(id, store[id]))
					case pick == 2 && len(live) > 20:
						index.mu.RLock()
						entry := index.internalToID[index.entryPoint]
						index.mu.RUnlock()
						remove(slices.Index(live, entry))
					case pick == 3 && len(live) > 40:
						for burst := 0; burst < 15; burst++ {
							remove(rng.Intn(len(live)))
						}
					default:
						add()
					}
					if op == 700 { // drain to empty, then grow again
						for len(live) > 0 {
							remove(rng.Intn(len(live)))
						}
						require.Zero(t, index.Size())
						for i := 0; i < 60; i++ {
							add()
						}
					}
					if op%250 == 249 {
						index = saveAndLoad(t, index, store)
					}
					requireEveryLiveVectorFound(t, index)
					requireNarrowSearchAnswers(t, index, randomVector())
				}
				require.Equal(t, len(live), index.Size())
				requireEveryLiveVectorFound(t, index)
			})
		}
	}
}

// requireContinuationReturnsEveryLiveDocument pages through a text
// continuation and requires each live document once and an exact end.
func requireContinuationReturnsEveryLiveDocument(t *testing.T, service *Service, live map[string]bool) {
	t.Helper()
	embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
	request := SearchContinuationRequest{Owner: "alice", N: 4}
	page, err := service.SearchTextContinuation(context.Background(), "", &SearchOptions{Limit: 4}, request, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
	require.NoError(t, err)
	ids := map[string]bool{}
	for {
		require.True(t, strings.HasPrefix(page.SearchMethod, "vector_hnsw"), "search method %q", page.SearchMethod)
		for _, result := range page.Results {
			id := searchResultID(result)
			require.False(t, ids[id], "duplicate %s", id)
			require.True(t, live[id], "%s is not live", id)
			ids[id] = true
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Len(t, ids, len(live))
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
}

// Continuation returns every live document, ending with
// eligible_population_exhausted, after runs of removed documents, removal
// of the document at the entry point and further inserts.
func TestContinuationReturnsEveryLiveDocumentAfterRemovalsAndInserts(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	live := map[string]bool{}
	indexDoc := func(i int) {
		id := fmt.Sprintf("doc-%d", i)
		node := &storage.Node{ID: storage.NodeID(id), ChunkEmbeddings: [][]float32{{1, float32(i) / 40}}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
		live[id] = true
	}
	remove := func(i int) {
		id := fmt.Sprintf("doc-%d", i)
		require.NoError(t, engine.DeleteNode(storage.NodeID(id)))
		require.NoError(t, service.RemoveNode(storage.NodeID(id)))
		delete(live, id)
	}
	for i := 0; i < 30; i++ {
		indexDoc(i)
	}
	graph, err := service.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !service.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)

	// Remove the document at the entry point first.
	graph.mu.RLock()
	entryID := graph.internalToID[graph.entryPoint]
	graph.mu.RUnlock()
	var entry int
	_, err = fmt.Sscanf(normalizeVectorResultIDToNodeID(entryID), "doc-%d", &entry)
	require.NoError(t, err, entryID)
	remove(entry)
	for _, run := range [][2]int{{5, 10}, {15, 21}} {
		for i := run[0]; i < run[1]; i++ {
			if live[fmt.Sprintf("doc-%d", i)] {
				remove(i)
			}
		}
	}
	requireContinuationReturnsEveryLiveDocument(t, service, live)

	for i := 30; i < 40; i++ {
		indexDoc(i)
	}
	requireContinuationReturnsEveryLiveDocument(t, service, live)
}

// The upper-layer descent can end on a removed vector, which can't seed the
// layer-0 search. Its live neighbours seed it instead: a narrow search finds
// the vectors next to it (E, the last live node the descent passed, links
// only to the removed vector), and an insert links among them, so a narrow
// search finds the new vector too.
func TestHNSWDescentEndingOnRemovedVector(t *testing.T) {
	nodes := []hnswFixtureNode{
		{id: "E", vec: []float32{-1, 0}, level: 1, links: [][]string{{"T"}, {"T"}}},
		{id: "T", vec: []float32{1, 0}, level: 1, links: [][]string{{"P", "Q"}, {"E"}}},
		{id: "P", vec: []float32{1, 0.1}, links: [][]string{{"T", "Q"}}},
		{id: "Q", vec: []float32{1, -0.1}, links: [][]string{{"T", "P"}}},
	}
	t.Run("search", func(t *testing.T) {
		index := newHNSWFixture(t, 2, 2, "E", nodes)
		index.Remove("T")
		results, err := index.SearchWithEf(context.Background(), []float32{1, 0.09}, 1, -1, 2)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "P", results[0].ID)
	})
	t.Run("insert", func(t *testing.T) {
		index := newHNSWFixture(t, 2, 2, "E", nodes)
		index.Remove("T")
		require.NoError(t, index.Add("N", []float32{1, 0.05}))
		index.mu.RLock()
		neighbors, _ := index.neighborsAtLevelLocked(index.idToInternal["N"], 0)
		linked := make([]string, 0, len(neighbors))
		for _, neighbor := range neighbors {
			linked = append(linked, index.internalToID[neighbor])
		}
		index.mu.RUnlock()
		require.Subset(t, []string{"P", "Q"}, linked)
		require.NotEmpty(t, linked)
		results, err := index.SearchWithEf(context.Background(), []float32{1, 0.05}, 1, -1, 2)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, "N", results[0].ID)
		requireEveryLiveVectorFound(t, index)
	})
}

var continuationNoThreshold = -1.0

// A live document that no graph link reaches (insert pruning can leave one)
// must still come back from a ranked continuation, which must end exactly.
func TestContinuationReturnsDocumentNoGraphLinkReaches(t *testing.T) {
	for _, docs := range []int{40, 200, 1000} {
		t.Run(fmt.Sprintf("docs=%d", docs), func(t *testing.T) {
			t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0")
			engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
			service := NewServiceWithDimensions(engine, 2)
			t.Cleanup(func() { _ = service.Close() })
			for i := 0; i < docs; i++ {
				angle := math.Pi * float64(i) / float64(docs) / 2
				node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), ChunkEmbeddings: [][]float32{{float32(math.Cos(angle)), float32(math.Sin(angle))}}}
				_, err := engine.CreateNode(node)
				require.NoError(t, err)
				require.NoError(t, service.IndexNode(node))
			}
			graph, err := service.getOrCreateHNSWIndex(context.Background(), 2)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return !service.hnswRebuildInFlight.Load() }, 10*time.Second, time.Millisecond)

			// Cut every link into one live, non-entry node, at every level.
			graph.mu.Lock()
			orphan := uint32(len(graph.nodeLevel) / 2)
			if orphan == graph.entryPoint {
				orphan++
			}
			for node := range graph.nodeLevel {
				for level := 0; level <= int(graph.nodeLevel[node]); level++ {
					neighbors, _ := graph.neighborsAtLevelLocked(uint32(node), level)
					kept := make([]uint32, 0, len(neighbors))
					for _, n := range neighbors {
						if n != orphan {
							kept = append(kept, n)
						}
					}
					graph.setNeighborsAtLevelLocked(uint32(node), level, kept)
				}
			}
			orphanID := normalizeVectorResultIDToNodeID(graph.internalToID[orphan])
			graph.mu.Unlock()

			embed := func(context.Context, string) ([]float32, error) { return []float32{1, 0}, nil }
			request := SearchContinuationRequest{Owner: "alice", N: 7}
			page, err := service.SearchTextContinuation(context.Background(), "", &SearchOptions{Limit: 7, MinSimilarity: &continuationNoThreshold}, request, nil, embed, service.Search, ChunkedSearchErrorPolicy{})
			require.NoError(t, err)
			ids := map[string]bool{}
			for {
				require.True(t, strings.HasPrefix(page.SearchMethod, "vector_hnsw"), "search method %q", page.SearchMethod)
				for _, result := range page.Results {
					ids[searchResultID(result)] = true
				}
				if !page.HasMore {
					break
				}
				request.QID = page.QID
				page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
				require.NoError(t, err)
			}
			t.Logf("docs=%d orphan=%s: %d returned, completion %s", docs, orphanID, len(ids), page.Completion)
			require.True(t, ids[orphanID], "orphan %s missing (%s after %d)", orphanID, page.Completion, len(ids))
			require.Len(t, ids, docs)
		})
	}
}

// A population-wide search resolves each live vector once (a file-backed
// store copies the coordinates on every lookup) and allocates a fixed
// amount, not per vector.
func TestHNSWPopulationWideScanResolvesEachVectorOnceWithoutPerVectorAllocations(t *testing.T) {
	const n, dims = 1000, 16
	rng := rand.New(rand.NewSource(5))
	store := map[string][]float32{}
	lookups := 0
	index := NewHNSWIndex(dims, DefaultHNSWConfig())
	index.vectorLookup = func(id string) ([]float32, bool) {
		lookups++
		v, ok := store[id]
		return v, ok
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("v-%d", i)
		store[id] = make([]float32, dims)
		for j := range store[id] {
			store[id][j] = float32(rng.NormFloat64())
		}
		require.NoError(t, index.Add(id, store[id]))
	}
	for i := 0; i < n; i += 10 {
		id := fmt.Sprintf("v-%d", i)
		index.Remove(id)
		delete(store, id)
	}
	live := index.Size()
	query := make([]float32, dims)
	query[0] = 1

	t.Run("one lookup per live vector", func(t *testing.T) {
		lookups = 0
		results, exhausted, err := index.searchWithEfExhaustion(context.Background(), query, live, -1, live)
		require.NoError(t, err)
		require.Len(t, results, live)
		require.True(t, exhausted)
		require.Equal(t, live, lookups, "vector lookups for one population-wide search")
	})
	t.Run("no per-vector allocations", func(t *testing.T) {
		allocs := testing.AllocsPerRun(5, func() {
			_, _, _ = index.searchWithEfExhaustion(context.Background(), query, 10, -1, live)
		})
		require.Less(t, allocs, float64(32), "allocations per population-wide search of %d vectors", live)
	})
}
