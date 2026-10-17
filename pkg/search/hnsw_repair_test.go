package search

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func checkHNSWEdgesDuring(t *testing.T) {
	hnswCheckEdges = true
	t.Cleanup(func() { hnswCheckEdges = false })
}

func randomHNSWVectors(n, dims int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = make([]float32, dims)
		for j := range vectors[i] {
			vectors[i][j] = float32(rng.NormFloat64())
		}
	}
	return vectors
}

// nearestHNSWVectors returns the indexes of the count vectors closest to
// center: a region whose removal leaves long tombstone paths.
func nearestHNSWVectors(vectors [][]float32, center []float32, count int) []int {
	order := make([]int, len(vectors))
	scores := make([]float64, len(vectors))
	for i, v := range vectors {
		order[i] = i
		var dot, norm float64
		for j := range v {
			dot += float64(v[j] * center[j])
			norm += float64(v[j] * v[j])
		}
		scores[i] = dot / (norm + 1e-12)
	}
	slices.SortFunc(order, func(a, b int) int {
		if scores[a] > scores[b] {
			return -1
		}
		if scores[a] < scores[b] {
			return 1
		}
		return a - b
	})
	return order[:count]
}

// unreachableLiveVectors returns the IDs of live vectors that the live links
// of layer 0 don't reach from the entry point.
func unreachableLiveVectors(index *HNSWIndex) []string {
	index.mu.RLock()
	defer index.mu.RUnlock()
	seen := map[uint32]bool{}
	if index.hasEntryPoint {
		seen[index.entryPoint] = true
		stack := []uint32{index.entryPoint}
		for len(stack) > 0 {
			node := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			neighbors, _ := index.neighborsAtLevelLocked(node, 0)
			for _, neighbor := range neighbors {
				if !seen[neighbor] && !index.deleted[neighbor] {
					seen[neighbor] = true
					stack = append(stack, neighbor)
				}
			}
		}
	}
	var missing []string
	for node, deleted := range index.deleted {
		if !deleted && !seen[uint32(node)] {
			missing = append(missing, index.internalToID[node])
		}
	}
	return missing
}

// requireEveryLiveVectorReachable checks the links, reachability from the
// entry point, and a search of the whole live population.
func requireEveryLiveVectorReachable(t *testing.T, index *HNSWIndex) {
	t.Helper()
	index.mu.RLock()
	require.NoError(t, index.checkEdgesLocked())
	live := index.liveCount
	index.mu.RUnlock()
	require.Empty(t, unreachableLiveVectors(index), "live vectors unreachable from the entry point")

	query := make([]float32, index.dimensions)
	for i := range query {
		query[i] = 1
	}
	results, err := index.searchWithEf(context.Background(), query, live, -1, live)
	require.NoError(t, err)
	require.Len(t, results, live)
}

func TestHNSWRemovalKeepsEveryLiveVectorReachable(t *testing.T) {
	t.Run("collinear chain, tombstone runs", func(t *testing.T) {
		checkHNSWEdgesDuring(t)
		index := NewHNSWIndex(2, DefaultHNSWConfig())
		for i := 0; i < 60; i++ {
			require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), []float32{1, float32(i) / 60}))
		}
		for _, run := range [][2]int{{10, 20}, {30, 35}, {40, 42}, {0, 3}} {
			for i := run[0]; i < run[1]; i++ {
				index.Remove(fmt.Sprintf("v-%d", i))
			}
		}
		for i := 50; i < 60; i += 3 {
			index.Remove(fmt.Sprintf("v-%d", i))
		}
		requireEveryLiveVectorReachable(t, index)
	})
	t.Run("random, removed regions", func(t *testing.T) {
		vectors := randomHNSWVectors(1500, 16, 7)
		index := NewHNSWIndex(16, DefaultHNSWConfig())
		for i, v := range vectors {
			require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), v))
		}
		for _, center := range [][]float32{vectors[0], vectors[1], vectors[2]} {
			for _, i := range nearestHNSWVectors(vectors, center, 300) {
				index.Remove(fmt.Sprintf("v-%d", i))
			}
		}
		require.Less(t, index.Size(), 1500-600)
		requireEveryLiveVectorReachable(t, index)
	})
}

// Removing runs of neighbouring documents leaves paths of several tombstones
// between live ones; continuation must still return every live document.
func TestContinuationAfterTombstoneRunsReturnsEveryLiveDocument(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_CPU_BRUTE_MAX_N", "0") // HNSW at any size
	checkHNSWEdgesDuring(t)
	engine := storage.NewNamespacedEngine(storage.NewMemoryEngine(), "nornic")
	service := NewServiceWithDimensions(engine, 2)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	for i := 0; i < 30; i++ {
		node := &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", i)), ChunkEmbeddings: [][]float32{{1, float32(i) / 30}}}
		_, err := engine.CreateNode(node)
		require.NoError(t, err)
		require.NoError(t, service.IndexNode(node))
	}
	_, err := service.getOrCreateHNSWIndex(context.Background(), 2)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !service.hnswRebuildInFlight.Load() }, 5*time.Second, time.Millisecond)
	removed := map[string]bool{}
	for _, run := range [][2]int{{5, 10}, {15, 21}} {
		for i := run[0]; i < run[1]; i++ {
			id := fmt.Sprintf("doc-%d", i)
			require.NoError(t, engine.DeleteNode(storage.NodeID(id)))
			require.NoError(t, service.RemoveNode(storage.NodeID(id)))
			removed[id] = true
		}
	}

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
			require.False(t, removed[id], "removed %s returned", id)
			ids[id] = true
		}
		if !page.HasMore {
			break
		}
		request.QID = page.QID
		page, err = service.SearchTextContinuation(context.Background(), "", nil, request, nil, nil, nil, ChunkedSearchErrorPolicy{})
		require.NoError(t, err)
	}
	require.Len(t, ids, 30-len(removed))
	require.Equal(t, SearchContinuationCollectionComplete, page.Completion)
}

// A query next to a large removed region relaxes no tombstone and expands a
// number of nodes set by ef, not by how many tombstones surround it.
func TestHNSWSearchWorkStaysWithinEfAmongTombstones(t *testing.T) {
	const ef = 16
	vectors := randomHNSWVectors(3000, 16, 11)
	index := NewHNSWIndex(16, DefaultHNSWConfig())
	for i, v := range vectors {
		require.NoError(t, index.Add(fmt.Sprintf("v-%d", i), v))
	}
	for _, i := range nearestHNSWVectors(vectors, vectors[0], 2000) {
		index.Remove(fmt.Sprintf("v-%d", i))
	}

	var expanded, expandedTombstones int
	hnswLayerExpansionHook = func(_ uint32, deleted bool) {
		expanded++
		if deleted {
			expandedTombstones++
		}
	}
	t.Cleanup(func() { hnswLayerExpansionHook = nil })
	for q := 0; q < 5; q++ {
		expanded, expandedTombstones = 0, 0
		results, err := index.searchWithEf(context.Background(), vectors[q], 1, -1, ef)
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Zero(t, expandedTombstones, "query %d", q)
		require.LessOrEqual(t, expanded, 8*ef, "query %d expanded %d nodes", q, expanded)
	}
}

// Graphs saved before removal repaired links keep their tombstones' links,
// including links between tombstones. Loading one repairs it.
func TestLoadHNSWIndexRepairsTombstoneLinksOfOlderGraphs(t *testing.T) {
	vectors := map[string][]float32{}
	index := NewHNSWIndex(2, DefaultHNSWConfig())
	for i := 0; i < 80; i++ {
		id := fmt.Sprintf("v-%d", i)
		vectors[id] = []float32{1, float32(i) / 80}
		require.NoError(t, index.Add(id, vectors[id]))
	}
	// Tombstone the way removal did before repair: flag only, links kept.
	index.mu.Lock()
	for _, run := range [][2]int{{10, 25}, {40, 44}, {60, 61}} {
		for i := run[0]; i < run[1]; i++ {
			id := fmt.Sprintf("v-%d", i)
			internal := index.idToInternal[id]
			index.deleted[internal] = true
			index.liveCount--
			delete(index.idToInternal, id)
			delete(vectors, id)
		}
	}
	linksTombstone := false
	for node := range index.nodeLevel {
		neighbors, _ := index.neighborsAtLevelLocked(uint32(node), 0)
		for _, neighbor := range neighbors {
			linksTombstone = linksTombstone || (!index.deleted[node] && index.deleted[neighbor])
		}
	}
	index.mu.Unlock()
	require.True(t, linksTombstone, "fixture holds live links to tombstones")
	path := filepath.Join(t.TempDir(), "hnsw")
	require.NoError(t, index.Save(path))

	checkHNSWEdgesDuring(t)
	loaded, err := LoadHNSWIndex(path, func(id string) ([]float32, bool) { v, ok := vectors[id]; return v, ok })
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, len(vectors), loaded.Size())
	requireEveryLiveVectorReachable(t, loaded)
}

// Every Add and Remove of a churn keeps the reverse links equal to the
// forward links (hnswCheckEdges panics otherwise) and no live link on a
// tombstone, in both vector storage modes, and no removal leaves a live vector
// unreachable that was reachable before it.
func TestHNSWChurnKeepsReverseLinksConsistent(t *testing.T) {
	for _, lookupMode := range []bool{false, true} {
		t.Run(fmt.Sprintf("lookup=%t", lookupMode), func(t *testing.T) {
			checkHNSWEdgesDuring(t)
			store := map[string][]float32{}
			var lookup VectorLookup
			if lookupMode {
				lookup = func(id string) ([]float32, bool) { v, ok := store[id]; return v, ok }
			}
			index := newHNSWIndex(8, DefaultHNSWConfig(), lookup)
			rng := rand.New(rand.NewSource(3))
			vector := func() []float32 { return randomHNSWVectors(1, 8, rng.Int63())[0] }
			var live []string
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("v-%d", i)
				store[id] = vector()
				require.NoError(t, index.Add(id, store[id]))
				live = append(live, id)
			}
			next := 200
			for op := 0; op < 1500; op++ {
				switch pick := rng.Intn(3); {
				case pick == 0 && len(live) > 20:
					at := rng.Intn(len(live))
					before := unreachableLiveVectors(index)
					index.Remove(live[at])
					for _, id := range unreachableLiveVectors(index) {
						require.Contains(t, before, id, "op %d: removing %s cut %s off", op, live[at], id)
					}
					delete(store, live[at])
					live = slices.Delete(live, at, at+1)
				case pick == 1:
					id := live[rng.Intn(len(live))]
					store[id] = vector()
					require.NoError(t, index.Update(id, store[id]))
				default:
					id := fmt.Sprintf("v-%d", next)
					next++
					store[id] = vector()
					require.NoError(t, index.Add(id, store[id]))
					live = append(live, id)
				}
				index.mu.RLock()
				err := index.checkEdgesLocked()
				index.mu.RUnlock()
				require.NoError(t, err, "after op %d", op)
			}
			require.Equal(t, len(live), index.Size())
		})
	}
}
